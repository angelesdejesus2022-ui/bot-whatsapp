import hashlib
import hmac
import logging
import os
from collections import defaultdict, deque

import requests
from flask import Flask, request, abort

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "mi_token_secreto")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
APP_SECRET = os.getenv("APP_SECRET")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GRAPH_VERSION = "v21.0"

SYSTEM_PROMPT = """
Eres el asistente virtual de WhatsApp de [NOMBRE DE TU NEGOCIO].
Tono: profesional, amable, claro y breve (máximo 5 líneas). Responde siempre en el idioma del cliente.

INFORMACIÓN DEL NEGOCIO:
- Rubro: [describe qué vendes o qué servicio ofreces]
- Horario: [Lunes a Sábado, 9:00 a 18:00]
- Ubicación: [dirección o "atención online"]
- Productos/servicios y precios: [lista aquí]
- Métodos de pago: [Yape, transferencia, efectivo, etc.]

REGLAS:
- Usa solo la información anterior. Si no sabes algo, di que un asesor humano lo contactará pronto.
- No inventes precios, promociones ni plazos.
- Si el cliente quiere comprar o agendar, pide su nombre y los datos necesarios.
- Si el cliente está molesto o pide hablar con una persona, indica que un asesor lo atenderá.
"""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

app = Flask(__name__)

historial = defaultdict(lambda: deque(maxlen=10))
procesados = deque(maxlen=500)


def responder_con_ia(numero, texto):
    historial[numero].append({"role": "user", "parts": [{"text": texto}]})
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": list(historial[numero]),
        "generationConfig": {"temperature": 0.4, "maxOutputTokens": 400},
    }
    try:
        r = requests.post(
            url, json=payload, headers={"x-goog-api-key": GEMINI_API_KEY}, timeout=30
        )
        r.raise_for_status()
        respuesta = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        log.error("Error con Gemini: %s", e)
        historial[numero].pop()
        return "Disculpa, tuve un inconveniente técnico. Un asesor te responderá en breve. 🙏"
    historial[numero].append({"role": "model", "parts": [{"text": respuesta}]})
    return respuesta


def enviar_whatsapp(numero, texto):
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}
    data = {
        "messaging_product": "whatsapp",
        "to": numero,
        "type": "text",
        "text": {"body": texto},
    }
    r = requests.post(url, json=data, headers=headers, timeout=20)
    if not r.ok:
        log.error("Error enviando WhatsApp: %s %s", r.status_code, r.text)


def marcar_leido(message_id):
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}
    data = {"messaging_product": "whatsapp", "status": "read", "message_id": message_id}
    try:
        requests.post(url, json=data, headers=headers, timeout=10)
    except Exception:
        pass


def firma_valida():
    if not APP_SECRET:
        return True
    firma = request.headers.get("X-Hub-Signature-256", "")
    esperado = "sha256=" + hmac.new(
        APP_SECRET.encode(), request.get_data(), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(firma, esperado)


@app.get("/webhook")
def verificar():
    if (
        request.args.get("hub.mode") == "subscribe"
        and request.args.get("hub.verify_token") == VERIFY_TOKEN
    ):
        return request.args.get("hub.challenge", ""), 200
    abort(403)


@app.post("/webhook")
def recibir():
    if not firma_valida():
        abort(403)
    data = request.get_json(silent=True) or {}
    try:
        for entry in data.get("entry", []):
            for change in entry.get("changes", []):
                for msg in change.get("value", {}).get("messages", []):
                    msg_id = msg["id"]
                    if msg_id in procesados:
                        continue
                    procesados.append(msg_id)
                    numero = msg["from"]
                    marcar_leido(msg_id)
                    if msg.get("type") == "text":
                        texto = msg["text"]["body"]
                        log.info("Mensaje de %s: %s", numero, texto)
                        enviar_whatsapp(numero, responder_con_ia(numero, texto))
                    else:
                        enviar_whatsapp(
                            numero,
                            "Por ahora solo puedo leer mensajes de texto. ¿Podrías escribirme tu consulta? 😊",
                        )
    except Exception as e:
        log.exception("Error procesando webhook: %s", e)
    return "ok", 200


@app.get("/")
def home():
    return "Bot de WhatsApp activo ✅"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
