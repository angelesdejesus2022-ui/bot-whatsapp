import logging
import os
import threading
from collections import defaultdict, deque

import requests
from flask import Flask, request, abort

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "clave123")
CHAKRA_TOKEN = os.getenv("CHAKRA_TOKEN")
CHAKRA_PLUGIN_ID = os.getenv("CHAKRA_PLUGIN_ID")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
WA_API_VERSION = os.getenv("WA_API_VERSION", "v22.0")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

SYSTEM_PROMPT = """
Eres el asistente virtual de WhatsApp del Colegio Angeles de Jesus.
Tono: amable, claro y breve (máximo 5 líneas). Responde en español.

INFORMACIÓN DEL COLEGIO:
- Niveles: [inicial, primaria, secundaria]
- Horario de atención: [Lunes a Viernes, 8:00 a 16:00]
- Dirección: [escribe la dirección]
- Matrícula y pensión: [escribe los montos]
- Requisitos de inscripción: [escribe los requisitos]
- Formas de pago: [Yape, transferencia, efectivo]

REGLAS:
- Usa solo la información anterior. Si no sabes algo, di que un asesor lo atenderá pronto.
- No inventes precios, fechas ni promesas.
- Si piden hablar con una persona, indica que un asesor los atenderá.
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
    url = (
        f"https://api.chakrahq.com/v1/ext/plugin/whatsapp/{CHAKRA_PLUGIN_ID}"
        f"/api/{WA_API_VERSION}/{PHONE_NUMBER_ID}/messages"
    )
    headers = {"Authorization": f"Bearer {CHAKRA_TOKEN}"}
    data = {
        "messaging_product": "whatsapp",
        "to": numero,
        "type": "text",
        "text": {"body": texto},
    }
    r = requests.post(url, json=data, headers=headers, timeout=20)
    if not r.ok:
        log.error("Error enviando por Chakra: %s %s", r.status_code, r.text)
    else:
        log.info("Respuesta enviada a %s", numero)


def atender(numero, texto):
    try:
        enviar_whatsapp(numero, responder_con_ia(numero, texto))
    except Exception as e:
        log.exception("Error atendiendo a %s: %s", numero, e)


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
    data = request.get_json(silent=True) or {}
    log.info("Evento recibido: %s", str(data)[:500])
    try:
        for entry in data.get("entry", []):
            for change in entry.get("changes", []):
                for msg in change.get("value", {}).get("messages", []):
                    msg_id = msg.get("id")
                    if msg_id in procesados:
                        continue
                    procesados.append(msg_id)
                    numero = msg["from"]
                    if msg.get("type") == "text":
                        texto = msg["text"]["body"]
                        log.info("Mensaje de %s: %s", numero, texto)
                        threading.Thread(target=atender, args=(numero, texto)).start()
                    else:
                        threading.Thread(
                            target=enviar_whatsapp,
                            args=(numero, "Por ahora solo puedo leer mensajes de texto. ¿Podrías escribirme tu consulta? 😊"),
                        ).start()
    except Exception as e:
        log.exception("Error procesando webhook: %s", e)
    return "ok", 200


@app.get("/")
def home():
    return "Bot de WhatsApp activo ✅"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
