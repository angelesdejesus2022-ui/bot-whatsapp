# ÁNGELES DE JESÚS - WHATSAPP AI

**Asistente virtual inteligente del I.E.P. Ángeles de Jesús – Perú.**

Sistema de atención automatizada para padres de familia y futuros alumnos, utilizando inteligencia artificial.

## Tecnologías

- Python
- Flask
- WhatsApp API mediante ChakraHQ
- Google Gemini AI
- Render
- GitHub

## Funcionalidades

- Atención automática a padres de familia.
- Información sobre matrículas y admisiones.
- Consultas relacionadas con el colegio.
- Memoria temporal independiente por contacto.
- Identificación de usuarios BSUID.
- Respuestas inteligentes mediante Google Gemini.
- Procesamiento de mensajes de WhatsApp.

## Instalación

Instalar las dependencias:

```bash
pip install -r requirements.txt
```

## Ejecución local

```bash
python app.py
```

## Producción

El servidor utiliza Gunicorn:

```bash
gunicorn app:app
```

## Seguridad

- No publicar claves API en GitHub.
- Configurar las credenciales mediante variables de entorno en Render.
- Proteger los tokens de WhatsApp y Google Gemini.

## Proyecto

Desarrollado para mejorar la atención y comunicación del I.E.P. Ángeles de Jesús.
