# 🧾 Bot de Automatización de Facturas (Telegram + GCP)

Este es un bot de Telegram diseñado para automatizar la gestión de facturas y tickets. El bot recibe imágenes de facturas, extrae la información clave (proveedor, CIF, fecha, importes, impuestos) mediante IA (OpenAI GPT-4o-mini), valida los datos y los almacena en una base de datos de **Google Cloud Firestore**, guardando además la imagen original en **Google Cloud Storage**.

---

## 🚀 Características Principales
- **Extracción Inteligente:** Usa modelos de visión de OpenAI para leer facturas complejas.
- **Validación Automática:** Comprueba cuadres de bases imponibles y cuotas de IVA.
- **Almacenamiento en la Nube:** Organización automática por carpetas `Año/Mes` en Cloud Storage.
- **Descarga de Reportes:** Genera archivos Excel consolidados de todas las facturas registradas.
- **Webhook Autoconfigurable:** El bot detecta automáticamente su URL al desplegarse en Cloud Run.

---

## 🛠️ Tecnologías y Arquitectura
- **Lenguaje:** Python 3.12
- **Framework Web:** [FastAPI](https://fastapi.tiangolo.com/) (Servidor de Webhook)
- **Bot Library:** [Aiogram 3.x](https://docs.aiogram.dev/)
- **Base de Datos:** [Google Cloud Firestore](https://cloud.google.com/firestore)
- **Almacenamiento:** [Google Cloud Storage](https://cloud.google.com/storage)
- **IA:** [OpenAI API](https://platform.openai.com/) (GPT-4o-mini)
- **Infraestructura:** [Google Cloud Run](https://cloud.google.com/run) (Serverless Docker)

---

## 💻 Prueba Local con Docker (Recomendado)

Esta es la forma más profesional de probar el bot emulando el entorno de la nube antes de desplegar.

### 1. Preparar credenciales de Google Cloud
1. Ve a la [Consola de GCP](https://console.cloud.google.com/).
2. Crea una **Cuenta de Servicio** con roles: `Cloud Datastore User` y `Storage Object Admin`.
3. Crea una **Llave JSON**, descárgala y guárdala en la raíz del proyecto como `gcp-key.json`.

### 2. Configurar Variables
Copia el archivo de ejemplo y rellena tus datos reales:
```bash
cp .env.example .env
```
*Asegúrate de que `GOOGLE_APPLICATION_CREDENTIALS=/app/gcp-key.json` esté configurado en el `.env`.*

### 3. Iniciar el sistema
```bash
docker-compose up --build
```

### 4. Conectar con Telegram (Webhook Local)
Como el bot corre en tu PC, Telegram no puede enviarle mensajes directamente.
1. Instala **Ngrok** y ejecuta: `ngrok http 8080`.
2. Copia la URL `https` que te da Ngrok (ej: `https://abcd-123.ngrok-free.app`).
3. Abre esa URL en tu navegador. El bot se autoconfigurará y verás `{"webhook_configured": true}`.

---

## 🌍 Despliegue en Producción (Google Cloud Run)

### 1. Preparar el entorno de Google Cloud
Asegúrate de tener instalada la [gcloud CLI](https://cloud.google.com/sdk/docs/install) y estar autenticado:
```bash
gcloud auth login
gcloud config set project tu-id-de-proyecto-gcp
```

### 2. Comando de Despliegue Directo
Ejecuta el siguiente comando para compilar y desplegar automáticamente:

```bash
gcloud run deploy bot-facturas \
    --source . \
    --region europe-southwest1 \
    --allow-unauthenticated \
    --set-env-vars="BOT_TOKEN=tu_token,OPENAI_API_KEY=tu_key,GCP_PROJECT_ID=tu_id,GCS_BUCKET_NAME=tu_id,AUTHORIZED_USERS=tu_id"
```

### 3. Activación del Webhook
Una vez completado el despliegue, Google te dará una **Service URL** (ej: `https://bot-facturas-xxxx.a.run.app`).

1. Abre esa URL en tu navegador para que el bot registre la nueva dirección en los servidores de Telegram.
2. Deberías ver: `{"status": "ok", "webhook_configured": true}`.

---

## 📂 Estructura del Proyecto
```text
├── src/
│   ├── bot.py        # Lógica de comandos y handlers de Telegram
│   ├── extractor.py  # Conexión con OpenAI Vision API
│   ├── database.py   # Operaciones con Google Firestore
│   ├── config.py     # Gestión de variables con Pydantic
│   └── excel.py      # Generación de reportes XLSX
├── main.py           # Punto de entrada (FastAPI + Uvicorn)
├── Dockerfile        # Definición de imagen para Cloud Run
├── docker-compose.yml # Orquestación para pruebas locales
└── .env.example      # Plantilla de configuración
```

---

## 📜 Licencia
Este proyecto es privado. Todos los derechos reservados.
