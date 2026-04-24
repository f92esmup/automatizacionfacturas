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

## 💻 Configuración Local

### 1. Clonar el repositorio
```bash
git clone <url-del-repo>
cd automatizacionfacturas
```

### 2. Crear entorno virtual e instalar dependencias
```bash
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Variables de Entorno (`.env`)
Crea un archivo `.env` en la raíz del proyecto con los siguientes valores:
```env
BOT_TOKEN=tu_token_de_telegram
MASTER_ADMIN_ID=tu_id_de_telegram
AUTHORIZED_USERS=id1,id2,id3
OPENAI_API_KEY=tu_key_de_openai
GCP_PROJECT_ID=tu_id_de_proyecto_gcp
GCS_BUCKET_NAME=tu_nombre_de_bucket
```

### 4. Ejecución
```bash
python main.py
```

---

## 🌍 Despliegue en Producción (Google Cloud Run)

Para desplegar el bot en la región de **Madrid (europe-southwest1)** y dejarlo funcionando en la nube:

### 1. Preparar el entorno de Google Cloud
Asegúrate de tener instalada la [gcloud CLI](https://cloud.google.com/sdk/docs/install) y estar autenticado:
```bash
gcloud auth login
gcloud config set project tu-id-de-proyecto-gcp
```

### 2. Comando de Despliegue Directo
Ejecuta el siguiente comando para compilar y desplegar automáticamente en Madrid:

```bash
gcloud run deploy bot-facturas \
    --source . \
    --region europe-southwest1 \
    --allow-unauthenticated \
    --set-env-vars="BOT_TOKEN=tu_token,OPENAI_API_KEY=tu_key,GCP_PROJECT_ID=tu_id,GCS_BUCKET_NAME=tu_bucket,AUTHORIZED_USERS=tu_id"
```

*Nota: La opción `--allow-unauthenticated` es crítica para que Telegram pueda enviar los mensajes al bot.*

### 3. Activación del Webhook
Una vez completado el despliegue, Google te dará una **Service URL** (ej: `https://bot-facturas-xxxx.a.run.app`).

1. Copia esa URL.
2. Ábrela en tu navegador.
3. El bot se autoconfigurará al recibir la visita y verás un mensaje de confirmación: `{"webhook_configured": true}`.

¡El bot ya estará listo para recibir imágenes en Telegram!

---

## 📁 Estructura del Proyecto
```text
├── src/
│   ├── bot.py        # Lógica de comandos y handlers de Telegram
│   ├── extractor.py  # Conexión con OpenAI Vision API
│   ├── database.py   # Operaciones con Google Firestore
│   ├── config.py     # Gestión de variables con Pydantic
│   └── excel.py      # Generación de reportes XLSX
├── main.py           # Punto de entrada (FastAPI + Uvicorn)
├── Dockerfile        # Definición de imagen para Cloud Run
└── requirements.txt  # Dependencias del proyecto
```

---

## 📜 Licencia
Este proyecto es privado. Todos los derechos reservados.
