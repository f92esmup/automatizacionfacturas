# Bot de Automatización de Facturas (Telegram + GCP)

Este es un bot de Telegram diseñado para automatizar la gestión de facturas y tickets. El bot recibe imágenes de facturas, extrae la información clave mediante IA, valida los datos y los almacena en una base de datos de Google Cloud Firestore, guardando además la imagen original en Google Cloud Storage.

---

## Estado Actual y Prueba de Concepto (PoC)

Actualmente, el proyecto se encuentra en fase de validación académica (Entrega 3 PLN). 
La **Prueba de Concepto (PoC)** que demuestra la viabilidad del módulo de comprensión conversacional se encuentra aislada y documentada en el siguiente notebook:
👉 `poc_clasificacion_intenciones.ipynb`

> **Requisito de ejecución:** Para que el notebook funcione correctamente, es necesario que el archivo de datos `synthetic_dataset.csv` se encuentre en la ruta `data/synthetic_dataset.csv`. Este archivo se genera automáticamente mediante el script `generate_synthetic_data.py`.

### Tareas Pendientes (Próximos Pasos)
- [ ] Integrar el modelo de la PoC (Clasificador SVM) dentro del flujo principal del bot.
- [ ] Refactorizar la arquitectura del proyecto (actualmente `bot.py` ha crecido excesivamente y necesita modularización).
- [ ] Delinear y parametrizar correctamente las consultas (SQL/NoSQL) a ejecutar según la intención detectada.

---

## Características Principales
- **Extracción con OpenAI Vision:** Utiliza GPT-4o-mini para extraer proveedor, CIF, fecha, importes e impuestos de imágenes.
- **Corrección Inteligente de Proveedores:** Si un proveedor ya está registrado, el bot utiliza automáticamente su CIF guardado para corregir posibles errores de lectura de la IA de forma silenciosa.
- **Validación Robusta:** Comprueba automáticamente la coherencia matemática de los impuestos y que los tipos de IVA sean legales en España.
- **Almacenamiento Organizado:** Las imágenes se guardan en Cloud Storage estructuradas por `Año/Mes`.
- **Reportes en Excel:** Genera archivos XLSX consolidados con limpieza automática de datos (manejo de zonas horarias y nulos) para su uso directo en contabilidad.
- **CI/CD Integrado:** Despliegue automático en Cloud Run mediante Google Cloud Build Triggers al hacer push a la rama principal.

---

## Tecnologías y Arquitectura
- **Lenguaje:** Python 3.12
- **Framework Web:** [FastAPI](https://fastapi.tiangolo.com/) (Webhooks)
- **Bot Library:** [Aiogram 3.x](https://docs.aiogram.dev/)
- **Modelado de Datos:** [Pydantic](https://docs.pydantic.dev/)
- **Base de Datos:** [Google Cloud Firestore](https://cloud.google.com/firestore)
- **Almacenamiento:** [Google Cloud Storage](https://cloud.google.com/storage)
- **IA:** [OpenAI API](https://platform.openai.com/) (GPT-4o-mini)
- **Infraestructura:** [Google Cloud Run](https://cloud.google.com/run) + [Secret Manager](https://cloud.google.com/secret-manager)
- **CI/CD:** [Google Cloud Build](https://cloud.google.com/build)

---

## Configuración Local

### 1. Requisitos
- Docker y Docker Compose.
- Cuenta de Servicio de GCP con roles: `Cloud Datastore User` y `Storage Object Admin`.

### 2. Configurar Variables
Copia el ejemplo y rellena tus datos:
```bash
cp .env.example .env
```
*Asegúrate de colocar tu llave JSON de GCP en la raíz como `gcp-key.json`.*

### 3. Iniciar
```bash
docker-compose up --build
```

---

## Despliegue y CI/CD (Google Cloud)

El proyecto está configurado para desplegarse automáticamente mediante un Cloud Build Trigger.

### 1. Secretos (Secret Manager)
El despliegue requiere que los siguientes secretos existan en Google Cloud Secret Manager:
- `BOT_TOKEN`: Token de tu bot de Telegram.
- `OPENAI_API_KEY`: Tu API Key de OpenAI.

### 2. Flujo de Trabajo
Cada vez que se realiza un `git push origin main`:
1. **Cloud Build** detecta el cambio.
2. Construye la imagen de Docker.
3. La sube a **Artifact Registry**.
4. Despliega la nueva versión en **Cloud Run**, inyectando los secretos automáticamente.

---

## Estructura del Proyecto
```text
├── src/
│   ├── bot.py        # Comandos de Telegram y lógica principal
│   ├── extractor.py  # Extracción de datos con OpenAI Vision
│   ├── database.py   # Consultas y persistencia en Firestore
│   ├── validator.py  # Reglas de negocio y validación de importes
│   ├── models.py     # Modelos de datos (Pydantic)
│   ├── config.py     # Gestión de configuración y secretos
│   └── excel.py      # Lógica de generación de reportes XLSX
├── main.py           # Servidor FastAPI para Webhooks
├── cloudbuild.yaml   # Configuración de CI/CD para Google Cloud
├── Dockerfile        # Definición de la imagen de producción
├── docker-compose.yml # Entorno de desarrollo local
└── .env.example      # Plantilla de configuración
```


