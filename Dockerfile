# Imagen base de Python 3.12-slim
FROM python:3.12-slim

# Entorno
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente (el .dockerignore evitará que entre basura)
COPY . .

# Puerto expuesto
EXPOSE 8080

# Ejecutar el bot con uvicorn directamente para producción
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
