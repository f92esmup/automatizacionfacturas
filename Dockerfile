# Imagen fundacional oficial optimizada (versión idéntica a tu virtual environment local Python 3.12)
FROM python:3.12-slim

# Buenas prácticas: forzar outputs al vuelo y evitar generación de bytecode obsoleto (Mantiene imagen inmutable)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Entorno de trabajo interno del ecosistema Linux virtualizado
WORKDIR /app

# Capa de Dependencias (Instalar previo a copiar el código permite a Docker cachear (reutilizar) esta capa si tu código cambia pero requirements.txt no)
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Capa de Código Fuente
COPY . /app/

# Punto de entrada de inicialización del Thread / EventLoop
CMD ["python", "bot_main.py"]
