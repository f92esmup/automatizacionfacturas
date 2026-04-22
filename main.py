import asyncio
import logging
from src.bot import start_bot
from dotenv import load_dotenv

# Cargar .env manualmente si no lo hace PydanticSettings (ya lo hace, pero por seguridad)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == "__main__":
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        logging.info("Saliendo...")
