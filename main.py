import logging
import uvicorn
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from aiogram import types

from src.bot import bot, dp
from src.config import config
from src.database import init_db

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    init_db()
    
    webhook_url = f"{config.webhook_url}{config.webhook_path}"
    logger.info(f"Configurando webhook en: {webhook_url}")
    
    await bot.set_webhook(
        url=webhook_url,
        drop_pending_updates=True,
        allowed_updates=dp.resolve_used_update_types()
    )
    
    yield
    
    # --- SHUTDOWN ---
    logger.info("Eliminando webhook...")
    await bot.delete_webhook()
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

# Endpoint de salud para Cloud Run
@app.get("/")
async def health_check():
    return {"status": "ok", "bot": "online"}

# Endpoint para recibir los updates de Telegram
@app.post(config.webhook_path)
async def bot_webhook(update: dict):
    telegram_update = types.Update(**update)
    await dp.feed_update(bot=bot, update=telegram_update)
    return {"ok": True}

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=config.web_server_host,
        port=config.port
    )
