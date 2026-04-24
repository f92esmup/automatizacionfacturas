import logging
import uvicorn
import os
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
    
    if config.webhook_url:
        webhook_url = f"{config.webhook_url.rstrip('/')}{config.webhook_path}"
        logger.info(f"Configurando webhook desde variable de entorno en: {webhook_url}")
        
        await bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=dp.resolve_used_update_types()
        )
    else:
        logger.info("WEBHOOK_URL no configurada. Se esperará a una petición al health check para autoconfigurarse.")
    
    yield
    
    # --- SHUTDOWN ---
    logger.info("Cerrando sesión del bot...")
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

# Endpoint de salud para Cloud Run y Autoconfiguración de Webhook
@app.get("/")
async def health_check(request: Request):
    # Intentar autoconfigurar el webhook si no hay una URL configurada
    if not config.webhook_url or os.getenv("K_SERVICE"):
        # Reconstruir la URL base
        base_url = str(request.base_url).rstrip("/")
        
        # En Cloud Run, request.base_url puede ser http si hay un proxy, 
        # pero Telegram requiere https. Forzamos https si no estamos en localhost.
        if "localhost" not in base_url and base_url.startswith("http://"):
            base_url = base_url.replace("http://", "https://")
            
        webhook_url = f"{base_url}{config.webhook_path}"
        logger.info(f"Autoconfigurando webhook en: {webhook_url}")
        
        try:
            await bot.set_webhook(
                url=webhook_url,
                drop_pending_updates=True,
                allowed_updates=dp.resolve_used_update_types()
            )
            return {
                "status": "ok", 
                "bot": "online", 
                "webhook_configured": True, 
                "webhook_url": webhook_url
            }
        except Exception as e:
            logger.error(f"Error configurando webhook dinámico: {e}")
            return {"status": "error", "message": str(e)}

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
