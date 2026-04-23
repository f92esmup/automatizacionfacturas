import asyncio
import sys
from aiogram import Bot
from src.config import config

async def set_webhook():
    if not config.bot_token or not config.webhook_url:
        print("ERROR: BOT_TOKEN o WEBHOOK_URL no configurados en env.yaml")
        return

    bot = Bot(token=config.bot_token)
    webhook_path = config.webhook_path
    # Asegurarse de que la URL termina correctamente
    base_url = config.webhook_url.rstrip('/')
    full_url = f"{base_url}{webhook_path}"
    
    print(f"Configurando Webhook en: {full_url}...")
    
    try:
        success = await bot.set_webhook(url=full_url, drop_pending_updates=True)
        if success:
            print("✅ Webhook configurado correctamente en Telegram.")
            info = await bot.get_webhook_info()
            print(f"Detalles: {info}")
        else:
            print("❌ Falló la configuración del Webhook.")
    except Exception as e:
        print(f"❌ Error conectando con Telegram: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(set_webhook())
