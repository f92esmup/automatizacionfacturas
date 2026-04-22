import os
import asyncio
import logging
import hashlib
import shutil
import re
from datetime import datetime
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, BufferedInputFile

from src.config import config
from src.extractor import extract_invoice_data
from src.database import insertar_factura, existe_hash_imagen, init_db
from src.excel import obtener_excel_buffer

logger = logging.getLogger(__name__)

bot = Bot(token=config.bot_token)
dp = Dispatcher()

def calcular_hash_imagen(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for bloque in iter(lambda: f.read(65536), b""):
            h.update(bloque)
    return h.hexdigest()

@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "👋 Bienvenido al bot de Facturación Simplificado.\n\n"
        "Envía una foto de tu factura para procesarla.\n"
        "Usa /excel para descargar el reporte."
    )

@dp.message(Command("excel"))
async def cmd_excel(message: Message):
    if config.allowed_users_list and message.from_user.id not in config.allowed_users_list:
        return
    
    status_msg = await message.answer("🛠 Generando reporte...")
    buffer = await asyncio.to_thread(obtener_excel_buffer)
    
    if buffer:
        doc = BufferedInputFile(buffer.read(), filename="reporte_facturas.xlsx")
        await message.answer_document(doc, caption="📊 Reporte contable.")
        await status_msg.delete()
    else:
        await status_msg.edit_text("⚠️ No hay facturas registradas.")

@dp.message(F.photo)
async def handle_photo(message: Message):
    if config.allowed_users_list and message.from_user.id not in config.allowed_users_list:
        return

    photo = message.photo[-1]
    temp_dir = Path(config.temp_dir)
    temp_dir.mkdir(exist_ok=True)
    
    filename = f"{photo.file_id}.jpg"
    filepath = temp_dir / filename

    try:
        await bot.download(photo, destination=str(filepath))
        
        hash_img = calcular_hash_imagen(str(filepath))
        if existe_hash_imagen(hash_img):
            await message.answer("⚠️ Esta factura ya fue registrada.")
            if filepath.exists():
                filepath.unlink()
            return

        reply_msg = await message.answer("🔄 Procesando con IA...")
        
        invoice = await asyncio.to_thread(extract_invoice_data, str(filepath))
        
        if not invoice:
            await reply_msg.edit_text("❌ No se pudo extraer información de la imagen.")
            return

        invoice.hash_archivo = hash_img
        res_id = await asyncio.to_thread(insertar_factura, invoice)
        
        if res_id != -1:
            # ── ORGANIZAR IMAGEN ──
            fecha_dt = invoice.fecha_expedicion
            year_str = str(fecha_dt.year)
            month_str = str(fecha_dt.month).zfill(2)
            
            # Limpiar nombre del proveedor
            prov_limpio = re.sub(r'[^a-zA-Z0-9_\-]', '_', invoice.proveedor_nombre)
            
            # Directorio: facturas_procesadas/YYYY/MM
            dest_dir = Path(config.processed_dir) / year_str / month_str
            dest_dir.mkdir(parents=True, exist_ok=True)
            
            nuevo_nombre = f"{prov_limpio}_{fecha_dt}_{hash_img[:8]}.jpg"
            dest_path = dest_dir / nuevo_nombre
            
            # Mover archivo
            shutil.move(str(filepath), str(dest_path))

            await reply_msg.edit_text(
                f"✅ **Factura registrada** (ID #{res_id})\n\n"
                f"👤 {invoice.proveedor_nombre}\n"
                f"📅 {invoice.fecha_expedicion} | 💶 {invoice.importe_total} €\n"
                f"📁 Guardada en: `{year_str}/{month_str}/{nuevo_nombre}`",
                parse_mode="Markdown"
            )
        else:
            await reply_msg.edit_text("❌ Error al guardar en la base de datos.")

    except Exception as e:
        logger.error(f"Error procesando foto: {e}")
        await message.answer("⚠️ Ocurrió un error inesperado.")

async def start_bot():
    init_db()
    logger.info("Iniciando bot...")
    await dp.start_polling(bot)
