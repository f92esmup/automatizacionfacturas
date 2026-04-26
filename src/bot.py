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

from google.cloud import storage
from src.config import config
from src.extractor import extract_invoice_data
from src.validator import validate_invoice
from src.database import (
    insertar_factura, 
    existe_hash_imagen, 
    init_db, 
    obtener_cif_por_nombre_proveedor
)
from src.excel import obtener_excel_buffer

logger = logging.getLogger(__name__)

bot = Bot(token=config.bot_token)
dp = Dispatcher()

# Inicializar cliente de Google Cloud Storage
storage_client = storage.Client(project=config.gcp_project_id)


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
    logger.info(f"Comando /excel recibido del usuario: {message.from_user.id}")
    if (
        config.allowed_users_list
        and message.from_user.id not in config.allowed_users_list
    ):
        logger.warning(f"Usuario {message.from_user.id} NO autorizado para /excel")
        return

    status_msg = await message.answer("🛠 Generando reporte...")
    
    try:
        buffer = await asyncio.to_thread(obtener_excel_buffer)

        if buffer:
            doc = BufferedInputFile(buffer.read(), filename="reporte_facturas.xlsx")
            await message.answer_document(doc, caption="📊 Reporte contable.")
            await status_msg.delete()
        else:
            await status_msg.edit_text("⚠️ No hay facturas registradas.")
            
    except Exception as e:
        logger.error(f"Error en comando /excel: {e}")
        await status_msg.edit_text("❌ ALGO HA IDO MAL al generar el reporte. Por favor, intenta de nuevo más tarde.")


@dp.message(F.photo)
async def handle_photo(message: Message):
    logger.info(f"Foto recibida del usuario: {message.from_user.id}")
    if (
        config.allowed_users_list
        and message.from_user.id not in config.allowed_users_list
    ):
        logger.warning(f"Usuario {message.from_user.id} NO autorizado para enviar fotos")
        await message.answer("🚫 No tienes autorización para procesar facturas.")
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

        # --- SUSTITUCIÓN SILENCIOSA DE CIF ---
        # Si el proveedor ya existe en nuestra DB, usamos su CIF guardado para evitar discrepancias de la IA
        cif_guardado = await asyncio.to_thread(obtener_cif_por_nombre_proveedor, invoice.proveedor_nombre)
        if cif_guardado and cif_guardado != invoice.cif_proveedor:
            logger.info(f"Sustitución silenciosa de CIF para {invoice.proveedor_nombre}: {invoice.cif_proveedor} -> {cif_guardado}")
            invoice.cif_proveedor = cif_guardado

        # --- VALIDACIÓN Y CORRECCIÓN ---
        invoice, warnings = validate_invoice(invoice)
        if warnings:
            warn_text = "⚠️ **Correcciones aplicadas:**\n" + "\n".join(
                [f"- {w}" for w in warnings]
            )
            await message.answer(warn_text, parse_mode="Markdown")

        invoice.hash_archivo = hash_img
        res_id = await asyncio.to_thread(insertar_factura, invoice)

        if res_id != "-1":
            # ── SUBIR IMAGEN A CLOUD STORAGE ──
            fecha_dt = invoice.fecha_expedicion
            year_str = str(fecha_dt.year)
            month_str = str(fecha_dt.month).zfill(2)

            # Limpiar nombre del proveedor
            prov_limpio = re.sub(r"[^a-zA-Z0-9_\-]", "_", invoice.proveedor_nombre)
            nuevo_nombre = f"{prov_limpio}_{fecha_dt}_{hash_img[:8]}.jpg"
            
            # Ruta en el bucket: facturas_procesadas/YYYY/MM/archivo.jpg
            blob_path = f"{config.processed_dir}/{year_str}/{month_str}/{nuevo_nombre}"
            
            try:
                bucket = storage_client.bucket(config.gcs_bucket_name)
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(filepath))
                logger.info(f"Imagen subida a GCS: {blob_path}")
            except Exception as e:
                logger.error(f"Error subiendo a GCS: {e}")
                await message.answer("⚠️ Error al subir la imagen a la nube, pero los datos se guardaron.")

            # Eliminar archivo temporal
            if filepath.exists():
                filepath.unlink()

            # Desglose de impuestos para el resumen
            ivas_texto = "\n".join(
                [
                    f"  • Base: {i.base_imponible} | Tipo: {i.porcentaje_iva}% | IVA: {i.cuota_iva}"
                    for i in invoice.impuestos
                ]
            )
            if not ivas_texto:
                ivas_texto = "  • Sin impuestos detectados"

            # Construir indicador de revisión
            revision_aviso = (
                "⚠️ **REQUIERE REVISIÓN MANUAL (Descuadre en Totales)**\n\n"
                if invoice.requiere_revision
                else ""
            )

            await reply_msg.edit_text(
                f"{revision_aviso}✅ **Factura registrada** (ID: `{res_id}`)\n\n"
                f"👤 **Proveedor:** {invoice.proveedor_nombre} (CIF: `{invoice.cif_proveedor}`)\n"
                f"🧾 **Nº Factura:** `{invoice.numero_registro}`\n"
                f"📅 **Fecha:** {invoice.fecha_expedicion}\n"
                f"💶 **Total:** {invoice.importe_total} €\n\n"
                f"📊 **Impuestos:**\n{ivas_texto}\n\n"
                f"☁️ Guardada en GCS: `{blob_path}`",
                parse_mode="Markdown",
            )
        else:
            await reply_msg.edit_text("❌ Error al guardar en la base de datos.")

    except Exception as e:
        logger.error(f"Error procesando foto: {e}")
        await message.answer("⚠️ Ocurrió un error inesperado.")

