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
from aiogram.types import (
    Message, 
    BufferedInputFile, 
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from google.cloud import storage
from src.config import config
from src.models import Invoice, TaxItem
from src.extractor import extract_invoice_data
from src.validator import validate_invoice
from src.database import (
    insertar_factura, 
    existe_hash_imagen, 
    init_db, 
    obtener_cif_por_nombre_proveedor,
    obtener_todos_proveedores
)
from src.excel import obtener_excel_buffer

logger = logging.getLogger(__name__)

bot = Bot(token=config.bot_token)
dp = Dispatcher()

# Inicializar cliente de Google Cloud Storage
storage_client = storage.Client(project=config.gcp_project_id)

class ManualInvoiceState(StatesGroup):
    waiting_for_provider_selection = State()
    waiting_for_provider_name = State()
    waiting_for_provider_cif = State()
    waiting_for_invoice_number = State()
    waiting_for_date = State()
    waiting_for_total = State()
    waiting_for_tax_base = State()
    waiting_for_tax_rate = State()
    waiting_for_confirmation = State()

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

# --- MANUAL ENTRY HANDLERS ---

@dp.message(Command("manual"))
async def cmd_manual(message: Message, state: FSMContext):
    if config.allowed_users_list and message.from_user.id not in config.allowed_users_list:
        return

    await state.clear()
    providers = await asyncio.to_thread(obtener_todos_proveedores)
    
    buttons = []
    # Añadir botones de proveedores existentes
    for p in providers[:10]: # Limitar a 10 para no saturar
        buttons.append([KeyboardButton(text=p['nombre'])])
    
    buttons.append([KeyboardButton(text="➕ Nuevo Proveedor")])
    buttons.append([KeyboardButton(text="⏭ Omitir")])
    buttons.append([KeyboardButton(text="❌ Cancelar")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    await message.answer("📝 Vamos a registrar una factura manualmente.\nSelecciona un proveedor o elige una opción:", reply_markup=keyboard)
    await state.set_state(ManualInvoiceState.waiting_for_provider_selection)

@dp.message(F.text == "❌ Cancelar")
async def process_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Operación cancelada.", reply_markup=ReplyKeyboardRemove())

@dp.message(ManualInvoiceState.waiting_for_provider_selection)
async def process_provider_selection(message: Message, state: FSMContext):
    if message.text == "➕ Nuevo Proveedor":
        await message.answer("Introduce el nombre del nuevo proveedor (o pulsa Omitir):", 
                             reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏭ Omitir")], [KeyboardButton(text="❌ Cancelar")]], resize_keyboard=True))
        await state.set_state(ManualInvoiceState.waiting_for_provider_name)
    elif message.text == "⏭ Omitir":
        await state.update_data(proveedor="Desconocido", cif="UNKNOWN")
        await ask_invoice_number(message, state)
    else:
        # Asumimos que eligió uno de la lista
        cif = await asyncio.to_thread(obtener_cif_por_nombre_proveedor, message.text)
        await state.update_data(proveedor=message.text, cif=cif or "UNKNOWN")
        await ask_invoice_number(message, state)

async def ask_invoice_number(message: Message, state: FSMContext):
    await message.answer("Introduce el número de factura (o pulsa Omitir):", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏭ Omitir")], [KeyboardButton(text="❌ Cancelar")]], resize_keyboard=True))
    await state.set_state(ManualInvoiceState.waiting_for_invoice_number)

@dp.message(ManualInvoiceState.waiting_for_provider_name)
async def process_manual_provider_name(message: Message, state: FSMContext):
    name = "Desconocido" if message.text == "⏭ Omitir" else message.text
    await state.update_data(proveedor=name)
    await message.answer("Introduce el CIF del proveedor (o pulsa Omitir):", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏭ Omitir")], [KeyboardButton(text="❌ Cancelar")]], resize_keyboard=True))
    await state.set_state(ManualInvoiceState.waiting_for_provider_cif)

@dp.message(ManualInvoiceState.waiting_for_provider_cif)
async def process_manual_provider_cif(message: Message, state: FSMContext):
    cif = "UNKNOWN" if message.text == "⏭ Omitir" else message.text
    await state.update_data(cif=cif)
    await ask_invoice_number(message, state)

@dp.message(ManualInvoiceState.waiting_for_invoice_number)
async def process_manual_invoice_number(message: Message, state: FSMContext):
    num = "S/N" if message.text == "⏭ Omitir" else message.text
    await state.update_data(numero_factura=num)
    await message.answer("Introduce la fecha (AAAA-MM-DD) o pulsa Omitir para hoy:", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏭ Omitir")], [KeyboardButton(text="❌ Cancelar")]], resize_keyboard=True))
    await state.set_state(ManualInvoiceState.waiting_for_date)

@dp.message(ManualInvoiceState.waiting_for_date)
async def process_manual_date(message: Message, state: FSMContext):
    if message.text == "⏭ Omitir":
        fecha = datetime.now().date()
    else:
        try:
            fecha = datetime.strptime(message.text, "%Y-%m-%d").date()
        except ValueError:
            await message.answer("Formato inválido. Usa AAAA-MM-DD (ej: 2024-05-20) o pulsa Omitir:")
            return
    
    await state.update_data(fecha=fecha)
    await message.answer("Introduce el importe total (ej: 120.50) o pulsa Omitir:", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⏭ Omitir")], [KeyboardButton(text="❌ Cancelar")]], resize_keyboard=True))
    await state.set_state(ManualInvoiceState.waiting_for_total)

@dp.message(ManualInvoiceState.waiting_for_total)
async def process_manual_total(message: Message, state: FSMContext):
    if message.text == "⏭ Omitir":
        total = 0.0
    else:
        try:
            total = float(message.text.replace(",", "."))
        except ValueError:
            await message.answer("Importe inválido. Introduce un número:")
            return
    
    await state.update_data(total=total)
    await state.update_data(impuestos=[]) # Inicializar lista de impuestos
    await ask_tax_step(message)

async def ask_tax_step(message: Message):
    keyboard = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Añadir Impuesto")],
        [KeyboardButton(text="✅ Terminar e Insertar")],
        [KeyboardButton(text="❌ Cancelar")]
    ], resize_keyboard=True)
    await message.answer("¿Deseas añadir el desglose de impuestos?", reply_markup=keyboard)

@dp.message(F.text == "➕ Añadir Impuesto")
async def process_add_tax(message: Message, state: FSMContext):
    await message.answer("Introduce la base imponible del impuesto:", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancelar")]], resize_keyboard=True))
    await state.set_state(ManualInvoiceState.waiting_for_tax_base)

@dp.message(ManualInvoiceState.waiting_for_tax_base)
async def process_tax_base(message: Message, state: FSMContext):
    try:
        base = float(message.text.replace(",", "."))
        await state.update_data(temp_base=base)
        await message.answer("Introduce el porcentaje de IVA (ej: 21):")
        await state.set_state(ManualInvoiceState.waiting_for_tax_rate)
    except ValueError:
        await message.answer("Base inválida. Introduce un número:")

@dp.message(ManualInvoiceState.waiting_for_tax_rate)
async def process_tax_rate(message: Message, state: FSMContext):
    try:
        tipo = float(message.text.replace(",", "."))
        data = await state.get_data()
        base = data['temp_base']
        cuota = round(base * (tipo / 100), 2)
        
        tax_item = TaxItem(base=base, tipo=tipo, cuota=cuota)
        impuestos = data.get('impuestos', [])
        impuestos.append(tax_item)
        
        await state.update_data(impuestos=impuestos)
        await message.answer(f"Impuesto añadido: Base {base} | IVA {tipo}% | Cuota {cuota}")
        await ask_tax_step(message)
        await state.set_state(None) # Reset state to wait for buttons
    except ValueError:
        await message.answer("Porcentaje inválido. Introduce un número:")

@dp.message(F.text == "✅ Terminar e Insertar")
async def process_manual_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    if not data:
        await message.answer("Error al recuperar los datos.", reply_markup=ReplyKeyboardRemove())
        return

    # Validación: No todos nulos/placeholders
    is_all_default = (
        data.get("proveedor") == "Desconocido" and
        data.get("cif") == "UNKNOWN" and
        data.get("numero_factura") == "S/N" and
        data.get("total") == 0.0 and
        not data.get("impuestos")
    )
    
    if is_all_default:
        await message.answer("❌ No se puede registrar una factura sin ningún dato. Inténtalo de nuevo con al menos un valor.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    try:
        invoice = Invoice(**data)
        res_id = await asyncio.to_thread(insertar_factura, invoice)
        
        if res_id != "-1":
            await message.answer(f"✅ Factura manual registrada con ID: `{res_id}`", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
        else:
            await message.answer("❌ Error al guardar en la base de datos.", reply_markup=ReplyKeyboardRemove())
            
    except Exception as e:
        logger.error(f"Error creando factura manual: {e}")
        await message.answer(f"❌ Error en los datos: {e}", reply_markup=ReplyKeyboardRemove())
    
    await state.clear()

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

