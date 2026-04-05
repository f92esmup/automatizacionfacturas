import os
import json
import logging
import asyncio
import shutil
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, BufferedInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
)

# Módulos del Sistema
from ocr_engine import OCRProcessor
from logic_mapper import preparar_para_db, limpiar_cif, ensure_float, parse_date
from database_manager import insertar_factura, init_db, registrar_evento, existe_hash_imagen
from excel_exporter import obtener_excel_buffer
from manual_labeler import construir_ground_truth

import hashlib

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("⚠️ No se encontró BOT_TOKEN en .env")

MASTER_ADMIN_ID = int(os.getenv("MASTER_ADMIN_ID", "0"))
if MASTER_ADMIN_ID == 0:
    logger.warning("MASTER_ADMIN_ID no configurado. Las alertas admin están desactivadas.")

AUTHORIZED_USERS_STR = os.getenv("AUTHORIZED_USERS", "")
ALLOWED_USERS = [int(u.strip()) for u in AUTHORIZED_USERS_STR.split(",") if u.strip().isdigit()]

TEMP_DIR     = Path("temp_tickets")
DATASET_DIR  = Path("donut_dataset") / "train"
METADATA_FILE = DATASET_DIR / "metadata.jsonl"

# Inicializar BD
init_db()
ocr_processor = OCRProcessor()

# Bot + Dispatcher con almacenamiento FSM en memoria
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# ──────────────────────────────────────────────────────────────────────────────
# MÁQUINA DE ESTADOS (FSM)
# ──────────────────────────────────────────────────────────────────────────────
class EtiquetadoFSM(StatesGroup):
    esperando_cif        = State()
    esperando_proveedor  = State()
    esperando_fecha      = State()
    esperando_total      = State()
    esperando_num_tramos = State()
    esperando_tramo      = State()   # se reutiliza para cada tramo


# ──────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────────────────────────────
def calcular_hash_imagen(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for bloque in iter(lambda: f.read(65536), b""):
            h.update(bloque)
    return h.hexdigest()


def archivo_ya_en_dataset(nombre: str) -> bool:
    if not METADATA_FILE.exists():
        return False
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        for linea in f:
            try:
                if json.loads(linea).get("file_name") == nombre:
                    return True
            except json.JSONDecodeError:
                continue
    return False


def guardar_en_dataset_sync(imagen_src: Path, datos: dict) -> bool:
    """Copia imagen y escribe metadata.jsonl. Retorna True si hubo cambios."""
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATASET_DIR / imagen_src.name
    if archivo_ya_en_dataset(imagen_src.name):
        return False
    if not dest.exists():
        shutil.copy2(imagen_src, dest)
    gt = construir_ground_truth(datos)
    linea = json.dumps({"file_name": imagen_src.name, "ground_truth": gt}, ensure_ascii=False)
    with open(METADATA_FILE, "a", encoding="utf-8") as f:
        f.write(linea + "\n")
    return True


def boton_etiquetar(filepath: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📝 Iniciar Etiquetado Manual",
            callback_data=f"etiquetar:{filepath}"
        )
    ]])


def es_admin(user_id: int) -> bool:
    return MASTER_ADMIN_ID != 0 and user_id == MASTER_ADMIN_ID


# ──────────────────────────────────────────────────────────────────────────────
# COMANDOS BÁSICOS
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message):
    logger.info(f"Usuario {message.from_user.id} usó /start.")
    await message.answer(
        "¡Bienvenido al ERP Facturador Automatizado! 👋\n\n"
        "Envía fotos de tus facturas y las procesaré automáticamente.\n\n"
        "Comandos:\n"
        "📊 /excel — Descarga el libro mayor en Excel\n"
        "❌ /cancelar — Cancela el etiquetado en curso (solo admin)"
    )


@dp.message(Command("excel"))
async def cmd_excel(message: Message):
    user_id = message.from_user.id
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        return
    status_msg = await message.answer("🛠 Generando reporte contable...")
    buffer = await asyncio.to_thread(obtener_excel_buffer)
    if buffer:
        doc = BufferedInputFile(buffer.read(), filename="reporte_contable_sii.xlsx")
        await message.answer_document(doc, caption="📊 Informe estructurado de todas las facturas.")
        await status_msg.delete()
        await asyncio.to_thread(registrar_evento, user_id, message.from_user.username or "?", "GENERACION_EXCEL", "EXITO")
    else:
        await status_msg.edit_text("⚠️ No hay facturas registradas aún.")


@dp.message(Command("cancelar"))
async def cmd_cancelar(message: Message, state: FSMContext):
    if not es_admin(message.from_user.id):
        return
    current = await state.get_state()
    if current:
        await state.clear()
        await message.answer("❌ Etiquetado cancelado.")
    else:
        await message.answer("No hay ningún etiquetado en curso.")


# ──────────────────────────────────────────────────────────────────────────────
# HANDLER DE FOTO  →  OCR  →  ALERTA AL ADMIN
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(Command("miid"))
async def cmd_miid(message: Message):
    """TEMPORAL: muestra el user ID. Eliminar tras confirmar MASTER_ADMIN_ID."""
    await message.answer(f"🆔 Tu user ID es: `{message.from_user.id}`", parse_mode="Markdown")


@dp.message(F.photo)
async def handle_photo(message: Message):
    user_id  = message.from_user.id
    username = message.from_user.username or "Desconocido"

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        return

    await asyncio.to_thread(registrar_evento, user_id, username, "RECEPCION_IMAGEN", "PENDIENTE")

    photo    = message.photo[-1]
    file_id  = photo.file_id
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"ticket_{timestamp}_{file_id}.jpg"
    filepath  = TEMP_DIR / filename

    try:
        file = await bot.get_file(file_id)
        await bot.download_file(file.file_path, str(filepath))

        hash_img = await asyncio.to_thread(calcular_hash_imagen, str(filepath))
        is_dupe  = await asyncio.to_thread(existe_hash_imagen, hash_img)

        if is_dupe:
            await asyncio.to_thread(registrar_evento, user_id, username, "PROCESO_OCR", "RECHAZADO_DUPLICADO")
            await message.answer("⚠️ Este ticket ya fue registrado anteriormente.")
            return

        reply_msg = await message.answer("🔄 Imagen recibida. Procesando con OCR...")

        # ── OCR ──
        t0 = time.perf_counter()
        ocr_raw = await asyncio.to_thread(ocr_processor.procesar_ticket, str(filepath))
        logger.info(f"OCR completado en {time.perf_counter() - t0:.3f}s")

        if not ocr_raw or not ocr_raw.get("total"):
            await asyncio.to_thread(registrar_evento, user_id, username, "PROCESO_OCR", "IMAGEN_INVALIDA")
            await reply_msg.edit_text("❌ No pude reconocer esta imagen como factura válida.")
            # Notificar admin para etiquetado manual aunque el OCR falle
            if MASTER_ADMIN_ID:
                await bot.send_photo(
                    chat_id=MASTER_ADMIN_ID,
                    photo=photo.file_id,
                    caption=(
                        f"⚠️ *OCR fallido* para imagen de @{username}\n"
                        f"Archivo: `{filename}`\n\n"
                        "Puedes etiquetarla manualmente:"
                    ),
                    parse_mode="Markdown",
                    reply_markup=boton_etiquetar(str(filepath))
                )
            return

        await asyncio.to_thread(registrar_evento, user_id, username, "PROCESO_OCR", "OCR_EXITOSO")
        await reply_msg.edit_text("🧩 OCR exitoso. Guardando en base de datos...")

        # ── Mapping y persistencia automática ──
        ocr_raw["hash_archivo"] = hash_img
        mapped = preparar_para_db(ocr_raw)
        inserted_id = await asyncio.to_thread(insertar_factura, mapped)

        if inserted_id != -1:
            tiene_aviso = mapped.get("requiere_revision") == 1
            aviso_txt   = f"\n\n⚠️ Detectado: {mapped.get('comentario_sii')}" if tiene_aviso else ""
            await reply_msg.edit_text(
                f"✅ **Factura registrada** (ID #{inserted_id})\n\n"
                f"👤 {mapped.get('proveedor_nombre')}\n"
                f"💶 {mapped.get('importe_total')} €"
                f"{aviso_txt}\n\n"
                "Usa /excel para descargar el informe."
            )
            await asyncio.to_thread(registrar_evento, user_id, username, "INSERCION_DB", "EXITO")

            # ── Notificar al admin si requiere_revision==1 ──
            if tiene_aviso and MASTER_ADMIN_ID:
                await bot.send_photo(
                    chat_id=MASTER_ADMIN_ID,
                    photo=photo.file_id,
                    caption=(
                        f"🔍 *Factura #{inserted_id} requiere revisión*\n"
                        f"Usuario: @{username}\n"
                        f"Proveedor OCR: `{mapped.get('proveedor_nombre')}`\n"
                        f"Total OCR: `{mapped.get('importe_total')} €`\n"
                        f"Motivo: _{mapped.get('comentario_sii')}_\n\n"
                        f"Archivo: `{filename}`"
                    ),
                    parse_mode="Markdown",
                    reply_markup=boton_etiquetar(str(filepath))
                )
        else:
            await reply_msg.edit_text("❌ Error al guardar en la base de datos.")
            await asyncio.to_thread(registrar_evento, user_id, username, "INSERCION_DB", "ERROR_SQLITE")

    except Exception as e:
        logger.error(f"Error procesando foto: {e}")
        await asyncio.to_thread(registrar_evento, user_id, username, "PROCESO_GLOBAL", "ERROR_SISTEMA")
        await message.answer("⚠️ Fallo crítico procesando el ticket.")


# ──────────────────────────────────────────────────────────────────────────────
# FSM — INICIO: Callback del botón "Iniciar Etiquetado Manual"
# ──────────────────────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("etiquetar:"))
async def cb_iniciar_etiquetado(callback: CallbackQuery, state: FSMContext):
    if not es_admin(callback.from_user.id):
        await callback.answer("⛔ Solo el administrador puede etiquetar.", show_alert=True)
        return

    filepath = callback.data.split("etiquetar:", 1)[1]
    await state.update_data(filepath=filepath, impuestos=[], tramo_actual=0, num_tramos=0)
    await state.set_state(EtiquetadoFSM.esperando_cif)

    await callback.message.answer(
        "📝 *Modo Etiquetado Manual*\n\n"
        f"Archivo: `{Path(filepath).name}`\n\n"
        "**Paso 1/5** — Introduce el *CIF/NIF* del proveedor:",
        parse_mode="Markdown"
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────────────────────
# FSM — PASO 1: CIF
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(EtiquetadoFSM.esperando_cif)
async def fsm_cif(message: Message, state: FSMContext):
    if not es_admin(message.from_user.id):
        return

    cif, invalido = limpiar_cif(message.text.strip())

    if invalido:
        await message.answer(
            f"⚠️ CIF `{cif}` tiene formato dudoso pero se acepta.\n"
            "¿Deseas usarlo igualmente? Reenvíalo o escribe uno correcto.",
            parse_mode="Markdown"
        )
        # Aceptamos igualmente y continuamos (el admin ya sabe lo que hace)

    await state.update_data(cif=cif, cif_invalido=invalido)
    await state.set_state(EtiquetadoFSM.esperando_proveedor)
    await message.answer(
        f"✅ CIF: `{cif}`\n\n**Paso 2/5** — Nombre del proveedor:",
        parse_mode="Markdown"
    )


# ──────────────────────────────────────────────────────────────────────────────
# FSM — PASO 2: Proveedor
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(EtiquetadoFSM.esperando_proveedor)
async def fsm_proveedor(message: Message, state: FSMContext):
    if not es_admin(message.from_user.id):
        return

    await state.update_data(proveedor=message.text.strip())
    await state.set_state(EtiquetadoFSM.esperando_fecha)
    await message.answer(
        "**Paso 3/5** — Fecha de expedición (DD/MM/AAAA o AAAA-MM-DD):",
        parse_mode="Markdown"
    )


# ──────────────────────────────────────────────────────────────────────────────
# FSM — PASO 3: Fecha
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(EtiquetadoFSM.esperando_fecha)
async def fsm_fecha(message: Message, state: FSMContext):
    if not es_admin(message.from_user.id):
        return

    from datetime import datetime as dt
    fecha_raw = message.text.strip()
    fecha = parse_date(fecha_raw)
    try:
        dt.strptime(fecha, "%Y-%m-%d")
    except ValueError:
        await message.answer("⚠️ Formato no reconocido. Usa DD/MM/AAAA o AAAA-MM-DD:")
        return

    await state.update_data(fecha=fecha)
    await state.set_state(EtiquetadoFSM.esperando_total)
    await message.answer(
        f"✅ Fecha: `{fecha}`\n\n**Paso 4/5** — Importe *total* de la factura (€):",
        parse_mode="Markdown"
    )


# ──────────────────────────────────────────────────────────────────────────────
# FSM — PASO 4: Total
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(EtiquetadoFSM.esperando_total)
async def fsm_total(message: Message, state: FSMContext):
    if not es_admin(message.from_user.id):
        return

    total = ensure_float(message.text.strip())
    if total <= 0:
        await message.answer("⚠️ Introduce un importe válido mayor que 0:")
        return

    await state.update_data(total=total)
    await state.set_state(EtiquetadoFSM.esperando_num_tramos)
    await message.answer(
        f"✅ Total: `{total:.2f} €`\n\n"
        "**Paso 5/5** — ¿Cuántos tramos de IVA tiene? (1, 2 o 3):",
        parse_mode="Markdown"
    )


# ──────────────────────────────────────────────────────────────────────────────
# FSM — PASO 5a: Número de tramos
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(EtiquetadoFSM.esperando_num_tramos)
async def fsm_num_tramos(message: Message, state: FSMContext):
    if not es_admin(message.from_user.id):
        return

    try:
        n = int(message.text.strip())
        assert 1 <= n <= 3
    except (ValueError, AssertionError):
        await message.answer("⚠️ Introduce 1, 2 o 3:")
        return

    await state.update_data(num_tramos=n, tramo_actual=1, impuestos=[])
    await state.set_state(EtiquetadoFSM.esperando_tramo)
    await message.answer(
        f"**Tramo 1 de {n}**\n\n"
        "Introduce los datos en este formato:\n"
        "`BASE ; %IVA ; CUOTA`\n\n"
        "Ejemplo: `100 ; 21 ; 21`\n"
        "_(Si la cuota es automática, puedes poner `100 ; 21` y la calcularé yo)_",
        parse_mode="Markdown"
    )


# ──────────────────────────────────────────────────────────────────────────────
# FSM — PASO 5b: Tramos de IVA (loop)
# ──────────────────────────────────────────────────────────────────────────────
@dp.message(EtiquetadoFSM.esperando_tramo)
async def fsm_tramo(message: Message, state: FSMContext):
    if not es_admin(message.from_user.id):
        return

    data = await state.get_data()
    partes = [p.strip() for p in message.text.replace(",", ".").split(";")]

    try:
        base  = float(partes[0])
        pct   = float(partes[1])
        cuota = float(partes[2]) if len(partes) >= 3 else round(base * pct / 100, 2)
    except (ValueError, IndexError):
        await message.answer("⚠️ Formato incorrecto. Usa: `BASE ; %IVA` o `BASE ; %IVA ; CUOTA`", parse_mode="Markdown")
        return

    # Validación aritmética
    cuota_calc = round(base * pct / 100, 2)
    aviso = ""
    if abs(cuota_calc - cuota) > 0.05:
        aviso = f"\n⚠️ Atención: {base} × {pct}% = {cuota_calc:.2f} ≠ {cuota:.2f} (diferencia de {abs(cuota_calc-cuota):.2f} €)"

    impuestos = data.get("impuestos", [])
    impuestos.append({"base_imponible": base, "porcentaje_iva": pct, "cuota_iva": cuota,
                      "porcentaje_receq": 0.0, "cuota_receq": 0.0})

    tramo_actual = data["tramo_actual"]
    num_tramos   = data["num_tramos"]

    await state.update_data(impuestos=impuestos, tramo_actual=tramo_actual + 1)

    if tramo_actual < num_tramos:
        # Pedir siguiente tramo
        await message.answer(
            f"✅ Tramo {tramo_actual} guardado.{aviso}\n\n"
            f"**Tramo {tramo_actual + 1} de {num_tramos}:**\n`BASE ; %IVA ; CUOTA`",
            parse_mode="Markdown"
        )
        return

    # ── Todos los tramos recibidos → validar y persistir ──
    total          = data["total"]
    suma_calculada = sum(imp["base_imponible"] + imp["cuota_iva"] for imp in impuestos)
    diff           = abs(suma_calculada - total)
    aviso_total    = f"\n⚠️ Suma bases+IVA = {suma_calculada:.2f} € ≠ Total = {total:.2f} € (Δ {diff:.2f} €)" if diff > 0.05 else ""

    # Construir diccionario final
    datos_factura = {
        "hash_archivo":      calcular_hash_imagen(data["filepath"]),
        "cif_proveedor":     data["cif"],
        "proveedor_nombre":  data["proveedor"],
        "numero_registro":   f"ADMIN-{int(time.time())}",
        "su_factura":        f"ADMIN-{int(time.time())}",
        "serie":             "1",
        "fecha_expedicion":  data["fecha"],
        "fecha_operacion":   data["fecha"],
        "importe_total":     total,
        "comentario_sii":    "Etiquetado manual vía Telegram",
        "requiere_revision": 1 if data.get("cif_invalido") or diff > 0.05 else 0,
        "tipo_rectificativa":          "S",
        "clase_abono_rectificativas":  "N",
        "impuestos":         impuestos,
    }

    await message.answer(f"💾 Guardando en base de datos...{aviso_total}")

    # Persistencia en BD
    inserted_id = await asyncio.to_thread(insertar_factura, datos_factura)

    # Persistencia en dataset Donut
    imagen_path = Path(data["filepath"])
    dataset_ok  = await asyncio.to_thread(guardar_en_dataset_sync, imagen_path, datos_factura)

    # Respuesta de confirmación
    if inserted_id != -1:
        db_txt = f"✅ BD — Factura ID #{inserted_id}"
    else:
        db_txt = "⚠️ BD — Error al insertar (posible UNIQUE constraint)"

    dataset_txt = "✅ Dataset — Imagen copiada y metadata.jsonl actualizado" if dataset_ok \
                  else "ℹ️ Dataset — Ya existía en metadata.jsonl (no duplicado)"

    await message.answer(
        f"🏁 *Etiquetado completado*\n\n"
        f"{db_txt}\n"
        f"{dataset_txt}\n\n"
        f"👤 `{data['cif']}` — {data['proveedor']}\n"
        f"📅 {data['fecha']} | 💶 {total:.2f} €{aviso_total}",
        parse_mode="Markdown"
    )

    await state.clear()


# ──────────────────────────────────────────────────────────────────────────────
# ARRANQUE
# ──────────────────────────────────────────────────────────────────────────────
async def main():
    TEMP_DIR.mkdir(exist_ok=True)
    logger.info(f"Admin maestro configurado: ID={MASTER_ADMIN_ID}")
    logger.info("Iniciando bot...")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.critical(f"Bot caído: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido.")
