import os
import logging
import asyncio
import time
from datetime import datetime
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, BufferedInputFile

# Módulos del Sistema (Pipeline ETL Inverso)
from ocr_engine import OCRProcessor
from logic_mapper import preparar_para_db
from database_manager import insertar_factura, init_db, registrar_evento
from excel_exporter import obtener_excel_buffer

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("⚠️ No se encontró la variable BOT_TOKEN en el archivo .env. Por favor arréglalo.")

AUTHORIZED_USERS_STR = os.getenv("AUTHORIZED_USERS", "")
ALLOWED_USERS = [int(u.strip()) for u in AUTHORIZED_USERS_STR.split(",") if u.strip().isdigit()]

# Inicializar Base de Datos (Seguridad de arranque)
logger.info("Verificando integridad de Base de Datos...")
init_db()

# Inicializar motor OCR globalmente
# Esta clase verificará si operará por MOCK o por red neuronal DocVQA
ocr_processor = OCRProcessor()

# Definir el directorio temporal
TEMP_DIR = "temp_tickets"

# Inicializar Bot y Dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: Message):
    logger.info(f"Usuario {message.from_user.id} ({message.from_user.username}) interactuó con /start.")
    welcome_text = (
        "¡Bienvenido al ERP Facturador Automatizado! 👋\n\n"
        "Envía fotos de tus recibos, tickets o facturas. "
        "Nuestro modelo IA los leerá y mapeará matemáticamente en nuestra base de datos.\n\n"
        "Comandos disponibles:\n"
        "📊 /excel - Descarga tu libro mayor contable estructurado en Excel."
    )
    await message.answer(welcome_text)

@dp.message(Command("excel"))
async def cmd_excel(message: Message):
    user_id = message.from_user.id
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        return

    logger.info(f"Usuario {message.from_user.id} ha solicitado un reporte Excel de facturación.")
    status_msg = await message.answer("🛠 Generando reporte contable. Un momento por favor...")
    
    # Off-loading al ThreadPool (Operación Bloqueante -> I/O Bound DB & CPU bound Pandas)
    buffer = await asyncio.to_thread(obtener_excel_buffer)
    
    if buffer:
        # aiogram.types.BufferedInputFile lee el binario
        document = BufferedInputFile(buffer.read(), filename="reporte_contable_sii.xlsx")
        await message.answer_document(document, caption="📊 Aquí tienes el informe estructurado con todos los tickets registrados.")
        await status_msg.delete()
        await asyncio.to_thread(registrar_evento, user_id, message.from_user.username or "Desconocido", "GENERACION_EXCEL", "EXITO")
    else:
        await status_msg.edit_text("⚠️ No se pudo generar. Es posible que aún no hayas registrado ninguna factura o base vacía.")
        await asyncio.to_thread(registrar_evento, user_id, message.from_user.username or "Desconocido", "GENERACION_EXCEL", "ERROR_BASE_VACIA")

@dp.message(F.photo)
async def handle_photo(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Desconocido"

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        return

    # Registrar inicio de acción
    await asyncio.to_thread(registrar_evento, user_id, username, "RECEPCION_IMAGEN", "PENDIENTE")

    photo = message.photo[-1]
    file_id = photo.file_id
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"ticket_{timestamp}_{file_id[:8]}.jpg"
    filepath = os.path.join(TEMP_DIR, filename)
    
    try:
        # Fase 1: Extracción del Archivo Físico
        file = await bot.get_file(file_id)
        await bot.download_file(file.file_path, filepath)
        
        try:
            reply_msg = await message.answer("🔄 Ticket almacenado. Inicializando Arquitectura KIE/OCR...")
            
            # Fase 2: Inferencia de Machine Learning (Aislada del Event Loop Async)
            start_time = time.perf_counter()
            ocr_result_crudo = await asyncio.to_thread(ocr_processor.procesar_ticket, filepath)
            end_time = time.perf_counter()
            logger.info(f"Rendimiento OCR: Inferencia completada en {end_time - start_time:.4f} segundos.")
            
            # Validación de Extracción
            if not ocr_result_crudo or not ocr_result_crudo.get('total'):
                await asyncio.to_thread(registrar_evento, user_id, username, "PROCESO_OCR", "IMAGEN_INVALIDA")
                await reply_msg.edit_text("No he podido reconocer esta imagen como una factura válida. Asegúrate de que se vea bien el CIF y el importe total.")
                return

            await asyncio.to_thread(registrar_evento, user_id, username, "PROCESO_OCR", "OCR_EXITOSO")
            await reply_msg.edit_text("🧩 Procesamiento Visual exitoso. Adaptando al modelo relacional SQL...")
            
            # Fase 3: Adaptation & Mapping (Anti-Corruption Layer)
            mapped_data = preparar_para_db(ocr_result_crudo)
            
            # Fase 4: Persistencia y Grabado Transaccional (Off-loaded)
            inserted_id = await asyncio.to_thread(insertar_factura, mapped_data)
            
            if inserted_id != -1:
                has_warning = mapped_data.get('ComentarioSII') == 'REVISAR: Error de suma'
                warning_text = "\n\n⚠️ Los datos se han guardado, pero detecto una posible discrepancia en los totales. Revísalo en el reporte." if has_warning else ""
                
                await reply_msg.edit_text(
                    f"✅ **¡Documento Contable Registrado!**\n\n"
                    f"👤 Proveedor: {mapped_data.get('Proveedor')}\n"
                    f"🆔 CLAVE INTERNA: #{inserted_id}\n"
                    f"💶 Importe Reconocido: {mapped_data.get('ImporteFactura')}€"
                    f"{warning_text}\n\n"
                    "Para revisar todos los mapeos, ejecuta /excel"
                )
                await asyncio.to_thread(registrar_evento, user_id, username, "INSERCION_DB", "EXITO")
            else:
                await reply_msg.edit_text("❌ Disculpa. Hubo un error de constraints de persistencia a nivel SQLite.")
                await asyncio.to_thread(registrar_evento, user_id, username, "INSERCION_DB", "ERROR_SQLITE")
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"Basura Temporal Limpiada: Archivo {filepath} purgado.")
            
    except Exception as e:
        await asyncio.to_thread(registrar_evento, user_id, username, "PROCESO_GLOBAL", "ERROR_SISTEMA")
        logger.error(f"Falla Sistémica Atendiendo Fotografía: {e}")
        await message.answer("⚠️ Fallo crítico irrecuperable procesando el ticket.")

async def main():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
        logger.info(f"Carpeta Transaccional Generada: {TEMP_DIR}/")

    logger.info("Iniciando Escucha Telemática (TCP/IP Async) del Bot...")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.critical(f"Bot se ha caído. Event Loop abortado: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot apagado y desconectado pacíficamente.")
