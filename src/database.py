import logging
from typing import Optional, List
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from src.config import config
from src.models import Invoice

logger = logging.getLogger(__name__)

# Inicializar cliente de Firestore
# Si config.gcp_project_id es None, usará las credenciales por defecto (ADC)
db = firestore.Client(project=config.gcp_project_id)

def init_db() -> None:
    """
    Firestore no requiere inicialización de esquema como SQL.
    Simplemente validamos la conexión o registramos el inicio.
    """
    logger.info("Firestore inicializado y listo.")

def existe_hash_imagen(hash_archivo: str) -> bool:
    if not hash_archivo:
        return False
    
    docs = db.collection("facturas").where(filter=FieldFilter("hash_archivo", "==", hash_archivo)).limit(1).get()
    return len(list(docs)) > 0

def obtener_cif_por_nombre_proveedor(nombre: str) -> Optional[str]:
    """
    Busca un proveedor por nombre en la colección 'proveedores' 
    y retorna su CIF si existe.
    """
    if not nombre:
        return None
    
    # Buscamos coincidencias exactas por el campo 'nombre'
    docs = db.collection("proveedores").where(filter=FieldFilter("nombre", "==", nombre)).limit(1).get()
    
    for doc in docs:
        return doc.get("cif_europeo")
    
    return None

def obtener_todos_proveedores() -> List[dict]:
    """
    Retorna una lista de diccionarios con la información de todos los proveedores.
    """
    try:
        docs = db.collection("proveedores").get()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        logger.error(f"Error obteniendo proveedores: {e}")
        return []

def insertar_factura(invoice: Invoice) -> str:
    """
    Inserta la factura en Firestore.
    Retorna el ID del documento creado o "-1" en caso de error.
    """
    try:
        # 1. Upsert Proveedor
        # Usamos el CIF como ID de documento para evitar duplicados y facilitar búsquedas
        prov_ref = db.collection("proveedores").document(invoice.cif_proveedor)
        prov_ref.set({
            "cif_europeo": invoice.cif_proveedor,
            "nombre": invoice.proveedor_nombre,
            # Podríamos añadir más campos si el modelo Invoice los tuviera
        }, merge=True)

        # 2. Insertar Factura
        # Convertimos el modelo Pydantic a dict (incluyendo impuestos anidados)
        invoice_data = invoice.model_dump()
        
        # Añadir timestamp de registro
        invoice_data["fecha_registro"] = firestore.SERVER_TIMESTAMP
        
        # Conversión de fechas (Firestore no guarda objetos date de Python nativamente)
        # Se guardarán como strings ISO o podemos convertirlas a datetime
        invoice_data["fecha_expedicion"] = str(invoice.fecha_expedicion)
        if invoice.fecha_operacion:
            invoice_data["fecha_operacion"] = str(invoice.fecha_operacion)

        # Creamos el documento en la colección 'facturas'
        # Dejamos que Firestore genere un ID único o podríamos usar uno basado en hash
        doc_ref = db.collection("facturas").document()
        doc_ref.set(invoice_data)
        
        logger.info(f"Factura guardada en Firestore con ID: {doc_ref.id}")
        return doc_ref.id

    except Exception as e:
        logger.error(f"Error insertando factura en Firestore: {e}")
        return "-1"
