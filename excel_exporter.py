import pandas as pd
import sqlite3
import io
import logging

# Configuración del logger
logger = logging.getLogger(__name__)

def obtener_excel_buffer(db_path: str = "facturas.db") -> io.BytesIO | None:
    """
    Se conecta a la base de datos, extrae todo el conjunto contable
    y lo encapsula en un archivo .xlsx residente en memoria RAM.
    """
    try:
        # Abrir conectividad con la persistencia
        conn = sqlite3.connect(db_path)
        
        # Ejecutar volcado crudo
        df = pd.read_sql_query("SELECT * FROM facturas", conn)
        conn.close()
        
        if df.empty:
            logger.info("Solicitud Excel denegada: La base de datos está vacía.")
            return None
            
        # Generar espacio de memoria binaria
        buffer = io.BytesIO()
        
        # Compilación del archivo de hojas de cálculo usando motor OpenPyXL
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name="Reporte Facturación")
            
        # IMPORTANTÍSIMO: Rebobinar el buffer binario al Byte 0 para que pueda ser leído
        buffer.seek(0)
        
        logger.info(f"Excel compilado correctamente con {len(df)} registros.")
        return buffer
        
    except sqlite3.OperationalError as e:
        logger.error(f"No existe la base de datos o falló SQL: {e}")
        return None
    except Exception as e:
        logger.error(f"Falla crítica en compilador Excel: {e}")
        return None
