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
            
        # 1. Carga de Columnas (Orden Exacto)
        COLUMNAS_CSV = [
            'FacturaRegistro', 'Serie', ' Su Factura', 'Fecha Expedición', 'Fecha Operación', 
            'Fecha Registro', 'CodigoCuenta', 'CIFEUROPEO', 'Proveedor', 'Comentario SII', 
            'Contrapartida', 'CodigoTransaccion', 'ClaveOperacionFactura', 'Importe Factura', 
            'Base Imponible1', '%Iva1', 'Cuota Iva1', '%RecEq1', 'Cuota Rec1', 'CodigoRetencion', 
            'Base Retención', '%Retención', 'Cuota Retenc.', 'Base Imponible2', '%Iva2', 
            'Cuota Iva2', '%RecEq2', 'Cuota Rec2', 'BaseImponible3', '%Iva3', 'Cuota Iva3', 
            '%RecEq3', 'Cuota Rec3', 'TipoRectificativa', 'ClaseAbonoRectificativas', 
            'EjercicioFacturaRectificada', 'SerieFacturaRectificada', 'NumeroFacturaRectificada', 
            'FechaFacturaRectificada', 'BaseImponibleRectificada', 'CuotaIvaRectificada', 
            'RecargoEquiRectificada', 'NumeroFActuraInicial', 'NumeroFacturaFinal', 
            'IdFacturaExterno', 'Codigo Postal', 'Cod. Provincia', 'Provincia', 'CodigoCanal', 
            'CodigoDelegación', 'CodDepartamento'
        ]

        # 2. Mapeo de Base de Datos
        MAPEO_DB_CSV = {
            'SuFactura': ' Su Factura',
            'FechaExpedicion': 'Fecha Expedición',
            'FechaOperacion': 'Fecha Operación',
            'FechaRegistro': 'Fecha Registro',
            'ComentarioSII': 'Comentario SII',
            'ImporteFactura': 'Importe Factura',
            'BaseImponible1': 'Base Imponible1',
            'PorcentajeIva1': '%Iva1',
            'CuotaIva1': 'Cuota Iva1',
            'PorcentajeRecEq1': '%RecEq1',
            'CuotaRec1': 'Cuota Rec1',
            'BaseRetencion': 'Base Retención',
            'PorcentajeRetencion': '%Retención',
            'CuotaRetenc': 'Cuota Retenc.',
            'BaseImponible2': 'Base Imponible2',
            'PorcentajeIva2': '%Iva2',
            'CuotaIva2': 'Cuota Iva2',
            'PorcentajeRecEq2': '%RecEq2',
            'CuotaRec2': 'Cuota Rec2',
            'BaseImponible3': 'BaseImponible3',
            'PorcentajeIva3': '%Iva3',
            'CuotaIva3': 'Cuota Iva3',
            'PorcentajeRecEq3': '%RecEq3',
            'CuotaRec3': 'Cuota Rec3',
            'CodigoPostal': 'Codigo Postal',
            'CodProvincia': 'Cod. Provincia',
            'CodigoDelegacion': 'CodigoDelegación',
            'NumeroFacturaInicial': 'NumeroFActuraInicial'
        }

        # 3. Construcción del DataFrame
        # Renombrar las columnas persistidas con los nombres del CSV original
        df = df.rename(columns=MAPEO_DB_CSV)

        # Añadir las columnas vacías que falten
        for col in COLUMNAS_CSV:
            if col not in df.columns:
                df[col] = None
        
        # Filtrar y ordenar según el array estricto del reporte contable
        df = df[COLUMNAS_CSV]

        # 4. Formato de Celdas (Conversión a datetimes nativos de pandas para openpyxl)
        for fecha_col in ['Fecha Expedición', 'Fecha Operación', 'Fecha Registro', 'FechaFacturaRectificada']:
            df[fecha_col] = pd.to_datetime(df[fecha_col], errors='coerce').dt.date

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
