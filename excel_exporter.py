import pandas as pd
import sqlite3
import io
import logging

# Configuración del logger
logger = logging.getLogger(__name__)

# Columnas exactas requeridas por la contabilidad en este mismo orden
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

def obtener_excel_buffer(db_path: str = "facturas.db") -> io.BytesIO | None:
    """
    Se conecta a la base de datos dividida, unifica facturas, proveedores e impuestos,
    y formatea al orden estricto de la plantilla contable.
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # 1. Obtenemos cabeceras unidas a proveedores
        query = '''
            SELECT f.*, p.nombre as proveedor_nombre, p.codigo_cuenta as proveedor_codigo_cuenta,
                   p.codigo_postal as prov_cp, p.provincia as prov_provincia
            FROM facturas f
            LEFT JOIN proveedores p ON f.cif_proveedor = p.cif_europeo
        '''
        df_facturas = pd.read_sql_query(query, conn)
        
        if df_facturas.empty:
            logger.info("Solicitud Excel denegada: La base de datos está vacía.")
            conn.close()
            return None
            
        # 2. Obtenemos impuestos
        df_impuestos = pd.read_sql_query("SELECT * FROM factura_impuestos", conn)
        conn.close()

        # 3. Transformaciones para aplanar la tabla (Pivoting Impuestos to slot 1, 2, 3)
        # Inicializamos ranuras vacías
        df_facturas['Base Imponible1'] = None
        df_facturas['Base Imponible2'] = None
        df_facturas['BaseImponible3'] = None
        df_facturas['%Iva1'] = None
        df_facturas['%Iva2'] = None
        df_facturas['%Iva3'] = None
        df_facturas['Cuota Iva1'] = None
        df_facturas['Cuota Iva2'] = None
        df_facturas['Cuota Iva3'] = None
        df_facturas['%RecEq1'] = None
        df_facturas['%RecEq2'] = None
        df_facturas['%RecEq3'] = None
        df_facturas['Cuota Rec1'] = None
        df_facturas['Cuota Rec2'] = None
        df_facturas['Cuota Rec3'] = None

        if not df_impuestos.empty:
            def apply_impuestos(row):
                imps = df_impuestos[df_impuestos['factura_id'] == row['id']].head(3).to_dict('records')
                for i, imp in enumerate(imps):
                    idx = i + 1
                    base_col = f'Base Imponible{idx}' if idx < 3 else 'BaseImponible3' # Ojo nomenclatura original Excel
                    row[base_col] = imp.get('base_imponible')
                    row[f'%Iva{idx}'] = imp.get('porcentaje_iva')
                    row[f'Cuota Iva{idx}'] = imp.get('cuota_iva')
                    row[f'%RecEq{idx}'] = imp.get('porcentaje_receq')
                    row[f'Cuota Rec{idx}'] = imp.get('cuota_receq')
                return row
            
            df_facturas = df_facturas.apply(apply_impuestos, axis=1)

        # 4. Mapeo final de base de datos a columnas estáticas del CSV referencial
        MAPEO_DB_CSV = {
            'numero_registro': 'FacturaRegistro',
            'serie': 'Serie',
            'su_factura': ' Su Factura',
            'fecha_expedicion': 'Fecha Expedición',
            'fecha_operacion': 'Fecha Operación',
            'fecha_registro': 'Fecha Registro',
            'proveedor_codigo_cuenta': 'CodigoCuenta',
            'cif_proveedor': 'CIFEUROPEO',
            'proveedor_nombre': 'Proveedor',
            'comentario_sii': 'Comentario SII',
            'contrapartida': 'Contrapartida',
            'codigo_transaccion': 'CodigoTransaccion',
            'clave_operacion': 'ClaveOperacionFactura',
            'importe_total': 'Importe Factura',
            'tipo_rectificativa': 'TipoRectificativa',
            'clase_abono_rectificativas': 'ClaseAbonoRectificativas',
            'ejercicio_factura_rectificada': 'EjercicioFacturaRectificada',
            'serie_factura_rectificada': 'SerieFacturaRectificada',
            'numero_factura_rectificada': 'NumeroFacturaRectificada',
            'fecha_factura_rectificada': 'FechaFacturaRectificada',
            'base_imponible_rectificada': 'BaseImponibleRectificada',
            'cuota_iva_rectificada': 'CuotaIvaRectificada',
            'recargo_equi_rectificada': 'RecargoEquiRectificada',
            'numero_factura_inicial': 'NumeroFActuraInicial',
            'numero_factura_final': 'NumeroFacturaFinal',
            'id_factura_externo': 'IdFacturaExterno',
            'prov_cp': 'Codigo Postal',
            'prov_provincia': 'Provincia',
            'codigo_canal': 'CodigoCanal',
            'codigo_delegacion': 'CodigoDelegación',
            'cod_departamento': 'CodDepartamento'
        }

        df_final = df_facturas.rename(columns=MAPEO_DB_CSV)

        # Añadir si falta alguna columna
        for col in COLUMNAS_CSV:
            if col not in df_final.columns:
                df_final[col] = None

        # Ordenar (esto descarta los campos de IDs internos de las 3 tablas que ya no nos valen para Excel)
        df_final = df_final[COLUMNAS_CSV]

        # 5. Formateo
        for fecha_col in ['Fecha Expedición', 'Fecha Operación', 'Fecha Registro', 'FechaFacturaRectificada']:
            df_final[fecha_col] = pd.to_datetime(df_final[fecha_col], errors='coerce').dt.date

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name="Reporte Facturación")
            
        buffer.seek(0)
        logger.info(f"Excel compilado correctamente con {len(df_final)} registros unificados.")
        return buffer

    except sqlite3.OperationalError as e:
        logger.error(f"No existe la base de datos o falló SQL: {e}")
        return None
    except Exception as e:
        logger.error(f"Falla crítica en compilador Excel: {e}")
        return None
