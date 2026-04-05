import io
import logging
import pandas as pd
import psycopg2

from database_manager import get_conn

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


def obtener_excel_buffer() -> io.BytesIO | None:
    """
    Conecta a PostgreSQL, une las tres tablas y genera el Excel
    en el formato estricto de la plantilla contable.
    """
    try:
        conn = get_conn()

        # 1. Cabeceras unidas a Proveedores
        query = '''
            SELECT f.*, p.nombre AS proveedor_nombre,
                         p.codigo_cuenta AS proveedor_codigo_cuenta,
                         p.codigo_postal AS prov_cp,
                         p.provincia     AS prov_provincia
            FROM facturas f
            LEFT JOIN proveedores p ON f.cif_proveedor = p.cif_europeo
        '''
        df_facturas = pd.read_sql_query(query, conn)

        if df_facturas.empty:
            logger.info("Solicitud Excel denegada: la base de datos está vacía.")
            conn.close()
            return None

        # 2. Tramos de IVA
        df_impuestos = pd.read_sql_query("SELECT * FROM factura_impuestos", conn)
        conn.close()

        # 3. Inicializar ranuras de IVA vacías
        for idx in range(1, 4):
            base_col = f'Base Imponible{idx}' if idx < 3 else 'BaseImponible3'
            df_facturas[base_col]          = None
            df_facturas[f'%Iva{idx}']      = None
            df_facturas[f'Cuota Iva{idx}'] = None
            df_facturas[f'%RecEq{idx}']    = None
            df_facturas[f'Cuota Rec{idx}'] = None

        # 4. Pivoting: aplanar impuestos en ranura 1, 2, 3 por factura
        if not df_impuestos.empty:
            def apply_impuestos(row):
                imps = df_impuestos[df_impuestos['factura_id'] == row['id']].head(3).to_dict('records')
                for i, imp in enumerate(imps):
                    slot = i + 1
                    base_col = f'Base Imponible{slot}' if slot < 3 else 'BaseImponible3'
                    row[base_col]          = imp.get('base_imponible')
                    row[f'%Iva{slot}']     = imp.get('porcentaje_iva')
                    row[f'Cuota Iva{slot}']= imp.get('cuota_iva')
                    row[f'%RecEq{slot}']   = imp.get('porcentaje_receq')
                    row[f'Cuota Rec{slot}']= imp.get('cuota_receq')
                return row

            df_facturas = df_facturas.apply(apply_impuestos, axis=1)

        # 5. Renombrar columnas al formato de la plantilla contable
        MAPEO_DB_CSV = {
            'numero_registro':              'FacturaRegistro',
            'serie':                        'Serie',
            'su_factura':                   ' Su Factura',
            'fecha_expedicion':             'Fecha Expedición',
            'fecha_operacion':              'Fecha Operación',
            'fecha_registro':               'Fecha Registro',
            'proveedor_codigo_cuenta':      'CodigoCuenta',
            'cif_proveedor':                'CIFEUROPEO',
            'proveedor_nombre':             'Proveedor',
            'comentario_sii':               'Comentario SII',
            'contrapartida':                'Contrapartida',
            'codigo_transaccion':           'CodigoTransaccion',
            'clave_operacion':              'ClaveOperacionFactura',
            'importe_total':                'Importe Factura',
            'tipo_rectificativa':           'TipoRectificativa',
            'clase_abono_rectificativas':   'ClaseAbonoRectificativas',
            'ejercicio_factura_rectificada':'EjercicioFacturaRectificada',
            'serie_factura_rectificada':    'SerieFacturaRectificada',
            'numero_factura_rectificada':   'NumeroFacturaRectificada',
            'fecha_factura_rectificada':    'FechaFacturaRectificada',
            'base_imponible_rectificada':   'BaseImponibleRectificada',
            'cuota_iva_rectificada':        'CuotaIvaRectificada',
            'recargo_equi_rectificada':     'RecargoEquiRectificada',
            'numero_factura_inicial':       'NumeroFActuraInicial',
            'numero_factura_final':         'NumeroFacturaFinal',
            'id_factura_externo':           'IdFacturaExterno',
            'prov_cp':                      'Codigo Postal',
            'prov_provincia':               'Provincia',
            'codigo_canal':                 'CodigoCanal',
            'codigo_delegacion':            'CodigoDelegación',
            'cod_departamento':             'CodDepartamento',
        }

        df_final = df_facturas.rename(columns=MAPEO_DB_CSV)

        # Asegurar que todas las columnas del template existen
        for col in COLUMNAS_CSV:
            if col not in df_final.columns:
                df_final[col] = None

        df_final = df_final[COLUMNAS_CSV]

        # 6. Formateo de fechas
        for fecha_col in ['Fecha Expedición', 'Fecha Operación', 'Fecha Registro', 'FechaFacturaRectificada']:
            df_final[fecha_col] = pd.to_datetime(df_final[fecha_col], errors='coerce').dt.date

        # 7. Generar buffer Excel
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name="Reporte Facturación")

        buffer.seek(0)
        logger.info(f"Excel compilado con {len(df_final)} registros desde PostgreSQL.")
        return buffer

    except psycopg2.OperationalError as e:
        logger.error(f"No se pudo conectar a PostgreSQL: {e}")
        return None
    except Exception as e:
        logger.error(f"Falla crítica en compilador Excel: {e}")
        return None
