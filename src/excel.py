import io
import logging
import pandas as pd
from src.database import get_conn

logger = logging.getLogger(__name__)

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
    try:
        conn = get_conn()
        df_facturas = pd.read_sql_query('''
            SELECT f.*, p.nombre AS proveedor_nombre, p.codigo_postal AS prov_cp
            FROM facturas f
            LEFT JOIN proveedores p ON f.cif_proveedor = p.cif_europeo
        ''', conn)
        df_impuestos = pd.read_sql_query("SELECT * FROM factura_impuestos", conn)
        conn.close()

        if df_facturas.empty: return None

        # Pivot impuestos
        for idx in range(1, 4):
            base_col = f'Base Imponible{idx}' if idx < 3 else 'BaseImponible3'
            for col in [base_col, f'%Iva{idx}', f'Cuota Iva{idx}', f'%RecEq{idx}', f'Cuota Rec{idx}']:
                df_facturas[col] = None

        def apply_impuestos(row):
            imps = df_impuestos[df_impuestos['factura_id'] == row['id']].head(3).to_dict('records')
            for i, imp in enumerate(imps):
                slot = i + 1
                base_col = f'Base Imponible{slot}' if slot < 3 else 'BaseImponible3'
                row[base_col]          = imp.get('base_imponible')
                row[f'%Iva{slot}']     = imp.get('porcentaje_iva')
                row[f'Cuota Iva{slot}']= imp.get('cuota_iva')
            return row

        df_facturas = df_facturas.apply(apply_impuestos, axis=1)

        MAPEO = {
            'numero_registro': 'FacturaRegistro', 'serie': 'Serie', 'su_factura': ' Su Factura',
            'fecha_expedicion': 'Fecha Expedición', 'fecha_operacion': 'Fecha Operación',
            'fecha_registro': 'Fecha Registro', 'cif_proveedor': 'CIFEUROPEO',
            'proveedor_nombre': 'Proveedor', 'importe_total': 'Importe Factura'
        }
        df_final = df_facturas.rename(columns=MAPEO)
        for col in COLUMNAS_CSV:
            if col not in df_final.columns: df_final[col] = None
        
        df_final = df_final[COLUMNAS_CSV]
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name="Reporte")
        buffer.seek(0)
        return buffer
    except Exception as e:
        logger.error(f"Error generando excel: {e}")
        return None
