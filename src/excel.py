import io
import logging
import pandas as pd
from src.database import db

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
        # Obtener todas las facturas de Firestore
        facturas_docs = db.collection("facturas").stream()
        list_facturas = []
        for doc in facturas_docs:
            data = doc.to_dict()
            data['id'] = doc.id
            list_facturas.append(data)

        if not list_facturas:
            return None

        df_facturas = pd.DataFrame(list_facturas)

        # Inicializar columnas de impuestos
        for idx in range(1, 4):
            base_col = f'Base Imponible{idx}' if idx < 3 else 'BaseImponible3'
            for col in [base_col, f'%Iva{idx}', f'Cuota Iva{idx}', f'%RecEq{idx}', f'Cuota Rec{idx}']:
                df_facturas[col] = None

        def apply_impuestos(row):
            # En Firestore, 'impuestos' es una lista de diccionarios ya presente en el documento
            imps = row.get('impuestos', [])
            for i, imp in enumerate(imps[:3]): # Máximo 3 slots en el Excel actual
                slot = i + 1
                base_col = f'Base Imponible{slot}' if slot < 3 else 'BaseImponible3'
                row[base_col]          = imp.get('base_imponible')
                row[f'%Iva{slot}']     = imp.get('porcentaje_iva')
                row[f'Cuota Iva{slot}']= imp.get('cuota_iva')
                row[f'%RecEq{slot}']   = imp.get('porcentaje_receq', 0)
                row[f'Cuota Rec{slot}']= imp.get('cuota_receq', 0)
            return row

        df_facturas = df_facturas.apply(apply_impuestos, axis=1)

        MAPEO = {
            'numero_registro': 'FacturaRegistro',
            'serie': 'Serie',
            'su_factura': ' Su Factura',
            'fecha_expedicion': 'Fecha Expedición',
            'fecha_operacion': 'Fecha Operación',
            'fecha_registro': 'Fecha Registro',
            'cif_proveedor': 'CIFEUROPEO',
            'proveedor_nombre': 'Proveedor',
            'importe_total': 'Importe Factura'
        }
        
        df_final = df_facturas.rename(columns=MAPEO)
        
        # Asegurar que todas las columnas requeridas existen
        for col in COLUMNAS_CSV:
            if col not in df_final.columns:
                df_final[col] = None
        
        df_final = df_final[COLUMNAS_CSV]
        
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name="Reporte")
        buffer.seek(0)
        return buffer
    except Exception as e:
        logger.error(f"Error generando excel desde Firestore: {e}")
        return None
