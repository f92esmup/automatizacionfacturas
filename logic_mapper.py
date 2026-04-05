import time
import logging
from datetime import datetime
from typing import Dict, Any
import re

# Configuración básica del logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROVINCIAS_MAP = {
    '01': 'Álava', '02': 'Albacete', '03': 'Alicante', '04': 'Almería', '05': 'Ávila',
    '06': 'Badajoz', '07': 'Baleares', '08': 'Barcelona', '09': 'Burgos', '10': 'Cáceres',
    '11': 'Cádiz', '12': 'Castellón', '13': 'Ciudad Real', '14': 'Córdoba', '15': 'A Coruña',
    '16': 'Cuenca', '17': 'Girona', '18': 'Granada', '19': 'Guadalajara', '20': 'Guipúzcoa',
    '21': 'Huelva', '22': 'Huesca', '23': 'Jaén', '24': 'León', '25': 'Lleida',
    '26': 'La Rioja', '27': 'Lugo', '28': 'Madrid', '29': 'Málaga', '30': 'Murcia',
    '31': 'Navarra', '32': 'Ourense', '33': 'Asturias', '34': 'Palencia', '35': 'Las Palmas',
    '36': 'Pontevedra', '37': 'Salamanca', '38': 'S.C. Tenerife', '39': 'Cantabria', '40': 'Segovia',
    '41': 'Sevilla', '42': 'Soria', '43': 'Tarragona', '44': 'Teruel', '45': 'Toledo',
    '46': 'Valencia', '47': 'Valladolid', '48': 'Vizcaya', '49': 'Zamora', '50': 'Zaragoza',
    '51': 'Ceuta', '52': 'Melilla'
}

def limpiar_cif(cif_crudo: str) -> tuple[str, bool]:
    """
    Limpia y valida un CIF/NIF usando expresiones regulares para el formato español.
    Devuelve una tupla (cif_limpiado, es_invalido).
    """
    if not cif_crudo:
        return "", True
        
    cif = re.sub(r'[^A-Z0-9]', '', str(cif_crudo).upper())
    
    # Intento de corrección OCR típica
    if len(cif) == 9:
        if cif[0] == 'O':
            cif = '0' + cif[1:]
        if cif[-1] == 'O':
            cif = cif[:-1] + '0'
            
    # Regex básico para CIF, NIF, NIE Español
    regex_format = r'^([A-W][0-9]{7}[A-J0-9]|[0-9]{8}[A-Z]|[XYZ][0-9]{7}[A-Z])$'
    es_invalido = False
    if cif and len(cif) >= 9 and not re.match(regex_format, cif):
        es_invalido = True
        
    return cif, es_invalido

def ensure_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        if isinstance(value, str):
            value = value.replace(',', '.')
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def parse_date(date_str: str) -> str:
    """Intenta parsear una fecha desconocida y devolver string YYYY-MM-DD"""
    try:
        if "-" in date_str:
            parts = date_str.split("-")
        elif "/" in date_str:
            parts = date_str.split("/")
        else:
            return date_str
            
        if len(parts) == 3:
            if len(parts[0]) == 4:
                return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
            elif len(parts[2]) == 4:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        return date_str
    except Exception:
        return date_str

def preparar_para_db(ocr_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma el esquema extraído desde el OCR al esquema normalizado
    requerido por database_manager.py para insertar en múltiples tablas.
    """
    mapped: Dict[str, Any] = {}
    
    # 1. Metadatos de proceso
    mapped['hash_archivo'] = ocr_data.get('hash_archivo')
    mapped['requiere_revision'] = 0
    mapped['comentario_sii'] = ''
    
    numero_factura = str(ocr_data.get('numero_factura', '')).strip()
    if not numero_factura:
        mapped['numero_registro'] = f"REG-{int(time.time())}"
        mapped['su_factura'] = 'S/N'
    else:
        mapped['numero_registro'] = numero_factura
        mapped['su_factura'] = numero_factura
        
    mapped['serie'] = str(ocr_data.get('serie', '1'))
    
    fecha_ocr_bruta = str(ocr_data.get('fecha', '1970-01-01')).strip()
    fecha_ocr_limpia = parse_date(fecha_ocr_bruta)
    mapped['fecha_expedicion'] = fecha_ocr_limpia
    mapped['fecha_operacion'] = fecha_ocr_limpia
    
    cif_crudo = ocr_data.get('cif', '')
    cif_limpio, cif_invalido = limpiar_cif(cif_crudo)
    mapped['cif_proveedor'] = cif_limpio
    if cif_invalido:
        mapped['requiere_revision'] = 1
        mapped['comentario_sii'] = 'REVISAR: CIF de formato dudoso'
    mapped['proveedor_nombre'] = str(ocr_data.get('proveedor', 'DESCONOCIDO'))
    
    mapped['tipo_rectificativa'] = 'S'
    mapped['clase_abono_rectificativas'] = 'N'
    
    codigo_postal = str(ocr_data.get('codigo_postal', '')).strip()
    if codigo_postal:
        mapped['codigo_postal'] = codigo_postal
        prefix = codigo_postal[:2]
        if prefix in PROVINCIAS_MAP:
            mapped['provincia'] = PROVINCIAS_MAP[prefix]

    mapped['importe_total'] = ensure_float(ocr_data.get('total', 0.0))

    # 2. Desglose de impuestos (arreglo en lugar de columnas hardcodeadas)
    desgloses = ocr_data.get('desgloses', [])
    mapped['impuestos'] = []
    
    suma_total_calculada = 0.0
    
    for idx, des in enumerate(desgloses):
        base = ensure_float(des.get('base', 0.0))
        tipo = ensure_float(des.get('tipo', 0.0))
        cuota = ensure_float(des.get('cuota', 0.0))
        
        if idx >= 3:
            # Según los requisitos, agrupar el excedente en la ranura 3
            mapped['impuestos'][2]['base_imponible'] += base
            mapped['impuestos'][2]['cuota_iva'] += cuota
            mapped['requiere_revision'] = 1
            comentario_actual = mapped.get('comentario_sii', '')
            mapped['comentario_sii'] = (comentario_actual + ' | Más de 3 tipos de IVA detectados').strip(' |')
        else:
            mapped['impuestos'].append({
                'base_imponible': base,
                'porcentaje_iva': tipo,
                'cuota_iva': cuota,
                'porcentaje_receq': 0.0,
                'cuota_receq': 0.0
            })
        
        suma_total_calculada += (base + cuota)
        
        # Validar consistencia Base * (Tipo / 100) == Cuota
        cuota_calculada = base * (tipo / 100)
        if base != 0 and abs(cuota_calculada - cuota) > 0.05:
            mapped['requiere_revision'] = 1
            comentario_actual = mapped.get('comentario_sii', '')
            mapped['comentario_sii'] = (comentario_actual + ' | Fallo matemático Base-Cuota').strip(' |')

    # 3. Validación Aritmética final
    if abs(suma_total_calculada - mapped['importe_total']) > 0.05 and desgloses:
        mapped['requiere_revision'] = 1
        comentario_actual = mapped.get('comentario_sii', '')
        mapped['comentario_sii'] = (comentario_actual + ' | Error de suma bases y total').strip(' |')
        
    return mapped

if __name__ == "__main__":
    # Prueba Unitaria Rápida
    import pprint
    ocr_raw_mock = {
        "hash_archivo": "12345abcde",
        "cif": " O-555.777.9O",
        "proveedor": "Distribuidora Nacional",
        "fecha": "2024-11-20",
        "total": "121.00",
        "numero_factura": "F-2024-X",
        "codigo_postal": "28012",
        "desgloses": [
            {"base": 100.0, "tipo": 21.0, "cuota": 21.0}
        ]
    }
    
    mapped_result = preparar_para_db(ocr_raw_mock)
    pprint.pprint(mapped_result)
