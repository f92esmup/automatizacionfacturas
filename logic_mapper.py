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

def limpiar_cif(cif_crudo: str) -> str:
    cif = re.sub(r'[^A-Z0-9]', '', str(cif_crudo).upper())
    if len(cif) == 9:
        if cif[0] == 'O':
            cif = '0' + cif[1:]
        if cif[-1] == 'O':
            cif = cif[:-1] + '0'
    return cif

def ensure_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def parse_date(date_str: str) -> str:
    """Intenta parsear una fecha desconocida y devolver string YYYY-MM-DD"""
    try:
        # Intenta parsear ISO-like (YYYY-MM-DD o DD-MM-YYYY)
        if "-" in date_str:
            parts = date_str.split("-")
        elif "/" in date_str:
            parts = date_str.split("/")
        else:
            return date_str
            
        if len(parts) == 3:
            # Si el primer parte es año (4 chars)
            if len(parts[0]) == 4:
                return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
            # Si el ultimo parte es año
            elif len(parts[2]) == 4:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        return date_str
    except Exception:
        return date_str

def preparar_para_db(ocr_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma el esquema extraído desde el OCR en el formato rígido 
    requerido por database_manager.py, actuando como un Adapter/Filtro.
    """
    # Diccionario final que se enviará al INSERT
    mapped: Dict[str, Any] = {}
    
    # 1. Valores Constantes Dinámicos
    mapped['FechaRegistro'] = datetime.now().strftime("%Y-%m-%d")
    mapped['ClaveOperacionFactura'] = 1
    
    # 2. Campos de Identificación Básica
    # FacturaRegistro: Si no existe un campo 'numero_factura', generamos un identificador único con timestamp.
    numero_factura = str(ocr_data.get('numero_factura', '')).strip()
    
    if not numero_factura:
        mapped['FacturaRegistro'] = f"REG-{int(time.time())}"
        mapped['SuFactura'] = 'S/N'
    else:
        mapped['FacturaRegistro'] = numero_factura
        mapped['SuFactura'] = numero_factura
    
    # La Serie por defecto se asigna a 1 si no se detecta (en el mock anterior no había serie)
    mapped['Serie'] = str(ocr_data.get('serie', '1'))
    
    # Fechas dictaminadas por el OCR (ej: si solo trae una 'fecha' mapear a ambas de la lógica contable)
    fecha_ocr_bruta = str(ocr_data.get('fecha', '1970-01-01')).strip()
    fecha_ocr_limpia = parse_date(fecha_ocr_bruta)
    mapped['FechaExpedicion'] = fecha_ocr_limpia
    mapped['FechaOperacion'] = fecha_ocr_limpia
    
    # 3. Limpieza de Cadenas (Strings)
    # Según requisitos, quitar todos los espacios, guiones y puntos del CIF y correcciones OCR
    cif_crudo = ocr_data.get('cif', '')
    mapped['CIFEUROPEO'] = limpiar_cif(cif_crudo)
    
    mapped['Proveedor'] = str(ocr_data.get('proveedor', 'DESCONOCIDO'))
    
    # 4. Valores por defecto para esquema contable y Mapeo de Provincias
    mapped['TipoRectificativa'] = 'S'
    mapped['ClaseAbonoRectificativas'] = 'N'
    
    codigo_postal = str(ocr_data.get('codigo_postal', '')).strip()
    if codigo_postal:
        mapped['CodigoPostal'] = codigo_postal
        prefix = codigo_postal[:2]
        if prefix in PROVINCIAS_MAP:
            mapped['CodProvincia'] = prefix
            mapped['Provincia'] = PROVINCIAS_MAP[prefix]

    # 4b. Limpieza y Type-Casting Numérico
    mapped['ImporteFactura'] = ensure_float(ocr_data.get('total', 0.0))

    # 5. Iteración y Desglose de IVAs Dinámico (hasta max 3)
    desgloses = ocr_data.get('desgloses', [])
    
    for i in range(1, 4):  # i toma valores 1, 2 y 3.
        base_key = f"BaseImponible{i}"
        porcent_key = f"PorcentajeIva{i}"
        cuota_key = f"CuotaIva{i}"
        
        lista_idx = i - 1
        
        if lista_idx < len(desgloses):
            desglose = desgloses[lista_idx]
            mapped[base_key] = ensure_float(desglose.get('base', 0.0))
            mapped[porcent_key] = ensure_float(desglose.get('tipo', 0.0))
            mapped[cuota_key] = ensure_float(desglose.get('cuota', 0.0))
        else:
            mapped[base_key] = 0.0
            mapped[porcent_key] = 0.0
            mapped[cuota_key] = 0.0

    # 6. Validación Aritmética
    suma_total_calculada = 0.0
    for i in range(1, 4):
        suma_total_calculada += mapped[f"BaseImponible{i}"] + mapped[f"CuotaIva{i}"]
    
    if abs(suma_total_calculada - mapped['ImporteFactura']) > 0.01:
        mapped['ComentarioSII'] = 'REVISAR: Error de suma'

    return mapped

if __name__ == "__main__":
    # Prueba Unitaria Rápida (Validación Mapeo)
    import pprint
    print("Iniciando prueba unitaria de logic_mapper...\n")
    
    # Simulación "cruda" que podría venir de `ocr_engine.py` (con espacios problemáticos, y 2 IVAs)
    ocr_raw_mock = {
        "cif": " O-555.777.9O",
        "proveedor": "Distribuidora Nacional",
        "fecha": "2024-11-20",
        "total": "121.00",
        "numero_factura": "F-2024-X",
        "codigo_postal": "28012",
        "desgloses": [
            {"base": 70.0, "tipo": 21.0, "cuota": 14.7},
            {"base": 30.1, "tipo": 10.0, "cuota": 3.0} # Error intencionado para probar validación
        ]
    }
    
    print("Datos sucios entrantes del OCR (2 desgloses):")
    pprint.pprint(ocr_raw_mock)
    
    mapped_result = preparar_para_db(ocr_raw_mock)
    
    print("\n------------------------------")
    print("Resultado transformado para BD:")
    print("------------------------------")
    pprint.pprint(mapped_result)
