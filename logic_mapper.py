import time
import logging
from typing import Dict, Any

# Configuración básica del logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preparar_para_db(ocr_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma el esquema extraído desde el OCR en el formato rígido 
    requerido por database_manager.py, actuando como un Adapter/Filtro.
    """
    # Diccionario final que se enviará al INSERT
    mapped: Dict[str, Any] = {}
    
    # 1. Valores Constantes
    mapped['FechaRegistro'] = 1
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
    fecha_ocr = str(ocr_data.get('fecha', '1970-01-01')).strip()
    mapped['FechaExpedicion'] = fecha_ocr
    mapped['FechaOperacion'] = fecha_ocr
    
    # 3. Limpieza de Cadenas (Strings)
    # Según requisitos, quitar todos los espacios del CIF
    cif_crudo = str(ocr_data.get('cif', ''))
    mapped['CIFEUROPEO'] = cif_crudo.replace(" ", "").upper()
    
    mapped['Proveedor'] = str(ocr_data.get('proveedor', 'DESCONOCIDO'))
    
    # 4. Limpieza y Type-Casting Numérico
    # Es fundamental para SQLite forzar a floats para evitar que Strings se inserten si el motor
    # de base de datos no es estricto por defecto.
    try:
        mapped['ImporteFactura'] = float(ocr_data.get('total', 0.0))
    except (ValueError, TypeError):
        mapped['ImporteFactura'] = 0.0
        logger.warning(f"No se pudo forzar 'total' a float. Valor recibido: {ocr_data.get('total')}")

    # 5. Iteración y Desglose de IVAs Dinámico (hasta max 3)
    desgloses = ocr_data.get('desgloses', [])
    
    for i in range(1, 4):  # i toma valores 1, 2 y 3.
        base_key = f"BaseImponible{i}"
        porcent_key = f"PorcentajeIva{i}"
        cuota_key = f"CuotaIva{i}"
        
        lista_idx = i - 1
        
        if lista_idx < len(desgloses):
            desglose = desgloses[lista_idx]
            try:
                mapped[base_key] = float(desglose.get('base', 0.0))
                mapped[porcent_key] = float(desglose.get('tipo', 0.0))
                mapped[cuota_key] = float(desglose.get('cuota', 0.0))
            except (ValueError, TypeError):
                logger.error(f"Error al convertir a float en el desglose {i}.")
                mapped[base_key] = 0.0
                mapped[porcent_key] = 0.0
                mapped[cuota_key] = 0.0
        else:
            mapped[base_key] = 0.0
            mapped[porcent_key] = 0.0
            mapped[cuota_key] = 0.0

    return mapped

if __name__ == "__main__":
    # Prueba Unitaria Rápida (Validación Mapeo)
    import pprint
    print("Iniciando prueba unitaria de logic_mapper...\n")
    
    # Simulación "cruda" que podría venir de `ocr_engine.py` (con espacios problemáticos, y 2 IVAs)
    ocr_raw_mock = {
        "cif": " B 555 777 99",
        "proveedor": "Distribuidora Nacional",
        "fecha": "2024-11-20",
        "total": "121.00",
        "numero_factura": "F-2024-X",
        "desgloses": [
            {"base": 70.0, "tipo": 21.0, "cuota": 14.7},
            {"base": 30.0, "tipo": 10.0, "cuota": 3.0} # Hay dos tipos de IVA en la factura
        ]
    }
    
    print("Datos sucios entrantes del OCR (2 desgloses):")
    pprint.pprint(ocr_raw_mock)
    
    mapped_result = preparar_para_db(ocr_raw_mock)
    
    print("\n------------------------------")
    print("Resultado transformado para BD:")
    print("------------------------------")
    pprint.pprint(mapped_result)
