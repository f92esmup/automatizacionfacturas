import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Asegurar que el directorio raíz está en el path
sys.path.append(os.getcwd())

# Cargar variables de entorno manualmente antes de importar la config si es necesario
# aunque pydantic-settings en src/config.py ya lo hace.
from src.extractor import extract_invoice_data
from src.config import config

def test_extraction():
    print(f"--- Probando extracción con modelo: {config.openai_model} ---")
    
    samples_dir = Path("samples")
    if not samples_dir.exists():
        print("Error: El directorio 'samples' no existe.")
        return

    # Extensiones de imagen comunes
    extensions = ("*.png", "*.jpg", "*.jpeg", "*.webp")
    image_files = []
    for ext in extensions:
        image_files.extend(samples_dir.glob(ext))

    if not image_files:
        print("No se encontraron imágenes en el directorio 'samples'.")
        return

    for img_path in image_files:
        print(f"\nProcesando: {img_path.name}...")
        try:
            invoice = extract_invoice_data(str(img_path))
            if invoice:
                print("Datos extraídos exitosamente:")
                print(f"  CIF: {invoice.cif}")
                print(f"  Proveedor: {invoice.proveedor}")
                print(f"  Nº Factura: {invoice.numero_factura}")
                print(f"  Fecha: {invoice.fecha}")
                print(f"  Total: {invoice.total}")
                print("  Impuestos:")
                for imp in invoice.impuestos:
                    print(f"    - Base: {imp.base}, Tipo: {imp.tipo}%, Cuota: {imp.cuota}")
            else:
                print("No se pudieron extraer datos de la imagen.")
        except Exception as e:
            print(f"Error procesando {img_path.name}: {e}")

if __name__ == "__main__":
    test_extraction()
