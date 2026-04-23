import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Asegurar que el directorio raíz está en el path
sys.path.append(os.getcwd())

# Cargar variables de entorno manualmente antes de importar la config si es necesario
# aunque pydantic-settings en src/config.py ya lo hace.
from src.extractor import extract_invoice_data
from src.validator import validate_invoice
from src.config import config

def test_extraction():
    print(f"--- Probando extracción y validación con modelo: {config.openai_model} ---")
    
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
                # Guardamos una copia simple de los valores originales para comparar
                orig_total = invoice.importe_total
                orig_impuestos = [(imp.base_imponible, imp.porcentaje_iva, imp.cuota_iva) for imp in invoice.impuestos]

                # --- VALIDACIÓN ---
                invoice, warnings = validate_invoice(invoice)
                
                if warnings:
                    print("\n  [ COMPARATIVA DE CORRECCIONES ]")
                    print(f"  Total: {orig_total} -> {invoice.importe_total}")
                    print("  Impuestos (Base | Tipo | Cuota):")
                    # Mostrar originales
                    print("    Originales:")
                    for b, t, c in orig_impuestos:
                        print(f"      • {b} | {t}% | {c}")
                    # Mostrar corregidos
                    print("    Corregidos:")
                    for imp in invoice.impuestos:
                        print(f"      • {imp.base_imponible} | {imp.porcentaje_iva}% | {imp.cuota_iva}")
                    
                    print("\n  ⚠️ Detalles de las advertencias:")
                    for w in warnings:
                        print(f"    - {w}")
                else:
                    print("  ✅ Extracción perfecta (no se requirieron correcciones).")

                print("\nDatos finales:")
                print(f"  CIF: {invoice.cif_proveedor}")
                print(f"  Proveedor: {invoice.proveedor_nombre}")
                print(f"  Nº Factura: {invoice.numero_registro}")
                print(f"  Fecha: {invoice.fecha_expedicion}")
                print(f"  Total Final: {invoice.importe_total}")
                print("  Impuestos Finales:")
                for imp in invoice.impuestos:
                    print(f"    • Base: {imp.base_imponible} | Tipo: {imp.porcentaje_iva}% | Cuota: {imp.cuota_iva}")
            else:
                print("No se pudieron extraer datos de la imagen.")
        except Exception as e:
            print(f"Error procesando {img_path.name}: {e}")

if __name__ == "__main__":
    test_extraction()
