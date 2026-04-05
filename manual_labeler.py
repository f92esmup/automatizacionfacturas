#!/usr/bin/env python3
"""
manual_labeler.py  —  Etiquetador Manual de Facturas
=====================================================
Flujo:
  1. Elige la imagen de factura de temp_tickets/.
  2. Calcula su SHA-256 → detecta duplicados en BD.
  3. Pide los datos por teclado (CIF, proveedor, fecha, importes, IVA…).
  4. Valida aritmética en tiempo real.
  5. Inserta en la BD (proveedores + facturas + factura_impuestos).
  6. Copia la imagen a donut_dataset/train/ y actualiza metadata.jsonl.
"""

import os
import sys
import json
import shutil
import hashlib
import re
import time
from pathlib import Path
from typing import List, Dict, Any

# ── Ajusta el path para importar los módulos del proyecto ──────────────────────
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from database_manager import init_db, existe_hash_imagen, insertar_factura
from logic_mapper import limpiar_cif, ensure_float, parse_date

# ── Rutas ──────────────────────────────────────────────────────────────────────
DB_PATH         = str(BASE_DIR / "facturas.db")
TEMP_DIR        = BASE_DIR / "temp_tickets"
DATASET_DIR     = BASE_DIR / "donut_dataset" / "train"
METADATA_FILE   = DATASET_DIR / "metadata.jsonl"

# ──────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────────────────────────────

def calcular_sha256(ruta: Path) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(65536), b""):
            h.update(bloque)
    return h.hexdigest()


def listar_imagenes() -> List[Path]:
    """Devuelve las imágenes soportadas en temp_tickets/."""
    extensiones = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"}
    imagenes = sorted(
        p for p in TEMP_DIR.iterdir()
        if p.is_file() and p.suffix.lower() in extensiones
    )
    return imagenes


def preguntar(prompt: str, obligatorio: bool = True) -> str:
    """Lee una línea de teclado con prompt coloreado; permite vacío si no es obligatorio."""
    while True:
        valor = input(f"  {prompt}: ").strip()
        if valor or not obligatorio:
            return valor
        print("   ⚠  Campo obligatorio, inténtalo de nuevo.")


def preguntar_float(prompt: str, obligatorio: bool = True) -> float:
    """Lee un número decimal por teclado."""
    while True:
        raw = preguntar(prompt, obligatorio)
        if not raw and not obligatorio:
            return 0.0
        try:
            return ensure_float(raw)
        except Exception:
            print("   ⚠  Introduce un número válido (p.ej. 1234.56 o 1234,56).")


def preguntar_fecha(prompt: str) -> str:
    """Lee una fecha y la normaliza a YYYY-MM-DD."""
    while True:
        raw = preguntar(prompt)
        fecha = parse_date(raw)
        try:
            from datetime import datetime
            datetime.strptime(fecha, "%Y-%m-%d")
            return fecha
        except ValueError:
            print("   ⚠  Formato de fecha no reconocido. Usa DD/MM/AAAA o AAAA-MM-DD.")


def archivo_ya_en_dataset(nombre_archivo: str) -> bool:
    """Comprueba si file_name ya aparece en metadata.jsonl."""
    if not METADATA_FILE.exists():
        return False
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        for linea in f:
            try:
                datos = json.loads(linea)
                if datos.get("file_name") == nombre_archivo:
                    return True
            except json.JSONDecodeError:
                continue
    return False


# ──────────────────────────────────────────────────────────────────────────────
# CONSTRUCCIÓN DEL GROUND TRUTH DONUT
# ──────────────────────────────────────────────────────────────────────────────

def construir_ground_truth(datos: Dict[str, Any]) -> str:
    """
    Genera la cadena XML que Donut usa como etiqueta de entrenamiento.

    Formato:
    <s_factura>
      <s_cif>B12345678</s_cif>
      <s_proveedor>Nombre S.L.</s_proveedor>
      <s_fecha>2024-03-15</s_fecha>
      <s_total>1210.00</s_total>
      <s_impuestos>
        <s_tramo>
          <s_base>1000.00</s_base>
          <s_pct_iva>21.0</s_pct_iva>
          <s_cuota_iva>210.00</s_cuota_iva>
        </s_tramo>
      </s_impuestos>
    </s_factura>
    """
    tramos_xml = ""
    for imp in datos.get("impuestos", []):
        tramos_xml += (
            f"<s_tramo>"
            f"<s_base>{imp['base_imponible']:.2f}</s_base>"
            f"<s_pct_iva>{imp['porcentaje_iva']:.1f}</s_pct_iva>"
            f"<s_cuota_iva>{imp['cuota_iva']:.2f}</s_cuota_iva>"
            f"</s_tramo>"
        )

    gt = (
        f"<s_factura>"
        f"<s_cif>{datos.get('cif_proveedor', '')}</s_cif>"
        f"<s_proveedor>{datos.get('proveedor_nombre', '')}</s_proveedor>"
        f"<s_fecha>{datos.get('fecha_expedicion', '')}</s_fecha>"
        f"<s_total>{datos.get('importe_total', 0):.2f}</s_total>"
        f"<s_impuestos>{tramos_xml}</s_impuestos>"
        f"</s_factura>"
    )
    return gt


# ──────────────────────────────────────────────────────────────────────────────
# ENTRADA INTERACTIVA DE DATOS
# ──────────────────────────────────────────────────────────────────────────────

def pedir_datos_factura(imagen_path: Path, hash_img: str) -> Dict[str, Any]:
    """
    CLI interactivo que pide todos los campos de la factura al usuario.
    Devuelve el diccionario listo para insertar_factura().
    """
    print("\n" + "═" * 60)
    print(f"  📄  Imagen  : {imagen_path.name}")
    print(f"  🔑  SHA-256 : {hash_img[:16]}…")
    print("═" * 60)

    # ── CIF ───────────────────────────────────────────────────────────────────
    while True:
        cif_crudo = preguntar("CIF / NIF del proveedor")
        cif, invalido = limpiar_cif(cif_crudo)
        if invalido:
            print(f"   ⚠  CIF '{cif}' tiene formato dudoso pero se acepta.")
            confirmar = input("   ¿Continuar igualmente? (s/N): ").strip().lower()
            if confirmar == "s":
                break
        else:
            break

    # ── Proveedor ─────────────────────────────────────────────────────────────
    proveedor = preguntar("Nombre del proveedor")

    # ── Número de factura ─────────────────────────────────────────────────────
    num_factura = preguntar("Número de factura (deja vacío para auto)", obligatorio=False)
    if not num_factura:
        num_factura = f"MANUAL-{int(time.time())}"

    # ── Fecha ─────────────────────────────────────────────────────────────────
    fecha = preguntar_fecha("Fecha de expedición (DD/MM/AAAA o AAAA-MM-DD)")

    # ── Importe Total ─────────────────────────────────────────────────────────
    total = preguntar_float("Importe TOTAL de la factura (€)")

    # ── Desglose IVA ──────────────────────────────────────────────────────────
    print("\n  — Desglose de IVA —")
    while True:
        try:
            n_tramos = int(input("  ¿Cuántos tramos de IVA tiene la factura? [1-3]: ").strip() or "1")
            if 1 <= n_tramos <= 3:
                break
        except ValueError:
            pass
        print("   ⚠  Introduce 1, 2 o 3.")

    impuestos = []
    suma_calculada = 0.0

    for i in range(1, n_tramos + 1):
        print(f"\n  Tramo {i}:")
        base   = preguntar_float(f"    Base imponible {i} (€)")
        pct    = preguntar_float(f"    % IVA {i} (p.ej. 21)")
        # Calcular cuota sugerida
        cuota_sugerida = round(base * pct / 100, 2)
        print(f"    → Cuota IVA calculada automáticamente: {cuota_sugerida:.2f} €")
        cuota_raw = input(f"    Cuota IVA {i} (€) [Enter = {cuota_sugerida:.2f}]: ").strip()
        cuota = ensure_float(cuota_raw) if cuota_raw else cuota_sugerida

        # Validación base × tipo
        cuota_check = round(base * pct / 100, 2)
        if base != 0 and abs(cuota_check - cuota) > 0.05:
            print(f"   ⚠  Inconsistencia: {base} × {pct}% = {cuota_check:.2f} ≠ {cuota:.2f}")

        impuestos.append({
            "base_imponible": base,
            "porcentaje_iva": pct,
            "cuota_iva":      cuota,
            "porcentaje_receq": 0.0,
            "cuota_receq":    0.0,
        })
        suma_calculada += base + cuota

    # ── Validación aritmética final ───────────────────────────────────────────
    print()
    diferencia = abs(suma_calculada - total)
    if diferencia > 0.05:
        print(f"   ⚠  AVISO: Suma(Bases+IVA) = {suma_calculada:.2f} € ≠ Total = {total:.2f} €  (Δ = {diferencia:.2f} €)")
        continuar = input("   ¿Guardar igualmente? (s/N): ").strip().lower()
        if continuar != "s":
            print("   ✖  Operación cancelada. Vuelve a introducir los datos.")
            return pedir_datos_factura(imagen_path, hash_img)   # recursión para reintentar
    else:
        print(f"   ✔  Aritmética correcta: Suma = {suma_calculada:.2f} € ≈ Total = {total:.2f} €")

    # ── Comentario opcional ───────────────────────────────────────────────────
    comentario = preguntar("Comentario SII (opcional, Enter para omitir)", obligatorio=False)
    requiere_revision = 1 if invalido else 0

    # ── Construir diccionario final ───────────────────────────────────────────
    datos = {
        "hash_archivo":     hash_img,
        "cif_proveedor":    cif,
        "proveedor_nombre": proveedor,
        "numero_registro":  num_factura,
        "su_factura":       num_factura,
        "serie":            "1",
        "fecha_expedicion": fecha,
        "fecha_operacion":  fecha,
        "importe_total":    total,
        "comentario_sii":   comentario or "",
        "requiere_revision": requiere_revision,
        "tipo_rectificativa":       "S",
        "clase_abono_rectificativas": "N",
        "impuestos":        impuestos,
    }
    return datos


# ──────────────────────────────────────────────────────────────────────────────
# PERSISTENCIA EN DATASET DONUT
# ──────────────────────────────────────────────────────────────────────────────

def guardar_en_dataset(imagen_src: Path, datos: Dict[str, Any]) -> bool:
    """
    Copia la imagen a donut_dataset/train/ y añade la línea al metadata.jsonl.
    Retorna True si se guardó, False si ya existía (duplicado en dataset).
    """
    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    dest_nombre = imagen_src.name
    dest_path   = DATASET_DIR / dest_nombre

    # Comprobación de duplicado en dataset
    if archivo_ya_en_dataset(dest_nombre):
        print(f"   ℹ  '{dest_nombre}' ya está en el dataset (metadata.jsonl). No se duplica.")
        return False

    # Copiar imagen
    if not dest_path.exists():
        shutil.copy2(imagen_src, dest_path)
        print(f"   📋  Imagen copiada → {dest_path}")
    else:
        print(f"   ℹ  La imagen ya existe físicamente en dataset, solo se añade metadata.")

    # Construir ground truth
    ground_truth = construir_ground_truth(datos)

    # Añadir línea en metadata.jsonl
    linea = json.dumps({"file_name": dest_nombre, "ground_truth": ground_truth}, ensure_ascii=False)
    with open(METADATA_FILE, "a", encoding="utf-8") as f:
        f.write(linea + "\n")

    print(f"   ✔  Metadata añadida a {METADATA_FILE}")
    return True


# ──────────────────────────────────────────────────────────────────────────────
# FLUJO PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────

def seleccionar_imagen() -> Path | None:
    """Muestra las imágenes disponibles y deja elegir una."""
    imagenes = listar_imagenes()
    if not imagenes:
        print(f"\n  ✖  No hay imágenes en {TEMP_DIR}. Añade facturas y vuelve a ejecutar.")
        return None

    print("\n  Imágenes disponibles en temp_tickets/:\n")
    for idx, img in enumerate(imagenes, 1):
        print(f"   [{idx:2d}]  {img.name}")
    print("   [ 0]  Salir\n")

    while True:
        try:
            opcion = int(input("  Selecciona número de imagen: ").strip())
            if opcion == 0:
                return None
            if 1 <= opcion <= len(imagenes):
                return imagenes[opcion - 1]
        except ValueError:
            pass
        print("   ⚠  Opción no válida.")


def main():
    print("\n" + "╔" + "═" * 58 + "╗")
    print("║        ETIQUETADOR MANUAL DE FACTURAS                   ║")
    print("║   Actualiza BD  +  Genera Dataset Donut                 ║")
    print("╚" + "═" * 58 + "╝")

    # Inicializar BD si aún no existe
    init_db(DB_PATH)

    while True:
        imagen_path = seleccionar_imagen()
        if imagen_path is None:
            print("\n  👋  Hasta luego.\n")
            break

        # ── 1. Calcular hash ──────────────────────────────────────────────────
        print(f"\n  Calculando SHA-256 de '{imagen_path.name}'…")
        hash_img = calcular_sha256(imagen_path)

        # ── 2. Comprobar duplicado en BD ──────────────────────────────────────
        ya_en_db = existe_hash_imagen(hash_img, DB_PATH)
        if ya_en_db:
            print(f"\n  ⚠  Esta imagen YA está registrada en la base de datos.")
            accion = input("  ¿Qué deseas hacer? [O]verwrite / [A]ñadir solo dataset / [S]altar: ").strip().upper()

            if accion == "S":
                print("  ⏭  Saltando imagen.\n")
                continue
            elif accion == "A":
                # Solo añadir al dataset, sin tocar la BD
                datos = pedir_datos_factura(imagen_path, hash_img)
                guardado = guardar_en_dataset(imagen_path, datos)
                if guardado:
                    print("\n  ✅  Añadido al dataset Donut (BD no modificada).\n")
                continue
            elif accion == "O":
                print("  ⚠  Se insertará una nueva entrada en BD (el hash duplicate puede causar error UNIQUE).")
                print("     Si la tabla tiene restricción UNIQUE en hash_archivo, la inserción fallará y se mostrará el error.")
            else:
                print("  Opción no reconocida. Saltando.\n")
                continue

        # ── 3. Entrada de datos ───────────────────────────────────────────────
        datos = pedir_datos_factura(imagen_path, hash_img)

        # ── 4. Insertar en BD ─────────────────────────────────────────────────
        print("\n  💾  Insertando en base de datos…")
        factura_id = insertar_factura(datos, DB_PATH)

        if factura_id == -1:
            print("  ✖  Error al insertar en BD. Comprueba los logs.")
            continuar_dataset = input("  ¿Guardar en dataset de todas formas? (s/N): ").strip().lower()
            if continuar_dataset != "s":
                continue
        else:
            print(f"  ✔  Factura registrada en BD con ID = {factura_id}")

        # ── 5. Guardar en dataset Donut ───────────────────────────────────────
        print("\n  📦  Guardando en dataset Donut…")
        guardar_en_dataset(imagen_path, datos)

        print("\n  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"  ✅  Proceso completado para '{imagen_path.name}'.")
        print("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

        # ── 6. ¿Continuar con otra imagen? ────────────────────────────────────
        otra = input("  ¿Etiquetar otra factura? (S/n): ").strip().lower()
        if otra == "n":
            print("\n  👋  Hasta luego.\n")
            break


if __name__ == "__main__":
    main()
