import os
import logging
import psycopg2
import psycopg2.extras
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# CONEXIÓN
# ──────────────────────────────────────────────────────────────────────────────
def get_conn():
    """Devuelve una conexión nueva a PostgreSQL usando variables de entorno."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "facturas_erp"),
        user=os.getenv("POSTGRES_USER", "erp_user"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


# ──────────────────────────────────────────────────────────────────────────────
# INICIALIZACIÓN DEL ESQUEMA
# ──────────────────────────────────────────────────────────────────────────────
def init_db() -> None:
    """
    Crea las 4 tablas si no existen. Idempotente: se puede llamar en cada arranque.
    """
    conn = get_conn()
    cur = conn.cursor()

    # 1. Proveedores (Maestro)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS proveedores (
            cif_europeo TEXT PRIMARY KEY,
            nombre      TEXT NOT NULL,
            codigo_cuenta TEXT,
            codigo_postal TEXT,
            provincia     TEXT
        )
    ''')

    # 2. Facturas (Cabecera)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id                          SERIAL PRIMARY KEY,
            cif_proveedor               TEXT,
            numero_registro             TEXT NOT NULL,
            serie                       TEXT,
            su_factura                  TEXT,
            fecha_expedicion            DATE,
            fecha_operacion             DATE,
            fecha_registro              TIMESTAMP DEFAULT NOW(),
            importe_total               REAL,
            comentario_sii              TEXT,
            contrapartida               TEXT DEFAULT \'40000000\',
            clave_operacion             TEXT DEFAULT \'1\',
            hash_archivo                TEXT UNIQUE,
            requiere_revision           INTEGER DEFAULT 0,
            codigo_transaccion          TEXT,
            tipo_rectificativa          TEXT,
            clase_abono_rectificativas  TEXT,
            ejercicio_factura_rectificada TEXT,
            serie_factura_rectificada   TEXT,
            numero_factura_rectificada  TEXT,
            fecha_factura_rectificada   TEXT,
            base_imponible_rectificada  REAL,
            cuota_iva_rectificada       REAL,
            recargo_equi_rectificada    REAL,
            numero_factura_inicial      TEXT,
            numero_factura_final        TEXT,
            id_factura_externo          TEXT,
            codigo_canal                TEXT,
            codigo_delegacion           TEXT,
            cod_departamento            TEXT,
            FOREIGN KEY (cif_proveedor) REFERENCES proveedores(cif_europeo)
        )
    ''')

    # 3. Desglose IVA (Detalle)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS factura_impuestos (
            id               SERIAL PRIMARY KEY,
            factura_id       INTEGER NOT NULL,
            base_imponible   REAL,
            porcentaje_iva   REAL,
            cuota_iva        REAL,
            porcentaje_receq REAL DEFAULT 0,
            cuota_receq      REAL DEFAULT 0,
            FOREIGN KEY (factura_id) REFERENCES facturas(id)
        )
    ''')

    # 4. Logs de auditoría
    cur.execute('''
        CREATE TABLE IF NOT EXISTS logs_actividad (
            id_log        SERIAL PRIMARY KEY,
            id_usuario    BIGINT NOT NULL,
            nombre_usuario TEXT,
            fecha_hora    TIMESTAMP DEFAULT NOW(),
            accion        TEXT NOT NULL,
            resultado     TEXT NOT NULL
        )
    ''')

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Esquema PostgreSQL inicializado correctamente.")


# ──────────────────────────────────────────────────────────────────────────────
# COMPROBACIÓN DE DUPLICADOS
# ──────────────────────────────────────────────────────────────────────────────
def existe_hash_imagen(hash_archivo: str) -> bool:
    """Devuelve True si ya existe una factura con ese hash SHA-256."""
    if not hash_archivo:
        return False
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM facturas WHERE hash_archivo = %s", (hash_archivo,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row is not None


# ──────────────────────────────────────────────────────────────────────────────
# INSERCIÓN ATÓMICA
# ──────────────────────────────────────────────────────────────────────────────
def insertar_factura(datos: Dict[str, Any]) -> int:
    """
    Inserta de forma atómica en las 3 tablas usando una transacción.
    Retorna el ID de la factura insertada o -1 en caso de error.
    """
    if not datos:
        logger.warning("Diccionario de datos vacío — INSERT cancelado.")
        return -1

    conn = get_conn()
    cur = conn.cursor()

    try:
        # ── 1. UPSERT de Proveedor ──────────────────────────────────────────
        cif = datos.get('cif_proveedor')
        if not cif:
            cif = 'NO_DETECTADO_' + str(datos.get('numero_registro', '0'))

        proveedor_nombre = datos.get('proveedor_nombre', 'DESCONOCIDO')

        cur.execute("SELECT cif_europeo FROM proveedores WHERE cif_europeo = %s", (cif,))
        if not cur.fetchone():
            cur.execute('''
                INSERT INTO proveedores (cif_europeo, nombre, codigo_postal, provincia)
                VALUES (%s, %s, %s, %s)
            ''', (cif, proveedor_nombre, datos.get('codigo_postal'), datos.get('provincia')))

        # ── 2. Insertar Cabecera de Factura ─────────────────────────────────
        cur.execute('''
            INSERT INTO facturas (
                cif_proveedor, numero_registro, serie, su_factura,
                fecha_expedicion, fecha_operacion, importe_total,
                comentario_sii, hash_archivo, requiere_revision,
                tipo_rectificativa, clase_abono_rectificativas,
                codigo_delegacion, numero_factura_inicial
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            cif,
            datos.get('numero_registro'),
            datos.get('serie'),
            datos.get('su_factura'),
            datos.get('fecha_expedicion'),
            datos.get('fecha_operacion'),
            datos.get('importe_total'),
            datos.get('comentario_sii'),
            datos.get('hash_archivo'),
            datos.get('requiere_revision', 0),
            datos.get('tipo_rectificativa'),
            datos.get('clase_abono_rectificativas'),
            datos.get('codigo_delegacion'),
            datos.get('numero_factura_inicial'),
        ))

        factura_id = cur.fetchone()[0]   # RETURNING id — equivalente a lastrowid

        # ── 3. Insertar Tramos de IVA ────────────────────────────────────────
        for imp in datos.get('impuestos', []):
            if imp.get('base_imponible', 0) > 0 or imp.get('cuota_iva', 0) > 0:
                cur.execute('''
                    INSERT INTO factura_impuestos (
                        factura_id, base_imponible, porcentaje_iva,
                        cuota_iva, porcentaje_receq, cuota_receq
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                ''', (
                    factura_id,
                    imp.get('base_imponible', 0),
                    imp.get('porcentaje_iva', 0),
                    imp.get('cuota_iva', 0),
                    imp.get('porcentaje_receq', 0),
                    imp.get('cuota_receq', 0),
                ))

        conn.commit()
        logger.info(f"Factura insertada en PostgreSQL (ID: {factura_id}).")
        return factura_id

    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error en BD durante inserción transaccional: {e}")
        return -1
    finally:
        cur.close()
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# AUDITORÍA
# ──────────────────────────────────────────────────────────────────────────────
def registrar_evento(id_usuario: int, nombre_usuario: str, accion: str, resultado: str) -> None:
    """Registra un evento de auditoría en logs_actividad."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute('''
            INSERT INTO logs_actividad (id_usuario, nombre_usuario, accion, resultado)
            VALUES (%s, %s, %s, %s)
        ''', (id_usuario, nombre_usuario, accion, resultado))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error registrando evento de log: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    init_db()
