import logging
import psycopg2
from typing import Optional
from src.config import config
from src.models import Invoice

logger = logging.getLogger(__name__)

def get_conn():
    return psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS proveedores (
                cif_europeo TEXT PRIMARY KEY,
                nombre      TEXT NOT NULL,
                codigo_cuenta TEXT,
                codigo_postal TEXT,
                provincia     TEXT
            )
        ''')
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
                contrapartida               TEXT DEFAULT '40000000',
                clave_operacion             TEXT DEFAULT '1',
                hash_archivo                TEXT UNIQUE,
                requiere_revision           INTEGER DEFAULT 0,
                FOREIGN KEY (cif_proveedor) REFERENCES proveedores(cif_europeo)
            )
        ''')
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
        conn.commit()
        logger.info("Base de datos inicializada correctamente.")
    finally:
        cur.close()
        conn.close()

def existe_hash_imagen(hash_archivo: str) -> bool:
    if not hash_archivo: return False
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM facturas WHERE hash_archivo = %s", (hash_archivo,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists

def insertar_factura(invoice: Invoice) -> int:
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Upsert proveedor
        cur.execute("SELECT cif_europeo FROM proveedores WHERE cif_europeo = %s", (invoice.cif_proveedor,))
        if not cur.fetchone():
            cur.execute("INSERT INTO proveedores (cif_europeo, nombre) VALUES (%s, %s)", 
                       (invoice.cif_proveedor, invoice.proveedor_nombre))
        
        # Insertar factura
        cur.execute('''
            INSERT INTO facturas (
                cif_proveedor, numero_registro, serie, su_factura,
                fecha_expedicion, fecha_operacion, importe_total,
                comentario_sii, hash_archivo, requiere_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            invoice.cif_proveedor, invoice.numero_registro, invoice.serie, invoice.su_factura,
            invoice.fecha_expedicion, invoice.fecha_operacion, invoice.importe_total,
            invoice.comentario_sii, invoice.hash_archivo, invoice.requiere_revision
        ))
        factura_id = cur.fetchone()[0]

        # Insertar impuestos
        for imp in invoice.impuestos:
            cur.execute('''
                INSERT INTO factura_impuestos (
                    factura_id, base_imponible, porcentaje_iva, cuota_iva
                ) VALUES (%s, %s, %s, %s)
            ''', (factura_id, imp.base_imponible, imp.porcentaje_iva, imp.cuota_iva))
        
        conn.commit()
        return factura_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Error insertando factura: {e}")
        return -1
    finally:
        cur.close()
        conn.close()
