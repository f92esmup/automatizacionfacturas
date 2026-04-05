import sqlite3
import logging
from typing import Dict, Any

# Configuración básica de logging para registrar errores o eventos
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_db(db_name: str = "facturas.db") -> None:
    """
    Inicializa la base de datos creando la estructura normalizada en 3 tablas.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 1. Tabla de Proveedores (Maestro)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proveedores (
            cif_europeo TEXT PRIMARY KEY,
            nombre TEXT NOT NULL,
            codigo_cuenta TEXT,
            codigo_postal TEXT,
            provincia TEXT
        )
    ''')

    # 2. Tabla de Facturas (Cabecera)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cif_proveedor TEXT,
            numero_registro TEXT NOT NULL,
            serie TEXT,
            su_factura TEXT,
            fecha_expedicion DATE,
            fecha_operacion DATE,
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
            importe_total REAL,
            comentario_sii TEXT,
            contrapartida TEXT DEFAULT '40000000',
            clave_operacion TEXT DEFAULT '1',
            hash_archivo TEXT UNIQUE,
            requiere_revision INTEGER DEFAULT 0,
            
            -- Otros campos requeridos por el mapping final u opcionales
            codigo_transaccion TEXT,
            tipo_rectificativa TEXT,
            clase_abono_rectificativas TEXT,
            ejercicio_factura_rectificada TEXT,
            serie_factura_rectificada TEXT,
            numero_factura_rectificada TEXT,
            fecha_factura_rectificada TEXT,
            base_imponible_rectificada REAL,
            cuota_iva_rectificada REAL,
            recargo_equi_rectificada REAL,
            numero_factura_inicial TEXT,
            numero_factura_final TEXT,
            id_factura_externo TEXT,
            codigo_canal TEXT,
            codigo_delegacion TEXT,
            cod_departamento TEXT,
            
            FOREIGN KEY (cif_proveedor) REFERENCES proveedores(cif_europeo)
        )
    ''')

    # 3. Tabla de Desglose IVA (Detalle)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS factura_impuestos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER NOT NULL,
            base_imponible REAL,
            porcentaje_iva REAL,
            cuota_iva REAL,
            porcentaje_receq REAL DEFAULT 0,
            cuota_receq REAL DEFAULT 0,
            FOREIGN KEY (factura_id) REFERENCES facturas(id)
        )
    ''')
    
    # 4. Tabla de logs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs_actividad (
            id_log INTEGER PRIMARY KEY AUTOINCREMENT,
            id_usuario INTEGER NOT NULL,
            nombre_usuario TEXT,
            fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            accion TEXT NOT NULL,
            resultado TEXT NOT NULL
        )
    ''')

    conn.commit()
    conn.close()
    logging.info(f"Base de datos '{db_name}' inicializada con estructura normalizada.")

def existe_hash_imagen(hash_archivo: str, db_name: str = "facturas.db") -> bool:
    """
    Verifica si una imagen ya fue procesada previamente comprobando su hash.
    """
    if not hash_archivo:
        return False
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM facturas WHERE hash_archivo = ?", (hash_archivo,))
    row = cursor.fetchone()
    conn.close()
    return row is not None

def insertar_factura(datos: Dict[str, Any], db_name: str = "facturas.db") -> int:
    """
    Inserta de forma atómica en las 3 tablas usando transacciones.
    Retorna el ID de la factura insertada o -1 en caso de error.
    """
    if not datos:
        logging.warning("El diccionario de datos está vacío. No se realizó el INSERT.")
        return -1

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    try:
        with conn:
            # 1. UPSERT de Proveedor
            cif = datos.get('cif_proveedor')
            # Si no hay CIF detectado, usamos un generico temporal para poder guardar.
            if not cif:
                cif = 'NO_DETECTADO_' + str(datos.get('numero_registro', '0'))
                
            proveedor_nombre = datos.get('proveedor_nombre', 'DESCONOCIDO')
            
            cursor.execute("SELECT cif_europeo FROM proveedores WHERE cif_europeo = ?", (cif,))
            if not cursor.fetchone():
                cursor.execute('''
                    INSERT INTO proveedores (cif_europeo, nombre, codigo_postal, provincia)
                    VALUES (?, ?, ?, ?)
                ''', (cif, proveedor_nombre, datos.get('codigo_postal'), datos.get('provincia')))

            # 2. Insertar Cabecera de Factura
            cursor.execute('''
                INSERT INTO facturas (
                    cif_proveedor, numero_registro, serie, su_factura, 
                    fecha_expedicion, fecha_operacion, importe_total, 
                    comentario_sii, hash_archivo, requiere_revision,
                    tipo_rectificativa, clase_abono_rectificativas,
                    codigo_delegacion, numero_factura_inicial
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                datos.get('numero_factura_inicial')
            ))
            
            factura_id = cursor.lastrowid

            # 3. Insertar Impuestos (Detalles)
            impuestos = datos.get('impuestos', [])
            for imp in impuestos:
                if imp.get('base_imponible', 0) > 0 or imp.get('cuota_iva', 0) > 0:
                    cursor.execute('''
                        INSERT INTO factura_impuestos (
                            factura_id, base_imponible, porcentaje_iva, cuota_iva, porcentaje_receq, cuota_receq
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        factura_id,
                        imp.get('base_imponible', 0),
                        imp.get('porcentaje_iva', 0),
                        imp.get('cuota_iva', 0),
                        imp.get('porcentaje_receq', 0),
                        imp.get('cuota_receq', 0)
                    ))

        # Si salimos del with conn sin excepciones, se hace commit automático
        logging.info(f"Factura insertada exitosamente (ID: {factura_id}).")
        return factura_id

    except sqlite3.Error as e:
        # En caso de error, 'with conn:' ya hizo discard/rollback implícito
        logging.error(f"Error en BD durante inserción transaccional: {e}")
        return -1
    finally:
        conn.close()

def registrar_evento(id_usuario: int, nombre_usuario: str, accion: str, resultado: str, db_name: str = "facturas.db") -> None:
    """
    Registra un evento de auditoría en la tabla logs_actividad.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO logs_actividad (id_usuario, nombre_usuario, accion, resultado)
            VALUES (?, ?, ?, ?)
        ''', (id_usuario, nombre_usuario, accion, resultado))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error registrando evento de log: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    # Inicialización local para probar la creación de las tablas.
    init_db()
