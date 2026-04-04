import sqlite3
import logging
from typing import Dict, Any

# Configuración básica de logging para registrar errores o eventos
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_db(db_name: str = "facturas.db") -> None:
    """
    Inicializa la base de datos creando la tabla 'facturas' si no existe.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Diseño del Esquema de Datos:
    # - Se ha agregado una clave primaria 'id' autoincremental por adherencia a la 1ª Forma Normal (1FN).
    # - Los caracteres especiales como '%' han sido sustituidos por 'Porcentaje_' y el punto '.' ha sido removido 
    #   para cumplir con las convenciones estándar de nomenclatura SQL y evitar inyecciones o fallos de parseo.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- --- Campos obligatorios/principales ---
            FacturaRegistro TEXT NOT NULL,
            Serie TEXT NOT NULL,
            SuFactura TEXT NOT NULL,
            FechaExpedicion TEXT NOT NULL,
            FechaOperacion TEXT NOT NULL,
            FechaRegistro INTEGER DEFAULT 1,
            CIFEUROPEO TEXT NOT NULL,
            Proveedor TEXT NOT NULL,
            ClaveOperacionFactura INTEGER DEFAULT 1,
            ImporteFactura REAL NOT NULL,
            
            BaseImponible1 REAL NOT NULL,
            PorcentajeIva1 REAL NOT NULL,
            CuotaIva1 REAL NOT NULL,
            
            BaseImponible2 REAL NOT NULL,
            PorcentajeIva2 REAL NOT NULL,
            CuotaIva2 REAL NOT NULL,
            
            BaseImponible3 REAL NOT NULL,
            PorcentajeIva3 REAL NOT NULL,
            CuotaIva3 REAL NOT NULL,

            -- --- Resto de campos (Secundarios/Opcionales) ---
            CodigoCuenta TEXT NULL,
            ComentarioSII TEXT NULL,
            Contrapartida TEXT NULL,
            CodigoTransaccion TEXT NULL,
            
            PorcentajeRecEq1 REAL NULL,
            CuotaRec1 REAL NULL,
            CodigoRetencion TEXT NULL,
            BaseRetencion REAL NULL,
            PorcentajeRetencion REAL NULL,
            CuotaRetenc REAL NULL,
            PorcentajeRecEq2 REAL NULL,
            CuotaRec2 REAL NULL,
            PorcentajeRecEq3 REAL NULL,
            CuotaRec3 REAL NULL,
            
            TipoRectificativa TEXT NULL,
            ClaseAbonoRectificativas TEXT NULL,
            EjercicioFacturaRectificada TEXT NULL,
            SerieFacturaRectificada TEXT NULL,
            NumeroFacturaRectificada TEXT NULL,
            FechaFacturaRectificada TEXT NULL,
            BaseImponibleRectificada REAL NULL,
            CuotaIvaRectificada REAL NULL,
            RecargoEquiRectificada REAL NULL,
            
            NumeroFacturaInicial TEXT NULL,
            NumeroFacturaFinal TEXT NULL,
            IdFacturaExterno TEXT NULL,
            CodigoPostal TEXT NULL,
            CodProvincia TEXT NULL,
            Provincia TEXT NULL,
            CodigoCanal TEXT NULL,
            CodigoDelegacion TEXT NULL,
            CodDepartamento TEXT NULL
        )
    ''')
    
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
    logging.info(f"Base de datos '{db_name}' inicializada con éxito.")

def insertar_factura(datos_dict: Dict[str, Any], db_name: str = "facturas.db") -> int:
    """
    Inserta una factura en la base de datos a partir de un diccionario mapeado.
    Retorna el ID de la fila insertada o -1 en caso de fallo.
    """
    if not datos_dict:
        logging.warning("El diccionario de datos está vacío. No se realizó el INSERT.")
        return -1

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Construcción dinámica de la query basada en las claves del diccionario.
    # Esto da versatilidad: si un campo opcional no viene en el dictionary, 
    # SQLite simplemente utilizará NULL o su valor DEFAULT (ej: FechaRegistro).
    columnas = ', '.join(datos_dict.keys())
    placeholders = ', '.join(['?'] * len(datos_dict))
    
    query = f"INSERT INTO facturas ({columnas}) VALUES ({placeholders})"
    
    try:
        cursor.execute(query, tuple(datos_dict.values()))
        conn.commit()
        last_id = cursor.lastrowid
        logging.info(f"Factura insertada exitosamente (ID: {last_id}).")
        return last_id
    except sqlite3.Error as e:
        logging.error(f"Error en BD durante inserción de factura: {e}")
        conn.rollback()
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
    # Inicialización local para probar la creación de la tabla.
    init_db()
