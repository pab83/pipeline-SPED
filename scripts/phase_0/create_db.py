import os
import psycopg2
from scripts.config.phase_0 import LOG_FILE

DB_NAME = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB"))
DB_USER = os.getenv("PGUSER", os.getenv("POSTGRES_USER"))
DB_PASSWORD = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD"))
DB_HOST = os.getenv("PGHOST")
DB_PORT = int(os.getenv("PGPORT"))
    

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def create_db():
    """
    Crea la estructura inicial de la base de datos y las tablas necesarias.
    
    Este script inicializa la tabla `files` en PostgreSQL con todas las 
    columnas técnicas requeridas para el seguimiento de archivos.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    
    cur = conn.cursor()
    cur.execute(        ## Ahora crea todas las columnas de la tabla files desde el principio para poder utilizar data_publisher desde phase_0. Sigue habiendo check de columnas en lso scripts que necesitan.
        """
        CREATE TABLE IF NOT EXISTS files (  
            id SERIAL PRIMARY KEY,
            full_path TEXT UNIQUE NOT NULL,
            file_name TEXT,
            file_type TEXT,
            size_bytes BIGINT,
            creation_year INT,
            modification_year INT,
            depth INT,
            is_pdf BOOLEAN,
            ocr_needed BOOLEAN,
            hash_pending BOOLEAN DEFAULT TRUE,
            xxhash64 TEXT,
            sha256 TEXT,
            first_seen TIMESTAMP DEFAULT NOW(),
            last_seen TIMESTAMP DEFAULT NOW(),
            text_excerpt TEXT,
            text_chars_extracted INT,
            is_canonical BOOLEAN DEFAULT FALSE,
            canonical_id INT REFERENCES files(id);
            categoria TEXT;
            last_classified TIMESTAMP;
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()

    log("Database schema ready")
    
if __name__ == "__main__":
    create_db()