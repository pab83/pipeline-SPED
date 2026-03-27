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
                canonical_id INT REFERENCES files(id),
                categoria TEXT,
                last_classified TIMESTAMP
            );
        """)

        # 2. Crear el SCHEMA pipeline_status
    log("Creando esquema 'pipeline_status'...")
    cur.execute("CREATE SCHEMA IF NOT EXISTS pipeline_status;")

        # 3. Crear tablas de seguimiento del Pipeline
    log("Creando tablas de control del pipeline...")
    cur.execute("""
            -- Tabla de Ejecuciones (Runs)
            CREATE TABLE IF NOT EXISTS pipeline_status.pipeline_runs (
                run_id BIGSERIAL PRIMARY KEY,
                status VARCHAR(20) DEFAULT 'pending' NOT NULL,
                current_phase INTEGER DEFAULT 0,
                total_files BIGINT DEFAULT 0,
                processed_files BIGINT DEFAULT 0,
                started_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                finished_at TIMESTAMP,
                error_message TEXT
            );

            -- Tabla de Fases
            CREATE TABLE IF NOT EXISTS pipeline_status.pipeline_phases (
                phase_id BIGSERIAL PRIMARY KEY,
                run_id BIGINT REFERENCES pipeline_status.pipeline_runs(run_id) ON DELETE CASCADE,
                phase_number INTEGER NOT NULL,
                status VARCHAR(20) DEFAULT 'pending' NOT NULL,
                processed_files BIGINT DEFAULT 0,
                total_files BIGINT DEFAULT 0,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                error_message TEXT
            );

            -- Tabla de Scripts/Logs
            CREATE TABLE IF NOT EXISTS pipeline_status.pipeline_scripts (
                script_id BIGSERIAL PRIMARY KEY,
                phase_id BIGINT REFERENCES pipeline_status.pipeline_phases(phase_id) ON DELETE CASCADE,
                script_name TEXT NOT NULL,
                status VARCHAR(20) DEFAULT 'pending' NOT NULL,
                logs TEXT,
                processed_files BIGINT DEFAULT 0,
                total_files BIGINT DEFAULT 0,
                error_message TEXT
            );
        """
    )

    conn.commit()
    cur.close()
    conn.close()

    log("Database schema ready")
    
if __name__ == "__main__":
    create_db()