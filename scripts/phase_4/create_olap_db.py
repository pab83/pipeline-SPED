import psycopg2
import os

OLAP_DB_HOST = os.getenv("OLAP_DB_HOST")
OLAP_DB_NAME = os.getenv("OLAP_DB_NAME")
OLAP_DB_USER = os.getenv("OLAP_DB_USER")
OLAP_DB_PASS = os.getenv("OLAP_DB_PASS")

def create_db():
    """Crea la base de datos si no existe."""
    conn = psycopg2.connect(
        host=OLAP_DB_HOST,
        user=OLAP_DB_USER,
        password=OLAP_DB_PASS,
        dbname="postgres"
    )

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{OLAP_DB_NAME}'")
    exists = cur.fetchone()

    if not exists:
        cur.execute(f"CREATE DATABASE {OLAP_DB_NAME}")

    cur.close()
    conn.close()
    
def delete_db():
    """ Elimina las tablas existentes si las hubiera """
    conn = psycopg2.connect(
        host=OLAP_DB_HOST,
        user=OLAP_DB_USER,
        password=OLAP_DB_PASS,
        dbname=OLAP_DB_NAME
    )

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
                    DROP TABLE IF EXISTS fact_file_excerpts CASCADE;
                    DROP TABLE IF EXISTS fact_files CASCADE;
                    DROP TABLE IF EXISTS dim_canonical_group;
                    DROP TABLE IF EXISTS dim_classification;
                    DROP TABLE IF EXISTS dim_filetype;
                    DROP TABLE IF EXISTS dim_directory;
                    """)
    
    conn.commit()
    print("Tablas eliminadas correctamente.")
    cur.close()
    conn.close()
    

def create_tables():
    """Crea las tablas de la base de datos si no existen."""
    conn = psycopg2.connect(
        host=OLAP_DB_HOST,
        user=OLAP_DB_USER,
        password=OLAP_DB_PASS,
        dbname=OLAP_DB_NAME
    )
    cur = conn.cursor()

    cur.execute("""
    -- 1. Dimensión de Directorios (Para agrupar por carpetas raíz)
    CREATE TABLE IF NOT EXISTS dim_directory (
        id SERIAL PRIMARY KEY,
        directory_name TEXT NOT NULL,       
        full_path TEXT UNIQUE NOT NULL,     
        parent_id INT REFERENCES dim_directory(id) ON DELETE CASCADE,
        depth INT,                           
        level1 TEXT,                         
        level2 TEXT,                        
        level3 TEXT,                         
        level4 TEXT,                         
        level5 TEXT                          
    );

    -- 2. Dimensión de extensiones
    CREATE TABLE IF NOT EXISTS dim_filetype (
        id SERIAL PRIMARY KEY,
        file_type TEXT UNIQUE
    );

    -- 3. Dimensión de Clasificación LLM
    CREATE TABLE IF NOT EXISTS dim_classification (
        id SERIAL PRIMARY KEY,
        categoria TEXT UNIQUE
    );
    
    -- 4. Dimensión de grupos de duplicados
    CREATE TABLE IF NOT EXISTS dim_canonical_group (
        id SERIAL PRIMARY KEY,
        canonical_id INT,          -- referencia al archivo canónico
        description TEXT           -- opcional: nota o nombre del grupo
    );
    
    -- 5. Tabla de Hechos de Archivos (completa)
    CREATE TABLE IF NOT EXISTS fact_files (
        id BIGSERIAL PRIMARY KEY,
        file_id INT NOT NULL, -- ID original de la OLTP
        name TEXT,
        directory_id INT REFERENCES dim_directory(id),
        filetype_id INT REFERENCES dim_filetype(id),
        classification_id INT REFERENCES dim_classification(id),
        canonical_group_id INT REFERENCES dim_canonical_group(id), 
        
        -- Métricas y atributos
        year_created INT,
        year_modified INT,
        size_bytes BIGINT,
        depth INT,
        is_pdf BOOLEAN,
        ocr_needed BOOLEAN,  
        xxhash64 TEXT,                     
        sha256 TEXT,                        
        text_chars_extracted INT DEFAULT 0,
        is_canonical BOOLEAN DEFAULT TRUE,
        
        -- Tiempos para análisis temporal
        first_seen TIMESTAMP,
        last_seen TIMESTAMP,
        last_classified TIMESTAMP,         -- nuevo
        last_sync_fase4 TIMESTAMP DEFAULT NOW(),
        CONSTRAINT uq_file_id UNIQUE (file_id) 
    );
    
    -- 6. Tabla de Hechos de Texto
    CREATE TABLE IF NOT EXISTS fact_file_excerpts (
        file_id INT PRIMARY KEY REFERENCES fact_files(file_id),
        text_excerpt TEXT,
        CONSTRAINT uq_fact_files_file_id UNIQUE (file_id)
    );

    -- Índices para que los Dashboards de Superset vuelen
    CREATE INDEX IF NOT EXISTS idx_fact_files_dir ON fact_files(directory_id);
    CREATE INDEX IF NOT EXISTS idx_fact_files_type ON fact_files(filetype_id);
    CREATE INDEX IF NOT EXISTS idx_fact_files_class ON fact_files(classification_id);
    CREATE INDEX IF NOT EXISTS idx_fact_files_year ON fact_files(year_created);
    CREATE INDEX IF NOT EXISTS idx_fact_files_canonical_group ON fact_files(canonical_group_id);
    CREATE INDEX IF NOT EXISTS idx_fact_files_hash ON fact_files(xxhash64);
    CREATE INDEX IF NOT EXISTS idx_fact_files_sha256 ON fact_files(sha256);
    """)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":

   ### print("Deleting OLAP database...")
   ### delete_db()
    
    print("Creating OLAP database...")
    create_db()

    print("Creating OLAP tables...")
    create_tables()

    print("Done.")
    
 
###     -- FORZAR RESTRICCIONES DE UNICIDAD (Crucial para ON CONFLICT)
###     ALTER TABLE fact_files DROP CONSTRAINT IF EXISTS uq_fact_files_file_id;
###     ALTER TABLE fact_files ADD CONSTRAINT uq_fact_files_file_id UNIQUE (file_id);

###     ALTER TABLE dim_filetype DROP CONSTRAINT IF EXISTS dim_filetype_file_type_key;
###     ALTER TABLE dim_filetype ADD CONSTRAINT dim_filetype_file_type_key UNIQUE (file_type);

###     ALTER TABLE dim_classification DROP CONSTRAINT IF EXISTS dim_classification_categoria_key;
###     ALTER TABLE dim_classification ADD CONSTRAINT dim_classification_categoria_key UNIQUE (categoria);  