import os
import time
from typing import Any
import psycopg2
from psycopg2 import OperationalError
from scripts.config.phase_2 import LOG_FILE

def log(msg: str) -> None:
    """Registra un mensaje en el log de la Fase 2 y lo muestra por consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """
    Establece conexión con PostgreSQL implementando reintentos con backoff exponencial.
    
    Args:
        retries: Número máximo de intentos de conexión.
        delay: Tiempo de espera base entre intentos.
    """
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb")),
                user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user")),
                password=os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass")),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after multiple attempts.")

def run_migrations() -> None:
    """
    Ejecuta las migraciones de esquema necesarias para soportar la Fase 2.
    
    Esta función prepara la base de datos para el almacenamiento de texto y vectores:
    
    1.  **Esquema de Archivos**: Añade columnas para extractos de texto y lógica de canonización.
    2.  **Soporte Vectorial**: Activa la extensión `pgvector` para búsquedas por similitud.
    3.  **Tabla de Embeddings**: Crea `file_embeddings` con soporte para vectores de 384 dimensiones.
    4.  **Indexación**: Crea un índice `IVFFlat` para optimizar búsquedas KNN (K-Nearest Neighbors).
    
    Note:
        Requiere que el servidor PostgreSQL tenga instalada la extensión `pgvector`.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT inet_server_addr(), inet_server_port()")
        addr, port = cur.fetchone()
        log(f"Connected to DB at {addr}:{port}")
    except Exception as e:
        log(f"Could not determine DB server address: {e}")

    log("Running Phase 2 DB migrations...")

    # 1. Nuevas columnas en la tabla maestra 'files'
    cur.execute(
        """
        ALTER TABLE files
        ADD COLUMN IF NOT EXISTS text_excerpt TEXT,
        ADD COLUMN IF NOT EXISTS text_chars_extracted INT,
        ADD COLUMN IF NOT EXISTS is_canonical BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS canonical_id INT REFERENCES files(id);
        """
    )

    # 2. Extensión pgvector
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    # 3. Tabla para almacenamiento de vectores (all-MiniLM-L6-v2)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS file_embeddings (
            file_id INT PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
            embedding vector(384) NOT NULL
        );
        """
    )

    # 4. Índice para búsquedas de similitud L2 (Distancia Euclidiana)
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM   pg_class c
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE  c.relname = 'file_embeddings_embedding_ivfflat_idx'
                AND    n.nspname = 'public'
            ) THEN
                CREATE INDEX file_embeddings_embedding_ivfflat_idx
                ON file_embeddings
                USING ivfflat (embedding vector_l2_ops)
                WITH (lists = 100);
            END IF;
        END$$;
        """
    )

    conn.commit()
    cur.close()
    conn.close()

    log("Phase 2 DB migrations completed.")

if __name__ == "__main__":
    run_migrations()