import os
import psycopg2
from psycopg2 import OperationalError
from scripts.config.general import LOG_FILE


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def get_db_connection(retries: int = 10, delay: int = 3):
    """ Intenta establecer una conexión a la base de datos con retries y backoff exponencial.
    Esto es útil para manejar situaciones donde la base de datos aún no está lista o hay problemas temporales de conexión."""
    import time

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb")),
                user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user")),
                password=os.getenv(
                    "PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass")
                ),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after multiple attempts.")


def run_migrations():
    """Ejecuta las migraciones necesarias para la Fase 2, incluyendo la adición de nuevas columnas a la tabla files y la creación de la tabla file_embeddings para almacenar embeddings usando pgvector. También se asegura de que la extensión pgvector esté activada en la base de datos. Las migraciones se ejecutan dentro de una transacción y se registran en el log."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT inet_server_addr(), inet_server_port()")
        addr, port = cur.fetchone()
        log(f"Connected to DB at {addr}:{port}")
    except Exception as e:
        log(f"Could not determine DB server address: {e}")

    log("Running Phase 2 DB migrations...")

    # Nuevas columnas en files para Phase 2 (texto y canonización)
    cur.execute(
        """
        ALTER TABLE files
        ADD COLUMN IF NOT EXISTS text_excerpt TEXT,
        ADD COLUMN IF NOT EXISTS text_chars_extracted INT,
        ADD COLUMN IF NOT EXISTS is_canonical BOOLEAN,
        ADD COLUMN IF NOT EXISTS canonical_id INT REFERENCES files(id);
        """
    )

    # Activar extensión pgvector (si no existe) para almacenar embeddings como vector(n)
    # IMPORTANTE: requiere Postgres >= 14 y la extensión instalada en el servidor.
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    # Tabla para embeddings usando pgvector.
    # Usamos dimensión 384, que es la que devuelve típicamente all-MiniLM-L6-v2.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS file_embeddings (
            file_id INT PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
            embedding vector(384) NOT NULL
        );
        """
    )

    # Índice aproximado para búsquedas KNN por similitud (IVFFlat).
    # Nota: requiere ANALYZE y ciertos parámetros para rendir bien, pero esto deja
    # la estructura base creada.
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

