import os
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values

from sentence_transformers import SentenceTransformer

from scripts.config.general import LOG_FILE
from scripts.config.phase_2 import EMBEDDING_MODEL_NAME


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def get_db_connection(retries: int = 10, delay: int = 3):
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


def main(batch_size: int = 256):
    log(f"Loading embedding model: {EMBEDDING_MODEL_NAME}")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    conn = get_db_connection()
    cur = conn.cursor()

    log("Starting embedding computation for Phase 2...")

    # Seleccionamos solo documentos con texto y sin embedding aún.
    # Guardaremos el embedding como vector(384) en Postgres (pgvector).
    cur.execute(
        """
        SELECT f.id, f.text_excerpt
        FROM files f
        LEFT JOIN file_embeddings e ON e.file_id = f.id
        WHERE f.text_excerpt IS NOT NULL
          AND f.text_excerpt <> ''
          AND e.file_id IS NULL
        ORDER BY f.id
        """
    )

    total = 0
    rows = cur.fetchmany(batch_size)

    while rows:
        ids = [row[0] for row in rows]
        texts = [row[1] for row in rows]

        log(f"Encoding batch of {len(texts)} documents...")
        embeddings = model.encode(texts, show_progress_bar=False)

        # Convertimos a lista de Python simple; psycopg2 se encarga de
        # castear a 'vector(384)' gracias a pgvector.
        data = [(fid, emb.tolist()) for fid, emb in zip(ids, embeddings)]

        execute_values(
            cur,
            """
            INSERT INTO file_embeddings (file_id, embedding)
            VALUES %s
            ON CONFLICT (file_id) DO UPDATE
            SET embedding = EXCLUDED.embedding
            """,
            data,
        )
        conn.commit()

        total += len(ids)
        log(f"Inserted/updated embeddings for {total} documents total.")

        rows = cur.fetchmany(batch_size)

    cur.close()
    conn.close()

    log("Embedding computation for Phase 2 completed.")


if __name__ == "__main__":
    main()

