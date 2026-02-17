#!/usr/bin/env python3
import os
import time
import logging
import psycopg2
from psycopg2 import OperationalError

from schemas.task import TargetModel
from scripts.producer import send_task
from scripts.config.general import LOG_FILE

logging.basicConfig(level=logging.INFO)

DEFAULT_PROMPT = "Describe la imagen en detalle."
BATCH_SIZE = 50


# --------------------------------------------------
# Logging
# --------------------------------------------------
def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


# --------------------------------------------------
# DB connection with retries
# --------------------------------------------------
def get_db_connection(retries: int = 10, delay: int = 3):
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(
                dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb")),
                user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user")),
                password=os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass")),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
        except OperationalError as e:
            log(f"Postgres not ready ({attempt}/{retries}): {e}")
            time.sleep(delay)

    raise RuntimeError("Could not connect to Postgres.")


# --------------------------------------------------
# Create moondream_task_map if not exists
# --------------------------------------------------
def ensure_task_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS moondream_task_map (
            id SERIAL PRIMARY KEY,
            file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            correlation_id UUID NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(file_id)
        );
    """)

    # Índices para rendimiento
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_moondream_file_id
        ON moondream_task_map(file_id);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_moondream_status
        ON moondream_task_map(status);
    """)

    conn.commit()
    cur.close()
    log("Tabla moondream_task_map verificada/creada.")


# --------------------------------------------------
# Fetch images not yet in task_map
def fetch_new_images(conn, batch_size):
    cur = conn.cursor()

    # 1️⃣ Cuántos archivos totales hay
    cur.execute("SELECT COUNT(*) FROM files;")
    total_files = cur.fetchone()[0]

    # 2️⃣ Cuántos parecen imágenes (solo por file_type)
    cur.execute("""
        SELECT COUNT(*)
        FROM files
        WHERE LOWER(file_type) IN ('png','jpg','jpeg','gif','bmp','tiff','webp');
    """)
    total_images = cur.fetchone()[0]

    # 3️⃣ Cuántos ya están en task_map
    cur.execute("SELECT COUNT(*) FROM moondream_task_map;")
    total_tasks = cur.fetchone()[0]

    log(f"[DEBUG] Total files: {total_files}")
    log(f"[DEBUG] Total image-like files: {total_images}")
    log(f"[DEBUG] Total in moondream_task_map: {total_tasks}")

    # 4️⃣ Query real
    cur.execute(f"""
        SELECT f.id, f.full_path, COALESCE(f.file_name,'')
        FROM files f
        LEFT JOIN moondream_task_map m
            ON f.id = m.file_id
        WHERE LOWER(f.file_type) IN ('.png','.jpg','.jpeg','.gif','.bmp','.tiff','.webp')
          AND m.file_id IS NULL
        ORDER BY f.id
        LIMIT {batch_size};
    """)

    rows = cur.fetchall()

    log(f"[DEBUG] Rows returned by query: {len(rows)}")

    if len(rows) > 0:
        log(f"[DEBUG] First returned ID: {rows[0][0]}")

    cur.close()
    return rows



# --------------------------------------------------
# Insert task map entry
# --------------------------------------------------
def insert_task_map(conn, file_id, correlation_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO moondream_task_map (file_id, correlation_id, status)
        VALUES (%s, %s, 'pending')
        ON CONFLICT (file_id) DO NOTHING;
    """, (file_id, correlation_id))
    conn.commit()
    cur.close()


# --------------------------------------------------
# Send batch
# --------------------------------------------------
def send_batch(conn):
    rows = fetch_new_images(conn, BATCH_SIZE)

    if not rows:
        return 0

    sent_count = 0

    for file_id, full_path, file_name in rows:
        try:
            message_id, correlation_id = send_task(
                file_path=full_path,
                target_model=TargetModel.MOONDREAM,
                source="describe_img_pipeline",
                prompt=DEFAULT_PROMPT
            )

            insert_task_map(conn, file_id, correlation_id)

            log(f"Tarea enviada: {file_name or 'sin nombre'} (file_id={file_id})")
            sent_count += 1

        except Exception as e:
            log(f"Error enviando archivo {file_id}: {e}")

    return sent_count


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    log("=" * 60)
    log("Iniciando envío incremental de imágenes a Moondream")
    log("=" * 60)

    conn = get_db_connection()

    ensure_task_table(conn)

    while True:
        sent = send_batch(conn)

        if sent == 0:
            log("No hay más imágenes nuevas para enviar.")
            break

        log(f"Batch enviado: {sent} imágenes")
        time.sleep(1)

    conn.close()
    log("Envío completado.")


if __name__ == "__main__":
    main()
