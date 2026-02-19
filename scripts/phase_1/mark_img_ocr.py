import os
from multiprocessing import Pool, cpu_count
import psycopg2
from psycopg2 import OperationalError
import cv2

from scripts.config.general import LOG_FILE

# Extensiones de imagen a procesar
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}

# Tamaño máximo para redimensionar imágenes grandes
MAX_IMAGE_SIZE = 1024

# Batch size
BATCH_SIZE = 500


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def get_db_connection(retries: int = 10, delay: int = 3):
    import time
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", "auditdb"),
                user=os.getenv("PGUSER", "user"),
                password=os.getenv("PGPASSWORD", "pass"),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", 5432)),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after multiple attempts.")


def process_image(row):
    """
    Procesa la imagen y devuelve (ocr_needed, file_id).
    """
    file_id, full_path = row
    try:
        ext = os.path.splitext(full_path)[1].lower()
        if ext not in IMAGE_EXTENSIONS:
            return None  # Ignorar no-imagenes

        img = cv2.imread(full_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            log(f"No se pudo leer la imagen: {full_path}")
            return (False, file_id)

        # Redimensionar si es muy grande
        h, w = img.shape[:2]
        if max(h, w) > MAX_IMAGE_SIZE:
            scale = MAX_IMAGE_SIZE / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))

        # Determina si parece un documento
        is_doc = looks_like_document(img)
        return (is_doc, file_id)

    except Exception as e:
        log(f"Error procesando imagen {full_path}: {e}")
        return (False, file_id)


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    # Mostrar info DB
    try:
        cur.execute("SELECT inet_server_addr(), inet_server_port()")
        addr, port = cur.fetchone()
        log(f"Connected to DB at {addr}:{port}")
    except Exception as e:
        log(f"Could not determine DB server address: {e}")

    log("Starting image OCR_needed marking (deduplicated by xxhash64)...")

    # Obtenemos hashes únicos de imágenes que NO necesitan OCR
    cur.execute(
        f"""
        SELECT DISTINCT xxhash64
        FROM files
        WHERE ocr_needed IS FALSE
          AND LOWER(SPLIT_PART(full_path,'.', -1)) IN %s
          AND xxhash64 IS NOT NULL
        """,
        (tuple(ext.strip('.') for ext in IMAGE_EXTENSIONS),),
    )
    unique_hashes = {row[0] for row in cur.fetchall()}
    log(f"Unique xxhash64 to process: {len(unique_hashes)}")

    offset = 0
    pool = Pool(processes=cpu_count())

    while True:
        # Seleccionamos solo imágenes con hashes que aún no hemos procesado
        cur.execute(
            f"""
            SELECT id, full_path, xxhash64
            FROM files
            WHERE ocr_needed IS FALSE
              AND LOWER(SPLIT_PART(full_path,'.', -1)) IN %s
              AND xxhash64 IS NOT NULL
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (tuple(ext.strip('.') for ext in IMAGE_EXTENSIONS), BATCH_SIZE, offset),
        )

        batch = cur.fetchall()
        if not batch:
            break

        # Filtrar duplicados por xxhash64 antes de procesar
        filtered_batch = []
        seen_hashes = set()
        for file_id, full_path, xxhash64 in batch:
            if xxhash64 not in seen_hashes and xxhash64 in unique_hashes:
                filtered_batch.append((file_id, full_path))
                seen_hashes.add(xxhash64)

        if not filtered_batch:
            offset += BATCH_SIZE
            continue

        # Multiprocesamiento
        results = pool.map(process_image, filtered_batch)
        results = [r for r in results if r is not None]

        if results:
            cur.executemany(
                "UPDATE files SET ocr_needed=%s WHERE id=%s", results
            )
            conn.commit()
            log(f"Processed batch of {len(results)} images (offset {offset})")

        offset += BATCH_SIZE

    pool.close()
    pool.join()
    cur.close()
    conn.close()
    log("Image OCR_needed marking completed.")


if __name__ == "__main__":
    main()
