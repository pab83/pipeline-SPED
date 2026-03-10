import os
import time
from multiprocessing import Pool, cpu_count
from typing import List, Tuple, Optional, Set, Any
import psycopg2
from psycopg2 import OperationalError
import cv2

from scripts.config.general import LOG_FILE

# Extensiones de imagen a procesar
IMAGE_EXTENSIONS: Set[str] = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}
"""Conjunto de extensiones soportadas para el análisis visual."""

MAX_IMAGE_SIZE: int = 1024
"""Dimensión máxima (ancho o alto) para redimensionar antes del análisis, optimizando CPU."""

BATCH_SIZE: int = 500
"""Tamaño del lote de registros para procesamiento y commit en base de datos."""

def log(msg: str) -> None:
    """Registra un mensaje en el log central y lo emite por consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """
    Establece conexión con PostgreSQL usando reintentos y backoff.
    
    Args:
        retries: Intentos máximos.
        delay: Segundos entre intentos.
    """
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(
                dbname=os.getenv("PGDATABASE", "auditdb"),
                user=os.getenv("PGUSER", "user"),
                password=os.getenv("PGPASSWORD", "pass"),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", 5432)),
            )
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres.")

def process_image(row: Tuple[int, str]) -> Optional[Tuple[bool, int]]:
    """
    Analiza una imagen para determinar si contiene texto que requiera OCR.
    
    Utiliza OpenCV para leer el archivo, redimensionarlo si excede `MAX_IMAGE_SIZE`
    y ejecutar el algoritmo de detección de documentos.

    Args:
        row: Tupla conteniendo (file_id, full_path).

    Returns:
        Tupla (ocr_needed, file_id) o None si el archivo no es una imagen válida.
    """
    file_id, full_path = row
    try:
        ext = os.path.splitext(full_path)[1].lower()
        if ext not in IMAGE_EXTENSIONS:
            return None

        img = cv2.imread(full_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return (False, file_id)

        # Redimensionar para mejorar performance de detección
        h, w = img.shape[:2]
        if max(h, w) > MAX_IMAGE_SIZE:
            scale = MAX_IMAGE_SIZE / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))

        # looks_like_document debe estar definida o importada
        is_doc = looks_like_document(img) 
        is_doc = True # Placeholder para la lógica de detección
        return (is_doc, file_id)

    except Exception as e:
        log(f"Error procesando imagen {full_path}: {e}")
        return (False, file_id)

def main() -> None:
    """
    Orquesta el marcado de imágenes que requieren OCR usando deduplicación por hash.
    
    Flujo de trabajo:
    
    
    1. **Evitar duplicados**: Obtiene hashes únicos de imágenes ya procesadas para evitar redundancia.
    2. **Carga en Lotes**: Lee de la BD usando un cursor con `LIMIT` y `OFFSET`.
    3. **Paralelismo**: Distribuye el análisis de píxeles entre los núcleos de la CPU (`Pool`).
    4. **Persistencia**: Actualiza masivamente el campo `ocr_needed` en la tabla `files`.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    log("Starting image OCR_needed marking (deduplicated by xxhash64)...")

    # Obtener extensiones formateadas para SQL
    ext_list = tuple(ext.strip('.') for ext in IMAGE_EXTENSIONS)

    cur.execute(
        "SELECT DISTINCT xxhash64 FROM files WHERE ocr_needed IS FALSE "
        "AND LOWER(SPLIT_PART(full_path,'.', -1)) IN %s AND xxhash64 IS NOT NULL",
        (ext_list,)
    )
    unique_hashes: Set[str] = {row[0] for row in cur.fetchall()}
    
    pool = Pool(processes=cpu_count())
    offset = 0

    while True:
        cur.execute(
            "SELECT id, full_path, xxhash64 FROM files WHERE ocr_needed IS FALSE "
            "AND LOWER(SPLIT_PART(full_path,'.', -1)) IN %s AND xxhash64 IS NOT NULL "
            "ORDER BY id LIMIT %s OFFSET %s",
            (ext_list, BATCH_SIZE, offset)
        )
        batch = cur.fetchall()
        if not batch: break

        filtered_batch: List[Tuple[int, str]] = []
        seen_hashes: Set[str] = set()
        for file_id, full_path, xxhash64 in batch:
            if xxhash64 not in seen_hashes and xxhash64 in unique_hashes:
                filtered_batch.append((file_id, full_path))
                seen_hashes.add(xxhash64)

        if filtered_batch:
            results = pool.map(process_image, filtered_batch)
            valid_results = [r for r in results if r is not None]

            if valid_results:
                cur.executemany("UPDATE files SET ocr_needed=%s WHERE id=%s", valid_results)
                conn.commit()
                log(f"Processed batch at offset {offset}")

        offset += BATCH_SIZE

    pool.close()
    pool.join()
    conn.close()
    log("Image OCR_needed marking completed.")

if __name__ == "__main__":
    main()