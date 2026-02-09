import os
import cv2
import psycopg2
from psycopg2 import OperationalError
from multiprocessing import Pool, cpu_count
from pathlib import Path
import unicodedata
import numpy as np


from scripts.config.general import LOG_FILE

# Extensiones de imagen a considerar
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}

# Tamaño máximo de imagen para procesar rápido
MAX_IMAGE_SIZE = 1024

BATCH_SIZE = 500  # Batch de DB para commits

# Base path opcional
BASE_PATH = os.getenv("BASE_PATH", None)  # /data o similar


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def get_db_connection(retries=10, delay=3):
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



def looks_like_document(img) -> bool:
    """
    Detecta si la imagen probablemente es un documento.
    img: imagen en escala de grises (cv2.IMREAD_GRAYSCALE)
    Devuelve True si parece un documento.
    """

    # 1️⃣ Tamaño mínimo
    h, w = img.shape[:2]
    if h < 200 or w < 200:
        return False

    # 2️⃣ Proporción
    ratio = max(h, w) / min(h, w)
    if ratio < 0.5 or ratio > 2.0:
        return False

    # 3️⃣ Bordes con Canny
    edges = cv2.Canny(img, 50, 150)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # 4️⃣ Buscar contorno grande y rectangular
    doc_like = False
    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < 0.2 * h * w:  # descarta contornos muy pequeños
            continue
        peri = cv2.arcLength(cnt, True)
        approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)
        if len(approx) == 4:  # tiene 4 vértices → rectángulo
            doc_like = True
            break

    if not doc_like:
        return False

    # 5️⃣ Variación de gris: fondo uniforme → doc
    stddev = np.std(img)
    if stddev > 60:  # muchas texturas → probablemente foto
        return False

    return True


def process_image(row):
    file_id, path_str = row
    try:
        ext = os.path.splitext(path_str)[1].lower()
        if ext not in IMAGE_EXTENSIONS:
            return None

        path_obj = Path(path_str)
        if not path_obj.is_absolute() and BASE_PATH:
            path_obj = Path(BASE_PATH) / path_obj

        path_obj = Path(unicodedata.normalize("NFC", str(path_obj)))

        if not path_obj.exists():
            log(f"No existe la ruta: {path_obj}")
            return None

        img = cv2.imread(str(path_obj), cv2.IMREAD_GRAYSCALE)
        if img is None:
            log(f"No se pudo leer: {path_obj}")
            return None

        h, w = img.shape[:2]
        if max(h, w) > MAX_IMAGE_SIZE:
            scale = MAX_IMAGE_SIZE / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))

        if looks_like_document(img):
            return file_id   # si hay que marcar OCR

        return None

    except Exception as e:
        log(f"Error procesando imagen {path_str}: {e}")
        return None

def main():
    log("=== Running img_looks_like_document.py ===")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT DISTINCT ON (xxhash64)
                   id, full_path
            FROM files
            WHERE ocr_needed = FALSE
              AND LOWER(SUBSTRING(full_path FROM '.+\\.([^.]+)$'))
                  IN ('jpg','jpeg','png','bmp','tif','tiff')
            ORDER BY xxhash64, id
            """
        )

        rows = cur.fetchall()
        log(f"Se encontraron {len(rows)} imágenes únicas para procesar.")

        total_processed = 0
        total_marked = 0

        with Pool(cpu_count()) as pool:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]

                results = pool.map(process_image, batch)

                ids_to_update = [fid for fid in results if fid is not None]

                if ids_to_update:
                    cur.executemany(
                        "UPDATE files SET ocr_needed = TRUE WHERE id = %s",
                        [(fid,) for fid in ids_to_update],
                    )
                    conn.commit()
                    total_marked += len(ids_to_update)

                total_processed += len(batch)
                log(
                    f"Batch procesado: {total_processed}/{len(rows)} | "
                    f"Marcados OCR: {total_marked}"
                )

    except Exception as e:
        log(f"Error en la ejecución principal: {e}")

    finally:
        cur.close()
        conn.close()
        log("=== img_looks_like_document.py completed ===")

if __name__ == "__main__":
    main()



