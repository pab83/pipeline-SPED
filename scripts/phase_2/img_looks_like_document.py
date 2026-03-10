import os
import unicodedata
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, List, Optional, Tuple, Set

import cv2
import numpy as np
import psycopg2
from psycopg2 import OperationalError

from scripts.config.general import LOG_FILE

# --- Configuración ---
IMAGE_EXTENSIONS: Set[str] = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}
MAX_IMAGE_SIZE: int = 1024
BATCH_SIZE: int = 500
BASE_PATH: Optional[str] = os.getenv("BASE_PATH", None)

def log(msg: str) -> None:
    """Registra un mensaje en el log central y lo emite por consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """Establece conexión con PostgreSQL con reintentos y backoff."""
    import time
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

def looks_like_document(img: np.ndarray) -> bool:
    """
    Algoritmo heurístico para detectar documentos en imágenes.
    
    Aplica los siguientes filtros de visión artificial:
    1. **Dimensiones**: Filtra miniaturas (< 200px).
    2. **Aspect Ratio**: Verifica que no sea una imagen excesivamente alargada.
    3. **Canny Edge Detection**: Identifica gradientes estructurales.
    4. **Contornos**: Busca formas cuadrangulares que ocupen al menos el 20% del área.
    5. **Análisis de Textura**: Calcula la desviación estándar para descartar ruido visual (fotos).
    """
    h, w = img.shape[:2]
    if h < 200 or w < 200: return False

    ratio = max(h, w) / min(h, w)
    if ratio < 0.5 or ratio > 2.0: return False

    # Detección de bordes y búsqueda de formas
    edges = cv2.Canny(img, 50, 150)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    doc_like = False
    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < 0.2 * h * w: continue
        
        peri = cv2.arcLength(cnt, True)
        approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)
        if len(approx) == 4: # Rectángulo detectado
            doc_like = True
            break

    if not doc_like: return False

    # Análisis de fondo (documentos suelen tener fondos uniformes)
    return np.std(img) <= 60

def process_image(row: Tuple[int, str]) -> Optional[int]:
    """
    Carga y pre-procesa una imagen para su análisis.
    
    Normaliza rutas (NFC), gestiona prefijos de volumen y redimensiona 
    la imagen a `MAX_IMAGE_SIZE` para optimizar el uso de CPU.
    """
    file_id, path_str = row
    try:
        ext = os.path.splitext(path_str)[1].lower()
        if ext not in IMAGE_EXTENSIONS: return None

        path_obj = Path(path_str)
        if not path_obj.is_absolute() and BASE_PATH:
            path_obj = Path(BASE_PATH) / path_obj

        path_obj = Path(unicodedata.normalize("NFC", str(path_obj)))
        if not path_obj.exists(): return None

        img = cv2.imread(str(path_obj), cv2.IMREAD_GRAYSCALE)
        if img is None: return None

        # Redimensionado eficiente
        h, w = img.shape[:2]
        if max(h, w) > MAX_IMAGE_SIZE:
            scale = MAX_IMAGE_SIZE / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))

        return file_id if looks_like_document(img) else None

    except Exception as e:
        log(f"Error en {path_str}: {e}")
        return None

def main() -> None:
    """
    Ejecuta el análisis visual masivo utilizando procesamiento paralelo.
    
    Selecciona imágenes únicas basadas en `xxhash64` para evitar procesar 
    múltiples veces el mismo archivo y actualiza el flag `ocr_needed`.
    """
    log("=== Running img_looks_like_document.py ===")
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT DISTINCT ON (xxhash64) id, full_path
            FROM files
            WHERE ocr_needed = FALSE
              AND LOWER(SUBSTRING(full_path FROM '.+\\.([^.]+)$')) IN ('jpg','jpeg','png','bmp','tif','tiff')
            ORDER BY xxhash64, id
        """)
        rows = cur.fetchall()
        log(f"Procesando {len(rows)} imágenes únicas.")

        total_marked = 0
        with Pool(cpu_count()) as pool:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                results = pool.map(process_image, batch)
                
                ids_to_update = [fid for fid in results if fid is not None]
                if ids_to_update:
                    cur.executemany("UPDATE files SET ocr_needed = TRUE WHERE id = %s", 
                                   [(fid,) for fid in ids_to_update])
                    conn.commit()
                    total_marked += len(ids_to_update)
                log(f"Progreso: {min(i + BATCH_SIZE, len(rows))}/{len(rows)} | Marcados: {total_marked}")

    finally:
        cur.close()
        conn.close()
        log("=== Analysis completed ===")

if __name__ == "__main__":
    main()