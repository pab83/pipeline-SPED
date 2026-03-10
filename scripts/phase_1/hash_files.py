import os
import time
import xxhash
import psycopg2
from psycopg2.extras import execute_values
import multiprocessing
from multiprocessing import Queue, Process
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from typing import List, Tuple, Optional, Any
from scripts.config.phase_1 import LOG_FILE

# ------------------------
# CONFIG
# ------------------------
BATCH_SIZE: int = 5000
"""Cantidad de registros por lote para inserciones masivas en BD."""

MAX_WORKERS: int = 8          
"""Número de procesos paralelos (CPU-bound)."""

THREADS_PER_WORKER: int = 8   
"""Hilos por proceso para gestionar I/O de lectura de archivos."""

FILE_QUEUE_SIZE: int = 100000
RESULT_QUEUE_SIZE: int = 100000
CHUNK_SIZE: int = 4 * 1024 * 1024
"""Tamaño del bloque de lectura para el cálculo de hash (4MB)."""

# ------------------------
# UTILS
# ------------------------
def log(msg: str) -> None:
    """Registra eventos en el log de la Fase 1 y en consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """
    Establece conexión con PostgreSQL implementando reintentos con backoff.
    
    Args:
        retries: Número máximo de intentos.
        delay: Tiempo de espera entre fallos.
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
        except Exception as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres.")

def update_with_retries(conn: Any, results: List[Tuple[int, int]], max_retries: int = 5) -> bool:
    """
    Ejecuta el UPDATE masivo de hashes con gestión de errores y rollback.
    
    Args:
        conn: Objeto de conexión psycopg2.
        results: Lista de tuplas (hash_value, file_id).
    """
    retries = 0
    while retries < max_retries:
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    UPDATE files AS f 
                    SET xxhash64 = v.hash 
                    FROM (VALUES %s) AS v(hash, id) 
                    WHERE f.id = v.id
                    """,
                    results
                )
                conn.commit()
                return True
        except Exception as e:
            retries += 1
            log(f"⚠️ DB error (attempt {retries}): {e}")
            conn.rollback()
            time.sleep(2)
    return False

# ------------------------
# HASH FUNCTIONS
# ------------------------
def compute_xxhash64(file_path: str) -> Optional[int]:
    """
    Calcula el hash xxhash64 de un archivo de forma eficiente.
    
    Args:
        file_path: Ruta física del archivo.
    Returns:
        Digest entero del hash o None si el archivo es ilegible.
    """
    try:
        with open(file_path, "rb") as f:
            return xxhash.xxh64(f.read()).intdigest()
    except Exception:
        return None

# ------------------------
# WORKERS
# ------------------------
def thread_worker(file_path: str) -> Optional[int]:
    """Encapsula el cálculo de hash para su uso en pools de hilos."""
    return compute_xxhash64(file_path)

def process_worker(file_queue: Queue, result_queue: Queue) -> None:
    """
    Worker a nivel de proceso que consume rutas y distribuye el trabajo en hilos.
    """
    while True:
        item = file_queue.get()
        if item is None: break
        file_id, file_path = item

        with ThreadPoolExecutor(max_workers=THREADS_PER_WORKER) as executor:
            future = executor.submit(thread_worker, file_path)
            h = future.result()
            if h is not None:
                result_queue.put((h, file_id))

# ------------------------
# CORE: READER / WRITER
# ------------------------
def db_reader(file_queue: Queue) -> None:
    """Lee registros pendientes de la BD y los inyecta en la cola de procesamiento."""
    conn = get_db_connection()
    cur = conn.cursor()
    last_id = 0
    while True:
        cur.execute(
            "SELECT id, full_path FROM files WHERE xxhash64 IS NULL AND id > %s ORDER BY id LIMIT %s",
            (last_id, BATCH_SIZE)
        )
        rows = cur.fetchall()
        if not rows: break
        last_id = rows[-1][0]
        for r in rows:
            file_queue.put(r)
    conn.close()

def db_writer(result_queue: Queue, total_files: int) -> None:
    """Consolidación de resultados en BD con barra de progreso en tiempo real."""
    conn = get_db_connection()
    buffer: List[Tuple[int, int]] = []
    pbar = tqdm(total=total_files, desc="Hashing xxhash64", unit="file")
    while True:
        item = result_queue.get()
        if item is None: break
        buffer.append(item)
        pbar.update(1)
        if len(buffer) >= BATCH_SIZE:
            update_with_retries(conn, buffer)
            buffer.clear()
    if buffer:
        update_with_retries(conn, buffer)
    conn.close()
    pbar.close()

def main() -> None:
    """
    Punto de entrada: Implementa una arquitectura Multi-process Multi-thread.
    
    1. **Reader**: Un proceso lee de la base de datos.
    2. **Workers**: N procesos (CPU) que lanzan hilos (I/O) para calcular hashes.
    3. **Writer**: Un proceso que agrupa resultados y actualiza la base de datos.
    """
    multiprocessing.set_start_method("spawn", force=True)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE xxhash64 IS NULL")
    total_files = cur.fetchone()[0]
    log(f"Total archivos pendientes: {total_files}")

    file_queue: Queue = Queue(FILE_QUEUE_SIZE)
    result_queue: Queue = Queue(RESULT_QUEUE_SIZE)

    writer = Process(target=db_writer, args=(result_queue, total_files))
    writer.start()

    workers = [Process(target=process_worker, args=(file_queue, result_queue)) for _ in range(MAX_WORKERS)]
    for p in workers: p.start()

    db_reader(file_queue)

    for _ in workers: file_queue.put(None)
    for w in workers: w.join()

    result_queue.put(None)
    writer.join()
    conn.close()
    log("XXHASH64 hashing completed")

if __name__ == "__main__":
    main()