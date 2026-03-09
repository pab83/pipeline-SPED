import os
import time
import hashlib
import xxhash
import psycopg2
from psycopg2.extras import execute_values
import multiprocessing
from multiprocessing import Queue, Process
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from scripts.config.phase_1 import LOG_FILE

# ------------------------
# CONFIG
# ------------------------
BATCH_SIZE = 5000
MAX_WORKERS = 8           # Procesos
THREADS_PER_WORKER = 8    # Hilos por proceso
FILE_QUEUE_SIZE = 100000
RESULT_QUEUE_SIZE = 100000
CHUNK_SIZE = 4 * 1024 * 1024

# ------------------------
# UTILS
# ------------------------
def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries=10, delay=3):
    """Intenta establecer una conexión a la base de datos con retries y backoff exponencial.
    Esto es útil para manejar situaciones donde la base de datos aún no está lista o hay problemas temporales de conexión."""
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

def update_with_retries(conn, results, max_retries=5, initial_delay=2):
    """Intenta actualizar la base de datos con los resultados, implementando retries con backoff exponencial en caso de errores."""
    retries = 0
    delay = initial_delay
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
            log(f"⚠️ DB error (attempt {retries}/{max_retries}): {e}")
            try:
                conn.rollback()
            except:
                pass
            if retries == max_retries:
                log("❌ Max retries reached. Aborting batch.")
                return False
            log(f"⏱️ Waiting {delay}s before retry...")
            time.sleep(delay)
            delay *= 2
            if conn.closed != 0:
                try:
                    conn = get_db_connection()
                except:
                    log("🚫 Could not recover DB connection.")
    return False

# ------------------------
# HASH FUNCTIONS
# ------------------------
def compute_xxhash64(file_path):
    """Calcula el hash xxhash64 de un archivo dado su path. Devuelve None si hay un error al leer el archivo."""
    try:
        with open(file_path, "rb") as f:
            return xxhash.xxh64(f.read()).intdigest()
    except Exception:
        return None

# ------------------------
# WORKERS
# ------------------------
def thread_worker(file_path):
    """Worker de hashing que se ejecuta en un hilo. Lee el archivo y calcula su hash xxhash64."""
    h = compute_xxhash64(file_path)
    return h

def process_worker(file_queue, result_queue):
    """Worker de proceso que consume del file_queue, lanza un pool de hilos para calcular hashes y pone los resultados en result_queue."""
    while True:
        item = file_queue.get()
        if item is None:
            break
        file_id, file_path = item

        # Thread pool por proceso
        with ThreadPoolExecutor(max_workers=THREADS_PER_WORKER) as executor:
            future = executor.submit(thread_worker, file_path)
            h = future.result()
            if h is not None:
                result_queue.put((h, file_id))

# ------------------------
# DB READER / WRITER
# ------------------------
def db_reader(file_queue):
    """Lee los archivos pendientes de hash desde la base de datos y los pone en el file_queue para que los workers los procesen."""
    conn = get_db_connection()
    cur = conn.cursor()
    last_id = 0
    while True:
        cur.execute(
            """
            SELECT id, full_path
            FROM files
            WHERE xxhash64 IS NULL
            AND id > %s
            ORDER BY id
            LIMIT %s
            """,
            (last_id, BATCH_SIZE)
        )
        rows = cur.fetchall()
        if not rows:
            break
        last_id = rows[-1][0]
        for r in rows:
            file_queue.put(r)
    conn.close()

def db_writer(result_queue, total_files):
    """Consume del result_queue y actualiza la base de datos con los hashes calculados. Implementa un sistema de batching y retries para manejar errores de conexión o bloqueos en la base de datos."""
    conn = get_db_connection()
    buffer = []
    processed = 0
    pbar = tqdm(total=total_files, desc="Hashing xxhash64", unit="file", mininterval=5)
    while True:
        item = result_queue.get()
        if item is None:
            break
        buffer.append(item)
        processed += 1
        pbar.update(1)
        if len(buffer) >= BATCH_SIZE:
            update_with_retries(conn, buffer)
            buffer.clear()
    if buffer:
        update_with_retries(conn, buffer)
    conn.close()
    pbar.close()

# ------------------------
# MAIN
# ------------------------
def main():
    """
    Orquesta la ejecución del hashing de los archivos.
    Configura el entorno y llama a las funciones principales.
    """
    multiprocessing.set_start_method("spawn", force=True)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE xxhash64 IS NULL")
    total_files = cur.fetchone()[0]
    log(f"Total archivos pendientes: {total_files}")

    file_queue = Queue(FILE_QUEUE_SIZE)
    result_queue = Queue(RESULT_QUEUE_SIZE)

    # Writer
    writer = Process(target=db_writer, args=(result_queue, total_files))
    writer.start()

    # Workers
    workers = []
    for _ in range(MAX_WORKERS):
        p = Process(target=process_worker, args=(file_queue, result_queue))
        p.start()
        workers.append(p)

    # Reader
    db_reader(file_queue)

    # Stop workers
    for _ in workers:
        file_queue.put(None)
    for w in workers:
        w.join()

    # Stop writer
    result_queue.put(None)
    writer.join()
    cur.close()
    conn.close()
    log("XXHASH64 hashing completed")

if __name__ == "__main__":
    main()