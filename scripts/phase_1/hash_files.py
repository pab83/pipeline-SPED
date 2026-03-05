import os
import time
import hashlib
import xxhash
import psycopg2
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from psycopg2.extras import execute_values
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from scripts.config.phase_1 import LOG_FILE


BATCH_SIZE = 5000
MAX_WORKERS = min(cpu_count(), 16)

# ------------------------
# UTILS
# ------------------------
def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries=10, delay=3):
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

def update_with_retries(conn, results, max_retries=5, initial_delay=2):
    """
    Intenta ejecutar el bulk update con reintentos exponenciales.
    Si la conexión se pierde, intenta reestablecerla.
    """
    retries = 0
    delay = initial_delay
    
    while retries < max_retries:
        try:
            # Necesitamos un cursor fresco por si el anterior quedó corrupto
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
                return True # Éxito
        
        except (OperationalError, InterfaceError, DatabaseError) as e:
            retries += 1
            log(f"⚠️ Error de DB (Intento {retries}/{max_retries}): {e}")
            
            try:
                conn.rollback() # Intentar limpiar la transacción fallida
            except:
                pass 

            if retries == max_retries:
                log("❌ Máximos reintentos alcanzados. Abortando lote.")
                return False

            log(f"⏱️ Esperando {delay}s antes de reintentar...")
            time.sleep(delay)
            delay *= 2 # Backoff exponencial (2, 4, 8, 16...)

            # Si la conexión está muerta, intentamos recrearla
            if conn.closed != 0:
                log("🔄 Conexión cerrada. Intentando reconectar...")
                try:
                    conn = get_db_connection()
                except:
                    log("🚫 No se pudo recuperar la conexión.")
    
    return False

# =========================
# HASH FUNCTIONS
# =========================
CHUNK_SIZE = 128 * 1024

def compute_xxhash64(file_path):
    try:
        h = xxhash.xxh64()
        with open(file_path, "rb", buffering=CHUNK_SIZE) as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.intdigest()
    except Exception as e:
        log(f"xxhash64 error {file_path}: {e}")
        return None

def compute_sha256(file_path):
    try:
        h = hashlib.sha256()
        with open(file_path, "rb", buffering=CHUNK_SIZE) as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        log(f"sha256 error {file_path}: {e}")
        return None

# =========================
# WORKERS
# =========================
def process_xxhash(row):
    file_id, full_path = row
    h = compute_xxhash64(full_path)
    return (h, file_id) if h is not None else None

def process_sha256(row):
    file_id, full_path = row
    h = compute_sha256(full_path)
    return (h, file_id) if h is not None else None

# =========================
# MAIN
# =========================
def main():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Contar total archivos pendientes
    cur.execute("SELECT COUNT(*) FROM files WHERE xxhash64 IS NULL;")
    total_files = cur.fetchone()[0]
    log(f"Total archivos pendientes: {total_files}")

    processed_count = 0
    last_id = 0

    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    pbar = tqdm(total=total_files, desc="Hashing xxhash64", unit="file",miniters=500,mininterval=5) #No se imprime mas de 1 cada 5s y cada 500files

    while True:
        cur.execute(
            "SELECT id, full_path FROM files WHERE xxhash64 IS NULL AND id > %s ORDER BY id LIMIT %s",
            (last_id, BATCH_SIZE),
        )
        batch = cur.fetchall()
        if not batch:
            break

        last_id = batch[-1][0]

        # prallelizar hashing 
        futures = {pool.submit(process_xxhash, row): row[0] for row in batch}
        results = []
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            processed_count += 1
            pbar.update(1)  

        if results:
            success = update_with_retries(conn, results)
            if success:
                log(f"✅ Batch actualizado exitosamente. Total procesados: {processed_count}")
            else:
                log(f"🚨 Error crítico: El lote de {len(results)} se perdió.")
                sys.exit(1)
    pbar.close()
    pool.shutdown(wait=True)
    cur.close()
    conn.close()
    log(f"XXHASH64 hashing completed. Total processed: {processed_count}")

if __name__ == "__main__":
    main()