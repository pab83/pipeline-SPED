import os
import hashlib
import xxhash
import psycopg2
from psycopg2 import OperationalError
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from scripts.config.phase_1 import LOG_FILE


BATCH_SIZE = 1000
MAX_WORKERS = min(cpu_count(), 6)

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

# =========================
# HASH FUNCTIONS
# =========================
def compute_xxhash64(file_path):
    try:
        h = xxhash.xxh64()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.intdigest()
    except Exception as e:
        log(f"xxhash64 error {file_path}: {e}")
        return None

def compute_sha256(file_path):
    try:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
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

    pbar = tqdm(total=total_files, desc="Hashing xxhash64", unit="file")

    while True:
        cur.execute(
            "SELECT id, full_path FROM files WHERE xxhash64 IS NULL AND id > %s ORDER BY id LIMIT %s",
            (last_id, BATCH_SIZE),
        )
        batch = cur.fetchall()
        if not batch:
            break

        last_id = batch[-1][0]

        futures = {pool.submit(process_xxhash, row): row[0] for row in batch}
        results = []
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            processed_count += 1
            pbar.update(1)  

        if results:
            cur.executemany("UPDATE files SET xxhash64=%s WHERE id=%s", results)
            conn.commit()
            log(f"Batch committed ({len(results)} files)")

    pbar.close()
    pool.shutdown(wait=True)
    cur.close()
    conn.close()
    log(f"XXHASH64 hashing completed. Total processed: {processed_count}")

if __name__ == "__main__":
    main()