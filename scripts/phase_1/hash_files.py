import os
import hashlib
import xxhash
import psycopg2
from psycopg2 import OperationalError
from multiprocessing import Pool, cpu_count

from scripts.config.general import LOG_FILE

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

def compute_xxhash64(file_path):
    """Calcula xxhash64 de un archivo dado"""
    try:
        h = xxhash.xxh64()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.intdigest()
    except Exception as e:
        log(f"Error calculando xxhash64 para {file_path}: {e}")
        return None

def compute_sha256(file_path):
    """Calcula sha256 de un archivo dado"""
    try:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        log(f"Error calculando sha256 para {file_path}: {e}")
        return None

def process_xxhash(row):
    file_id, full_path = row
    hash64 = compute_xxhash64(full_path)
    return (hash64, file_id) if hash64 is not None else None

def process_sha256(row):
    file_id, full_path = row
    hash256 = compute_sha256(full_path)
    return (hash256, file_id) if hash256 is not None else None

def main():
    conn = get_db_connection()
    cur = conn.cursor()

    log("Starting xxhash64 calculation for all files without xxhash64...")

    offset = 0
    pool = Pool(processes=cpu_count())

    # 1️⃣ Calcular xxhash64 para todos los archivos que no lo tengan
    while True:
        cur.execute(
            "SELECT id, full_path FROM files WHERE xxhash64 IS NULL ORDER BY id LIMIT %s OFFSET %s",
            (BATCH_SIZE, offset)
        )
        batch = cur.fetchall()
        if not batch:
            break

        results = pool.map(process_xxhash, batch)
        results = [r for r in results if r is not None]

        if results:
            cur.executemany(
                "UPDATE files SET xxhash64=%s WHERE id=%s",
                results
            )
            conn.commit()
            log(f"Updated xxhash64 for batch of {len(results)} files (offset {offset})")

        offset += BATCH_SIZE

    log("xxhash64 calculation completed.")

    # 2️⃣ Encontrar posibles duplicados por xxhash64
    log("Finding possible duplicates using xxhash64...")
    cur.execute(
        """
        SELECT xxhash64, array_agg(id ORDER BY id) AS ids
        FROM files
        WHERE xxhash64 IS NOT NULL
        GROUP BY xxhash64
        HAVING COUNT(*) > 1
        """
    )
    groups = cur.fetchall()
    log(f"Found {len(groups)} xxhash64 groups with potential duplicates.")

    # 3️⃣ Calcular sha256 solo para estos posibles duplicados
    for xxh, ids in groups:
        cur.execute(
            "SELECT id, full_path FROM files WHERE id = ANY(%s)",
            (ids,)
        )
        files_to_hash = cur.fetchall()
        results = pool.map(process_sha256, files_to_hash)
        results = [r for r in results if r is not None]

        if results:
            cur.executemany(
                "UPDATE files SET sha256=%s WHERE id=%s",
                results
            )
            conn.commit()
            log(f"Calculated sha256 for {len(results)} files in xxhash64 group {xxh}")

    pool.close()
    pool.join()
    cur.close()
    conn.close()
    log("SHA256 calculation for possible duplicates completed.")

if __name__ == "__main__":
    main()
