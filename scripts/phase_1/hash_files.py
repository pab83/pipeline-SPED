import os
import hashlib
import xxhash
import psycopg2
from psycopg2 import OperationalError
from multiprocessing import Pool, cpu_count
from scripts.config.general import LOG_FILE

BATCH_SIZE = 1000
MAX_WORKERS = min(cpu_count(), 8)

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3):
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

def process_xxhash(row):
    file_id, full_path = row
    result = compute_xxhash64(full_path)
    return (result, file_id) if result is not None else None

def process_sha256(row):
    file_id, full_path = row
    result = compute_sha256(full_path)
    return (result, file_id) if result is not None else None

# =========================
# MAIN
# =========================

def main():
    conn = get_db_connection()
    cur = conn.cursor()
    pool = Pool(processes=MAX_WORKERS)

    # ======================================
    # XXHASH64 CALCULATION
    # ======================================
    log("Starting xxhash64 calculation...")

    last_id = 0

    while True:
        cur.execute(
            """
            SELECT id, full_path
            FROM files
            WHERE xxhash64 IS NULL AND id > %s
            ORDER BY id
            LIMIT %s
            """,
            (last_id, BATCH_SIZE),
        )

        batch = cur.fetchall()
        if not batch:
            break

        last_id = batch[-1][0]

        results = pool.map(process_xxhash, batch)
        results = [r for r in results if r]

        if results:
            cur.executemany(
                "UPDATE files SET xxhash64=%s WHERE id=%s",
                results
            )
            conn.commit()
            log(f"Updated xxhash64 for {len(results)} files (last_id={last_id})")

    log("xxhash64 phase completed.")

    # ======================================
    # DETEC XXHASH64 DUPLICATES
    # ======================================
    log("Scanning for duplicate xxhash64 groups...")

    cur.execute(
        """
        SELECT id, xxhash64
        FROM files
        WHERE xxhash64 IS NOT NULL
        ORDER BY xxhash64, id
        """
    )

    current_hash = None
    group_ids = []

    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for file_id, xxh in rows:
            if xxh != current_hash:
                # Procesar grupo anterior
                if current_hash is not None and len(group_ids) > 1:
                    process_duplicate_group(cur, pool, group_ids)

                current_hash = xxh
                group_ids = [file_id]
            else:
                group_ids.append(file_id)

    # Último grupo
    if current_hash is not None and len(group_ids) > 1:
        process_duplicate_group(cur, pool, group_ids)

    conn.commit()
    pool.close()
    pool.join()
    cur.close()
    conn.close()

    log("Duplicate verification completed.")

# =========================
# DUPLICATE PROCESSING
# =========================

def process_duplicate_group(cur, pool, ids):
    """
    Calcula sha256 solo para IDs sin sha256.
    Se ejecuta grupo por grupo, evitando arrays gigantes.
    """
    cur.execute(
        """
        SELECT id, full_path
        FROM files
        WHERE id = ANY(%s)
        AND sha256 IS NULL
        """,
        (ids,),
    )

    files = cur.fetchall()
    if not files:
        return

    results = pool.map(process_sha256, files)
    results = [r for r in results if r]

    if results:
        cur.executemany(
            "UPDATE files SET sha256=%s WHERE id=%s",
            results
        )

if __name__ == "__main__":
    main()