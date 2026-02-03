import os
import hashlib
import xxhash
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from scripts.config.phase_1 import LOG_FILE

# =========================
# Configuración
# =========================
CHUNK_SIZE = 8192
BATCH_DB_SIZE = 500
LOG_EVERY = 100
MAX_WORKERS = os.cpu_count() or 4

# =========================
# Logging
# =========================
def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# =========================
# DB connection
# =========================
DB_NAME = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb"))
DB_USER = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user"))
DB_PASSWORD = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

# =========================
# Hashing helpers
# =========================
def compute_xxhash(file_id, full_path):
    try:
        h = xxhash.xxh64()
        with open(full_path, "rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return (file_id, h.hexdigest(), None)
    except Exception as e:
        return (file_id, None, str(e))

def compute_sha256(file_id, full_path):
    try:
        h = hashlib.sha256()
        with open(full_path, "rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return (file_id, h.hexdigest(), None)
    except Exception as e:
        return (file_id, None, str(e))

# =========================
# Main
# =========================
def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    cur = conn.cursor()

    # =========================
    # PHASE A — XXHASH ONLY
    # =========================
    cur.execute("""
        SELECT id, full_path
        FROM files
        WHERE hash_pending = TRUE
          AND xxhash64 IS NULL
    """)
    files = cur.fetchall()

    if not files:
        log("No files pending xxhash.")
    else:
        log(f"Calculating xxhash64 for {len(files)} files...")

        batch = []
        processed = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(compute_xxhash, fid, path): fid
                for fid, path in files
            }

            for future in tqdm(as_completed(futures), total=len(files), desc="xxhash"):
                file_id, xxh, error = future.result()
                processed += 1

                if error:
                    log(f"ERROR xxhash file_id={file_id}: {error}")
                    continue

                batch.append((xxh, file_id))

                if len(batch) >= BATCH_DB_SIZE:
                    cur.executemany("""
                        UPDATE files
                        SET xxhash64 = %s
                        WHERE id = %s
                    """, batch)
                    conn.commit()
                    batch.clear()

                if processed % LOG_EVERY == 0:
                    log(f"xxhash {processed}/{len(files)}")

        if batch:
            cur.executemany("""
                UPDATE files
                SET xxhash64 = %s
                WHERE id = %s
            """, batch)
            conn.commit()

    # =========================
    # PHASE B — FIND COLLISIONS
    # =========================
    cur.execute("""
        SELECT xxhash64
        FROM files
        WHERE hash_pending = TRUE
          AND xxhash64 IS NOT NULL
        GROUP BY xxhash64
        HAVING COUNT(*) > 1
    """)
    collisions = {row[0] for row in cur.fetchall()}

    log(f"Detected {len(collisions)} xxhash collisions.")

    # =========================
    # PHASE C — SHA256 ONLY FOR COLLISIONS
    # =========================
    if collisions:
        cur.execute("""
            SELECT id, full_path
            FROM files
            WHERE hash_pending = TRUE
              AND xxhash64 = ANY(%s)
        """, (list(collisions),))
        colliding_files = cur.fetchall()

        log(f"Calculating sha256 for {len(colliding_files)} colliding files...")

        batch = []

        with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
            futures = {
                executor.submit(compute_sha256, fid, path): fid
                for fid, path in colliding_files
            }

            for future in tqdm(as_completed(futures), total=len(colliding_files), desc="sha256"):
                file_id, sha, error = future.result()

                if error:
                    log(f"ERROR sha256 file_id={file_id}: {error}")
                    continue

                batch.append((sha, file_id))

                if len(batch) >= BATCH_DB_SIZE:
                    cur.executemany("""
                        UPDATE files
                        SET sha256 = %s
                        WHERE id = %s
                    """, batch)
                    conn.commit()
                    batch.clear()

        if batch:
            cur.executemany("""
                UPDATE files
                SET sha256 = %s
                WHERE id = %s
            """, batch)
            conn.commit()

    # =========================
    # PHASE D — FINALIZE
    # =========================
    cur.execute("""
        UPDATE files
        SET
            hash_pending = FALSE,
            last_seen = NOW()
        WHERE hash_pending = TRUE
          AND xxhash64 IS NOT NULL
    """)
    conn.commit()

    cur.close()
    conn.close()
    log("Hashing completed (xxhash + sha256 on collision).")

# =========================
if __name__ == "__main__":
    main()
