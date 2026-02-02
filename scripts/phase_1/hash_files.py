import os
import psycopg2
import xxhash
import hashlib
from tqdm import tqdm
from scripts.config import LOG_FILE


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


# Permite configurar la conexión vía variables de entorno (funciona bien con Docker Compose)
DB_NAME = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb"))
DB_USER = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user"))
DB_PASSWORD = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
)
cur = conn.cursor()

# Esquema nuevo: usamos files.id como PK y actualizamos sha256 / xxhash64
cur.execute("SELECT id, full_path FROM files")
files = cur.fetchall()

for file_id, full_path in tqdm(files, desc="Calculating hashes"):
    try:
        h_xx = xxhash.xxh64()
        h_sha = hashlib.sha256()
        with open(full_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h_xx.update(chunk)
                h_sha.update(chunk)
        cur.execute(
            """
            UPDATE files
            SET
                xxhash64 = %s,
                sha256 = %s,
                hash_pending = FALSE,
                last_seen = NOW()
            WHERE id = %s
            """,
            (h_xx.hexdigest(), h_sha.hexdigest(), file_id),
        )
    except Exception as e:
        log(f"Error hashing {full_path}: {e}")

conn.commit()
cur.close()
conn.close()
log("Hashes calculated and updated in database")

