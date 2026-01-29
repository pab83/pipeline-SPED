import psycopg2
import xxhash
import hashlib
from tqdm import tqdm
from scripts.config import LOG_FILE

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

conn = psycopg2.connect(dbname="auditdb", user="user", password="pass")
cur = conn.cursor()

cur.execute("SELECT file_id, full_path FROM files")
files = cur.fetchall()

for file_id, full_path in tqdm(files, desc="Calculating hashes"):
    try:
        h_xx = xxhash.xxh64()
        h_sha = hashlib.sha256()
        with open(full_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h_xx.update(chunk)
                h_sha.update(chunk)
        cur.execute("""
            UPDATE files SET xxhash=%s, sha256=%s, updated_at=NOW() WHERE file_id=%s
        """, (h_xx.hexdigest(), h_sha.hexdigest(), file_id))
    except Exception as e:
        log(f"Error hashing {full_path}: {e}")

conn.commit()
cur.close()
conn.close()
log("Hashes calculated and updated in database")
