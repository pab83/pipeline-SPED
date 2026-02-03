import os
import psycopg2
from scripts.config.general import LOG_FILE


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)



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


cur.execute(
    """
    SELECT sha256, array_agg(id) AS ids
    FROM files
    WHERE sha256 IS NOT NULL
    GROUP BY sha256, xxhash64
    HAVING COUNT(*) > 1;
    """
)
duplicates = cur.fetchall()

for sha, ids in duplicates:
    log(f"Duplicates detected (SHA-256={sha}): {ids}")

cur.close()
conn.close()