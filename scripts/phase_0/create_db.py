import os
import psycopg2
from scripts.config.phase_0 import LOG_FILE


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
    CREATE TABLE IF NOT EXISTS files (
        id SERIAL PRIMARY KEY,
        full_path TEXT UNIQUE NOT NULL,
        file_name TEXT,
        file_type TEXT,
        size_bytes BIGINT,
        creation_year INT,
        modification_year INT,
        depth INT,
        is_pdf BOOLEAN,
        ocr_needed BOOLEAN,
        hash_pending BOOLEAN DEFAULT TRUE,
        xxhash64 TEXT,
        sha256 TEXT,
        first_seen TIMESTAMP DEFAULT NOW(),
        last_seen TIMESTAMP DEFAULT NOW()
    );
    """
)

conn.commit()
cur.close()
conn.close()

log("Database schema ready")