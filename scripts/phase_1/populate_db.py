import os
import psycopg2
from scripts.config.phase_0 import CSV_OCR_FILE
from scripts.config.general import LOG_FILE


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

# Esquema base de files (alineado con lo que usan Phase 1 y Phase 2)
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

import csv


with open(CSV_OCR_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute(
            """
            INSERT INTO files (
                full_path,
                file_name,
                file_type,
                size_bytes,
                creation_year,
                modification_year,
                depth,
                is_pdf,
                ocr_needed
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (full_path) DO UPDATE
            SET
                file_name = EXCLUDED.file_name,
                file_type = EXCLUDED.file_type,
                size_bytes = EXCLUDED.size_bytes,
                creation_year = EXCLUDED.creation_year,
                modification_year = EXCLUDED.modification_year,
                depth = EXCLUDED.depth,
                is_pdf = EXCLUDED.is_pdf,
                ocr_needed = EXCLUDED.ocr_needed,
                last_seen = NOW();
            """,
            (
                row["full_path"],
                row["file_name"],
                row["file_type"],
                int(row["size_bytes"]),
                int(row["creation_year"]),
                int(row["modification_year"]),
                int(row["depth"]),
                row["is_pdf"].lower() == "true",
                str(row["ocr_needed"]).lower() == "true",
            ),
        )

conn.commit()
cur.close()
conn.close()
log("Database populated with metadata and OCR flags")