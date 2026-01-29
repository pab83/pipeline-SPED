import psycopg2
from scripts.config import CSV_OCR_FILE, LOG_FILE

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

conn = psycopg2.connect(dbname="auditdb", user="user", password="pass")
cur = conn.cursor()

# Create table if not exists
cur.execute("""
CREATE TABLE IF NOT EXISTS files (
    file_id SERIAL PRIMARY KEY,
    full_path TEXT UNIQUE NOT NULL,
    file_name TEXT,
    file_type TEXT,
    size_bytes BIGINT,
    creation_year INT,
    modification_year INT,
    depth INT,
    pdf_flag BOOLEAN,
    ocr_needed BOOLEAN,
    text_extracted BOOLEAN DEFAULT FALSE,
    xxhash TEXT,
    sha256 TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
""")
conn.commit()

import csv
with open(CSV_OCR_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute("""
            INSERT INTO files (full_path, file_name, file_type, size_bytes,
                               creation_year, modification_year, depth,
                               pdf_flag, ocr_needed)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (full_path) DO UPDATE
            SET pdf_flag = EXCLUDED.pdf_flag,
                ocr_needed = EXCLUDED.ocr_needed,
                updated_at = NOW();
        """, (
            row["full_path"], row["file_name"], row["file_type"],
            int(row["size_bytes"]), int(row["creation_year"]),
            int(row["modification_year"]), int(row["depth"]),
            row["pdf_flag"].lower() == "true",
            row["ocr_needed"] == "True"
        ))

conn.commit()
cur.close()
conn.close()
log("Database populated with metadata and OCR flags")
