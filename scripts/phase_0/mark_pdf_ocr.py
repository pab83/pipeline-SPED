import os
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader
from tqdm import tqdm
from scripts.config.phase_0 import LOG_FILE


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def pdf_needs_ocr(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            resources = page.get("/Resources")
            if resources and "/Font" in resources:
                return False
        return True
    except Exception:
        return True


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

cur.execute("SELECT id, full_path FROM files WHERE is_pdf = TRUE;")
pdf_rows = cur.fetchall()

log(f"PDFs detected: {len(pdf_rows)}")

updates = []

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(pdf_needs_ocr, path): file_id for file_id, path in pdf_rows}

    for future in tqdm(as_completed(futures), total=len(futures), desc="Marking PDFs"):
        file_id = futures[future]
        needs_ocr = future.result()
        updates.append((needs_ocr, file_id))

for ocr_needed, file_id in updates:
    cur.execute(
        "UPDATE files SET ocr_needed = %s WHERE id = %s;",
        (ocr_needed, file_id),
    )

conn.commit()
cur.close()
conn.close()

log("OCR flags updated in database")