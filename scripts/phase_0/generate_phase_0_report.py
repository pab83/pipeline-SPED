import os
import psycopg2
import csv
from collections import Counter
from scripts.config.general import LOG_FILE, LARGE_FILE_THRESHOLD
from scripts.config.phase_0 import AUDIT_SUMMARY_FILE


REPORT_FILE = AUDIT_SUMMARY_FILE
os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)


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

cur.execute("""
    SELECT file_type, depth, is_pdf, ocr_needed, size_bytes, full_path
    FROM files;
""")

rows = cur.fetchall()

total_files = 0
pdf_files = 0
ocr_needed_count = 0
file_types = Counter()
depth_counts = Counter()
large_files = []

for file_type, depth, is_pdf, ocr_needed, size_bytes, full_path in rows:
    total_files += 1
    file_types[file_type] += 1
    depth_counts[depth] += 1

    if is_pdf:
        pdf_files += 1
        if ocr_needed:
            ocr_needed_count += 1

    if size_bytes >= LARGE_FILE_THRESHOLD:
        large_files.append((full_path, size_bytes))

with open(REPORT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Metric", "Value"])
    writer.writerow(["Total files", total_files])
    writer.writerow(["Total PDFs", pdf_files])
    writer.writerow(["PDFs needing OCR", ocr_needed_count])
    writer.writerow([])
    writer.writerow(["File type distribution", "Count"])
    for ft, count in file_types.items():
        writer.writerow([ft, count])
    writer.writerow([])
    writer.writerow(["Files per depth", "Count"])
    for depth, count in sorted(depth_counts.items()):
        writer.writerow([depth, count])
    writer.writerow([])
    writer.writerow(["Large files (over threshold)"])
    writer.writerow(["Path", "Size (bytes)"])
    for path, size in large_files:
        writer.writerow([path, size])

cur.close()
conn.close()

log(f"Audit summary generated: {REPORT_FILE}")
