import csv
import os
from collections import Counter
from scripts.config.phase_0 import CSV_OCR_FILE, LOG_FILE, AUDIT_SUMMARY_FILE
from scripts.config.general import LARGE_FILE_THRESHOLD

# ================= PATH SETUP =================

REPORT_FILE = AUDIT_SUMMARY_FILE
os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)

# ================= UTILITIES =================

def log(msg):
    """Append message to log file and print"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# ================= VARIABLES =================

# Variables para el resumen
total_files = 0
pdf_files = 0
ocr_needed_count = 0
file_types = Counter()
large_files = []
depth_counts = Counter()

# ================= PROCESS CSV =================

with open(CSV_OCR_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        total_files += 1
        file_types[row["file_type"]] += 1
        depth_counts[int(row["depth"])] += 1

        if row["pdf_flag"].lower() == "true":
            pdf_files += 1
            if str(row["ocr_needed"]).lower() == "true":
                ocr_needed_count += 1

        if int(row["size_bytes"]) >= LARGE_FILE_THRESHOLD:
            large_files.append((row["full_path"], int(row["size_bytes"])))

# ================= WRITE SUMMARY =================

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
    writer.writerow(["Large files (over 100MB)"])
    writer.writerow(["Path", "Size (bytes)"])
    for path, size in large_files:
        writer.writerow([path, size])

log(f"Audit summary generated: {REPORT_FILE}")
