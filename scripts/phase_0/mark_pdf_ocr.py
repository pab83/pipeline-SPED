import csv
from PyPDF2 import PdfReader
from tqdm import tqdm
from scripts.config.phase_0 import CSV_FILE, CSV_OCR_FILE, LOG_FILE

# ================= UTILITIES =================

def log(msg):
    """Append message to log file and print"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# ================= CORE =================

def pdf_needs_ocr(pdf_path):
    """
    Fast heuristic OCR check.
    Returns True if PDF is likely scanned (needs OCR).
    """
    try:
        reader = PdfReader(pdf_path)

        for page in reader.pages:
            resources = page.get("/Resources")
            if not resources:
                continue

            # Presence of fonts strongly indicates real text
            if "/Font" in resources:
                return False

        return True
    except Exception:
        # If unreadable, assume OCR needed (safe default)
        return True

# ================= PROCESS CSV =================

rows_out = []
with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in tqdm(reader, desc="Marking PDFs (OCR)", unit="files"):
        if row["pdf_flag"].lower() == "true":
            row["ocr_needed"] = pdf_needs_ocr(row["full_path"])
        else:
            row["ocr_needed"] = False
        rows_out.append(row)

with open(CSV_OCR_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows_out[0].keys())
    writer.writeheader()
    writer.writerows(rows_out)

log(f"OCR flags updated in {CSV_OCR_FILE}")
