from scripts.config.general import *

# Directory to scan
BASE_PATH = os.path.join("Z:\\2012")

# CSV outputs
CSV_FILE = os.path.join(CSV_DIR, "file_audit.csv")
CSV_OCR_FILE = os.path.join(CSV_DIR, "file_audit_ocr.csv")
AUDIT_SUMMARY_FILE = os.path.join(REPORTS_DIR, "audit_summary.csv")

# Scan config
MAX_THREADS = 2
BUFFER_SIZE = 10_000
BATCH_SIZE = 1_000
