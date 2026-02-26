from scripts.config.general import *
import os

# Directory to scan
# - En Windows (sin Docker) usará por defecto Z:\2012
# - En Docker usará la variable de entorno BASE_PATH (por defecto /data)
BASE_PATH = os.getenv("BASE_PATH")#, os.path.join("Z:\\\\2012"))

# CSV outputs
CSV_FILE = os.path.join(CSV_DIR, "file_audit_2.csv")
CSV_OCR_FILE = os.path.join(CSV_DIR, "file_audit_ocr_2.csv")
AUDIT_SUMMARY_FILE = os.path.join(REPORTS_DIR, "audit_summary_2.csv")

# -----------------------------
# Archivo de log
# -----------------------------
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"run_{os.getenv('RUN_ID', 'X')}_phase_0.log")


# Scan config
MAX_THREADS = 2
BUFFER_SIZE = 10_000
BATCH_SIZE = 1_000
