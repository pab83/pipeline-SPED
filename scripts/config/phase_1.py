from scripts.config.general import *

# Database config
DB_NAME = "auditdb"
DB_USER = "user"
DB_PASS = "pass"
DB_HOST = "localhost"
DB_PORT = 5432

DUPLICATES_FILE = os.path.join(CSV_DIR, "duplicates.csv")
SUMMARY_FILE = os.path.join(REPORTS_DIR, "phase_1_summary.csv")
# -----------------------------
# Archivo de log
# -----------------------------
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"run_{os.getenv('RUN_ID', 'X')}_phase_1.log")