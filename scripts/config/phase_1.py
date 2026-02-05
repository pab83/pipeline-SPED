from scripts.config.general import *

# Database config
DB_NAME = "auditdb"
DB_USER = "user"
DB_PASS = "pass"
DB_HOST = "localhost"
DB_PORT = 5432

DUPLICATES_FILE = os.path.join(CSV_DIR, "duplicates_summary.csv")
SUMMARY_FILE = os.path.join(REPORTS_DIR, "phase_1_summary.csv")