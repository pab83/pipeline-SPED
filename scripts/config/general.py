import os

# ================= PROJECT =================
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# RESOURCES
RESOURCES_DIR = os.path.join(PROJECT_DIR, "resources")
CSV_DIR = os.path.join(RESOURCES_DIR, "csv")
LOG_DIR = os.path.join(RESOURCES_DIR, "logs")
REPORTS_DIR = os.path.join(RESOURCES_DIR, "reports")
TMP_DIR = os.path.join(RESOURCES_DIR, "tmp")

# LOGGING
LOG_FILE = os.path.join(LOG_DIR, "run.log")

# THRESHOLDS
LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB

# ENSURE DIRECTORIES EXIST
for d in [CSV_DIR, LOG_DIR, REPORTS_DIR, TMP_DIR]:
    os.makedirs(d, exist_ok=True)
