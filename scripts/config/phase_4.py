from scripts.config.general import *
import os
# -----------------------------
# Archivo de log
# -----------------------------
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"run_{os.getenv('RUN_ID', 'X')}_phase_4.log")