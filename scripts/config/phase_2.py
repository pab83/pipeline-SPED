from scripts.config.general import *
import os

# =========================
# Phase 2 specific config
# =========================

# Límite de caracteres de texto que guardaremos por documento
TEXT_CHAR_LIMIT = int(os.getenv("TEXT_CHAR_LIMIT", "8000"))

# Umbral de similitud coseno para considerar dos documentos casi duplicados
SEMANTIC_SIM_THRESHOLD = float(os.getenv("SEMANTIC_SIM_THRESHOLD", "0.9"))

# Modelo por defecto para embeddings (SentenceTransformers)
EMBEDDING_MODEL_NAME = os.getenv(
    "EMBEDDING_MODEL_NAME",
    "sentence-transformers/all-MiniLM-L6-v2",
)

# Tamaño aproximado de bucket por tamaño de fichero (en bytes) para limitar comparaciones
SIZE_BUCKET_BYTES = int(os.getenv("SIZE_BUCKET_BYTES", str(10 * 1024 * 1024)))

# -----------------------------
# Archivo de log
# -----------------------------
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"run_{os.getenv('RUN_ID', 'X')}_phase_2.log")

