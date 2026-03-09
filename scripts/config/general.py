"""
Configuración General del Proyecto.
Gestiona las rutas del sistema, límites de archivos y políticas de reintentos.
"""
import os

# ================= PROJECT =================
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
"""Ruta absoluta al directorio raíz del proyecto."""

# RESOURCES
RESOURCES_DIR = os.path.join(PROJECT_DIR, "resources")
"""Ruta al directorio de recursos"""

CSV_DIR = os.path.join(RESOURCES_DIR, "csv")
"""Ruta al directorio de CSV"""

LOG_DIR = os.path.join(RESOURCES_DIR, "logs")
"""Ruta al directorio de logs"""

REPORTS_DIR = os.path.join(RESOURCES_DIR, "reports")
"""Ruta al directorio de reportes"""


TMP_DIR = os.path.join(RESOURCES_DIR, "tmp")
"""Ruta al directorio temporal"""

# ENSURE DIRECTORIES EXIST
for d in [CSV_DIR, LOG_DIR, REPORTS_DIR, TMP_DIR]:
    os.makedirs(d, exist_ok=True)

# THRESHOLDS
LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB
"""Umbral para considerar un archivo como 'grande' (por defecto 100MB)."""

# RETRIES
MAX_RETRIES = 5
"""Número máximo de reintentos para operaciones fallidas en la pipeline."""

RETRY_DELAY = 60 
"""Tiempo de espera (en segundos) entre reintentos."""