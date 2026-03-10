import os
from scripts.config.phase_2 import LOG_FILE
from scripts.helpers.logs import set_log_file
from scripts.helpers.orchestrate import (
    execute_phase_logic,
    get_or_create_phase_id,
    run_script
)

# ----------------------------
# Parámetros de ejecución
# ----------------------------
RUN_ID: int = int(os.getenv("RUN_ID", "0"))
"""ID de ejecución global para el seguimiento de procesos en la Fase 2."""

PHASE_NUMBER: int = 2
"""Identificador de la Fase 2: Migración, Deduplicación y Extracción de Texto."""

SCRIPTS: list[str] = [
    "migrate_phase_2.py",          # Prepara la estructura de tablas para la Fase 2
    "dedup.py",                    # Identifica y marca duplicados por hash
    "extract_text.py",             # Ejecuta OCR y extracción de texto digital
    "img_looks_like_document.py",  # Clasificación visual avanzada de documentos
]
"""Lista secuencial de scripts que componen el flujo de trabajo de la Fase 2."""

def main() -> None:
    """
    Orquesta la ejecución integral de la Fase 2 del pipeline.
    
    Esta fase transforma los metadatos crudos en información accionable mediante:
    
    1.  **Migración**: Actualización del esquema de base de datos para almacenar contenido.
    2.  **Deduplicación**: Filtrado de archivos redundantes basados en firmas digitales.
    3.  **Análisis Visual**: Clasificación de imágenes para optimizar el uso de motores OCR.
    4.  **Extracción**: Procesamiento de texto mediante lectura de capas digitales.

    La función configura el entorno de logs y delega la ejecución a la lógica 
    genérica de orquestación.
    """
    # Establecer el archivo de log específico para la Fase 2
    set_log_file(LOG_FILE)
    
    # Delegar la orquestación a la función genérica
    execute_phase_logic(
        run_id=RUN_ID,
        phase_number=PHASE_NUMBER,
        scripts_list=SCRIPTS
    )

if __name__ == "__main__":
    main()