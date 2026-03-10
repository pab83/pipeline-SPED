import os
from scripts.config.phase_1 import LOG_FILE
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
"""ID de ejecución global para el seguimiento de la integridad en la base de datos."""

PHASE_NUMBER: int = 1
"""Identificador de la Fase 1: Hashing y Verificación de Integridad."""

SCRIPTS: list[str] = [
    "hash_files.py",             # Calcula hashes de los archivos
    "generate_phase_1_report.py" # Genera resumen de duplicados y cambios
]
"""Lista de scripts encargados de calcular las huellas digitales de los archivos."""

def main() -> None:
    """
    Orquesta la ejecución de la Fase 1 de la pipeline.
    
    Esta función prepara el entorno de logging específico para la fase de hashing
    y lanza la ejecución secuencial de los scripts definidos en `SCRIPTS` a través
    de la lógica genérica de orquestación.

    Pasos:
    
    
    1. Define el archivo de salida para logs (`LOG_FILE`).
    2. Ejecuta `execute_phase_logic` para procesar los hashes de archivos pendientes.
    """
    # Establecer el archivo de log antes de ejecutar la lógica
    set_log_file(LOG_FILE)
    
    # Delegar la orquestación a la función genérica
    execute_phase_logic(
        run_id=RUN_ID,
        phase_number=PHASE_NUMBER,
        scripts_list=SCRIPTS
    )

if __name__ == "__main__":
    main()