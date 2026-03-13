import os
from scripts.config.phase_0 import LOG_FILE
from scripts.helpers.logs import set_log_file
from scripts.helpers.orchestrate import (
    execute_phase_logic,
    get_or_create_phase_id,
    run_script
)

# ----------------------------
# Parámetros de ejecución
# ----------------------------
RUN_ID = int(os.getenv("RUN_ID", "0"))
"""ID de ejecución global heredado del entorno para trazar el estado en la base de datos."""

PHASE_NUMBER = 0
"""Identificador numérico de esta fase (0: Escaneo e Indexación)."""

SCRIPTS = [
     "create_db.py",
     "scan_files.py",
     "mark_pdf_ocr.py",
     "generate_phase_0_report.py"
]
"""Lista secuencial de scripts que componen la lógica interna de la Fase 0."""

def main():
    """
    Punto de entrada para la orquestación de la Fase 0.
    Esta función realiza las siguientes acciones:
    
    1. Configura el sistema de logs específico para la fase mediante `LOG_FILE`.
    
    2. Delega la ejecución a `execute_phase_logic`, que se encarga de recorrer 
       la lista `SCRIPTS` y actualizar los estados en la base de datos.
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