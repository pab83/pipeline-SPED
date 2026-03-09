import os

# Configuración y Excepciones
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
RUN_ID = int(os.getenv("RUN_ID", "0"))  
PHASE_NUMBER = 1
SCRIPTS = [
        "hash_files.py",    # Calcula hashes de los archivos
        "generate_phase_1_report.py"    
    ] 


def main():
    """
    Orquesta la ejecución de la fase 0 de la pipeline.
    Configura el archivo de log específico para esta fase.    
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
   