import os

# Configuración y Excepciones
from scripts.config.phase_3 import LOG_FILE
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
PHASE_NUMBER = 3  
SCRIPTS = [
        #"describe_img.py",
        #"process_ocr_tasks.py",
        #"process_files.py",

    ] 

def main():
    """ Función principal que se ejecuta al iniciar el script. Establece el archivo de log y luego delega la orquestación de la fase a la función genérica execute_phase_logic, pasando el run_id, phase_number y la lista de scripts a ejecutar. Cada script en la lista SCRIPTS se ejecutará en orden dentro de execute_phase_logic, que se encargará de manejar la ejecución, logging y cualquier error que pueda ocurrir durante la ejecución de los scripts individuales."""
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