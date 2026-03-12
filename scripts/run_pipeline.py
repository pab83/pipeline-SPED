import os
import sys
import subprocess
from dotenv import load_dotenv
load_dotenv()
from scripts.helpers.db_status import *
from scripts.helpers.orchestrate import run_phase
from scripts.exceptions import PipelineCancelledException



RUN_ID = int(os.environ.get("RUN_ID", 0))  
"""
ID único de la ejecución actual, obtenido de las variables de entorno. 
Se utiliza para vincular todos los logs y estados de base de datos a este proceso específico.
"""
PHASES = [
    #"scripts.phase_0.run_phase_0",
    #"scripts.phase_1.run_phase_1",
    #"scripts.phase_2.run_phase_2",
    #"scripts.phase_3.run_phase_3",
    "scripts.phase_4.run_phase_4"

]
"""
Lista ordenada de módulos que componen el ciclo de vida de la pipeline.
El orquestador importa y ejecuta cada una de estas funciones de manera secuencial.
"""


    

def main():
    """
    Punto de entrada principal que orquesta el ciclo de vida completo de la pipeline.

    Este método coordina la ejecución secuencial de todas las fases definidas en `PHASES`.
    Gestiona el estado global en la base de datos (inicio, fin, cancelación o error)
    y asegura que cada fase reciba su identificador correspondiente.

    Flujo de ejecución:
    1. Marca el inicio del `RUN_ID` en la base de datos.
    2. Itera sobre `PHASES`, creando o recuperando el `phase_id`.
    3. Ejecuta cada fase mediante `run_phase`.
    4. Notifica la finalización exitosa o gestiona errores globales.

    Raises:
        PipelineCancelledException: Capturada si el usuario interrumpe el proceso (Ctrl+C),
            marcando el run como 'cancelled' en la DB.
        SystemExit: Se lanza al finalizar para devolver el código de salida (0 o 1) al sistema operativo.
        Exception: Cualquier error no controlado marca el run como 'error' antes de salir.
    """
    try:
        print("=== Starting full pipeline ===")
        mark_run_started(RUN_ID)

        for idx, phase_module in enumerate(PHASES):
            # Obtener o crear phase_id
            phase_id = get_or_create_phase_id(RUN_ID, phase_number=idx)
            
            # Ejecutar la fase pasando phase_id
            run_phase(phase_module, phase_id)

        mark_run_finished(RUN_ID)
        print("\n=== Pipeline completed ===")

    except PipelineCancelledException:
        print(f"--- Pipeline {RUN_ID} stopped by user ---")
        mark_run_cancelled(RUN_ID)
        sys.exit(0)

    except Exception as e:
        print(f"--- Pipeline {RUN_ID} failed with error: {e} ---")
        mark_run_finished(RUN_ID)  # marca finished pero con status "error"
        sys.exit(1)
        
if __name__ == "__main__":
    main()