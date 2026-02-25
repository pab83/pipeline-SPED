import os
import sys
import psutil
import subprocess
from typing import List


from scripts.exceptions import PipelineCancelledException
from scripts.helpers.db_status import (
    check_cancelled,
    get_or_create_phase_id,
    mark_phase_cancelled,
    mark_phase_finished,
    mark_phase_started,
    update_phase_status,
    update_script_status
)
from scripts.helpers.logs import log

RUN_ID = int(os.getenv("RUN_ID", "0"))

def execute_phase_logic(run_id, phase_number, scripts_list):
    """
    Lógica común para todas las fases del pipeline.
    """
    phase_id = None
    try:
        log(f"=== Phase {phase_number} ===")
        
        phase_id = get_or_create_phase_id(run_id, phase_number)
        mark_phase_started(phase_id)
        
        phase_module = f"scripts.phase_{phase_number}"

        for script in scripts_list:
            run_script(phase_id, script, phase_module)

        mark_phase_finished(phase_id)
        update_phase_status(phase_id)
        log(f"=== Phase {phase_number} completed ===")
        
    except PipelineCancelledException:
        if phase_id:
            mark_phase_cancelled(phase_id)
        log(f"Phase {phase_number} stopped due to cancellation.")
        sys.exit(64)
        raise
    
    except Exception as e:
        log(f"FATAL ERROR in Phase {phase_number}: {e}")
        sys.exit(1)

        
def run_script(phase_id, script_name, phase_module):
    logs_buffer = []
    log(f"=== Running {script_name} ===", logs_buffer)

    module = f"{phase_module}.{script_name.replace('.py','')}"
    update_script_status(phase_id, script_name, status="running", logs=logs_buffer)
    try:
        process = subprocess.Popen(
        ["python", "-m", module],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

        # Leer línea a línea mientras corre
        for line in process.stdout:
            line = line.rstrip()
            log(line, logs_buffer)  # añade al buffer y opcionalmente imprime en consola
            update_script_status(phase_id, script_name, status="running", logs=logs_buffer)
        
            if check_cancelled(RUN_ID):
                log(f"CANCEL SIGNAL DETECTED. Killing {script_name}...", logs_buffer)
                
                # Matar el proceso y sus hijos
                p = psutil.Process(process.pid)
                for child in p.children(recursive=True):
                    child.kill()
                p.kill()
                
                update_script_status(phase_id, script_name, status="cancelled", logs=logs_buffer)
                raise PipelineCancelledException(f"Run {RUN_ID} was cancelled by user.")

        process.wait()

        # Al finalizar
        if process.returncode == 0:
            log(f"{script_name} completed successfully.", logs_buffer)
            update_script_status(phase_id, script_name, status="finished", logs=logs_buffer)
        else:
            log(f"FATAL: {script_name} failed with exit code {process.returncode}", logs_buffer)
            update_script_status(
                phase_id, script_name, status="error",
                logs=logs_buffer, error=f"Exit code {process.returncode}"
            )
            raise RuntimeError(f"{script_name} failed with exit code {process.returncode}")

    except Exception as e:
        log(f"EXCEPTION: {e}", logs_buffer)
        update_script_status(phase_id, script_name, status="error", logs=logs_buffer, error=str(e))
        raise


    