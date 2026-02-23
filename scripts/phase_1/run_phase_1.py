import os
import subprocess
from scripts.helpers.db_status import *
from db import SessionLocal
from models import PipelinePhase
from typing import List

# ----------------------------
# Parámetros de ejecución
# ----------------------------
RUN_ID = int(os.getenv("RUN_ID", "0"))  
PHASE_NUMBER = 1
SCRIPTS: List[str] = [
        "populate_db.py",   # Llena la DB con metadata inicial
        "hash_files.py",    # Calcula hashes de los archivos
        "generate_phase_1_report.py"    
    ] 

# ----------------------------
# Helpers
# ----------------------------
def log(msg, logs_buffer=None):
    """Escribe en archivo y en memoria para enviar a DB"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)
    if logs_buffer is not None:
        logs_buffer.append(msg)

def get_or_create_phase_id(run_id, phase_number):
    """Obtiene el phase_id en DB o lo crea si no existe"""
    db = SessionLocal()
    try:
        phase = db.query(PipelinePhase).filter_by(run_id=run_id, phase_number=phase_number).first()
        if not phase:
            phase = PipelinePhase(run_id=run_id, phase_number=phase_number, status="running")
            db.add(phase)
            db.commit()
            db.refresh(phase)
        return phase.phase_id
    finally:
        db.close()

def run_script(phase_id, script_name, phase_module):
    logs_buffer = []
    log(f"=== Running {script_name} ===", logs_buffer)

    module = f"{phase_module}.{script_name.replace('.py','')}"

    try:
        result = subprocess.run(
            ["python", "-m", module],
            capture_output=True,
            text=True,
            check=False
        )

        if result.stdout:
            logs_buffer.extend(result.stdout.splitlines())
        if result.stderr:
            logs_buffer.extend(result.stderr.splitlines())

        if result.returncode == 0:
            log(f"{script_name} completed successfully.", logs_buffer)
            update_script_status(phase_id, script_name, status="finished", logs=logs_buffer)
        else:
            log(f"FATAL: {script_name} failed with exit code {result.returncode}", logs_buffer)
            update_script_status(
                phase_id, script_name, status="error",
                logs=logs_buffer, error=f"Exit code {result.returncode}"
            )
            raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")

    except Exception as e:
        log(f"EXCEPTION: {e}", logs_buffer)
        update_script_status(phase_id, script_name, status="error", logs=logs_buffer, error=str(e))
        raise

# ----------------------------
# Main
# ----------------------------
def main():
    log(f"=== Phase {PHASE_NUMBER} ===")

    PHASE_ID = get_or_create_phase_id(RUN_ID, PHASE_NUMBER)
    mark_phase_started(PHASE_ID)
    PHASE_MODULE = f"scripts.phase_{PHASE_NUMBER}"

    for script in SCRIPTS:
        run_script(PHASE_ID, script, PHASE_MODULE)

    mark_phase_finished(PHASE_ID)
    update_phase_status(PHASE_ID)
    update_run_status(RUN_ID)
    log(f"=== Phase {PHASE_NUMBER} completed ===")
    
if __name__ == "__main__":
    main()
    
