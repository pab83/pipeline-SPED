import os
import subprocess
from scripts.helpers.db_status import *
from api.db import SessionLocal
from api.models import PipelinePhase
from typing import List
from scripts.config.phase_0 import *

# ----------------------------
# Parámetros de ejecución
# ----------------------------
RUN_ID = int(os.getenv("RUN_ID", "0")) 
PHASE_NUMBER = 0
SCRIPTS: List[str] = [
        "scan_files.py",
        "mark_pdf_ocr.py",
        "generate_phase_0_report.py"
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
    
def check_cancelled(run_id):
    db = SessionLocal() 
    try:
        run = db.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
        return run.status == "cancelled" if run else False
    finally:
        db.close()

 
# ----------------------------
# Main
# ----------------------------
def main():
    log(f"=== Phase {PHASE_NUMBER} ===")
    PHASE_ID = get_or_create_phase_id(RUN_ID, PHASE_NUMBER)
    mark_phase_started(PHASE_ID)
    PHASE_MODULE = f"scripts.phase_{PHASE_NUMBER}"

    for script in SCRIPTS:
        
        if check_cancelled(RUN_ID):
            print(f"Run {RUN_ID} was cancelled. Stopping execution.")
            raise RuntimeError("Cancelled")
        
        run_script(PHASE_ID, script, PHASE_MODULE)

    mark_phase_finished(PHASE_ID)
    update_phase_status(PHASE_ID)
    log(f"=== Phase {PHASE_NUMBER} completed ===")


if __name__ == "__main__":
   main()
   