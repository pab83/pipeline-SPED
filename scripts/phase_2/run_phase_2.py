import subprocess
from scripts.helpers.db_status import *
from api.db import SessionLocal
from api.models import PipelinePhase
from typing import List
from scripts.config.phase_2 import *  # Importamos configuración específica de la fase 2

# ----------------------------
# Parámetros de ejecución
# ----------------------------
RUN_ID = int(os.getenv("RUN_ID", "0"))
PHASE_NUMBER = 2
SCRIPTS: List[str] = [
        "migrate_phase_2.py",
        "dedup.py", # Only dedup by hash for now.
        "extract_text.py",
        "img_looks_like_document.py",
        "process_ocr_tasks.py",
    ]

# -----------------------------
# Archivo de log
# -----------------------------
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"run_{RUN_ID}_phase_{PHASE_NUMBER}.log")

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
        run_script(phase_id=PHASE_ID, script_name=script, phase_module=PHASE_MODULE)
        
    mark_phase_finished(PHASE_ID)
    update_phase_status(PHASE_ID)
    update_run_status(RUN_ID)
    log(f"=== Phase {PHASE_NUMBER} completed ===")

if __name__ == "__main__":
    main()
    