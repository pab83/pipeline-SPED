from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from api.db import SessionLocal
from api.models import *
from scripts.helpers.db_status import *
from typing import List
import subprocess
import os

app = FastAPI(title="Pipeline API")

# ---------------------------------
# Helper: DB session
# ---------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------------------------
# Helper: Chequeo zombies
# ---------------------------------
def recover_stale_runs(db: Session):
    running = db.query(PipelineRun).filter(PipelineRun.status == "running").all()
    for run in running:
        mark_run_finished(run.run_id)

# ---------------------------------
# Helper: lanzar script python externo
# ---------------------------------
def launch_script(script_path: str, run_id: int, phase_number: int = None):
    env = os.environ.copy()
    env["RUN_ID"] = str(run_id)
    if phase_number is not None:
        env["PHASE_NUMBER"] = str(phase_number)

    subprocess.Popen(
        ["python", script_path],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

# ---------------------------------
# Endpoint: iniciar pipeline completa
# ---------------------------------
@app.post("/start")
def start_pipeline(db: Session = Depends(get_db)):
    # Chequeo rápido de zombies
    active_runs = db.query(PipelineRun).filter(PipelineRun.status == "running").count()
    if active_runs > 0:
        recover_stale_runs(db)
        raise HTTPException(status_code=400, detail="Another run is already running")

    # Crear run
    new_run = PipelineRun(status="running")
    db.add(new_run)
    db.commit()
    db.refresh(new_run)
    run_id = new_run.run_id
    mark_run_started(run_id)

    script_path = "scripts/run_pipeline.py"
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="Pipeline script not found")

    try:
        launch_script(script_path, run_id)
    except Exception as e:
        mark_run_finished(run_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Pipeline started", "run_id": run_id}

# ---------------------------------
# Endpoint: iniciar fase específica
# ---------------------------------
@app.post("/run_phase/{phase_number}")
def run_phase_api(phase_number: int, db: Session = Depends(get_db)):

    if phase_number not in [0, 1, 2, 3]:
        raise HTTPException(status_code=400, detail="Phase number out of range")

    # Crear run independiente
    new_run = PipelineRun(status="running")
    db.add(new_run)
    db.commit()
    db.refresh(new_run)
    run_id = new_run.run_id
    mark_run_started(run_id)

    script_path = f"scripts/run_phase_{phase_number}.py"
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="Phase script not found")

    try:
        launch_script(script_path, run_id, phase_number)
    except Exception as e:
        mark_run_finished(run_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {"message": f"Phase {phase_number} started", "run_id": run_id}

# ---------------------------------
# Endpoint: obtener estado completo de un run
# ---------------------------------
@app.get("/status/{run_id}", response_model=RunStatus)
def get_run_status(run_id: int, db: Session = Depends(get_db)):
    update_run_status(run_id, db)

    run = db.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    phases_list: List[PhaseStatus] = []
    for phase in run.phases:
        scripts_list: List[ScriptStatus] = []
        for script in phase.scripts:
            scripts_list.append(
                ScriptStatus(
                    script_name=script.script_name,
                    status=script.status,
                    processed_files=script.processed_files,
                    total_files=script.total_files,
                    error_message=script.error_message,
                    logs=(script.logs or "").split("\n"),
                )
            )
        phases_list.append(
            PhaseStatus(
                phase_number=phase.phase_number,
                status=phase.status,
                processed_files=phase.processed_files,
                total_files=phase.total_files,
                error_message=phase.error_message,
                scripts=scripts_list,
            )
        )

    return RunStatus(
        run_id=run.run_id,
        status=run.status,
        current_phase=run.current_phase,
        processed_files=run.processed_files,
        total_files=run.total_files,
        phases=phases_list,
    )