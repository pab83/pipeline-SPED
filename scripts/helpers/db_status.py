from api.db import SessionLocal
from api.models import *
from datetime import datetime
from sqlalchemy.orm import Session

# variable global para la sesión actual del script
CURRENT_DB_SESSION = None

def get_db():
    global CURRENT_DB_SESSION
    if CURRENT_DB_SESSION is None:
        CURRENT_DB_SESSION = SessionLocal()
    return CURRENT_DB_SESSION

def close_db():
    global CURRENT_DB_SESSION
    if CURRENT_DB_SESSION:
        CURRENT_DB_SESSION.close()
        CURRENT_DB_SESSION = None

# ------------ RUN STATUS MANAGEMENT --------------
def update_run_status(run_id: int, processed_files: int = None):
    db = get_db()
    
    # Obtener todas las fases del run
    phases = db.query(PipelinePhase).filter_by(run_id=run_id).all()
            
    if not phases:
        return

    statuses = [p.status for p in phases]

    if not statuses:
        new_status = "pending"
    elif "running" in statuses:
        new_status = "running"
    elif "error" in statuses:
        new_status = "error"
    elif "cancelled" in statuses:
        new_status = "cancelled"
    elif all(s == "finished" for s in statuses):
        new_status = "finished"
    else:
        new_status = "failed"

    run = db.query(PipelineRun).filter_by(run_id=run_id).first()
    if run:
        run.status = new_status
        if processed_files is not None:
            run.processed_files = processed_files
        db.commit()

def mark_run_started(run_id: int):
    db = get_db()
    run = db.query(PipelineRun).filter_by(run_id=run_id).first()
    if run:
        run.started_at = datetime.now()
        run.status = "running"
        db.commit()

def mark_run_finished(run_id: int):
    db = get_db()
    run = db.query(PipelineRun).filter_by(run_id=run_id).first()
    if run:
        run.finished_at = datetime.now()
        if run.status != "error":
            run.status = "finished"
        db.commit()

def mark_run_cancelled(run_id: int):
    db = get_db()
    run = db.query(PipelineRun).filter_by(run_id=run_id).first()
    if run:
        run.finished_at = datetime.now()
        run.status = "cancelled"
        db.commit()

# ------------ PHASE STATUS MANAGEMENT --------------
def update_phase_status(phase_id: int):
    db = get_db()
    scripts = db.query(PipelineScript).filter_by(phase_id=phase_id).all()

    if not scripts:
        return

    statuses = [s.status for s in scripts]
    if not statuses:
        new_status = "pending"
    elif "running" in statuses:
        new_status = "running"
    elif "error" in statuses:
        new_status = "error"
    elif "cancelled" in statuses:
        new_status = "cancelled"
    elif all(s == "finished" for s in statuses):
        new_status = "finished"
    else:
        new_status = "failed"

    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.status = new_status
        db.commit()
        update_run_status(phase.run_id)

def mark_phase_started(phase_id: int):
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.started_at = datetime.now()
        phase.status = "running"
        
    run = db.query(PipelineRun).filter_by(run_id=phase.run_id).first()
    if run:
        run.current_phase = phase.phase_number
    
    db.commit()

def mark_phase_finished(phase_id: int):
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.finished_at = datetime.now()
        if phase.status != "error":
            phase.status = "finished"
        db.commit()

def mark_phase_cancelled(phase_id: int):
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.status = "cancelled"
        phase.finished_at = datetime.now()
        db.commit()

def mark_phase_error(phase_id: int, error_msg: str):
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.status = "error"
        phase.error_message = error_msg
        phase.finished_at = datetime.now()
        db.commit()

# ------------ SCRIPT STATUS MANAGEMENT --------------
def update_script_status(phase_id, script_name, logs=None):
    db = get_db()
    script = db.query(PipelineScript).filter_by(phase_id=phase_id, script_name=script_name).first()
    if not script:
        script = PipelineScript(phase_id=phase_id, script_name=script_name, status="pending")
        db.add(script)

    if logs:
        script.logs = "\n".join(logs)

    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        if phase.status == "cancelled":
            script.status = "cancelled"
        elif phase.status == "error" and script.status == "running":
            script.status = "error"
        elif phase.status == "finished" and script.status != "error":
            script.status = "finished"

    db.commit()

def mark_script_running(phase_id, script_name, logs=None):
    db = get_db()
    script = db.query(PipelineScript).filter_by(phase_id=phase_id, script_name=script_name).first()
    if not script:
        script = PipelineScript(phase_id=phase_id, script_name=script_name)
        db.add(script)

    script.status = "running"
    script.started_at = datetime.now()
    if logs:
        script.logs = "\n".join(logs)
    db.commit()

    update_phase_status(phase_id)

def mark_script_finished(phase_id, script_name, logs=None):
    db = get_db()
    script = db.query(PipelineScript).filter_by(phase_id=phase_id, script_name=script_name).first()
    if not script:
        script = PipelineScript(phase_id=phase_id, script_name=script_name)
        db.add(script)

    script.status = "finished"
    script.finished_at = datetime.now()
    if logs:
        script.logs = "\n".join(logs)
    db.commit()

    update_phase_status(phase_id)

def mark_script_error(phase_id, script_name, error_msg, logs=None):
    db = get_db()
    script = db.query(PipelineScript).filter_by(phase_id=phase_id, script_name=script_name).first()
    if not script:
        script = PipelineScript(phase_id=phase_id, script_name=script_name)
        db.add(script)

    script.status = "error"
    script.error_message = error_msg
    script.finished_at = datetime.now()
    if logs:
        script.logs = "\n".join(logs)
    db.commit()

    update_phase_status(phase_id)

def mark_script_cancelled(phase_id, script_name, logs=None):
    db = get_db()
    script = db.query(PipelineScript).filter_by(phase_id=phase_id, script_name=script_name).first()
    if not script:
        script = PipelineScript(phase_id=phase_id, script_name=script_name)
        db.add(script)

    script.status = "cancelled"
    script.finished_at = datetime.now()
    if logs:
        script.logs = "\n".join(logs)
    db.commit()

    update_phase_status(phase_id)

# ------------ AUXILIARY FUNCTIONS --------------
def get_or_create_phase_id(run_id, phase_number):
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(run_id=run_id, phase_number=phase_number).first()
    if not phase:
        phase = PipelinePhase(run_id=run_id, phase_number=phase_number, status="running")
        db.add(phase)
        db.commit()
        db.refresh(phase)
    return phase.phase_id

def check_cancelled(run_id):
    db = get_db()
    run = db.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
    return run.status == "cancelled" if run else False