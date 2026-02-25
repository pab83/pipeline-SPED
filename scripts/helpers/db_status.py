from api.db import SessionLocal
from api.models import *
from datetime import datetime
from sqlalchemy.orm import Session

def update_script_status(phase_id, script_name, status, logs=None, error=None):
    db = SessionLocal()
    try:
        script = db.query(PipelineScript).filter_by(phase_id=phase_id, script_name=script_name).first()
        if not script:
            script = PipelineScript(phase_id=phase_id, script_name=script_name)
            db.add(script)

        script.status = status
        script.logs = "\n".join(logs) if logs else script.logs
        script.error_message = error
        db.commit()
    finally:
        db.close()
        
def update_phase_status(phase_id: int):
    """
    Recalcula el estado de la fase basado en los scripts de esa fase.
    - Si todos los scripts finished -> PhaseStatus = 'finished'
    - Si algún script error -> PhaseStatus = 'error'
    - Si algún script running y ninguno error -> PhaseStatus = 'running'
    """
    db = SessionLocal()
    try:
        scripts = db.query(PipelineScript).filter_by(phase_id=phase_id).all()
        
        for s in scripts:                           # Actualizar para tener logs del script que esta corriendo o con error
            if s.status in ["running", "error"]:
                update_script_status(phase_id, s.script_name, s.status, s.logs.split("\n") if s.logs else None, s.error_message)
            
        if not scripts:
            return  # No hay scripts registrados aún

        statuses = [s.status for s in scripts]

        if "error" in statuses:
            new_status = "error"
        elif all(s == "finished" for s in statuses):
            new_status = "finished"
        elif all(s == "cancelled" for s in statuses):
            new_status = "cancelled"
        else:
            new_status = "running"

        # Actualizar la fase
        phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
        if phase:
            phase.status = new_status
            db.commit()

        # Opcional: actualizar también el estado del run completo
        run = db.query(PipelineRun).filter_by(run_id=phase.run_id).first()
        if run:
            # Si alguna fase con error -> run = error
            phases = db.query(PipelinePhase).filter_by(run_id=run.run_id).all()
            phase_statuses = [p.status for p in phases]
            if "error" in phase_statuses:
                run.status = "error"
            elif all(s == "finished" for s in phase_statuses):
                run.status = "finished"
            
            db.commit()

    finally:
        db.close()
        

def update_run_status(run_id: int, processed_files: int = None):
    """
    Recalcula el estado de un run completo basado en el estado de sus fases.
    - Si alguna fase = 'error' -> run = 'error'
    - Si todas las fases = 'finished' -> run = 'finished'
    - Si alguna fase = 'running' y ninguna error -> run = 'running'
    """
    db = SessionLocal()  # Crear sesión propia
    try:    
        # Obtener todas las fases del run
        phases = db.query(PipelinePhase).filter_by(run_id=run_id).all()
        
        for p in phases:                           # Actualizar para tener logs de la fase que esta corriendo o con error
            if p.status in ["running", "error"]:
                update_phase_status(p.phase_id) 
                
        if not phases:
            return  # No hay fases registradas aún

        # Calcular estados del run
        statuses = [p.status for p in phases]

        if not statuses:
            new_status = "pending"
        elif "running" in statuses:
            # Si hay al menos uno corriendo, el padre sigue corriendo
            new_status = "running"
        elif "error" in statuses:
            # Si nada corre y hay un error, el resultado es error
            new_status = "error"
        elif "cancelled" in statuses:
            # Si nada corre y hay una cancelación, el resultado es cancelled
            new_status = "cancelled"
        elif all(s == "finished" for s in statuses):
            # Solo si TODOS terminaron bien, es finished
            new_status = "finished"
        else:
            # Por seguridad, si no hay nada corriendo pero no todo terminó, 
            # probablemente se quedó huérfano (stale)
            new_status = "failed"

        # Actualizar el run
        run = db.query(PipelineRun).filter_by(run_id=run_id).first()
        if run:
            run.status = new_status
            db.commit()
        if processed_files is not None and run:
            run.processed_files = processed_files
            db.commit()
    finally:
        db.close()  # Cerrar sesión
        
# Tiempos de ejecución
def mark_phase_started(phase_id: int):
    """Rellena started_at y marca la fase como running"""
    db = SessionLocal()
    try:
        phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
        if phase:
            phase.started_at = datetime.now()
            phase.status = "running"
            db.commit()
    finally:
        db.close()

def mark_phase_finished(phase_id: int):
    """Rellena finished_at y marca la fase como finished"""
    db = SessionLocal()
    try:
        phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
        if phase:
            phase.finished_at = datetime.now()
            if phase.status != "error":
                phase.status = "finished"
            db.commit()
    finally:
        db.close()
        

def mark_run_started(run_id: int):
    """Rellena started_at y marca el run como running"""
    db = SessionLocal()
    try:
        run = db.query(PipelineRun).filter_by(run_id=run_id).first()
        if run:
            run.started_at = datetime.now()
            run.status = "running"
            db.commit()
    finally:
        db.close()

def mark_run_finished(run_id: int):
    """Rellena finished_at y marca el run como finished"""
    db = SessionLocal()
    try:
        run = db.query(PipelineRun).filter_by(run_id=run_id).first()
        if run:
            run.finished_at = datetime.now()
            if run.status != "error":
                run.status = "finished"
            db.commit()
    finally:
        db.close()
        

def mark_run_cancelled(run_id: int):
    """Rellena finished_at y marca el run como cancelled"""
    db = SessionLocal()
    try:
        run = db.query(PipelineRun).filter_by(run_id=run_id).first()
        if run:
            run.finished_at = datetime.now()
            run.status = "cancelled"
            db.commit()
    finally:
        db.close()
   
   
def mark_phase_cancelled(phase_id: int):
    db = SessionLocal()
    try:
        phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
        if phase:
            phase.status = "cancelled"
            phase.finished_at = datetime.now()
            db.commit()
    finally:
        db.close()

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

def check_cancelled(run_id):
    db = SessionLocal() 
    try:
        run = db.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
        return run.status == "cancelled" if run else False
    finally:
        db.close()
