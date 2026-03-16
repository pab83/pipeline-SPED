from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from api.db import SessionLocal
from api.models import *
from scripts.helpers.db_status import *
from typing import List
import subprocess
import os

app = FastAPI(title="Pipeline API")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

# ---------------------------------
# Helper: DB session
# ---------------------------------
def get_db():
    """ Proporciona una sesión de base de datos para los endpoints. Esta función se utiliza como dependencia en los endpoints para asegurar que cada solicitud tenga acceso a una sesión de base de datos que se cierra automáticamente al finalizar la solicitud, evitando así problemas de conexiones abiertas o fugas de memoria. La función utiliza un contexto de generador para manejar la apertura y cierre de la sesión de manera eficiente."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------------------------
# Helper: Chequeo zombies
# ---------------------------------
def recover_stale_runs(db: Session):
    """ Marca como cancelados los runs que estén en estado 'running' al iniciar un nuevo run, asumiendo que son ejecuciones colgadas. Esto es útil para evitar conflictos y asegurar que solo haya una ejecución activa del pipeline a la vez. El helper se llama desde el endpoint de inicio del pipeline para hacer un chequeo rápido antes de permitir una nueva ejecución."""
    running = db.query(PipelineRun).filter(PipelineRun.status == "running").all()
    for run in running:
        mark_run_cancelled(run.run_id)
    close_db() # Cerrar sesión de los helpers para evitar conflictos(se abre CURRENT_DB_SESSION al llamar a mark_run_cancelled)

# ---------------------------------
# Helper: lanzar script python externo
# ---------------------------------
def launch_script(script_path: str, run_id: int, phase_number: int = None):
    """ Lanza un script Python en un proceso independiente, pasando el run_id y opcionalmente el phase_number como variables de entorno. Esto permite que el script ejecutado tenga acceso a esta información para actualizar el estado del pipeline en la base de datos. El script se ejecuta con la opción -u para asegurar que la salida se imprima en tiempo real, lo que es útil para el monitoreo de logs."""
    env = os.environ.copy()
    env["RUN_ID"] = str(run_id)
    if phase_number is not None:
        env["PHASE_NUMBER"] = str(phase_number)

    subprocess.Popen(
        ["python", "-u", script_path],
        env=env,
        start_new_session=True
    )

# ---------------------------------
# Endpoint: iniciar pipeline completa
# ---------------------------------
@app.post("/start")
def start_pipeline(db: Session = Depends(get_db)):
    """
    Inicia la ejecución de la pipeline completa.
    
    Este endpoint realiza un chequeo previo de 'zombies' (ejecuciones colgadas). 
    Si detecta una ejecución activa, la cancela antes de permitir una nueva.
    Lanza el script 'run_pipeline.py' en un proceso independiente.
    """
    
    # Chequeo rápido de zombies
    active_runs = db.query(PipelineRun).filter(PipelineRun.status == "running").count()
    if active_runs > 0:
        recover_stale_runs(db)
        raise HTTPException(status_code=400, detail="Another run is already running. Stale runs have been marked as cancelled. Please try again.")
    
    # Crear run
    new_run = PipelineRun(status="running")
    db.add(new_run)
    db.commit()
    db.refresh(new_run)
    run_id = new_run.run_id
    

    script_path = os.path.join(PROJECT_ROOT, "scripts", "run_pipeline.py")
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="Pipeline script not found")

    try:
        launch_script(script_path, run_id)
    except Exception as e:
        mark_run_finished(run_id)
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        close_db() # Cerrar sesión de los helpers para evitar conflictos

    return {"message": "Pipeline started", "run_id": run_id}

# ---------------------------------
# Endpoint: iniciar fase específica
# ---------------------------------
@app.post("/run_phase/{phase_number}")
def run_phase_api(phase_number: int, db: Session = Depends(get_db)):
    """
    Ejecuta una fase específica de la pipeline (0, 1, 2 o 3).
    
    Permite correr una etapa de forma independiente sin disparar todo el flujo.
    Valida que el número de fase sea correcto y lanza el script correspondiente 
    ubicado en la carpeta de la fase.
    """
    
    if phase_number not in [0, 1, 2, 3, 4]:
        raise HTTPException(status_code=400, detail="Phase number out of range")

    # Crear run independiente
    new_run = PipelineRun(status="running")
    db.add(new_run)
    db.commit()
    db.refresh(new_run)
    run_id = new_run.run_id

    script_path = os.path.join(PROJECT_ROOT, "scripts", f"phase_{phase_number}", f"run_phase_{phase_number}.py")
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="Phase script not found")

    try:
        launch_script(script_path, run_id, phase_number)
    except Exception as e:
        mark_run_finished(run_id)
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        close_db() # Cerrar sesión de los helpers para evitar conflictos

    return {"message": f"Phase {phase_number} started", "run_id": run_id}

# ---------------------------------
# Endpoint: obtener estado completo de un run
# ---------------------------------
@app.get("/status/{run_id}", response_model=RunStatus)
def get_run_status(run_id: int, db: Session = Depends(get_db)):
    """
    Obtiene el reporte detallado de un run específico por su ID.
    
    Devuelve el estado general, la fase actual y un desglose de cada script ejecutado, 
    incluyendo mensajes de error y logs de salida si están disponibles.
    """
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
                    error_message=script.error_message,
                    logs=(script.logs or "").split("\n"),
                )
            )
        phases_list.append(
            PhaseStatus(
                phase_number=phase.phase_number,
                status=phase.status,
                error_message=phase.error_message,
                scripts=scripts_list,
            )
        )

    return RunStatus(
        run_id=run.run_id,
        status=run.status,
        current_phase=run.current_phase,
        processed_files=run.processed_files,
        phases=phases_list,
    )
    
# ---------------------------------
# Endpoint:detener pipeline activo(s) por run_id o todos los activos
# ---------------------------------
@app.post("/stop")
def stop_pipeline(run_id: Optional[int] = None, db: Session = Depends(get_db)):
    """
    Detiene ejecuciones en curso y las marca como canceladas.
    
    - Si se proporciona **run_id**: Cancela solo esa ejecución específica.
    - Si NO se proporciona: Cancela **TODAS** las ejecuciones que tengan estado 'running'.
    Ideal para paradas de emergencia o limpieza de procesos.
    """
    # Base de la consulta para buscar solo los que están en ejecución
    query = db.query(PipelineRun).filter(PipelineRun.status == "running")
    
    if run_id:
        # Caso 1: Detener uno específico
        active_runs = query.filter(PipelineRun.run_id == run_id).all()
        if not active_runs:
            raise HTTPException(status_code=404, detail=f"Active run {run_id} not found")
    else:
        # Caso 2: Detener todos los activos
        active_runs = query.all()
        if not active_runs:
            raise HTTPException(status_code=400, detail="No active runs to stop")

    # Guardamos los IDs para el mensaje de respuesta antes de aplicar cambios
    stopped_ids = [run.run_id for run in active_runs]
    
    # Marcamos todos como cancelados
    for run in active_runs:
        run.status = "cancelled"
        run.finished_at = datetime.utcnow()
    
    db.commit()

    return {
        "message": "Termination signal sent",
        "stopped_count": len(stopped_ids),
        "run_ids": stopped_ids
    }
    
# ---------------------------------
# Endpoint: cambiar variable de entorno por URL
# ---------------------------------
@app.post("/change_focus/{folder_path:path}")
def change_focus(folder_path: str):
    """
    Actualiza el directorio de trabajo base (BASE_PATH) mediante la URL.
    
    Permite redirigir dinámicamente dónde operarán los scripts de la pipeline.
    El path proporcionado se concatenará con el prefijo '/data'.
    """
    if not folder_path:
        new_path = "/data"
    else:
        # Construir el path absoluto basado en tu lógica (ejemplo usando PROJECT_ROOT)
        new_path = os.path.join( "/data", folder_path)
    
    # Actualizar la variable de entorno en el proceso de la API
    # Esto servirá para que launch_script lo herede
    os.environ["BASE_PATH"] = new_path
    
    print(f"DEBUG: BASE_PATH actualizado a: {new_path}")
    
    return {
        "message": "Focus path updated",
        "new_folder": folder_path,
        "full_path": new_path
    }