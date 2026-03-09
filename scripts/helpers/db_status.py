from api.db import SessionLocal
from api.models import *
from datetime import datetime
from sqlalchemy.orm import Session

# variable global para la sesión actual del script
CURRENT_DB_SESSION = None

def get_db():
    """ Devuelve la sesión de base de datos actual, creando una nueva si no existe. Esto permite que todos los scripts compartan la misma sesión durante su ejecución, lo que es útil para mantener el estado y evitar múltiples conexiones innecesarias. La función también maneja la creación de la sesión de manera perezosa, es decir, solo se crea cuando se necesita por primera vez."""
    global CURRENT_DB_SESSION
    if CURRENT_DB_SESSION is None:
        CURRENT_DB_SESSION = SessionLocal()
    return CURRENT_DB_SESSION

def close_db():
    """ Cierra la sesión de base de datos actual si existe. Esto es importante para liberar recursos y evitar conexiones abiertas innecesarias después de que el script ha terminado su ejecución. Se recomienda llamar a esta función al final de cada script para asegurarse de que la conexión a la base de datos se cierre correctamente."""
    global CURRENT_DB_SESSION
    if CURRENT_DB_SESSION:
        CURRENT_DB_SESSION.close()
        CURRENT_DB_SESSION = None

# ------------ RUN STATUS MANAGEMENT --------------
def update_run_status(run_id: int, processed_files: int = None):
    """ Actualiza el estado del run en la base de datos basado en el estado de sus fases. Si se proporciona processed_files, también actualiza ese campo en la tabla PipelineRun. La función consulta todas las fases asociadas al run_id dado, determina el nuevo estado del run basado en los estados de las fases (por ejemplo, si alguna fase está corriendo, el run está corriendo; si todas las fases están terminadas, el run está terminado; etc.), y luego actualiza el registro del run en la base de datos con el nuevo estado y la cantidad de archivos procesados si se proporcionó."""
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
    """ Marca el inicio de un run en la base de datos. Actualiza el campo started_at con la fecha y hora actual, y establece el estado del run a "running". Esto es útil para llevar un registro de cuándo comenzó la ejecución del pipeline y para indicar que el run está actualmente en progreso. Se recomienda llamar a esta función al inicio de la ejecución del pipeline para asegurarse de que el estado del run se actualice correctamente desde el principio."""
    db = get_db()
    run = db.query(PipelineRun).filter_by(run_id=run_id).first()
    if run:
        run.started_at = datetime.now()
        run.status = "running"
        db.commit()

def mark_run_finished(run_id: int):
    """ Marca el final de un run en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, y establece el estado del run a "finished" si no está en estado "error". Esto es útil para llevar un registro de cuándo terminó la ejecución del pipeline y para indicar que el run ha finalizado correctamente. Se recomienda llamar a esta función al final de la ejecución del pipeline para asegurarse de que el estado del run se actualice correctamente al finalizar."""
    db = get_db()
    run = db.query(PipelineRun).filter_by(run_id=run_id).first()
    if run:
        run.finished_at = datetime.now()
        if run.status != "error":
            run.status = "finished"
        db.commit()

def mark_run_cancelled(run_id: int):
    """ Marca un run como cancelado en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, y establece el estado del run a "cancelled". Esto es útil para llevar un registro de cuándo se canceló la ejecución del pipeline y para indicar que el run no se completó debido a una cancelación. Se recomienda llamar a esta función cuando el usuario interrumpe la ejecución del pipeline (por ejemplo, con Ctrl+C) para asegurarse de que el estado del run se actualice correctamente a "cancelled"."""
    db = get_db()
    run = db.query(PipelineRun).filter_by(run_id=run_id).first()
    if run:
        run.finished_at = datetime.now()
        run.status = "cancelled"
        db.commit()

# ------------ PHASE STATUS MANAGEMENT --------------
def update_phase_status(phase_id: int):
    """ Actualiza el estado de una fase en la base de datos basado en el estado de sus scripts. La función consulta todos los scripts asociados al phase_id dado, determina el nuevo estado de la fase basado en los estados de los scripts (por ejemplo, si algún script está corriendo, la fase está corriendo; si todas las fases están terminadas, la fase está terminada; etc.), y luego actualiza el registro de la fase en la base de datos con el nuevo estado. Después de actualizar el estado de la fase, también llama a update_run_status para asegurarse de que el estado del run asociado se actualice correctamente basado en el nuevo estado de la fase."""
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
    """ Marca el inicio de una fase en la base de datos. Actualiza el campo started_at con la fecha y hora actual, y establece el estado de la fase a "running". Además, actualiza el campo current_phase del run asociado para reflejar que esta fase es la que se está ejecutando actualmente. Esto es útil para llevar un registro de cuándo comenzó la ejecución de cada fase y para indicar cuál fase está actualmente en progreso dentro del run. Se recomienda llamar a esta función al inicio de la ejecución de cada fase para asegurarse de que el estado de la fase y del run se actualice correctamente desde el principio."""
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
    """ Marca el final de una fase en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, y establece el estado de la fase a "finished" si no está en estado "error". Esto es útil para llevar un registro de cuándo terminó la ejecución de cada fase y para indicar que la fase ha finalizado correctamente. Se recomienda llamar a esta función al final de la ejecución de cada fase para asegurarse de que el estado de la fase se actualice correctamente al finalizar."""
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.finished_at = datetime.now()
        if phase.status != "error":
            phase.status = "finished"
        db.commit()

def mark_phase_cancelled(phase_id: int):
    """ Marca una fase como cancelada en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, y establece el estado de la fase a "cancelled". Esto es útil para llevar un registro de cuándo se canceló la ejecución de cada fase y para indicar que la fase no se completó debido a una cancelación. Se recomienda llamar a esta función cuando el usuario interrumpe la ejecución de una fase (por ejemplo, con Ctrl+C) para asegurarse de que el estado de la fase se actualice correctamente a "cancelled"."""
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.status = "cancelled"
        phase.finished_at = datetime.now()
        db.commit()

def mark_phase_error(phase_id: int, error_msg: str):
    """ Marca una fase como con error en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, establece el estado de la fase a "error", y almacena el mensaje de error proporcionado. Esto es útil para llevar un registro de cuándo ocurrió un error durante la ejecución de una fase y para almacenar información sobre el error que ocurrió. Se recomienda llamar a esta función cuando se capture una excepción durante la ejecución de una fase para asegurarse de que el estado de la fase se actualice correctamente a "error" y para registrar el mensaje de error."""
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(phase_id=phase_id).first()
    if phase:
        phase.status = "error"
        phase.error_message = error_msg
        phase.finished_at = datetime.now()
        db.commit()

# ------------ SCRIPT STATUS MANAGEMENT --------------
def update_script_status(phase_id, script_name, logs=None):
    """ Actualiza el estado de un script en la base de datos basado en el estado de su fase. La función consulta el script específico asociado al phase_id y script_name dados, actualiza sus logs si se proporcionan, y luego determina el nuevo estado del script basado en el estado de la fase (por ejemplo, si la fase está cancelada, el script también se marca como cancelado; si la fase tiene error, el script se marca como error; etc.). Después de actualizar el estado del script, también llama a update_phase_status para asegurarse de que el estado de la fase asociada se actualice correctamente basado en el nuevo estado del script."""
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
    """ Marca el inicio de un script en la base de datos. Actualiza el campo started_at con la fecha y hora actual, y establece el estado del script a "running". Esto es útil para llevar un registro de cuándo comenzó la ejecución de cada script dentro de una fase y para indicar cuál script está actualmente en progreso. Se recomienda llamar a esta función al inicio de la ejecución de cada script para asegurarse de que el estado del script se actualice correctamente desde el principio."""
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
    """ Marca el final de un script en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, y establece el estado del script a "finished" si no está en estado "error". Esto es útil para llevar un registro de cuándo terminó la ejecución de cada script dentro de una fase y para indicar que el script ha finalizado correctamente. Se recomienda llamar a esta función al final de la ejecución de cada script para asegurarse de que el estado del script se actualice correctamente al finalizar."""
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
    """ Marca un script como con error en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, establece el estado del script a "error", y almacena el mensaje de error proporcionado. Esto es útil para llevar un registro de cuándo ocurrió un error durante la ejecución de cada script dentro de una fase y para almacenar información sobre el error que ocurrió. Se recomienda llamar a esta función cuando se capture una excepción durante la ejecución de un script para asegurarse de que el estado del script se actualice correctamente a "error" y para registrar el mensaje de error."""
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
    """ Marca un script como cancelado en la base de datos. Actualiza el campo finished_at con la fecha y hora actual, y establece el estado del script a "cancelled". Esto es útil para llevar un registro de cuándo se canceló la ejecución de cada script dentro de una fase y para indicar que el script no se completó debido a una cancelación. Se recomienda llamar a esta función cuando el usuario interrumpe la ejecución de un script (por ejemplo, con Ctrl+C) para asegurarse de que el estado del script se actualice correctamente a "cancelled"."""
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
    """ Obtiene el phase_id para un run_id y phase_number dados. Si no existe una fase con ese run_id y phase_number, crea una nueva fase con estado "running" y devuelve su phase_id. Esto es útil para asegurarse de que cada fase tenga un registro en la base de datos antes de comenzar su ejecución, y para obtener el phase_id necesario para actualizar el estado de la fase durante la ejecución."""
    db = get_db()
    phase = db.query(PipelinePhase).filter_by(run_id=run_id, phase_number=phase_number).first()
    if not phase:
        phase = PipelinePhase(run_id=run_id, phase_number=phase_number, status="running")
        db.add(phase)
        db.commit()
        db.refresh(phase)
    return phase.phase_id

def check_cancelled(run_id):
    """ Verifica si un run ha sido marcado como cancelado en la base de datos. Consulta el estado del run con el run_id dado y devuelve True si el estado es "cancelled", o False en caso contrario. Esto es útil para que los scripts puedan verificar periódicamente si el usuario ha solicitado cancelar la ejecución del pipeline, y así poder detener su ejecución de manera ordenada si es necesario."""
    db = get_db()
    run = db.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
    return run.status == "cancelled" if run else False