import os
import sys
import psutil
import subprocess
from typing import List
from scripts.config.general import MAX_RETRIES, RETRY_DELAY

from scripts.exceptions import PipelineCancelledException
from scripts.helpers.db_status import (
    check_cancelled,
    get_or_create_phase_id,
    mark_phase_cancelled,
    mark_phase_finished,
    mark_phase_started,
    mark_phase_error,
    update_phase_status,
    update_script_status,
    mark_script_running,
    mark_script_finished,
    mark_script_error,
    mark_script_cancelled,
    get_db,
    close_db
)
from scripts.helpers.logs import log

RUN_ID = int(os.getenv("RUN_ID", "0"))

#----------- Logica genérica de orquestación -----------  
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
        mark_phase_finished(phase_id) 
        sys.exit(1)

#----------- Función para ejecutar scripts -----------        
def run_script(phase_id, script_name, phase_module):
    """ Ejecuta un script específico dentro de una fase, manejando logs, cancelación, errores y actualizando la DB automáticamente. Implementa una lógica de reintentos en caso de que el script falle, con un número máximo de intentos definido por MAX_RETRIES y un retraso entre intentos definido por RETRY_DELAY. Durante la ejecución del script, se capturan las salidas estándar y de error para actualizar los logs en tiempo real, y se verifica periódicamente si el usuario ha solicitado cancelar la ejecución del pipeline para detener el script de manera ordenada si es necesario."""
    logs_buffer = []
    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        success = False
        log(f"=== Running {script_name} == Attempt {attempt}/{MAX_RETRIES}:  ===", logs_buffer)

        module = f"{phase_module}.{script_name.replace('.py','')}"
        
        db = get_db()
        try:
            mark_script_running(phase_id, script_name, logs=logs_buffer)
            process = subprocess.Popen(
                [sys.executable, "-m", module],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            # Leer línea a línea mientras corre
            for line in process.stdout:
                line = line.rstrip()
                log(line, logs_buffer)
                update_script_status(phase_id, script_name, logs=logs_buffer)

                if check_cancelled(RUN_ID):
                    log(f"CANCEL SIGNAL DETECTED. Killing {script_name}...", logs_buffer)
                    p = psutil.Process(process.pid)
                    for child in p.children(recursive=True):
                        child.kill()
                    p.kill()

                    mark_script_cancelled(phase_id, script_name, logs=logs_buffer)
                    raise PipelineCancelledException(f"Run {RUN_ID} was cancelled by user.")

            process.wait()

            if process.returncode == 0:
                mark_script_finished(phase_id, script_name, logs=logs_buffer)
            else:
                # FALLO DEL SCRIPT (Exit code != 0)
                error_msg = f"Exit code {process.returncode}"
                log(f"⚠️ {script_name} failed (Attempt {attempt}) with {error_msg}", logs_buffer)
                
                if attempt < MAX_RETRIES:
                    log(f"Retrying in {RETRY_DELAY}s...", logs_buffer)
                    update_script_status(phase_id, script_name, logs=logs_buffer)
                    time.sleep(RETRY_DELAY)
                else:
                    # Agotamos intentos
                    mark_script_error(phase_id, script_name, error_msg=error_msg, logs=logs_buffer)
                    raise RuntimeError(f"{script_name} failed after {MAX_RETRIES} attempts.")
            
        except KeyboardInterrupt:
            log("KeyboardInterrupt detected. Cancelling run...", logs_buffer)
            try:
                p = psutil.Process(process.pid)
                for child in p.children(recursive=True):
                    child.kill()
                p.kill()
            except Exception:
                pass
            
            mark_script_cancelled(phase_id, script_name, logs=logs_buffer)
            raise PipelineCancelledException("Pipeline interrupted by user (Ctrl+C)")
        
        except Exception as e:
            log(f"EXCEPTION: {e}", logs_buffer)
            mark_script_error(phase_id, script_name, error_msg=str(e), logs=logs_buffer)
            raise

        finally:
            pass
        
    close_db()  
    

#----------- Función para ejecutar fases completas -----------  
def run_phase(module, phase_id):
    """
    Ejecuta una fase como proceso hijo.
    Maneja logs, cancelación, errores y actualiza la DB automáticamente.
    """
    logs_buffer = []
    log(f"=== Running phase {module} ===", logs_buffer)

    db = get_db()
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", module],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in process.stdout:
            line = line.rstrip()
            log(line, logs_buffer)
            update_phase_status(phase_id)

            if check_cancelled(RUN_ID):
                log(f"CANCEL SIGNAL DETECTED. Killing phase {module}...", logs_buffer)
                p = psutil.Process(process.pid)
                for child in p.children(recursive=True):
                    child.kill()
                p.kill()

                mark_phase_cancelled(phase_id)
                raise PipelineCancelledException(f"Run {RUN_ID} was cancelled by user.")

        process.wait()

        if process.returncode == 0:
            log(f"Phase {module} completed successfully.", logs_buffer)
            mark_phase_finished(phase_id)
        elif process.returncode == 64:
            mark_phase_cancelled(phase_id)
            raise PipelineCancelledException()
        else:
            mark_phase_error(phase_id, f"Exit code {process.returncode}")
            raise RuntimeError(f"Phase failed: {module}")

    except KeyboardInterrupt:
        log(f"Phase {module} interrupted by user", logs_buffer)

        try:
            p = psutil.Process(process.pid)
            for child in p.children(recursive=True):
                child.kill()
            p.kill()
        except Exception:
            pass

        mark_phase_cancelled(phase_id)
        raise PipelineCancelledException()

    except Exception as e:
        log(f"EXCEPTION in phase {module}: {e}", logs_buffer)
        mark_phase_error(phase_id, str(e))
        raise

    finally:
        close_db()  