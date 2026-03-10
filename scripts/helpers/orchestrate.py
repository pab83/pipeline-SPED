import os
import sys
import time
import psutil
import subprocess
from typing import List, Optional, Any
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

RUN_ID: int = int(os.getenv("RUN_ID", "0"))

def execute_phase_logic(run_id: int, phase_number: int, scripts_list: List[str]) -> None:
    """
    Orquesta la ejecución secuencial de scripts dentro de una fase específica.
    
    Se encarga de inicializar la fase en la base de datos, iterar sobre la lista 
    de scripts proporcionada y gestionar la finalización o cancelación global de la fase.
    """
    phase_id: Optional[int] = None
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
    
    except Exception as e:
        log(f"FATAL ERROR in Phase {phase_number}: {e}")
        if phase_id:
            mark_phase_finished(phase_id) 
        sys.exit(1)

def run_script(phase_id: int, script_name: str, phase_module: str) -> None:
    """
    Ejecuta un script individual como un subproceso con lógica de reintentos y monitoreo.
    
    Este método gestiona:
    
    1. **Ciclo de Vida**: Marcado de inicio, éxito o error en la base de datos.
    2. **Streaming de Logs**: Captura la salida del subproceso en tiempo real.
    3. **Cancelación Activa**: Monitoriza la señal de cancelación en la DB para matar el proceso si es necesario.
    4. **Reintentos**: Basado en las constantes `MAX_RETRIES` y `RETRY_DELAY`.
    """
    logs_buffer: List[str] = []
    attempt: int = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        log(f"=== Running {script_name} == Attempt {attempt}/{MAX_RETRIES}: ===", logs_buffer)

        module = f"{phase_module}.{script_name.replace('.py','')}"
        
        get_db() # Asegurar conexión
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
                break # Éxito, salir del bucle de reintentos
            else:
                error_msg = f"Exit code {process.returncode}"
                log(f"⚠️ {script_name} failed (Attempt {attempt}) with {error_msg}", logs_buffer)
                
                if attempt < MAX_RETRIES:
                    log(f"Retrying in {RETRY_DELAY}s...", logs_buffer)
                    update_script_status(phase_id, script_name, logs=logs_buffer)
                    time.sleep(RETRY_DELAY)
                else:
                    mark_script_error(phase_id, script_name, error_msg=error_msg, logs=logs_buffer)
                    raise RuntimeError(f"{script_name} failed after {MAX_RETRIES} attempts.")
            
        except KeyboardInterrupt:
            log("KeyboardInterrupt detected. Cancelling run...", logs_buffer)
            try:
                p = psutil.Process(process.pid)
                for child in p.children(recursive=True): child.kill()
                p.kill()
            except Exception: pass
            
            mark_script_cancelled(phase_id, script_name, logs=logs_buffer)
            raise PipelineCancelledException("Pipeline interrupted by user (Ctrl+C)")
        
        except Exception as e:
            log(f"EXCEPTION: {e}", logs_buffer)
            mark_script_error(phase_id, script_name, error_msg=str(e), logs=logs_buffer)
            raise
    
    close_db()

def run_phase(module: str, phase_id: int) -> None:
    """
    Ejecuta una fase completa como un subproceso independiente.
    
    Utilizado habitualmente por orquestadores de alto nivel para encapsular 
    la ejecución de un módulo de fase completo, manteniendo el aislamiento de procesos.
    """
    logs_buffer: List[str] = []
    log(f"=== Running phase {module} ===", logs_buffer)

    get_db()
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
                for child in p.children(recursive=True): child.kill()
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
            for child in p.children(recursive=True): child.kill()
            p.kill()
        except Exception: pass
        mark_phase_cancelled(phase_id)
        raise PipelineCancelledException()

    except Exception as e:
        log(f"EXCEPTION in phase {module}: {e}", logs_buffer)
        mark_phase_error(phase_id, str(e))
        raise
    finally:
        close_db()