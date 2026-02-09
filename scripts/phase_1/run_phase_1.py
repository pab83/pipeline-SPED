import os
import subprocess
from scripts.config.phase_1 import LOG_FILE

def log(msg):
    """Loggea en consola y archivo"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def run_script(script_name):
    log(f"=== Running {script_name} ===")

    module = f"scripts.phase_1.{script_name.replace('.py', '')}"

    try:
        subprocess.run(
            ["python", "-m", module],
            check=True,
        )
        log(f"{script_name} completed successfully.")

    except subprocess.CalledProcessError as e:
        log(f"FATAL: {script_name} failed with exit code {e.returncode}")
        raise

if __name__ == "__main__":
    log("=== Phase 1 ===")

    scripts = [
        "populate_db.py",   # Llena la DB con metadata inicial
        "hash_files.py",    # Calcula hashes de los archivos
        "generate_phase_1_report.py"    
    ]

    for script in scripts:
        run_script(script)

    log("=== Phase 1 completed ===")
