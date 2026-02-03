import os
from scripts.config.phase_1 import LOG_FILE

def log(msg):
    """Loggea en consola y archivo"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def run_script(script_name):
    """Ejecuta un script de Phase 1 como módulo"""
    log(f"=== Running {script_name} ===")
    # Convertimos a módulo para que imports relativos funcionen
    module = f"scripts.phase_1.{script_name.replace('.py','')}"
    exit_code = os.system(f"python -m {module}")
    if exit_code != 0:
        log(f"WARNING: {script_name} exited with code {exit_code}")
    else:
        log(f"{script_name} completed successfully.")

if __name__ == "__main__":
    log("=== Phase 1 ===")

    scripts = [
        #"populate_db.py",   # Llena la DB con metadata inicial
        #"hash_files.py",    # Calcula hashes de los archivos
        "generate_phase1_report.py"    
    ]

    for script in scripts:
        run_script(script)

    log("=== Phase 1 completed ===")
