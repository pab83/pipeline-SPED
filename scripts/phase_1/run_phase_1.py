import os
from ..config.phase_1 import LOG_FILE

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def run_script(script_name):
    log(f"=== Running {script_name} ===")
    exit_code = os.system(f"python scripts/phase_1/{script_name}")
    if exit_code != 0:
        log(f"WARNING: {script_name} exited with code {exit_code}")
    else:
        log(f"{script_name} completed successfully.")

if __name__ == "__main__":
    log("=== Phase 1: Populate DB and calculate hashes ===")

    scripts = [
        "populate_db.py",
        "hash_files.py"
    ]

    for script in scripts:
        run_script(script)

    log("=== Phase 1 completed ===")
