import os
import subprocess
from scripts.config.phase_2 import LOG_FILE

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def run_script(script_name):
    log(f"=== Running {script_name} ===")

    module = f"scripts.phase_2.{script_name.replace('.py', '')}"

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
    log("=== Phase 2: Deduplication ===")
    scripts = [
        #"migrate_phase_2.py",
        #"extract_text.py",
        "img_looks_like_document.py",
        #####"compute_embeddings.py",
        #####"dedup.py",
    ]

    for script in scripts:
        run_script(script)

    log("=== Phase 2 completed ===")
