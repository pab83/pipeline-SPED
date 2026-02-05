import os
from scripts.config.phase_0 import LOG_FILE

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def run_script(script_name):
    log(f"=== Running {script_name} ===")
    module = f"scripts.phase_0.{script_name.replace('.py','')}"
    exit_code = os.system(f"python -m {module}")
    if exit_code != 0:
        log(f"WARNING: {script_name} exited with code {exit_code}")
    else:
        log(f"{script_name} completed successfully.")

if __name__ == "__main__":
    log("=== Phase 0: Audit files ===")

    scripts = [
        "scan_files.py",           # Generate initial CSV
        "mark_pdf_ocr.py",         # Add OCR flags
        "generate_phase_0_report.py" # Generate audit summary
    ]

    for script in scripts:
        run_script(script)

    log("=== Phase 0 completed ===")

