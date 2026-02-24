import os
import sys
import subprocess
from scripts.helpers.db_status import *

RUN_ID = int(os.environ.get("RUN_ID", 0))  
PHASES = [
    "scripts.phase_0.run_phase_0",
    "scripts.phase_1.run_phase_1",
    "scripts.phase_2.run_phase_2",
    "scripts.phase_3.run_phase_3",
]

def run_phase(module):
    print(f"\n=== Running {module} ===")
    env = os.environ.copy()

    result = subprocess.run(
        [sys.executable, "-m", module],
        env=env
    )

    if result.returncode != 0:
        raise RuntimeError(f"Phase failed: {module}")
    

def main():
    print("=== Starting full pipeline ===")
    mark_run_started(RUN_ID)
    for phase in PHASES:
        run_phase(phase)
        
    mark_run_finished(RUN_ID)
    print("\n=== Pipeline completed  ===")
    
if __name__ == "__main__":
    main()