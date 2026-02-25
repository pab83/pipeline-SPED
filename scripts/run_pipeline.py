import os
import sys
import subprocess
from scripts.helpers.db_status import *
from scripts.exceptions import PipelineCancelledException

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
    # Si el proceso hijo (la fase) salió con nuestro código 64
    if result.returncode == 64:
        raise PipelineCancelledException()

    if result.returncode != 0:
        raise RuntimeError(f"Phase failed: {module}")
    

def main():
        try:
            print("=== Starting full pipeline ===")
            mark_run_started(RUN_ID)
            for phase in PHASES:
                run_phase(phase)
            
            mark_run_finished(RUN_ID)
            print("\n=== Pipeline completed  ===")
                    
        except PipelineCancelledException:
            print(f"--- Pipeline {RUN_ID} stopped by user ---")
            mark_run_cancelled(RUN_ID)
            sys.exit(0)
        except Exception as e:
            print(f"--- Pipeline {RUN_ID} failed with error: {e} ---")
            mark_run_finished(RUN_ID)  # Marca como finished pero con status "error"
            sys.exit(1)
        
if __name__ == "__main__":
    main()