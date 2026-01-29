import os

PHASES = [
    "scripts.phase_0.run_phase_0"
]

def run_phase(module):
    print(f"\n=== Running {module} ===")
    code = os.system(f"python -m {module}")
    if code != 0:
        raise RuntimeError(f"Phase failed: {module}")

if __name__ == "__main__":
    print("=== Starting full pipeline ===")

    for phase in PHASES:
        run_phase(phase)

    print("\n=== Pipeline completed successfully ===")
