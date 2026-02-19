import os

PHASES = [
    "scripts.phase_0.run_phase_0",
    "scripts.phase_1.run_phase_1",
    "scripts.phase_2.run_phase_2",
    #"scripts.phase_3.run_phase_3",
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

    print("\n=== Pipeline completed  ===")
