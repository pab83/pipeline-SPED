import os
import csv
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from tqdm import tqdm
from scripts.config.phase_0 import BASE_PATH, CSV_FILE, LOG_FILE, MAX_THREADS, BUFFER_SIZE, BATCH_SIZE
from scripts.config.general import CSV_DIR  # Para crear carpeta CSV si no existe

# ================= CONFIG =================

OUTPUT_CSV = CSV_FILE  # Usamos CSV_FILE del config, no hardcode

MAX_THREADS = MAX_THREADS           # Ajustar según sweet spot TrueNAS
BUFFER_SIZE = BUFFER_SIZE
BATCH_SIZE = BATCH_SIZE             # Batch más pequeño = mejor ARC locality
BASE_SEP_COUNT = BASE_PATH.count(os.sep)

# ================= UTILITIES =================

def log(msg):
    """Append message to log file and print"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def list_top_directories(base_path):
    """List first-level directories"""
    with os.scandir(base_path) as it:
        return [
            entry.path
            for entry in it
            if entry.is_dir(follow_symlinks=False)
        ]

def generate_files(base_path):
    """DFS local in one subtree"""
    stack = [base_path]
    while stack:
        path = stack.pop()
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        yield entry
        except Exception:
            continue

def process_file(entry):
    """Extract metadata from a file"""
    try:
        stat = entry.stat()
        ext = os.path.splitext(entry.name)[1].lower()
        full_path = normalize_path(entry.path)

        return [
            full_path,
            entry.name,
            ext,
            stat.st_size,
            datetime.fromtimestamp(stat.st_ctime).year,
            datetime.fromtimestamp(stat.st_mtime).year,
            full_path.count(os.sep) - BASE_SEP_COUNT,
            ext == ".pdf"
        ]
    except Exception:
        return [entry.path, None, None, None, None, None, None, None]

def normalize_path(path: str, base_path: str = None) -> str:
    """
    Normaliza una ruta de archivo:
    - Quita espacios al inicio y al final
    - Convierte separadores a '/' (cross-platform)
    - Opcional: relativiza respecto a base_path
    - Expande ~ y variables de entorno
    """
    if not path:
        return ""

    # Quita espacios, saltos de línea, tabulaciones
    path = path.strip()

    # Expande home y variables de entorno
    path = os.path.expanduser(os.path.expandvars(path))

    # Convierte separadores a '/'
    path = os.path.normpath(path)

    # Asegura que es absoluta si se pasa base_path
    if base_path and not os.path.isabs(path):
        path = os.path.join(base_path, path)
        path = os.path.normpath(path)

    return path

def chunks(iterable, size):
    """Yield successive chunks from iterable"""
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch

# ================= CORE =================

def audit():
    inicio = time.time()
    total = 0
    buffer = []

    roots = list_top_directories(BASE_PATH)
    log(f"Top-level directories detected: {len(roots)}")

    os.makedirs(CSV_DIR, exist_ok=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "full_path", "file_name", "file_type", "size_bytes",
            "creation_year", "modification_year",
            "depth", "is_pdf"
        ])

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            with tqdm(
                desc="Indexing files",
                unit="files",
                dynamic_ncols=True,
                smoothing=0.05
            ) as pbar:

                for root in roots:
                    for batch in chunks(generate_files(root), BATCH_SIZE):
                        futures = [executor.submit(process_file, e) for e in batch]

                        for future in as_completed(futures):
                            buffer.append(future.result())
                            total += 1

                            if total % 100 == 0:
                                pbar.update(100)

                            if len(buffer) >= BUFFER_SIZE:
                                writer.writerows(buffer)
                                buffer.clear()

                if buffer:
                    writer.writerows(buffer)

    duracion = time.time() - inicio
    log(f"\nFiles processed: {total:,}")
    log(f"Total time: {duracion:.2f} seconds")
    if total:
        log(f"Average speed: {total/duracion:.0f} files/s")
    log(f"CSV saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    audit()
