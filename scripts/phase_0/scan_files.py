import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm
from scripts.helpers.db_status import update_run_status
from scripts.config.phase_0 import BASE_PATH, LOG_FILE, MAX_THREADS, BUFFER_SIZE, BATCH_SIZE

# ================= CONFIG =================

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


def normalize_path(path: str, base_path: str = None) -> str:
    """
    Normaliza una ruta de archivo:
    - Quita espacios
    - Convierte separadores
    - Expande ~ y variables de entorno
    - Opcionalmente relativiza respecto a base_path
    """
    if not path:
        return ""

    path = path.strip()
    path = os.path.expanduser(os.path.expandvars(path))
    path = os.path.normpath(path)

    if base_path and not os.path.isabs(path):
        path = os.path.join(base_path, path)
        path = os.path.normpath(path)

    return path


def process_file(entry):
    """Extract metadata from a file"""
    try:
        stat = entry.stat()
        ext = os.path.splitext(entry.name)[1].lower()
        full_path = normalize_path(entry.path)

        return (
            full_path,
            entry.name,
            ext,
            stat.st_size,
            datetime.fromtimestamp(stat.st_ctime).year,
            datetime.fromtimestamp(stat.st_mtime).year,
            full_path.count(os.sep) - BASE_SEP_COUNT,
            ext == ".pdf"
        )
    except Exception:
        return None


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
    """Escanea el sistema de archivos desde BASE_PATH, extrae metadata de cada archivo y la guarda en la base de datos.
    Utiliza múltiples hilos para acelerar el proceso y una estrategia de buffer para optimizar las inserciones en la base de datos.
    Al finalizar, actualiza el estado del run en la base de datos con el total de archivos procesados y el tiempo de ejecución.
    """
    
    inicio = time.time()
    total = 0
    buffer = []

    # DB connection
    DB_NAME = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb"))
    DB_USER = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user"))
    DB_PASSWORD = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass"))
    DB_HOST = os.getenv("PGHOST", "localhost")
    DB_PORT = int(os.getenv("PGPORT", "5432"))

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    cur = conn.cursor()

    roots = list_top_directories(BASE_PATH)
    log(f"Top-level directories detected: {len(roots)}")

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
                        result = future.result()
                        if result:
                            buffer.append(result)
                            total += 1
                            pbar.update(1)

                        if len(buffer) >= BUFFER_SIZE:
                            execute_batch(
                                cur,
                                """
                                INSERT INTO files (
                                    full_path, file_name, file_type, size_bytes,
                                    creation_year, modification_year,
                                    depth, is_pdf
                                )
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (full_path) DO UPDATE
                                SET
                                    file_name = EXCLUDED.file_name,
                                    size_bytes = EXCLUDED.size_bytes,
                                    modification_year = EXCLUDED.modification_year,
                                    last_seen = NOW()
                                WHERE files.size_bytes != EXCLUDED.size_bytes
                                   OR files.modification_year != EXCLUDED.modification_year;
                                """,
                                buffer
                            )
                            conn.commit()
                            buffer.clear()

            # Flush final buffer
            if buffer:
                execute_batch(
                    cur,
                    """
                    INSERT INTO files (
                        full_path, file_name, file_type, size_bytes,
                        creation_year, modification_year,
                        depth, is_pdf
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (full_path) DO UPDATE
                    SET
                        file_name = EXCLUDED.file_name,
                        size_bytes = EXCLUDED.size_bytes,
                        modification_year = EXCLUDED.modification_year,
                        last_seen = NOW()
                    WHERE files.size_bytes != EXCLUDED.size_bytes
                       OR files.modification_year != EXCLUDED.modification_year;
                    """,
                    buffer
                )
                conn.commit()

    update_run_status(int(os.getenv("RUN_ID")), processed_files=total)

    cur.close()
    conn.close()

    duracion = time.time() - inicio
    log(f"\nFiles processed: {total:,}")
    log(f"Total time: {duracion:.2f} seconds")
    if total:
        log(f"Average speed: {total/duracion:.0f} files/s")


if __name__ == "__main__":
    audit()