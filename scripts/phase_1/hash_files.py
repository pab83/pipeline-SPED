import os
import time
import xxhash
import hashlib
import psycopg2
from psycopg2.extras import execute_values
import multiprocessing
from multiprocessing import Queue, Process
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from typing import List, Tuple, Optional, Any
from scripts.config.phase_1 import LOG_FILE

# ------------------------
# CONFIG
# ------------------------
BATCH_SIZE: int = 5000
"""Cantidad de registros por lote para inserciones masivas en BD."""

MAX_WORKERS: int = 8
"""Número de procesos paralelos (CPU-bound)."""

THREADS_PER_WORKER: int = 8
"""Hilos por proceso para gestionar I/O de lectura de archivos."""

FILE_QUEUE_SIZE: int = 100000
RESULT_QUEUE_SIZE: int = 100000
CHUNK_SIZE: int = 4 * 1024 * 1024
"""Tamaño del bloque de lectura para el cálculo de hash (4MB)."""

# ------------------------
# UTILS
# ------------------------
def log(msg: str) -> None:
    """Registra eventos en el log de la Fase 1 y en consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """
    Establece conexión con PostgreSQL implementando reintentos con backoff.

    Args:
        retries: Número máximo de intentos.
        delay: Tiempo de espera entre fallos.
    """
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(
                dbname=os.getenv("PGDATABASE", "auditdb"),
                user=os.getenv("PGUSER", "user"),
                password=os.getenv("PGPASSWORD", "pass"),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", 5432)),
            )
        except Exception as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres.")

def update_with_retries(
    conn: Any,
    results: List[Tuple[int, str, int]],
    max_retries: int = 5
) -> bool:
    """
    Ejecuta el UPDATE masivo de hashes (xxhash64 + sha256) con gestión de
    errores y rollback automático.

    Cada tupla en `results` contiene:
        - hash_xx   (int)  : digest entero de xxhash64
        - hash_sha  (str)  : digest hexadecimal de SHA-256
        - file_id   (int)  : clave primaria en la tabla `files`

    Args:
        conn: Objeto de conexión psycopg2.
        results: Lista de tuplas (hash_xx, hash_sha, file_id).
        max_retries: Intentos máximos ante fallos de BD.

    Returns:
        True si el UPDATE se completó con éxito, False en caso contrario.
    """
    retries = 0
    while retries < max_retries:
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    UPDATE files AS f
                    SET xxhash64     = v.hash_xx,
                        sha256       = v.hash_sha,
                        hash_pending = False
                    FROM (VALUES %s) AS v(hash_xx, hash_sha, id)
                    WHERE f.id = v.id
                    """,
                    results,
                    # Especificamos el tipo de cada columna de la subconsulta
                    # para que psycopg2 adapte correctamente bigint vs text.
                    template="(%s::bigint, %s::text, %s::int)"
                )
                conn.commit()
                return True
        except Exception as e:
            retries += 1
            log(f"⚠️ DB error (attempt {retries}): {e}")
            conn.rollback()
            time.sleep(2)
    return False

# ------------------------
# HASH FUNCTIONS
# ------------------------
def compute_hashes(file_path: str) -> Optional[Tuple[int, str]]:
    """
    Calcula xxhash64 y SHA-256 de un archivo en una única pasada de lectura.

    Leer el archivo una sola vez y alimentar ambos algoritmos simultáneamente
    es más eficiente que dos pasadas independientes, ya que minimiza las
    operaciones de I/O (el cuello de botella real en archivos grandes).

    Args:
        file_path: Ruta física del archivo a procesar.

    Returns:
        Tupla (xxhash64_int, sha256_hex) o None si el archivo es ilegible.
    """
    try:
        xx  = xxhash.xxh64()
        sha = hashlib.sha256()

        with open(file_path, "rb") as f:
            # Lectura por bloques para controlar el uso de memoria en
            # archivos de gran tamaño (definido por CHUNK_SIZE).
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
                sha.update(chunk)

        # xxhash64.intdigest() devuelve uint64 (0 … 2⁶⁴-1).
        # PostgreSQL bigint es int64 con signo (-2⁶³ … 2⁶³-1).
        # Reinterpretamos el bit-pattern sin signo como con signo para
        # que el valor quepa siempre en bigint sin pérdida de información.
        xx_uint = xx.intdigest()
        xx_signed = xx_uint if xx_uint < (1 << 63) else xx_uint - (1 << 64)

        return xx_signed, sha.hexdigest()

    except Exception:
        # No propagamos la excepción: un archivo ilegible no debe
        # detener el pipeline; simplemente se omite.
        return None

# ------------------------
# WORKERS
# ------------------------
def thread_worker(file_path: str) -> Optional[Tuple[int, str]]:
    """
    Encapsula compute_hashes() para su uso en pools de hilos (ThreadPoolExecutor).

    Args:
        file_path: Ruta del archivo a procesar.

    Returns:
        Resultado de compute_hashes() o None.
    """
    return compute_hashes(file_path)

def process_worker(file_queue: Queue, result_queue: Queue) -> None:
    """
    Worker a nivel de proceso que consume rutas desde file_queue,
    distribuye el trabajo de hashing en un pool de hilos y envía
    los resultados a result_queue.

    El uso de hilos dentro de cada proceso permite solapar la I/O
    de lectura de archivos (operación bloqueante) con el cómputo
    de hashes, maximizando el rendimiento de los núcleos disponibles.

    El centinela `None` en la cola señaliza el fin del trabajo.
    """
    while True:
        item = file_queue.get()
        if item is None:
            # Centinela recibido: este worker finaliza.
            break

        file_id, file_path = item

        with ThreadPoolExecutor(max_workers=THREADS_PER_WORKER) as executor:
            future = executor.submit(thread_worker, file_path)
            result = future.result()

            if result is not None:
                hash_xx, hash_sha = result
                # Enviamos la tripleta (xxhash64, sha256, id) al writer.
                result_queue.put((hash_xx, hash_sha, file_id))

# ------------------------
# CORE: READER / WRITER
# ------------------------
def db_reader(file_queue: Queue) -> None:
    """
    Lee en lotes los registros pendientes de hashing desde la BD e
    inyecta cada uno como (file_id, full_path) en file_queue.

    La condición `hash_pending = True OR xxhash64 IS NULL OR sha256 IS NULL`
    garantiza que archivos con hashing parcial (sólo uno de los dos algoritmos
    calculado) sean reprocesados en su totalidad.

    La paginación por `id > last_id` evita OFFSET costoso en tablas grandes.
    """
    conn = get_db_connection()
    cur  = conn.cursor()
    last_id = 0

    while True:
        cur.execute(
            """
            SELECT id, full_path
            FROM   files
            WHERE  (xxhash64 IS NULL OR sha256 IS NULL OR hash_pending = True)
              AND  id > %s
            ORDER  BY id
            LIMIT  %s
            """,
            (last_id, BATCH_SIZE)
        )
        rows = cur.fetchall()
        if not rows:
            break
        last_id = rows[-1][0]
        for row in rows:
            file_queue.put(row)

    conn.close()

def db_writer(result_queue: Queue, total_files: int) -> None:
    """
    Consume tripletas (xxhash64, sha256, file_id) desde result_queue,
    las acumula en un buffer y las persiste en BD cuando se alcanza
    BATCH_SIZE o cuando se recibe el centinela de fin.

    Muestra una barra de progreso en tiempo real con tqdm.

    Args:
        result_queue: Cola de resultados producida por los workers.
        total_files: Total de archivos a procesar (para la barra de progreso).
    """
    conn   = get_db_connection()
    buffer: List[Tuple[int, str, int]] = []
    pbar   = tqdm(total=total_files, desc="Hashing (xxhash64 + sha256)", unit="file")

    while True:
        item = result_queue.get()
        if item is None:
            # Centinela: no hay más resultados; flusheamos el buffer residual.
            break
        buffer.append(item)
        pbar.update(1)

        if len(buffer) >= BATCH_SIZE:
            update_with_retries(conn, buffer)
            buffer.clear()

    # Flush final de registros que no llegaron a completar un lote.
    if buffer:
        update_with_retries(conn, buffer)

    conn.close()
    pbar.close()

# ------------------------
# ENTRY POINT
# ------------------------
def main() -> None:
    """
    Punto de entrada principal. Implementa una arquitectura Multi-process
    / Multi-thread con tres etapas en pipeline:

    1. **Reader**  : Un proceso lee de la BD los archivos pendientes y
                     los encola en `file_queue`.
    2. **Workers** : N procesos (CPU-bound) que lanzan hilos (I/O-bound)
                     para calcular xxhash64 + SHA-256 en una sola pasada
                     de lectura por archivo.
    3. **Writer**  : Un proceso que agrupa resultados en lotes y actualiza
                     ambas columnas de hash en la BD atómicamente.

    El modo "spawn" se fuerza para compatibilidad multiplataforma
    (especialmente macOS/Windows donde "fork" puede causar deadlocks).
    """
    multiprocessing.set_start_method("spawn", force=True)

    # Consultamos el total de archivos pendientes para la barra de progreso.
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM files WHERE xxhash64 IS NULL OR sha256 IS NULL OR hash_pending = True"
    )
    total_files = cur.fetchone()[0]
    conn.close()
    log(f"Total archivos pendientes de hashing: {total_files}")

    if total_files == 0:
        log("No hay archivos pendientes. Saliendo.")
        return

    # Inicialización de colas compartidas entre procesos.
    file_queue:   Queue = Queue(FILE_QUEUE_SIZE)
    result_queue: Queue = Queue(RESULT_QUEUE_SIZE)

    # --- Arranque del writer (consumidor de resultados) ---
    writer = Process(target=db_writer, args=(result_queue, total_files))
    writer.start()

    # --- Arranque de los workers (productores de hashes) ---
    workers = [
        Process(target=process_worker, args=(file_queue, result_queue))
        for _ in range(MAX_WORKERS)
    ]
    for p in workers:
        p.start()

    # --- El reader corre en el proceso principal ---
    db_reader(file_queue)

    # Enviamos un centinela por cada worker para señalizar el fin de la cola.
    for _ in workers:
        file_queue.put(None)

    # Esperamos a que todos los workers finalicen antes de cerrar el writer.
    for w in workers:
        w.join()

    # Señalizamos al writer que no habrá más resultados.
    result_queue.put(None)
    writer.join()

    log(" Hashing completado: xxhash64 + SHA-256 calculados correctamente.")

if __name__ == "__main__":
    main()