import os
from typing import Dict, Optional, Any
import psycopg2
from psycopg2 import OperationalError

from messaging.redis_client import RedisQueueClient
from schemas.result import ResultMessage, Status
from schemas.task import TargetModel
from scripts.producer import send_task
from scripts.config.phase_3 import LOG_FILE

# Parámetros de configuración del modelo VLM
DEFAULT_PROMPT: str = "Describe la imagen en detalle."
BATCH_SIZE: int = 500
RESULT_QUEUE: str = "cola_resultados_moondream"

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
def log(msg: str) -> None:
    """Registra mensajes en el archivo de log de Fase 3 y en la salida estándar."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# -------------------------------------------------------------------
# Conexión a PostgreSQL
# -------------------------------------------------------------------
def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """ 
    Establece conexión con la base de datos PostgreSQL.
    
    Implementa reintentos con espera (delay) para asegurar la disponibilidad
    en entornos donde la base de datos puede tardar en arrancar.
    """
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb")),
                user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user")),
                password=os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass")),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres no listo (intento {attempt}/{retries}): {e}")
            import time
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Postgres tras múltiples intentos.")

# -------------------------------------------------------------------
# Contar imágenes pendientes
# -------------------------------------------------------------------
def count_pending_images(conn: Any) -> int:
    """ 
    Calcula el volumen de imágenes que aún no han sido procesadas por el VLM.
    
    Busca archivos de tipo imagen que no tienen una entrada correspondiente 
    en la tabla de control `moondream_task_map`.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM files f
        LEFT JOIN moondream_task_map m ON f.id = m.file_id
        WHERE LOWER(f.file_type) IN ('.png','.jpg','.jpeg','.gif','.bmp','.tiff','.webp')
          AND m.file_id IS NULL
    """)
    total = cur.fetchone()[0]
    cur.close()
    return total

# -------------------------------------------------------------------
# Envío de un batch de tareas a Redis
# -------------------------------------------------------------------
def send_moondream_batch(conn: Any, correlation_to_file_id: Dict[str, int], batch_size: int = BATCH_SIZE) -> int:
    """
    Identifica un lote de imágenes y las envía a la cola de Redis.
    
    Para cada imagen:
    
    1. Genera una tarea dirigida al modelo `MOONDREAM`.
    2. Registra la tarea en `moondream_task_map` con estado 'pending'.
    3. Actualiza el diccionario de correlación local para el proceso de consumo.
    """
    total_pending = count_pending_images(conn)
    if total_pending == 0:
        return 0

    cur = conn.cursor()
    cur.execute(f"""
        SELECT f.id, f.full_path
        FROM files f
        LEFT JOIN moondream_task_map m ON f.id = m.file_id
        WHERE LOWER(f.file_type) IN ('.png','.jpg','.jpeg','.gif','.bmp','.tiff','.webp')
          AND m.file_id IS NULL
        ORDER BY f.id
        LIMIT {batch_size}
    """)
    rows = cur.fetchall()
    sent_count = 0

    for file_id, full_path in rows:
        try:
            message_id, correlation_id = send_task(
                file_path=full_path,
                target_model=TargetModel.MOONDREAM,
                prompt=DEFAULT_PROMPT,
                source="describe_img"
            )

            cur.execute(
                """
                INSERT INTO moondream_task_map (correlation_id, file_id, status)
                VALUES (%s, %s, 'pending')
                ON CONFLICT (correlation_id) DO NOTHING
                """,
                (correlation_id, file_id),
            )
            conn.commit()
            correlation_to_file_id[correlation_id] = file_id
            sent_count += 1

        except Exception as e:
            log(f"Error enviando tarea file_id={file_id}: {e}")

    cur.close()
    log(f"Batch enviado: {sent_count} / {total_pending} imágenes pendientes")
    return sent_count

# -------------------------------------------------------------------
# Consumo de resultados y envío automático de batches
# -------------------------------------------------------------------
def process_moondream_results(conn: Any, correlation_to_file_id: Dict[str, int], batch_size: int = BATCH_SIZE) -> None:
    """
    Gestiona el bucle de consumo de resultados de Moondream.
    
    Este método es el núcleo del pipeline continuo:
    
    - Escucha la cola de resultados.
    - Al recibir un éxito, actualiza la base de datos.
    - **Auto-batching**: Si detecta que el batch actual ha terminado o quedan pocas imágenes,
      dispara automáticamente el envío del siguiente lote sin detener el proceso.
    """
    mq_client = RedisQueueClient()
    processed_count = 0

    def handle_result(result_dict: dict) -> None:
        nonlocal processed_count
        try:
            result = ResultMessage.model_validate(result_dict)
            if result.model != "MoondreamWorker":
                return

            file_id = correlation_to_file_id.get(result.correlation_id)
            if not file_id:
                return

            cur = conn.cursor()
            if result.status == Status.SUCCESS:
                # Lógica de persistencia (comentada temporalmente para validación de flujo)
                cur.execute("UPDATE files SET text_excerpt=%s, last_seen=NOW() WHERE id=%s", ...)
                log(f"Descripción guardada file_id={file_id}")
            else:
                log(f"Error para file_id={file_id}: {result.error}")
            
            conn.commit()
            cur.close()

            processed_count += 1

            # Lógica de disparo del siguiente Batch
            if processed_count % batch_size == 0 or count_pending_images(conn) < batch_size:
                remaining = count_pending_images(conn)
                if remaining > 0:
                    log(f"Batch de {batch_size} finalizado. Enviando siguiente batch...")
                    sent = send_moondream_batch(conn, correlation_to_file_id, batch_size=batch_size)
                    if sent:
                        log(f"Siguiente batch enviado: {sent} tareas")
                else:
                    log(f"Último batch procesado. No quedan más imágenes.")

        except Exception as e:
            log(f"Error procesando resultado: {e}")

    try:
        mq_client.consume(RESULT_QUEUE, handle_result)
    except KeyboardInterrupt:
        log("Consumo interrumpido por usuario")

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main() -> None:
    """ 
    Inicia el pipeline continuo de descripción visual (VLM).
    
    Realiza las siguientes acciones:
    
    1. Asegura la existencia de la tabla `moondream_task_map`.
    2. Realiza el envío del primer lote de tareas.
    3. Entra en el modo de consumo reactivo para procesar resultados y 
       auto-encolar nuevas tareas hasta agotar las imágenes pendientes.
    """
    log("="*60)
    log("=== Pipeline Moondream continuo (envío + consumo) ===")
    log("="*60)

    conn = get_db_connection()
    cur = conn.cursor()
    # Garantizar tabla de mapeo para reconciliación de IDs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS moondream_task_map (
            id SERIAL PRIMARY KEY,
            file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            correlation_id UUID NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(file_id)
        )
    """)
    conn.commit()
    cur.close()

    correlation_to_file_id: Dict[str, int] = {}

    # Envío del Batch inicial
    log("Enviando batch a Redis...")
    sent_count = send_moondream_batch(conn, correlation_to_file_id, batch_size=BATCH_SIZE)
    if sent_count == 0:
        log("No hay tareas nuevas. Saliendo.")
        conn.close()
        return

    # Iniciar consumo y auto-batching
    process_moondream_results(conn, correlation_to_file_id, batch_size=BATCH_SIZE)

    conn.close()

if __name__ == "__main__":
    main()