import os
import time
from typing import Dict, Optional
import psycopg2
from psycopg2 import OperationalError

from messaging.redis_client import RedisQueueClient
from schemas.result import ResultMessage, Status
from schemas.task import TargetModel
from scripts.producer import send_task
from scripts.config.general import LOG_FILE


def log(msg: str) -> None:
    """Log a mensaje tanto a archivo como a stdout."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def get_db_connection(retries: int = 10, delay: int = 3):
    """ Intenta establecer una conexión a la base de datos con retries y backoff exponencial.
    Esto es útil para manejar situaciones donde la base de datos aún no está lista o hay problemas temporales de conexión."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb")),
                user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user")),
                password=os.getenv(
                    "PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass")
                ),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after multiple attempts.")


def extract_text_from_result(result: ResultMessage) -> Optional[str]:
    """
    Extrae el texto del campo 'result' de un ResultMessage de OCR.
    
    El campo 'result' puede ser:
    - Un string directamente (el texto extraído)
    - Un dict con campos como 'text', 'content', etc.
    - None si hay error
    """
    if result.status != Status.SUCCESS:
        if result.error:
            log(f"Error en OCR: {result.error.type} - {result.error.message}")
        return None

    if result.result is None:
        return None

    # Si result.result es un string, lo devolvemos directamente
    if isinstance(result.result, str):
        return result.result

    # Si es un dict, buscamos campos comunes donde puede estar el texto
    if isinstance(result.result, dict):
        # Intentamos campos comunes: 'text', 'content', 'extracted_text', etc.
        for key in ["text", "content", "extracted_text", "ocr_text", "text_content"]:
            if key in result.result and isinstance(result.result[key], str):
                return result.result[key]
        
        # Si no encontramos, devolvemos el dict serializado como string
        log(f"Warning: result.result es dict pero no tiene campo de texto conocido: {list(result.result.keys())}")
        return str(result.result)

    # Si es otro tipo, lo convertimos a string
    return str(result.result)


def send_ocr_tasks(conn, correlation_to_file_id: Dict[str, int]) -> int:
    """
    Lee documentos con ocr_needed=true y (text_excerpt IS NULL OR text_excerpt = ''),
    envía tareas OCR y guarda el mapeo correlation_id -> file_id.
    
    Devuelve el número de tareas enviadas.
    """
    cur = conn.cursor()
    
    cur.execute(
        """
        SELECT id, full_path
        FROM files
        WHERE ocr_needed = TRUE
          AND (text_excerpt IS NULL OR text_excerpt = '')
        ORDER BY id
        """
    )
    
    rows = cur.fetchall()
    sent_count = 0
    
    for file_id, full_path in rows:
        try:
            # Enviamos la tarea y obtenemos message_id y correlation_id
            message_id, correlation_id = send_task(
                file_path=full_path,
                target_model=TargetModel.OCR,
                source="process_ocr_tasks"
            )
                        
            cur.execute(
                """
                INSERT INTO ocr_task_map (correlation_id, file_id)
                VALUES (%s, %s)
                ON CONFLICT (correlation_id) DO NOTHING
                """,
                (correlation_id, file_id),
            )
            conn.commit()

            
            log(f"Enviada tarea OCR para archivo {file_id} ({full_path}) - correlation_id: {correlation_id}")
            sent_count += 1
            
        except Exception as e:
            log(f"Error enviando tarea para archivo {file_id}: {e}")
    
    cur.close()
    return sent_count


def process_ocr_results(
    conn,
    correlation_to_file_id: Dict[str, int],
    max_results: Optional[int] = None,
) -> int:
    """
    Consume resultados de OCR y actualiza text_excerpt en la BD.
    
    Args:
        conn: Conexión a PostgreSQL
        correlation_to_file_id: Dict para hacer match correlation_id -> file_id
        max_results: Máximo número de resultados a procesar (None = ilimitado)
    
    Devuelve el número de resultados procesados exitosamente.
    """
    mq_client = RedisQueueClient()
    RESULT_QUEUE = "cola_resultados"
    
    processed_count = 0
    
    def handle_result(result_dict: dict):
        """ Maneja un resultado recibido de la cola. Valida el mensaje, extrae el file_id usando el correlation_id, extrae el texto del resultado y actualiza la base de datos. Si ocurre algún error durante el procesamiento, lo loguea y continúa con el siguiente resultado. """
        nonlocal processed_count
        
        try:
            result = ResultMessage.model_validate(result_dict)
            
            # Solo procesamos resultados de OCR
            if result.model != "OCRWorker":
                log(f"Ignorando resultado de modelo '{result.model}' (esperado 'OCRWorker')")
                return
            
            cur = conn.cursor()

            cur.execute(
                "SELECT file_id FROM ocr_task_map WHERE correlation_id = %s",
                (result.correlation_id,)
            )

            row = cur.fetchone()

            if not row:
                log(f"Info: correlation_id no encontrado en BD: {result.correlation_id}")
                cur.close()
                return

            file_id = row[0]
            cur.close()

            
            if file_id is None:
                log(f"Warning: No se encontró file_id para correlation_id {result.correlation_id}")
                return
            
            # Extraemos el texto
            text = extract_text_from_result(result)
            
            if text is None:
                log(f"Error: No se pudo extraer texto del resultado para file_id {file_id}")
                return
            
            # Actualizamos la BD
            cur = conn.cursor()

            cur.execute(
                """
                UPDATE files
                SET text_excerpt = %s,
                    text_chars_extracted = %s,
                    last_seen = NOW()
                WHERE id = %s
                """,
                (text, len(text), file_id),
            )

            cur.execute(
                "DELETE FROM ocr_task_map WHERE correlation_id = %s",
                (result.correlation_id,)
            )

            conn.commit()
            cur.close()
            
            processed_count += 1
            log(f"Actualizado file_id {file_id} con {len(text)} caracteres extraídos")
            
            # Si alcanzamos el máximo, paramos
            if max_results and processed_count >= max_results:
                raise KeyboardInterrupt("Alcanzado máximo de resultados")
                
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log(f"Error procesando resultado: {e}")
    
    try:
        # Consumo bloqueante infinito hasta KeyboardInterrupt o max_results
        mq_client.consume(RESULT_QUEUE, handle_result)
            
    except KeyboardInterrupt:
        log("Interrumpido por el usuario o límite alcanzado")
    
    return processed_count


def main():
    """
    Proceso principal:
    1. Lee documentos con ocr_needed=true y text_excerpt IS NULL
    2. Envía tareas OCR usando producer.send_task
    3. Consume resultados y actualiza text_excerpt en BD
    
    Nota: Este script asume que el servidor OCR devuelve el mismo
    correlation_id que se envió en el TaskMessage. Si no es así,
    necesitarás ajustar la lógica de matching.
    """
    log("=" * 60)
    log("Iniciando procesamiento de tareas OCR")
    log("=" * 60)
    
    # Conectamos a BD
    conn = get_db_connection()
    
    # Mapeo temporal: correlation_id -> file_id
    # Este dict se llena cuando enviamos las tareas y se usa para hacer
    # match cuando llegan los resultados.
    correlation_to_file_id: Dict[str, int] = {}
    
    # Fase 1: Enviar tareas
    log("\nFase 1: Enviando tareas OCR...")
    sent_count = send_ocr_tasks(conn, correlation_to_file_id)
    log(f"Enviadas {sent_count} tareas OCR")
    
    if sent_count == 0:
        log("No hay documentos pendientes de OCR. Saliendo.")
        conn.close()
        return
    
    # Fase 2: Consumir resultados
    log("\nFase 2: Consumiendo resultados...")
    log("Presiona Ctrl+C para detener el consumo de resultados")
    
    processed_count = process_ocr_results(
        conn,
        correlation_to_file_id,
        max_results=None,  # Procesa todos los resultados disponibles
    )
    
    log(f"\nProcesados {processed_count} resultados exitosamente")
    log("=" * 60)
    
    conn.close()


if __name__ == "__main__":
    main()
