import os
import logging
import requests
import psycopg2
from psycopg2.extras import DictCursor
from tqdm import tqdm
import json
import re


# ----------------------------
# CONFIG
# ----------------------------
DB_HOST = os.environ.get("PG_HOST", "db")
DB_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
DB_NAME = os.environ.get("POSTGRES_DB")
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

LLM_URL = os.environ.get("LLM_URL")  
BATCH_SIZE = 500

# ----------------------------
# LOGGING
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------------------
# FUNCIÓN DE CLASIFICACIÓN
# ----------------------------
def clasificar_documento(file):
    """
    file: dict con campos de la BBDD
    Devuelve la categoría como string
    """
    # Sanitizamos los campos antes de crear el prompt
    file['text_excerpt'] = sanitize_text(file.get('text_excerpt', ''), max_chars=3000)
    file['file_name'] = sanitize_text(file.get('file_name', ''), max_chars=500)
    #file['descripcion_imagen'] = sanitize_text(file.get('descripcion_imagen', ''), max_chars=1000)

    prompt = f"""
Eres un clasificador documental. Categoriza SOLO en: Factura, Presupuesto, Boletines, Informe, Fotografia, Otro.

Nombre archivo: {file['file_name']}
Tipo archivo: {file['file_type']}
Año de creación: {file['creation_year']}
OCR necesario: {file['ocr_needed']}

Texto OCR:
{file['text_excerpt']}

Descripcion imagen:
{file.get('descripcion_imagen', '')} 

Responde SOLO con la categoria exacta.
"""

    payload = {
        "model": "qwen2.5-3b-instruct",
        "messages": [
            {"role": "system", "content": "Eres un clasificador documental."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0
    }

    try:    
        # Validación rápida de JSON
        json.dumps(payload)  # esto falla si hay caracteres no UTF-8 o None
    except Exception as e:
        logger.error(f"JSON inválido para archivo {file['file_name']}: {e}")
        return None

    # Reintentos para 400 Bad Request
    for attempt in range(3):
        try:
            response = requests.post(LLM_URL, json=payload, timeout=60)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"].strip()
        except requests.HTTPError as e:
            if response.status_code == 400:
                logger.warning(f"400 Bad Request para {file['file_name']}, intento {attempt+1}/3")
                # Pequeña pausa antes de reintentar
                import time
                time.sleep(2)
                continue
            else:
                logger.error(f"Error clasificando archivo {file['file_name']}: {e}")
                return None
        except Exception as e:
            logger.error(f"Error clasificando archivo {file['file_name']}: {e}")
            return None

    logger.error(f"No se pudo clasificar {file['file_name']} tras 3 intentos por 400 Bad Request")
    return None

    
    
# ----------------------------
# FUNCIONES AUXILIARES
# ----------------------------
def sanitize_text(text: str, max_chars: int = 3000) -> str:
    """
    Limpia un texto para enviarlo a un LLM:
    - Reemplaza caracteres no UTF-8 por espacio
    - Elimina caracteres de control
    - Acorta a `max_chars` para evitar requests demasiado largos
    """
    if not text:
        return ""
    
    # Asegura UTF-8 y reemplaza caracteres inválidos
    text = text.encode("utf-8", errors="replace").decode("utf-8")
    
    # Elimina caracteres de control (excepto salto de línea)
    text = re.sub(r"[\x00-\x09\x0b-\x1f\x7f]", " ", text)
    
    # Acorta el texto para no saturar la request
    if len(text) > max_chars:
        text = text[:max_chars] + " …[truncated]…"
    
    return text


# ----------------------------
# PROCESAMIENTO DE BBDD
# ----------------------------
def procesar_archivos():
    conn_str = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    
    # Abrimos conexión con psycopg2
    with psycopg2.connect(conn_str, cursor_factory=DictCursor) as conn:
        with conn.cursor() as cur:
            # Crear columna categoria y last_classified si no existen
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns
                        WHERE table_name='files' AND column_name='categoria'
                    ) THEN
                        ALTER TABLE files ADD COLUMN categoria TEXT;
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns
                        WHERE table_name='files' AND column_name='last_classified'
                    ) THEN
                        ALTER TABLE files ADD COLUMN last_classified TIMESTAMP;
                    END IF;
                END$$;
            """)
            conn.commit()

            # Seleccionar batch de archivos pendientes
            cur.execute("""
                SELECT *
                FROM files
                WHERE categoria IS NULL
                ORDER BY id
                LIMIT %s
            """, (BATCH_SIZE,))
            rows = cur.fetchall()

            if not rows:
                logger.info("No hay archivos pendientes de clasificación.")
                return

            for row in tqdm(rows, desc="Procesando archivos"):
                categoria = clasificar_documento(row)
                if categoria:
                #    cur.execute("""
                #        UPDATE files
                #        SET categoria = %s,
                #            last_classified = NOW()
                #        WHERE id = %s
                #    """, (categoria, row['id']))
                #    conn.commit()
                    logger.info(f"Archivo {row['file_name']} clasificado como '{categoria}'")
                else:
                    logger.warning(f"No se pudo clasificar archivo {row['file_name']}")

            logger.info("Batch finalizado.")

# ----------------------------
# EJECUCIÓN
# ----------------------------
if __name__ == "__main__":
    procesar_archivos()
