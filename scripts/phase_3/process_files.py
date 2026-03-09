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
BATCH_SIZE = 50

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
    """ Clasifica un documento usando un LLM. Recibe un dict con campos del archivo (incluyendo full_path, file_type, text_excerpt y descripcion_imagen) y construye un prompt para el LLM que le pide inferir la categoría, proyecto y año del documento. La función maneja la respuesta del LLM, parsea el JSON resultante y devuelve un dict con las claves 'categoria', 'proyecto' y 'anio'. Si ocurre algún error durante la clasificación o el JSON es inválido, devuelve 'Desconocido' para cada campo. Se implementan retries para manejar errores temporales de conexión o respuestas 400 Bad Request del LLM."""
    file['text_excerpt'] = sanitize_text(file.get('text_excerpt', ''), max_chars=3000)
    file['full_path'] = sanitize_text(file.get('full_path', ''), max_chars=1000)

    prompt = f"""
Eres un clasificador documental. Categorias: Factura, Presupuesto, Boletines, Informe, Fotografia, Otro. 

Para cada archivo:
- Usa el full_path del archivo para inferir el proyecto o tarea al que pertenece (El proyecto se suele indicar con código de 6 letras, por ejemplo CSBORA, CSBORB, CSBORC). 
- Usa el creation_year proporcionado SOLO si no hay un año explícito en el texto del archivo.
- Responde SOLO con un objeto JSON con keys "categoria", "anio" y "proyecto".
- Si no puedes determinar alguno de los campos, pon "Desconocido" para ese campo.

Ruta completa del archivo: {file['full_path']}
Tipo archivo: {file['file_type']}
Año de creación: {file.get('creation_year', 'Desconocido')}


Texto OCR:
{file['text_excerpt']}

Descripcion imagen:
{file.get('descripcion_imagen', '')} 
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
        json.dumps(payload)
    except Exception as e:
        logger.error(f"JSON inválido para archivo {file.get('full_path', 'Desconocido')}: {e}")
        return {"categoria": "Desconocido", "proyecto": "Desconocido", "anio": "Desconocido"}

    for attempt in range(3):
        try:
            response = requests.post(LLM_URL, json=payload, timeout=60)
            response.raise_for_status()
            raw_text = response.json()["choices"][0]["message"]["content"].strip()

            # Parsear JSON
            resultado = json.loads(clean_llm_json(raw_text))
            return {
                "categoria": resultado.get("categoria", "Desconocido"),
                "anio": resultado.get("anio", "Desconocido"),
                "proyecto": resultado.get("proyecto", "Desconocido")
            }

        except json.JSONDecodeError:
            logger.error(f"Respuesta JSON inválida para {file.get('full_path', 'Desconocido')}: {raw_text}")
            return {"categoria": "Desconocido", "proyecto": "Desconocido", "anio": "Desconocido"}
        except requests.HTTPError as e:
            if e.response and e.response.status_code == 400:
                logger.warning(f"400 Bad Request para {file.get('full_path', 'Desconocido')}, intento {attempt+1}/3")
                import time
                time.sleep(2)
                continue
            else:
                logger.error(f"Error clasificando archivo {file.get('full_path', 'Desconocido')}: {e}")
                return {"categoria": "Desconocido", "proyecto": "Desconocido", "anio": "Desconocido"}
        except Exception as e:
            logger.error(f"Error clasificando archivo {file.get('full_path', 'Desconocido')}: {e}")
            return {"categoria": "Desconocido", "proyecto": "Desconocido", "anio": "Desconocido"}

    logger.error(f"No se pudo clasificar {file.get('full_path', 'Desconocido')} tras 3 intentos por 400 Bad Request")
    return {"categoria": "Desconocido", "proyecto": "Desconocido", "anio": "Desconocido"}

# ----------------------------
# FUNCIONES AUXILIARES
# ----------------------------
def sanitize_text(text: str, max_chars: int = 3000) -> str:
    """ Limpia el texto de caracteres no imprimibles, asegura que esté en UTF-8 y lo trunca a un máximo de caracteres si es necesario. Esto es útil para evitar problemas con texto corrupto o demasiado largo al enviarlo al LLM. Devuelve el texto limpio y truncado si es necesario."""
    if not text:
        return ""
    text = text.encode("utf-8", errors="replace").decode("utf-8")
    text = re.sub(r"[\x00-\x09\x0b-\x1f\x7f]", " ", text)
    if len(text) > max_chars:
        text = text[:max_chars] + " …[truncated]…"
    return text

def clean_llm_json(raw_text: str) -> str:
    """
    Elimina backticks y encabezados de tipo ```json para obtener JSON limpio.
    """
    # Eliminamos ```json o ``` al inicio y ``` al final
    cleaned = re.sub(r"^```(json)?", "", raw_text.strip())
    cleaned = re.sub(r"```$", "", cleaned.strip())
    return cleaned.strip()
# ----------------------------
# PROCESAMIENTO DE BBDD
# ----------------------------
def procesar_archivos():
    """ Procesa archivos que aún no tienen categoría, obteniendo su texto y descripción de imagen, clasificándolos con el LLM y actualizando la base de datos con la categoría, proyecto y año inferidos. El script se conecta a la base de datos, crea la columna 'categoria' si no existe, selecciona archivos sin categoría, procesa cada archivo llamando a la función de clasificación y actualiza la base de datos con los resultados. Se utiliza tqdm para mostrar el progreso del procesamiento."""
    conn_str = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    
    with psycopg2.connect(conn_str, cursor_factory=DictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='files' AND column_name='categoria'
                    ) THEN
                        ALTER TABLE files ADD COLUMN categoria TEXT;
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='files' AND column_name='last_classified'
                    ) THEN
                        ALTER TABLE files ADD COLUMN last_classified TIMESTAMP;
                    END IF;
                END$$;
            """)
            conn.commit()

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
                resultado = clasificar_documento(row)
                categoria = resultado["categoria"]
                proyecto  = resultado["proyecto"]
                anio      = resultado["anio"]
                
                # Guardar en BBDD (descomentarlo si quieres)
                # cur.execute("""
                #     UPDATE files
                #     SET categoria = %s,
                #         last_classified = NOW()
                #     WHERE id = %s
                # """, (categoria, row['id']))
                # conn.commit()

                logger.info(f"Archivo {row.get('full_path', 'Desconocido')} clasificado como '{categoria}', pertenece al proyecto '{proyecto}' y año '{anio}'")

            logger.info("Batch finalizado.")

# ----------------------------
# EJECUCIÓN
# ----------------------------
if __name__ == "__main__":
    procesar_archivos()
