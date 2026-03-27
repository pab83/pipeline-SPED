import os
import logging
import requests
import psycopg2
from psycopg2.extras import DictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import json
import re
from typing import Dict, Any

# ----------------------------
# CONFIGURACIÓN
# ----------------------------
DB_HOST = os.environ.get("PG_HOST", "db")
DB_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
DB_NAME = os.environ.get("POSTGRES_DB")
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

LLM_URL = os.environ.get("LLM_URL")  
BATCH_SIZE = 50

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def sanitize_text(text: str, max_chars: int = 3000) -> str:
    """
    Limpia y normaliza el texto antes de enviarlo al LLM.
    
    Elimina caracteres no imprimibles que podrían corromper el payload JSON
    y trunca el contenido para respetar la ventana de contexto del modelo.
    """
    if not text: return ""
    text = text.encode("utf-8", errors="replace").decode("utf-8")
    text = re.sub(r"[\x00-\x09\x0b-\x1f\x7f]", " ", text)
    if len(text) > max_chars:
        text = text[:max_chars] + " …[truncated]…"
    return text

def clean_llm_json(raw_text: str) -> str:
    """Elimina markdown blocks (```json) de la respuesta del LLM para extraer el JSON puro."""
    cleaned = re.sub(r"^```(json)?", "", raw_text.strip())
    cleaned = re.sub(r"```$", "", cleaned.strip())
    return cleaned.strip()

def clasificar_documento(file: Dict[str, Any]) -> Dict[str, str]:
    """
    Realiza la inferencia semántica mediante LLM.
    
    Envía metadatos y texto extraído al modelo para determinar:
    
    - **Categoría**: Tipo de documento (Factura, Informe, etc.)
    - **Proyecto**: Identificación de códigos (ej. CSBORA).
    - **Año**: Cronología del documento.
    
    Implementa una política de reintentos (3) ante errores 400 o JSON inválido.
    """
    text_excerpt = sanitize_text(file.get('text_excerpt', ''), max_chars=500)
    path = sanitize_text(file.get('full_path', ''), max_chars=500)

    prompt = f"""
        Eres un clasificador documental. Categorias: Factura, Presupuesto, Boletines, Informe, Fotografia, Otro. 

        Para cada archivo:
        - Usa el full_path y text_excerpt para inferir el proyecto (códigos de 6 letras, por ejemplo CSBORA).
        - Usa file_type, full_path y text_excerpt para decidir a que categoria de clasificación corresponde.
        - Usa el creation_year SOLO si no hay un año explícito en el texto.
        - Responde SOLO con un objeto JSON con keys "categoria", "anio" y "proyecto".

        Ruta: {path}
        Tipo: {file['file_type']}
        Año Creación: {file.get('creation_year', 'Desconocido')}

        Texto OCR o descripción de imagen:
        {text_excerpt}

        """

    payload = {
        "model": "qwen2.5-7b-instruct-q4_k_m",
        "messages": [
            {"role": "system", "content": "Eres un clasificador documental experto."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0,
        "max_tokens": 60
    }

    for attempt in range(3):
        try:
            response = requests.post(LLM_URL, json=payload, timeout=300)
            response.raise_for_status()
            raw_text = response.json()["choices"][0]["message"]["content"].strip()
            
            resultado = json.loads(clean_llm_json(raw_text))
            return {
                "categoria": resultado.get("categoria", "Desconocido"),
                "anio": str(resultado.get("anio", "Desconocido")),
                "proyecto": resultado.get("proyecto", "Desconocido")
            }
        except Exception as e:
            logger.warning(f"Intento {attempt+1}/3 fallido para {path}: {e}")
            if attempt == 2: break
            
    return {"categoria": "Desconocido", "proyecto": "Desconocido", "anio": "Desconocido"}

def procesar_archivos():
    """
    Orquesta el batch de clasificación en bucle continuo.
    
    1. Asegura la existencia de columnas de metadatos en la DB.
    2. Procesa en batches de BATCH_SIZE hasta que no queden registros pendientes.
    3. Actualiza la DB con la inferencia del LLM.
    """
    conn_str = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    
    with psycopg2.connect(conn_str, cursor_factory=DictCursor) as conn:

        # ── Migración: añadir columnas si no existen ──────────────────────────
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE files ADD COLUMN IF NOT EXISTS categoria TEXT;
                ALTER TABLE files ADD COLUMN IF NOT EXISTS proyecto TEXT;
                ALTER TABLE files ADD COLUMN IF NOT EXISTS anio TEXT;
                ALTER TABLE files ADD COLUMN IF NOT EXISTS last_classified TIMESTAMP;
            """)
            conn.commit()

        # ── Bucle principal: un batch por iteración ───────────────────────────
        batch_num = 0
        total_clasificados = 0

        while True:
            batch_num += 1

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM files WHERE categoria IS NULL AND text_excerpt IS NOT NULL LIMIT %s",
                    (BATCH_SIZE,)
                )
                rows = [dict(r) for r in cur.fetchall()]

            if not rows:
                logger.info(
                    f"Sin registros pendientes. "
                    f"Proceso completado: {total_clasificados} archivos clasificados en {batch_num - 1} batch(es)."
                )
                break

            logger.info(f"[Batch {batch_num}] Procesando {len(rows)} registros...")

            # ── Clasificación paralela (4 workers = --parallel 4 del LLM) ────
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(clasificar_documento, row): row for row in rows}

                for future in tqdm(
                    as_completed(futures),
                    total=len(rows),
                    desc=f"Batch {batch_num}",
                ):
                    row = futures[future]
                    try:
                        res = future.result()
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE files
                                SET categoria = %s,
                                    proyecto  = %s,
                                    anio      = %s,
                                    last_classified = NOW()
                                WHERE id = %s
                                """,
                                (res["categoria"], res["proyecto"], res["anio"], row["id"]),
                            )
                            conn.commit()
                        total_clasificados += 1
                        logger.info(f"ID {row['id']} -> {res['categoria']} | Proyecto: {res['proyecto']}")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error en ID {row['id']}: {e}")
                    
if __name__ == "__main__":
    procesar_archivos()
