import os
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader
from PyPDF2.generic import IndirectObject
from tqdm import tqdm
from typing import Optional
from scripts.config.phase_0 import LOG_FILE

# --- CONFIGURACIÓN Y LOGS ---
BATCH_SIZE = 1000 
"""Tamaño del lote para realizar commits en la base de datos y optimizar la escritura."""

MAX_WORKERS = 4
"""Número de hilos paralelos para el análisis de cabeceras de PDF."""

def log(msg):
    """Registra mensajes en el archivo de log de la Fase 0 y en la consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def pdf_needs_ocr(pdf_path: str) -> Optional[bool]:
    """
    Analiza la estructura interna del PDF para detectar capas de texto.

    Busca diccionarios de fuentes (`/Font`) en los recursos de cada página. 
    Si encuentra fuentes, asume que el PDF tiene texto digital y no requiere OCR.

    Args:
        pdf_path: Ruta absoluta al archivo PDF.

    Returns:
        bool: `True` si es un PDF escaneado (imagen), `False` si tiene texto, 
              `None` si hubo un error de lectura.
    """
    try:
        if not os.path.exists(pdf_path):
            log(f"DEBUG: No encuentro el archivo en: {pdf_path}")
            return None

        reader = PdfReader(pdf_path)

        for page in reader.pages:
            resources = page.get("/Resources")

            # Resolver IndirectObject para PDFs con estructuras complejas
            if isinstance(resources, IndirectObject):
                resources = resources.get_object()

            if resources and isinstance(resources, dict):
                if "/Font" in resources:
                    return False  # Tiene fuentes → texto digital detectado

        return True  # No encontró fuentes → es una imagen o escaneo

    except Exception as e:
        log(f"Error procesando {pdf_path}: {e}")
        return None
    
def get_db_connection():
    """Establece una conexión a PostgreSQL utilizando variables de entorno."""
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "auditdb"),
        user=os.getenv("PGUSER", "user"),
        password=os.getenv("PGPASSWORD", "pass"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432"))
    )

def main():
    """
    Orquesta el proceso de marcado masivo de PDFs en la base de datos.

    Filtra los registros donde `is_pdf = TRUE` y `ocr_needed` es NULL. 
    Actualiza la columna `ocr_needed` basándose en el análisis de `pdf_needs_ocr`.
    
    Características:
    
    1. **Resiliencia**: Solo procesa pendientes (NULL), permitiendo reintentos.
    2. **Paralelismo**: Usa `ThreadPoolExecutor` para no bloquearse en I/O de disco.
    3. **Checkpoints**: Realiza commits cada 1000 registros para asegurar el progreso.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # PRIORIDAD: Solo traer lo que falta (Resiliencia)
    query = "SELECT id, full_path FROM files WHERE is_pdf = TRUE AND ocr_needed IS NULL;"
    cur.execute(query)
    pdf_rows = cur.fetchall()

    if not pdf_rows:
        log("No hay archivos pendientes de procesar.")
        return

    log(f"Total pendientes: {len(pdf_rows)}")

    processed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Lanzamos las tareas
        future_to_id = {executor.submit(pdf_needs_ocr, path): file_id for file_id, path in pdf_rows}
        
        # Barra de progreso
        pbar = tqdm(as_completed(future_to_id), total=len(future_to_id), desc="Analizando PDFs")
        
        for future in pbar:
            file_id = future_to_id[future]
            try:
                needs_ocr = future.result()
                
                # Solo actualizamos si obtuvimos un resultado (True/False)
                # Si fue None (error), lo saltamos para reintentar en la próxima ejecución
                if needs_ocr is not None:
                    cur.execute(
                        "UPDATE files SET ocr_needed = %s WHERE id = %s;",
                        (needs_ocr, file_id)
                    )
                    processed_count += 1

                # CHECKPOINT: Guardado cada 100 archivos
                if processed_count % BATCH_SIZE == 0 and processed_count > 0:
                    conn.commit()
                    log(f"Checkpoint: {processed_count} archivos procesados y guardados en la base de datos.")

            except Exception as e:
                log(f"Error en hilo para ID {file_id}: {e}")

    # Commit final para los restantes   
    conn.commit()
    cur.close()
    conn.close()
    log(f"Proceso finalizado. Se actualizaron {processed_count} archivos.")

if __name__ == "__main__":
    main()