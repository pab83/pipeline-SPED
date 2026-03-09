import os
import psycopg2
from psycopg2 import OperationalError
from PyPDF2 import PdfReader
from docx import Document


from scripts.config.general import LOG_FILE
from scripts.config.phase_2 import TEXT_CHAR_LIMIT


def log(msg: str) -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def get_db_connection(retries: int = 10, delay: int = 3):
    """Intenta establecer una conexión a la base de datos con retries y backoff exponencial.
    Esto es útil para manejar situaciones donde la base de datos aún no está lista o hay problemas temporales de conexión."""
    import time

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb")),
                user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user")),
                password=os.getenv(
                    "PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass")
                ),
                host=os.getenv("PGHOST"),
                port=int(os.getenv("PGPORT", "5432")),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)

    raise RuntimeError("Could not connect to Postgres after multiple attempts.")


def safe_read_text_file(path: str, max_chars: int) -> str:
    """Lee un archivo de texto con diferentes codificaciones, limitando la cantidad de caracteres leídos.
    Intenta leer el archivo con varias codificaciones comunes (UTF-8, Latin-1, CP1252) y devuelve el contenido leído hasta el límite de caracteres especificado. Si ocurre algún error durante la lectura con una codificación, se intenta con la siguiente. Si todas las codificaciones fallan, se devuelve una cadena vacía."""   
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            with open(path, "r", encoding=enc, errors="ignore") as f:
                content = f.read(max_chars + 1000)
                return content[:max_chars]
        except Exception:
            continue
    return ""


def extract_text_from_pdf(path: str, max_chars: int, ocr_needed: bool) -> str:
    """Extrae texto de un archivo PDF, limitando la cantidad de caracteres extraídos.
    Si ocr_needed es True, se omite la extracción (se deja para una futura fase de OCR). De lo contrario, se utiliza PyPDF2 para extraer texto de las páginas del PDF, acumulando texto hasta alcanzar el límite de caracteres especificado. Si ocurre algún error durante la lectura, se captura y se devuelve una cadena vacía."""
    if ocr_needed:
        log(f"Skipping OCR for now (ocr_needed=True) for: {path}")
        return ""

    try:
        reader = PdfReader(path)
        parts = []
        total_len = 0

        for page in reader.pages:
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""

            if text:
                parts.append(text)
                total_len += len(text)

            if total_len >= max_chars:
                break

        return "\n".join(parts)[:max_chars]

    except Exception as e:
        log(f"Error reading PDF {path}: {e}")
        return ""


def extract_text_from_docx(path: str, max_chars: int) -> str:
    """Extrae texto de un archivo DOCX, limitando la cantidad de caracteres extraídos.
    Lee los párrafos y tablas del documento, acumulando texto hasta alcanzar el límite de caracteres especificado. Si ocurre algún error durante la lectura, se captura y se devuelve una cadena vacía."""
    try:
        doc = Document(path)
        parts = []
        total_len = 0

        # Párrafos
        for p in doc.paragraphs:
            if p.text:
                parts.append(p.text)
                total_len += len(p.text)
                if total_len >= max_chars:
                    break

        # Tablas
        if total_len < max_chars:
            for table in doc.tables:
                for row in table.rows:
                    for cell in row.cells:
                        if cell.text:
                            parts.append(cell.text)
                            total_len += len(cell.text)
                            if total_len >= max_chars:
                                break

        return "\n".join(parts)[:max_chars]

    except Exception as e:
        log(f"Error reading DOCX {path}: {e}")
        return ""


def main(batch_size: int = 500):
    """Extrae texto de archivos PDF, DOCX y TXT que aún no tienen texto extraído en la base de datos.
    Para PDFs, si ocr_needed es True, se omite la extracción (se deja para una futura fase de OCR). Para otros archivos, se extrae el texto usando métodos específicos según el tipo de archivo. El texto extraído se guarda en la base de datos junto con la cantidad de caracteres extraídos."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Log DB real
    try:
        cur.execute("SELECT inet_server_addr(), inet_server_port()")
        addr, port = cur.fetchone()
        log(f"Connected to DB at {addr}:{port}")
    except Exception as e:
        log(f"Could not determine DB server address: {e}")

    log("Starting text extraction for Phase 2...")
    total_processed = 0

    while True:
        cur.execute(
            """
            SELECT id
            FROM files
            WHERE text_excerpt IS NULL
            ORDER BY id
            LIMIT %s
            """,
            (batch_size,),
        )

        ids = [row[0] for row in cur.fetchall()]
        if not ids:
            break

        log(f"Processing batch of {len(ids)} files...")

        updates = []

        for file_id in ids:
            cur.execute(
                """
                SELECT full_path, file_type, is_pdf, ocr_needed
                FROM files
                WHERE id = %s
                """,
                (file_id,),
            )

            row = cur.fetchone()
            if not row:
                continue

            full_path, file_type, is_pdf, ocr_needed = row

            if not full_path or not os.path.exists(full_path):
                log(f"Path not found, skipping id={file_id}: {full_path}")
                updates.append(("", 0, file_id))
                total_processed += 1
                continue

            text = ""

            try:
                if is_pdf:
                    text = extract_text_from_pdf(
                        full_path, TEXT_CHAR_LIMIT, bool(ocr_needed)
                    )

                    if ocr_needed:
                        updates.append(("", 0, file_id))
                        total_processed += 1
                        continue

                else:
                    ext = (file_type or "").lower()

                    if ext in (".txt", ".log", ".md"):
                        text = safe_read_text_file(full_path, TEXT_CHAR_LIMIT)

                    elif ext == ".docx":
                        text = extract_text_from_docx(full_path, TEXT_CHAR_LIMIT)

            except Exception as e:
                log(f"Error extracting text for id={file_id}: {e}")
                updates.append(("", 0, file_id))
                total_processed += 1
                continue

            if text:
                updates.append((text, len(text), file_id))
            else:
                updates.append(("", 0, file_id))

            total_processed += 1

        if updates:
            cur.executemany(
                """
                UPDATE files
                SET text_excerpt = %s,
                    text_chars_extracted = %s
                WHERE id = %s
                """,
                updates,
            )
            conn.commit()
            log(
                f"Committed {len(updates)} files "
                f"(total processed {total_processed})."
            )

    cur.close()
    conn.close()
    log("Text extraction for Phase 2 completed.")


if __name__ == "__main__":
    main()

