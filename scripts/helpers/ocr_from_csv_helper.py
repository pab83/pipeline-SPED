import os
import csv
import psycopg2
from psycopg2 import OperationalError

from scripts.config.general import LOG_FILE

def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries=10, delay=3):
    import time
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", "auditdb"),
                user=os.getenv("PGUSER", "user"),
                password=os.getenv("PGPASSWORD", "pass"),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", 5432)),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after multiple attempts.")

def update_ocr_needed_from_csv(csv_path: str, batch_size: int = 500):
    """
    Actualiza la columna ocr_needed en la DB según el CSV.
    CSV debe tener al menos las columnas: full_path,ocr_needed
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            updates = []
            total = 0

            for row in reader:
                full_path = row["full_path"]
                ocr_needed_str = row["ocr_needed"].strip().lower()
                ocr_needed = ocr_needed_str in ("1", "true", "yes")
                updates.append((ocr_needed, full_path))

                # Commit por batches
                if len(updates) >= batch_size:
                    cur.executemany(
                        "UPDATE files SET ocr_needed=%s WHERE full_path=%s",
                        updates,
                    )
                    conn.commit()
                    total += len(updates)
                    log(f"Commit batch: {total} filas actualizadas.")
                    updates.clear()

            # Último batch
            if updates:
                cur.executemany(
                    "UPDATE files SET ocr_needed=%s WHERE full_path=%s",
                    updates,
                )
                conn.commit()
                total += len(updates)
                log(f"Commit final: {total} filas actualizadas.")

    except Exception as e:
        log(f"Error actualizando DB desde CSV: {e}")
    finally:
        cur.close()
        conn.close()
        log("Actualización de ocr_needed completada.")

if __name__ == "__main__":
    CSV_PATH = "./resources/csv/file_audit_ocr.csv"  
    update_ocr_needed_from_csv(CSV_PATH)
