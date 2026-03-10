import os
import csv
import time
import traceback
from typing import Any, Optional, List, Tuple
import psycopg2
from psycopg2 import OperationalError
from scripts.config.phase_1 import LOG_FILE, DUPLICATES_FILE, SUMMARY_FILE, REPORTS_DIR

def log(msg: str) -> None:
    """Registra eventos en el log de la fase y en la consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """
    Establece conexión con la base de datos PostgreSQL con reintentos.
    
    Args:
        retries: Número de intentos antes de fallar.
        delay: Tiempo de espera entre reintentos.
    """
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("PGDATABASE", "auditdb"),
                user=os.getenv("PGUSER", "user"),
                password=os.getenv("PGPASSWORD", "pass"),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
            return conn
        except OperationalError as e:
            log(f"⚠️ Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after multiple attempts.")

def generate_reports() -> None:
    """
    Genera informes analíticos sobre archivos duplicados detectados por hash.
    
    El proceso realiza las siguientes acciones:
    
    1.  **Identificación SQL**: Agrupa registros por `xxhash64` y `sha256` donde el conteo es > 1.
    2.  **Exportación CSV**: Genera un listado técnico detallado en `DUPLICATES_FILE`.
    3.  **Informe Ejecutivo**: Crea un resumen legible en `SUMMARY_FILE` con estadísticas de grupos y rutas.
    
    Este reporte es fundamental para procesos posteriores de deduplicación o ahorro de espacio.
    """
    largest_group: Optional[Tuple[Any, ...]] = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Consulta de duplicados mediante agregación de rutas
        cur.execute("""
            SELECT
                xxhash64,
                sha256,
                COUNT(*) AS total,
                ARRAY_AGG(full_path ORDER BY full_path) AS paths
            FROM files
            WHERE sha256 IS NOT NULL
            GROUP BY xxhash64, sha256
            HAVING COUNT(*) > 1
            ORDER BY total DESC
        """)
        duplicates: List[Tuple[Any, ...]] = cur.fetchall()

        total_groups: int = len(duplicates)
        total_files: int = sum(row[2] for row in duplicates)
        if duplicates:
            largest_group = duplicates[0]

        # Preparación de archivos de salida
        csv_file: str = DUPLICATES_FILE
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)

        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["xxhash64", "sha256", "total_files", "paths"])
            for xxh, sha, count, paths in duplicates:
                writer.writerow([xxh, sha, count, "; ".join(paths)])

        report_file: str = os.path.join(REPORTS_DIR, SUMMARY_FILE)
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

        with open(report_file, "w", encoding="utf-8") as f:
            f.write("=== Phase 1 Duplicate Files Report ===\n\n")
            f.write(f"Total duplicate groups: {total_groups}\n")
            f.write(f"Total files involved in duplicates: {total_files}\n")
            
            if largest_group:
                first_file_name = os.path.basename(largest_group[3][0]) if largest_group[3] else "N/A"
                f.write(f"Largest duplicate group: {largest_group[2]} files, {first_file_name}, xxhash64={largest_group[0]}\n")

            f.write("\nSummary of all duplicate groups:\n")
            for xxh, sha, count, paths in duplicates:
                first_file_name = os.path.basename(paths[0]) if paths else "N/A"
                f.write(f"- {count} files, {first_file_name}, xxhash64={xxh}\n")
                for path in paths:
                    f.write(f"    {path}\n")
            f.write("\nReport generation completed ✅\n")

        log("Phase 1 report generated: " + report_file)
        cur.close()
        conn.close()

    except Exception as e:
        log("❌ Fatal error in generate_phase1_report")
        log(str(e))
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    generate_reports()