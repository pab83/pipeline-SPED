import os
import csv
import time
import traceback
import psycopg2
from psycopg2 import OperationalError
from scripts.config.phase_1 import LOG_FILE, DUPLICATES_FILE, SUMMARY_FILE, REPORTS_DIR

# =============================
# Logging
# =============================
def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# =============================
# DB connection con retry
# =============================
def get_db_connection(retries=10, delay=3):
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

# =============================
# Main
# =============================
def main():
    """Genera un informe de los archivos duplicados en la base de datos.
    Consulta la base de datos para encontrar grupos de archivos con el mismo hash, guarda un CSV con los detalles y genera un informe legible con estadísticas sobre los duplicados encontrados."""
    # Definir por defecto para que no falle si no hay duplicados
    largest_group = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # =============================
        # Query duplicados
        # =============================
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
        duplicates = cur.fetchall()

        total_groups = len(duplicates)
        total_files = sum(row[2] for row in duplicates)
        if duplicates:
            largest_group = duplicates[0]

        # =============================
        # Guardar CSV
        # =============================
        csv_file = DUPLICATES_FILE
        csv_dir = os.path.dirname(csv_file)
        if not csv_dir:
            raise RuntimeError(f"Invalid DUPLICATES_FILE path: {csv_file}")
        os.makedirs(csv_dir, exist_ok=True)

        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["xxhash64", "sha256", "total_files", "paths"])
            for xxh, sha, count, paths in duplicates:
                writer.writerow([xxh, sha, count, "; ".join(paths)])

        # =============================
        # Guardar informe legible
        # =============================
        report_file = os.path.join(REPORTS_DIR, SUMMARY_FILE)
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

# =============================
if __name__ == "__main__":
    main()


