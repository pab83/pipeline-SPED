import os
import csv
import psycopg2
from scripts.config.phase_1 import LOG_FILE, REPORT_DIR

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def main():
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "auditdb"),
        user=os.getenv("PGUSER", "user"),
        password=os.getenv("PGPASSWORD", "pass"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
    )
    cur = conn.cursor()

    log("Generating Phase 1 report on duplicates...")

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

    log(f"Found {total_groups} duplicate groups involving {total_files} files.")

    # =============================
    # Guardar CSV
    # =============================
    os.makedirs(REPORT_DIR, exist_ok=True)
    csv_file = os.path.join(REPORT_DIR, "phase1_duplicates.csv")

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["xxhash64", "sha256", "total_files", "paths"])
        for xxh, sha, count, paths in duplicates:
            writer.writerow([xxh, sha, count, "; ".join(paths)])

    log(f"CSV report saved to {csv_file}")

    # =============================
    # Resumen rápido
    # =============================
    log("=== Phase 1 duplicates summary ===")
    log(f"Duplicate groups: {total_groups}")
    log(f"Total files involved in duplicates: {total_files}")
    if duplicates:
        max_group = duplicates[0]
        log(f"Largest duplicate group: {max_group[2]} files, xxhash64={max_group[0]}")
    log("Phase 1 report generation completed ✅")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
