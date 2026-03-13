import os
import psycopg2
import csv
from scripts.config.general import LARGE_FILE_THRESHOLD
from scripts.config.phase_0 import LOG_FILE,AUDIT_SUMMARY_FILE

REPORT_FILE: str = AUDIT_SUMMARY_FILE
"""Ruta del archivo CSV donde se volcará el resumen del escaneo."""

os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)

def log(msg: str) -> None:
    """
    Registra un mensaje en el log general y lo muestra por consola.
    
    Args:
        msg: Mensaje a registrar.
    """
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def generate_report() -> None:
    """
    Genera un informe detallado en formato CSV sobre el estado de la Fase 0.
    
    Este proceso extrae métricas clave directamente de PostgreSQL:
    
    1.  **Métricas Globales**: Conteo total de archivos, PDFs e incidencia de OCR.
    2.  **Distribución por Tipo**: Agrupación por extensión de archivo.
    3.  **Distribución por Profundidad**: Análisis de la estructura de carpetas.
    4.  **Archivos Grandes**: Listado de archivos que superan el `LARGE_FILE_THRESHOLD`.
    
    Para optimizar el uso de memoria, utiliza un **cursor de servidor** (`itersize`) 
    al procesar el listado de archivos pesados.
    """
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "auditdb"),
        user=os.getenv("PGUSER", "user"),
        password=os.getenv("PGPASSWORD", "pass"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
    )

    with conn.cursor() as cur, open(REPORT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # 1️⃣ MÉTRICAS GLOBALES
        cur.execute("""
            SELECT 
                COUNT(*) AS total_files,
                COUNT(*) FILTER (WHERE is_pdf) AS total_pdfs,
                COUNT(*) FILTER (WHERE is_pdf AND ocr_needed) AS ocr_needed
            FROM files;
        """)
        total_files, pdf_files, ocr_needed_count = cur.fetchone()

        writer.writerow(["Metric", "Value"])
        writer.writerow(["Total files", total_files])
        writer.writerow(["Total PDFs", pdf_files])
        writer.writerow(["PDFs needing OCR", ocr_needed_count])
        writer.writerow([])

        # 2️⃣ DISTRIBUCIÓN POR TIPO
        writer.writerow(["File type distribution", "Count"])
        cur.execute("""
            SELECT file_type, COUNT(*)
            FROM files
            GROUP BY file_type
            ORDER BY COUNT(*) DESC;
        """)
        for row in cur:
            writer.writerow(row)
        writer.writerow([])

        # 3️⃣ DISTRIBUCIÓN POR PROFUNDIDAD
        writer.writerow(["Files per depth", "Count"])
        cur.execute("""
            SELECT depth, COUNT(*)
            FROM files
            GROUP BY depth
            ORDER BY depth;
        """)
        for row in cur:
            writer.writerow(row)
        writer.writerow([])

        # 4️⃣ ARCHIVOS GRANDES (STREAMING)
        writer.writerow(["Large files (over threshold)"])
        writer.writerow(["Path", "Size (bytes)"])

        # Server-side cursor para no cargar todo en RAM
        large_cur = conn.cursor(name="large_files_cursor")
        large_cur.itersize = 10000

        large_cur.execute("""
            SELECT full_path, size_bytes
            FROM files
            WHERE size_bytes >= %s
            ORDER BY size_bytes DESC;
        """, (LARGE_FILE_THRESHOLD,))

        for row in large_cur:
            writer.writerow(row)

        large_cur.close()

    conn.close()
    log(f"Audit summary generated: {REPORT_FILE}")

if __name__ == "__main__":
    generate_report()