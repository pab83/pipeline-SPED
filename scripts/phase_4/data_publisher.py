
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm
from helpers.logs import log

# ---------------------
# Cargar .env
# ---------------------

OLTP_DB_HOST = os.getenv("PGHOST")
OLTP_DB_NAME = os.getenv("POSTGRES_DB")
OLTP_DB_USER = os.getenv("POSTGRES_USER")
OLTP_DB_PASS = os.getenv("POSTGRES_PASSWORD")

OLAP_DB_HOST = os.getenv("OLAP_DB_HOST")
OLAP_DB_NAME = os.getenv("OLAP_DB_NAME")
OLAP_DB_USER = os.getenv("OLAP_DB_USER")
OLAP_DB_PASS = os.getenv("OLAP_DB_PASS")



# ---------------------
# Conexiones
# ---------------------
def get_oltp_connection():
    return psycopg2.connect(
        host=OLTP_DB_HOST,
        dbname=OLTP_DB_NAME,
        user=OLTP_DB_USER,
        password=OLTP_DB_PASS
    )

def get_olap_connection():
    return psycopg2.connect(
        host=OLAP_DB_HOST,
        dbname=OLAP_DB_NAME,
        user=OLAP_DB_USER,
        password=OLAP_DB_PASS
    )


# ---------------------
# Funciones de limpieza
# ---------------------
def safe_str(val, default=""):
    if val is None:
        return default
    return str(val).strip()

def safe_int(val, default=None):
    try:
        return int(val) if val is not None else default
    except:
        return default

def safe_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return val != 0
    if isinstance(val, str):
        return val.lower() in ("t", "true", "1")
    return default

def safe_timestamp(val):
    return val if val is not None else None

def build_directory_levels(full_path):
    parts = Path(full_path).parts
    return [parts[i] if i < len(parts) else None for i in range(5)]


# ---------------------
# Dimensiones
# ---------------------
def get_or_create_directory(cur_olap, full_path, dir_cache):
    if full_path in dir_cache:
        return dir_cache[full_path]

    cur_olap.execute("SELECT id FROM dim_directory WHERE full_path=%s", (full_path,))
    row = cur_olap.fetchone()
    if row:
        dir_cache[full_path] = row[0]
        return row[0]

    parent_path = str(Path(full_path).parent)
    parent_id = None if parent_path == full_path else get_or_create_directory(cur_olap, parent_path, dir_cache)

    depth = len(Path(full_path).parts)
    levels = build_directory_levels(full_path)
    cur_olap.execute("""
        INSERT INTO dim_directory (directory_name, full_path, parent_id, depth, level1, level2, level3, level4, level5)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (
        Path(full_path).name,
        full_path,
        parent_id,
        depth,
        levels[0],
        levels[1],
        levels[2],
        levels[3],
        levels[4]
    ))
    dir_id = cur_olap.fetchone()[0]
    dir_cache[full_path] = dir_id
    return dir_id

def get_or_create_dim(cur_olap, table, col, value, default_value, cache):
    val = safe_str(value, default_value)
    if val in cache:
        return cache[val]

    cur_olap.execute(f"SELECT id FROM {table} WHERE {col}=%s", (val,))
    row = cur_olap.fetchone()
    if row:
        cache[val] = row[0]
        return row[0]

    cur_olap.execute(f"INSERT INTO {table} ({col}) VALUES (%s) RETURNING id", (val,))
    dim_id = cur_olap.fetchone()[0]
    cache[val] = dim_id
    return dim_id

def get_or_create_canonical_group(cur_olap, canonical_id, cache):
    if canonical_id is None:
        return None
    if canonical_id in cache:
        return cache[canonical_id]

    cur_olap.execute("SELECT id FROM dim_canonical_group WHERE canonical_id=%s", (canonical_id,))
    row = cur_olap.fetchone()
    if row:
        cache[canonical_id] = row[0]
        return row[0]

    cur_olap.execute("INSERT INTO dim_canonical_group (canonical_id) VALUES (%s) RETURNING id", (canonical_id,))
    group_id = cur_olap.fetchone()[0]
    cache[canonical_id] = group_id
    return group_id


# ---------------------
# ETL
# ---------------------
def process_files(fact_batch_size=1000, excerpt_batch_size=100):
    oltp = get_oltp_connection()
    olap = get_olap_connection()
    # Named cursor para no saturar memoria
    oltp_cur = oltp.cursor(name='oltp_cursor')
    olap_cur = olap.cursor()

    log("Extracting files from OLTP...")

    oltp_cur.execute("""
        SELECT
            id, full_path, file_name, file_type, size_bytes, creation_year, modification_year,
            depth, is_pdf, ocr_needed, xxhash64, sha256, first_seen, last_seen,
            text_excerpt, text_chars_extracted, is_canonical, canonical_id, categoria, last_classified
        FROM files WHERE hash_pending=false 
    """) ## Ahora mismo pilla los q tienen hash_pending false(rpr/2012) hay mas con hash pero sin has_pending actualizado

    dir_cache = {}
    type_cache = {}
    class_cache = {}
    canonical_cache = {}

    fact_inserts = []
    excerpt_inserts = []
    inserted_facts = 0

    # Estimación de total para barra de progreso
    oltp_cur.execute("SELECT COUNT(*) FROM files")
    total_files = oltp_cur.fetchone()[0]
    pbar = tqdm(total=total_files, desc="ETL files", mininterval=3,maxinterval=30)

    rows = oltp_cur.fetchmany(500)
    while rows:
        for row in rows:
            try:
                (file_id, full_path, file_name, file_type, size_bytes, creation_year, modification_year,
                 depth, is_pdf, ocr_needed, xxhash64, sha256, first_seen, last_seen,
                 text_excerpt, text_chars_extracted, is_canonical, canonical_id, categoria, last_classified) = row

                directory_id = get_or_create_directory(olap_cur, full_path, dir_cache)
                filetype_id = get_or_create_dim(olap_cur, "dim_filetype", "file_type", file_type, "tipo desconocido", type_cache)
                classification_id = get_or_create_dim(olap_cur, "dim_classification", "categoria", categoria, "sin clasificación", class_cache)
                canonical_group_id = get_or_create_canonical_group(olap_cur, canonical_id, canonical_cache)

                xxhash_clean = safe_str(xxhash64, None)
                sha256_clean = safe_str(sha256, None)

                fact_tuple = (
                    file_id, directory_id, filetype_id, classification_id, canonical_group_id,
                    safe_int(creation_year), safe_int(modification_year), safe_int(size_bytes),
                    safe_int(depth), safe_bool(is_pdf), safe_bool(ocr_needed),
                    xxhash_clean, sha256_clean,
                    safe_int(text_chars_extracted, 0), safe_bool(is_canonical, True),
                    safe_timestamp(first_seen), safe_timestamp(last_seen), safe_timestamp(last_classified)
                )

                # Evitar UPDATE innecesario
                olap_cur.execute("SELECT xxhash64, sha256, last_seen FROM fact_files WHERE file_id=%s", (file_id,))
                existing = olap_cur.fetchone()
                if existing:
                    if existing[0] == xxhash_clean and existing[1] == sha256_clean and existing[2] == last_seen:
                        pbar.update(1)
                        continue
                    else:
                        fact_inserts.append(fact_tuple)
                else:
                    fact_inserts.append(fact_tuple)

                if text_excerpt:
                    excerpt_inserts.append((file_id, text_excerpt))

                # Ejecutar batch inserts
                if len(fact_inserts) >= fact_batch_size:
                    execute_values(olap_cur, """
                        INSERT INTO fact_files (
                            file_id, directory_id, filetype_id, classification_id, canonical_group_id,
                            year_created, year_modified, size_bytes, depth, is_pdf, ocr_needed,
                            xxhash64, sha256, text_chars_extracted, is_canonical,
                            first_seen, last_seen, last_classified
                        ) VALUES %s
                        ON CONFLICT (file_id) DO UPDATE SET
                            directory_id=EXCLUDED.directory_id,
                            filetype_id=EXCLUDED.filetype_id,
                            classification_id=EXCLUDED.classification_id,
                            canonical_group_id=EXCLUDED.canonical_group_id,
                            year_created=EXCLUDED.year_created,
                            year_modified=EXCLUDED.year_modified,
                            size_bytes=EXCLUDED.size_bytes,
                            depth=EXCLUDED.depth,
                            is_pdf=EXCLUDED.is_pdf,
                            ocr_needed=EXCLUDED.ocr_needed,
                            xxhash64=EXCLUDED.xxhash64,
                            sha256=EXCLUDED.sha256,
                            text_chars_extracted=EXCLUDED.text_chars_extracted,
                            is_canonical=EXCLUDED.is_canonical,
                            first_seen=EXCLUDED.first_seen,
                            last_seen=EXCLUDED.last_seen,
                            last_classified=EXCLUDED.last_classified
                    """, fact_inserts)
                    inserted_facts += len(fact_inserts)
                    log(f"Inserted/Updated {inserted_facts} fact_files so far...")
                    fact_inserts = []

                if len(excerpt_inserts) >= excerpt_batch_size:
                    execute_values(olap_cur, """
                        INSERT INTO fact_file_excerpts (file_id, text_excerpt) VALUES %s
                        ON CONFLICT (file_id) DO UPDATE SET text_excerpt=EXCLUDED.text_excerpt
                    """, excerpt_inserts)
                    excerpt_inserts = []

                pbar.update(1)

            except Exception as e:
                log(f"Error processing file_id={file_id}: {e}")
                pbar.update(1)
                continue

        rows = oltp_cur.fetchmany(500)

    # Insertar los que queden
    if fact_inserts:
        execute_values(olap_cur, """
            INSERT INTO fact_files (
                file_id, directory_id, filetype_id, classification_id, canonical_group_id,
                year_created, year_modified, size_bytes, depth, is_pdf, ocr_needed,
                xxhash64, sha256, text_chars_extracted, is_canonical,
                first_seen, last_seen, last_classified
            ) VALUES %s
            ON CONFLICT (file_id) DO UPDATE SET
                directory_id=EXCLUDED.directory_id,
                filetype_id=EXCLUDED.filetype_id,
                classification_id=EXCLUDED.classification_id,
                canonical_group_id=EXCLUDED.canonical_group_id,
                year_created=EXCLUDED.year_created,
                year_modified=EXCLUDED.year_modified,
                size_bytes=EXCLUDED.size_bytes,
                depth=EXCLUDED.depth,
                is_pdf=EXCLUDED.is_pdf,
                ocr_needed=EXCLUDED.ocr_needed,
                xxhash64=EXCLUDED.xxhash64,
                sha256=EXCLUDED.sha256,
                text_chars_extracted=EXCLUDED.text_chars_extracted,
                is_canonical=EXCLUDED.is_canonical,
                first_seen=EXCLUDED.first_seen,
                last_seen=EXCLUDED.last_seen,
                last_classified=EXCLUDED.last_classified
        """, fact_inserts)
        inserted_facts += len(fact_inserts)

    if excerpt_inserts:
        execute_values(olap_cur, """
            INSERT INTO fact_file_excerpts (file_id, text_excerpt) VALUES %s
            ON CONFLICT (file_id) DO UPDATE SET text_excerpt=EXCLUDED.text_excerpt
        """, excerpt_inserts)

    olap.commit()
    oltp_cur.close()
    olap_cur.close()
    oltp.close()
    olap.close()
    pbar.close()
    log(f"ETL finished successfully. Total fact_files processed: {inserted_facts}")


if __name__ == "__main__":
    process_files()