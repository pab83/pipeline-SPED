import os
import psycopg2
from psycopg2 import OperationalError

from scripts.config.general import LOG_FILE
from scripts.config.phase_2 import SEMANTIC_SIM_THRESHOLD, SIZE_BUCKET_BYTES


def log(msg: str) -> None:
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
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
            return conn
        except OperationalError as e:
            log(f"Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after multiple attempts.")


def choose_canonical(candidates):
    """
    Elige el fichero canónico entre una lista de dicts con metadatos.
    Criterios:
      - Preferir PDFs con texto (ocr_needed=False) sobre otros.
      - Mayor tamaño (size_bytes).
      - Año de modificación más reciente.
      - Menor profundidad (depth).
      - ID más bajo como último criterio.
    """

    def key(c):
        # 0 = mejor, 1 = peor
        quality_flag = 0 if (c["is_pdf"] and not c["ocr_needed"]) else 1
        size = c["size_bytes"] or 0
        mod_year = c["modification_year"] or 0
        depth = c["depth"] or 0
        return (
            quality_flag,
            -size,
            -mod_year,
            depth,
            c["id"],
        )

    return min(candidates, key=key)


def l2_distance_to_cosine_similarity(distance: float) -> float:
    """
    Convierte una distancia L2 aproximada a una similitud coseno aproximada.
    Solo es una aproximación: asumimos vectores normalizados en norma ~1.
    Para vectores normalizados, ||a - b||^2 = 2 - 2 cos(theta)  =>  cos(theta) = 1 - d^2/2
    """
    return max(-1.0, min(1.0, 1.0 - (distance * distance) / 2.0))


def hash_level_canonicalization(cur):
    """
    Primero hacemos canonización por duplicados exactos (sha256),
    para no procesar innecesariamente múltiples copias idénticas.
    """
    log("Running hash-level canonicalization (sha256)...")
    cur.execute(
        """
        SELECT sha256, array_agg(id ORDER BY id) AS ids
        FROM files
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1;
        """
    )
    groups = cur.fetchall()
    updates = []
    for sha, ids in groups:
        canonical_id = ids[0]
        for fid in ids:
            if fid == canonical_id:
                updates.append((True, None, fid))
            else:
                updates.append((False, canonical_id, fid))

    if updates:
        cur.executemany(
            """
            UPDATE files
            SET is_canonical = %s,
                canonical_id = %s
            WHERE id = %s
            """,
            updates,
        )
        log(f"Hash-level canonicalization applied to {len(updates)} rows.")
    else:
        log("No hash-level duplicates found.")


def semantic_canonicalization(cur, conn):
    """
    Aplica deduplicación semántica usando embeddings guardados en file_embeddings.
    Solo opera sobre ficheros que todavía no apuntan a otro canónico (canonical_id IS NULL).
    """
    log("Running semantic canonicalization...")

    # Seleccionamos solo IDs de candidatos para semantic dedup.
    cur.execute(
        """
        SELECT f.id
        FROM files f
        JOIN file_embeddings e ON e.file_id = f.id
        WHERE f.canonical_id IS NULL
        ORDER BY f.id
        """
    )

    candidate_ids = [row[0] for row in cur.fetchall()]
    if not candidate_ids:
        log("No candidates found for semantic deduplication.")
        return

    # Para evitar O(n^2), en lugar de cargar todos los embeddings en Python,
    # delegamos la búsqueda de vecinos cercanos en pgvector (IVFFlat).
    # Recorremos cada candidato, consultamos sus K vecinos más cercanos y
    # agrupamos por similitud coseno >= SEMANTIC_SIM_THRESHOLD.

    total_links = 0
    updates = []

    # Mapeo file_id -> cluster_id (para no re-agrupar lo mismo)
    cluster_of = {}
    clusters = {}
    next_cluster_id = 1

    # Número de vecinos a consultar por documento (incluyéndose a sí mismo).
    # Cuanto más alto, más posibilidades de capturar duplicados; también más coste.
    K = 20

    for fid in candidate_ids:
        # Si ya está en un cluster, saltamos (ya será tratado como miembro).
        if fid in cluster_of:
            continue

        # Traemos metadatos y embedding del documento actual
        cur.execute(
            """
            SELECT
                f.file_type,
                f.size_bytes,
                f.creation_year,
                f.modification_year,
                f.depth,
                f.is_pdf,
                f.ocr_needed,
                e.embedding
            FROM files f
            JOIN file_embeddings e ON e.file_id = f.id
            WHERE f.id = %s
            """,
            (fid,),
        )
        row = cur.fetchone()
        if not row:
            continue

        (
            file_type,
            size_bytes,
            creation_year,
            modification_year,
            depth,
            is_pdf,
            ocr_needed,
            embedding,
        ) = row

        # Normalizamos algunos metadatos
        btype = (file_type or "").lower()
        size = size_bytes or 0
        bucket_idx = size // max(1, SIZE_BUCKET_BYTES)

        # Buscamos vecinos con pgvector restringiendo a mismo tipo y bucket de tamaño
        cur.execute(
            """
            SELECT
                f.id,
                f.file_type,
                f.size_bytes,
                f.creation_year,
                f.modification_year,
                f.depth,
                f.is_pdf,
                f.ocr_needed,
                e.embedding,
                (e.embedding <-> %s)::float AS l2_distance
            FROM files f
            JOIN file_embeddings e ON e.file_id = f.id
            WHERE f.canonical_id IS NULL
              AND (f.file_type IS NULL OR lower(f.file_type) = %s)
              AND (f.size_bytes IS NULL
                   OR (f.size_bytes / GREATEST(1, %s)) = %s)
            ORDER BY e.embedding <-> %s
            LIMIT %s
            """,
            (
                embedding,
                btype,
                SIZE_BUCKET_BYTES,
                bucket_idx,
                embedding,
                K,
            ),
        )

        neighbors = cur.fetchall()
        if not neighbors:
            continue

        # Generamos lista de candidatos similares según umbral
        cluster_candidates = []
        for (
            nid,
            n_file_type,
            n_size_bytes,
            n_creation_year,
            n_modification_year,
            n_depth,
            n_is_pdf,
            n_ocr_needed,
            n_embedding,
            l2_distance,
        ) in neighbors:
            if nid == fid:
                # Siempre incluimos el propio documento
                cluster_candidates.append(
                    {
                        "id": nid,
                        "file_type": (n_file_type or "").lower(),
                        "size_bytes": n_size_bytes,
                        "creation_year": n_creation_year,
                        "modification_year": n_modification_year,
                        "depth": n_depth,
                        "is_pdf": n_is_pdf,
                        "ocr_needed": n_ocr_needed,
                    }
                )
                continue

            sim = l2_distance_to_cosine_similarity(l2_distance)
            if sim >= SEMANTIC_SIM_THRESHOLD:
                cluster_candidates.append(
                    {
                        "id": nid,
                        "file_type": (n_file_type or "").lower(),
                        "size_bytes": n_size_bytes,
                        "creation_year": n_creation_year,
                        "modification_year": n_modification_year,
                        "depth": n_depth,
                        "is_pdf": n_is_pdf,
                        "ocr_needed": n_ocr_needed,
                    }
                )

        # Si no hay más que uno, no formamos cluster nuevo
        if len(cluster_candidates) < 2:
            continue

        # Creamos un nuevo cluster para este conjunto
        cid = next_cluster_id
        next_cluster_id += 1

        clusters[cid] = cluster_candidates
        for item in cluster_candidates:
            cluster_of[item["id"]] = cid

        total_links += len(cluster_candidates)

    total_clusters = len(clusters)
    total_in_clusters = total_links

    # Ahora, para cada cluster, escogemos canónico y marcamos en BD
    for cid, members in clusters.items():
        canonical = choose_canonical(members)
        canonical_id = canonical["id"]

        for item in members:
            fid = item["id"]
            if fid == canonical_id:
                updates.append((True, None, fid))
            else:
                updates.append((False, canonical_id, fid))

    if updates:
        cur.executemany(
            """
            UPDATE files
            SET is_canonical = %s,
                canonical_id = %s
            WHERE id = %s
            """,
            updates,
        )
        conn.commit()
        log(
            f"Semantic canonicalization applied to {len(updates)} rows "
            f"across {total_clusters} clusters ({total_in_clusters} docs in clusters)."
        )
    else:
        log("No semantic duplicate clusters found above the threshold.")


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    hash_level_canonicalization(cur)
    conn.commit()

    #semantic_canonicalization(cur, conn)

    cur.close()
    conn.close()
    log("Phase 2 deduplication (hash only) completed.")


if __name__ == "__main__":
    main()
