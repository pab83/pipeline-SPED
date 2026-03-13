import os
import time
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2 import OperationalError

from scripts.config.phase_2 import SEMANTIC_SIM_THRESHOLD, SIZE_BUCKET_BYTES, LOG_FILE

def log(msg: str) -> None:
    """Registra un mensaje en el log de la Fase 2 y lo emite por consola."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def get_db_connection(retries: int = 10, delay: int = 3) -> Any:
    """
    Establece conexión con PostgreSQL con reintentos y backoff.
    """
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(
                dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "auditdb")),
                user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "user")),
                password=os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "pass")),
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
            )
        except OperationalError as e:
            log(f"⚠️ Postgres not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres.")

def choose_canonical(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aplica heurísticas para seleccionar el mejor archivo representante de un grupo.
    
    Criterios de prioridad (de mayor a menor):
    
    1.  **Calidad**: PDFs digitales (sin necesidad de OCR) sobre imágenes u otros.
    2.  **Volumen**: Archivos con mayor tamaño en bytes (asumiendo más información).
    3.  **Recencia**: Año de modificación más cercano al presente.
    4.  **Accesibilidad**: Menor profundidad en la estructura de directorios.
    5.  **Persistencia**: ID de base de datos más bajo como desempate final.
    """
    def key(c: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
        quality_flag = 0 if (c.get("is_pdf") and not c.get("ocr_needed")) else 1
        size = c.get("size_bytes") or 0
        mod_year = c.get("modification_year") or 0
        depth = c.get("depth") or 0
        return (quality_flag, -size, -mod_year, depth, c["id"])

    return min(candidates, key=key)

def l2_distance_to_cosine_similarity(distance: float) -> float:
    """
    Convierte distancia L2 (Euclidiana) a Similitud Coseno aproximada.
    Asume vectores normalizados. Fórmula: $cos(\theta) = 1 - \frac{d^2}{2}$.
    """
    return max(-1.0, min(1.0, 1.0 - (distance * distance) / 2.0))

def hash_level_canonicalization(cur: Any) -> None:
    """
    Identifica duplicados exactos mediante SHA256 y marca correctamente todos los registros.
    
    - `is_canonical = True` → archivo único o canonizado
    - `is_canonical = False` → duplicado de otro archivo
    - `canonical_id` apunta al registro canonizado
    """
    log("Running hash-level canonicalization (sha256)...")

    # 1️⃣ Recupera todos los registros con hash
    cur.execute("""
        SELECT sha256, array_agg(id ORDER BY id) AS ids
        FROM files
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
    """)
    groups = cur.fetchall()
    updates = []

    for sha, ids in groups:
        if len(ids) == 1:
            # Archivo único → canonizado
            updates.append((True, None, ids[0]))
        else:
            # Múltiples duplicados → el primero es canonizado
            canonical_id = ids[0]
            for fid in ids:
                if fid == canonical_id:
                    updates.append((True, None, fid))
                else:
                    updates.append((False, canonical_id, fid))

    if updates:
        cur.executemany("""
            UPDATE files
            SET is_canonical = %s, canonical_id = %s
            WHERE id = %s
        """, updates)
        log(f"Hash-level canonicalization applied to {len(updates)} rows.")
    else:
        log("No hay registros con SHA256 para procesar.")

def semantic_canonicalization(cur: Any, conn: Any) -> None:
    """
    Deduplicación inteligente basada en contenido semántico (Embeddings).
    
    Utiliza `pgvector` e índices `IVFFlat` para encontrar documentos similares 
    incluso si el hash es distinto (ej. misma carta escaneada dos veces).
    """
    log("Running semantic canonicalization...")
    cur.execute("""
        SELECT f.id FROM files f
        JOIN file_embeddings e ON e.file_id = f.id
        WHERE f.canonical_id IS NULL ORDER BY f.id
    """)
    candidate_ids = [row[0] for row in cur.fetchall()]
    if not candidate_ids: return

    cluster_of: Dict[int, int] = {}
    clusters: Dict[int, List[Dict[str, Any]]] = {}
    next_cluster_id = 1
    K = 20 # Vecinos a consultar

    for fid in candidate_ids:
        if fid in cluster_of: continue

        cur.execute("""
            SELECT f.file_type, f.size_bytes, f.creation_year, f.modification_year, 
                   f.depth, f.is_pdf, f.ocr_needed, e.embedding
            FROM files f JOIN file_embeddings e ON e.file_id = f.id WHERE f.id = %s
        """, (fid,))
        row = cur.fetchone()
        if not row: continue

        # Búsqueda de vecinos similares en pgvector
        cur.execute("""
            SELECT f.id, f.file_type, f.size_bytes, f.creation_year, f.modification_year, 
                   f.depth, f.is_pdf, f.ocr_needed, (e.embedding <-> %s)::float AS dist
            FROM files f JOIN file_embeddings e ON e.file_id = f.id
            WHERE f.canonical_id IS NULL AND (f.file_type IS NULL OR lower(f.file_type) = %s)
            ORDER BY e.embedding <-> %s LIMIT %s
        """, (row[7], (row[0] or "").lower(), row[7], K))

        neighbors = cur.fetchall()
        cluster_candidates = []
        for n in neighbors:
            sim = l2_distance_to_cosine_similarity(n[8])
            if n[0] == fid or sim >= SEMANTIC_SIM_THRESHOLD:
                cluster_candidates.append({
                    "id": n[0], "file_type": n[1], "size_bytes": n[2],
                    "modification_year": n[4], "depth": n[5], "is_pdf": n[6], "ocr_needed": n[7]
                })

        if len(cluster_candidates) > 1:
            cid = next_cluster_id
            next_cluster_id += 1
            clusters[cid] = cluster_candidates
            for item in cluster_candidates: cluster_of[item["id"]] = cid

    # Aplicar actualizaciones
    updates = []
    for _, members in clusters.items():
        canonical = choose_canonical(members)
        for m in members:
            updates.append((m["id"] == canonical["id"], 
                            None if m["id"] == canonical["id"] else canonical["id"], 
                            m["id"]))

    if updates:
        cur.executemany("UPDATE files SET is_canonical=%s, canonical_id=%s WHERE id=%s", updates)
        conn.commit()
        log(f"Semantic clusters processed: {len(clusters)}")

def main() -> None:
    """
    Punto de entrada para la deduplicación de Fase 2.
    
    Coordina la limpieza de datos priorizando la identidad exacta por hash. 
    (La deduplicación semántica puede activarse según necesidad).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    hash_level_canonicalization(cur)
    conn.commit()
    # semantic_canonicalization(cur, conn) # Desactivado por defecto para estabilidad
    cur.close()
    conn.close()
    log("Deduplication completed.")

if __name__ == "__main__":
    main()