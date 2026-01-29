import psycopg2
from scripts.config import LOG_FILE

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

conn = psycopg2.connect(dbname="auditdb", user="user", password="pass")
cur = conn.cursor()

cur.execute("""
    SELECT sha256, array_agg(file_id) AS ids
    FROM files
    WHERE sha256 IS NOT NULL
    GROUP BY sha256
    HAVING COUNT(*) > 1
""")
duplicates = cur.fetchall()

for sha, ids in duplicates:
    log(f"Duplicates detected (SHA-256={sha}): {ids}")

cur.close()
conn.close()
