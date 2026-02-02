## Docker (WSL2 + Ubuntu) para pipeline + PostgreSQL

### Qué se crea
- **Contenedor `pipeline`**: ejecuta `python -m scripts.run_pipeline`
- **Contenedor `db`**: PostgreSQL 16
- **Red**: `rpr-net` (bridge) para que `pipeline` conecte a `db` por hostname `db`
- **Volúmenes**:
  - `./resources` se persiste (logs/csv/reports/tmp)
  - `${NETWORK_PATH}` se monta como `/data` (ruta de red montada en WSL2)

### 1) Preparar variables
1. Copia `.env.example` a `.env` y ajusta credenciales si quieres.
2. Ajusta `NETWORK_PATH` a una ruta Linux dentro de WSL2 (recomendado).

### 2) Montar la ubicación de red en WSL2 (recomendado)
Ejemplo SMB:

```bash
sudo mkdir -p /mnt/rpr_share
sudo mount -t cifs //SERVIDOR/SHARE /mnt/rpr_share \
  -o username=USUARIO,password='PASSWORD',iocharset=utf8,vers=3.0
```

Si quieres que sea persistente, añade una entrada a `/etc/fstab` (opcional).

### 3) Levantar todo
Desde la raíz del proyecto:

```bash
docker compose --env-file .env up --build
```

### Notas importantes
- En tu código actual, `scripts/phase_1/populate_db.py` hace `psycopg2.connect(dbname="auditdb", user="user", password="pass")` sin host/puerto.
  - Con Docker, **lo ideal** es cambiarlo a usar `host=db` (o `PGHOST`) para que conecte al contenedor `db`.
- La pipeline ahora mismo solo ejecuta `phase_0` (según `scripts/run_pipeline.py`). Si añades fases que escriban en Postgres, ya tienes la red lista.
