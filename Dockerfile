FROM python:3.12-slim

WORKDIR /app

# 1️⃣ Flags Python (menos logs, no .pyc)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 2️⃣ Cache HuggingFace
ENV TRANSFORMERS_CACHE=/app/.cache/huggingface
ENV HF_HOME=/app/.cache/huggingface

# 3️⃣ System deps mínimos
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 4️⃣ Dependencias pesadas (casi nunca cambian)
RUN pip install --no-cache-dir \
        torch sentence-transformers

# 5️⃣ Dependencias ligeras (cambian más a menudo)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
        numpy \
        PyPDF2 \
        tqdm \
        psycopg2-binary \
        xxhash \
        python-docx \
        opencv-python-headless

# 6️⃣ Copiar código que cambia constantemente
COPY scripts/ /app/scripts/
COPY resources/ /app/resources/

# 7️⃣ PYTHONPATH
ENV PYTHONPATH=/app

# 8️⃣ DB defaults
ENV PGHOST=db \
    PGPORT=5432 \
    PGDATABASE=auditdb \
    PGUSER=user \
    PGPASSWORD=pass

# 9️⃣ Default command
CMD ["python", "-m", "scripts.run_pipeline"]

