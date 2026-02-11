FROM python:3.12-slim

WORKDIR /app

# Flags Python (menos logs, no .pyc)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Cache HuggingFace
ENV TRANSFORMERS_CACHE=/app/.cache/huggingface
ENV HF_HOME=/app/.cache/huggingface

# System deps mínimos
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Dependencias pesadas (casi nunca cambian)
RUN pip install --no-cache-dir \
        torch sentence-transformers

# Dependencias ligeras (cambian más a menudo)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
        numpy \
        PyPDF2 \
        tqdm \
        psycopg2-binary \
        xxhash \
        python-docx \
        opencv-python-headless

# Copiar código que cambia constantemente
COPY scripts/ /app/scripts/
COPY resources/ /app/resources/

# PYTHONPATH
ENV PYTHONPATH=/app

# DB defaults
ENV PGHOST=db \
    PGPORT=5432 \
    PGDATABASE=auditdb \
    PGUSER=user \
    PGPASSWORD=pass

# Default command
CMD ["python", "-m", "scripts.run_pipeline"]

