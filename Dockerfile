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


# Dependencias ligeras (cambian más a menudo)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiar dependencias de la API
COPY api/requirements.txt /app/api/requirements.txt
RUN pip install --no-cache-dir -r /app/api/requirements.txt

# Copiar código que cambia constantemente
COPY scripts/ /app/scripts/
COPY resources/ /app/resources/
COPY messaging/ /app/messaging
COPY schemas/ /app/schemas
COPY api/ /app/api/

# PYTHONPATH
ENV PYTHONPATH=/app:$PYTHONPATH


# DB defaults
ENV PGHOST=db \
    PGPORT=5432 \
    PGDATABASE=auditdb \
    PGUSER=user \
    PGPASSWORD=pass

# Puerto para API 
EXPOSE 8000

# Default command
#CMD ["python", "-m", "scripts.run_pipeline"]
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
