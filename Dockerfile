FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg2 / networking (keep minimal)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scripts/ /app/scripts/
COPY resources/ /app/resources/

# Make sure python can import "scripts.*"
ENV PYTHONPATH=/app

# Default DB envs (overridden by compose)
ENV PGHOST=db \
    PGPORT=5432 \
    PGDATABASE=auditdb \
    PGUSER=user \
    PGPASSWORD=pass

# Default command: run the pipeline entrypoint
CMD ["python", "-m", "scripts.run_pipeline"]
