FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Application code
COPY ./app /app/app
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

# Default environment values (can be overridden in compose)
ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    OUTPUT_FILE=/data/output.jl \
    PROGRESS_FILE=/data/crawl_progress.json \
    CRAWL_DEFAULT_SCOPE=in-domain \
    CRAWL_DEFAULT_DEPTH=3 \
    MAX_CHARS=100000

VOLUME ["/data"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]