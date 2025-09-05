# Base image
FROM python:3.10-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first (better layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Copy application code (exclude secrets via .dockerignore)
COPY . /app

# Expose UI ports (Streamlit web.py / 后台.py)
EXPOSE 8501 8502

# Default command: boot script will decide to show setup page or run full app
CMD ["python", "boot.py"]