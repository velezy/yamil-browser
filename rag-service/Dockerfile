# rag-service - Standalone Dockerfile
ARG PYTHON_VERSION=3.13

FROM python:${PYTHON_VERSION}-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install shared library (assemblyline-common)
COPY assemblyline-common/ /tmp/assemblyline-common
RUN pip install --no-cache-dir gunicorn uvicorn[standard] /tmp/assemblyline-common[observability] \
    && rm -rf /tmp/assemblyline-common

# Install service-specific requirements
COPY requirements.txt /tmp/requirements.txt
RUN grep -v "^-e " /tmp/requirements.txt > /app/requirements.txt && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    rm /tmp/requirements.txt

# Copy service code
COPY src/ /app/
COPY migrations/ /app/migrations/

# Health check
ARG PORT=8022
ENV PORT=${PORT}
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

EXPOSE ${PORT}

# Production ASGI server (gunicorn + uvicorn workers)
CMD ["sh", "-c", "gunicorn main:app -k uvicorn.workers.UvicornWorker -b 0.0.0.0:${PORT} --workers ${UVICORN_WORKERS:-4} --timeout 300 --keep-alive 75 --worker-connections 1000 --access-logfile - --error-logfile -"]
