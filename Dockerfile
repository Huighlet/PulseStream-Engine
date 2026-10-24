# ==============================================================================
# PulseStream-Engine: High-Throughput Event Streaming Broker & Partition Log
# ==============================================================================
FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pulsestream ./pulsestream
RUN pip install --no-cache-dir -e .

EXPOSE 9092

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:9092/health || exit 1

ENTRYPOINT ["pulsestream", "serve", "--host", "0.0.0.0", "--port", "9092"]
