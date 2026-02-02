# WeSense Ingester (Unified)
# Build context: parent directory (wesense/)
# Build: docker build -f wesense-ingester-wesense/Dockerfile -t wesense-ingester-wesense .
#
# Unified ingester for WeSense WiFi + LoRa + TTN webhook.
# Subscribes to wesense/v2/wifi/# and wesense/v2/lora/# topics.
# Optionally runs TTN webhook HTTP server (TTN_WEBHOOK_ENABLED=true).
#
# Expects wesense-ingester-core to be available at ../wesense-ingester-core
# when building with docker-compose (which sets the build context).

FROM python:3.11-slim

WORKDIR /app

# Copy dependency files first for better layer caching
COPY wesense-ingester-core/ /tmp/wesense-ingester-core/
COPY wesense-ingester-wesense/requirements-docker.txt .

# Install gcc, build all pip packages, then remove gcc in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    pip install --no-cache-dir /tmp/wesense-ingester-core && \
    pip install --no-cache-dir -r requirements-docker.txt && \
    apt-get purge -y --auto-remove gcc && \
    rm -rf /var/lib/apt/lists/* /tmp/wesense-ingester-core

# Copy application code
COPY wesense-ingester-wesense/main.py .

# Copy protobuf compiled module
RUN mkdir -p /app/proto
COPY wesense-ingester-wesense/proto/wesense_homebrew_v2_pb2.py /app/proto/

# Create directories for cache, logs, and config
RUN mkdir -p /app/cache /app/logs /app/config

ENV TZ=UTC

CMD ["python", "-u", "main.py"]
