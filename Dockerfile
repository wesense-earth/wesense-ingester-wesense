# WeSense Ingester - WiFi
# Build: docker build -t wesense-ingester-wesense .
#
# Ingests WeSense WiFi sensor data:
# - Subscribes to wesense/v2/wifi/# topics
# - Decodes WeSense v2.1 protobuf (SensorReadingV2)
# - Reverse geocodes lat/lon using offline GeoNames database
# - Writes to ClickHouse sensor_readings table
#
# Note: LoRa data is handled by wesense-ingester-lora (with metadata caching)

FROM python:3.11-slim

LABEL maintainer="WeSense Project"
LABEL description="WeSense Ingester - WiFi sensor data with v2.1 protobuf support"
LABEL version="2.1.0"

WORKDIR /app

# Install gosu for user switching
RUN apt-get update && \
    apt-get install -y --no-install-recommends gosu && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingester application
COPY ingester_wesense.py .

# Create proto directory and copy protobuf files
RUN mkdir -p /app/proto
COPY proto/wesense_homebrew_v2_pb2.py /app/proto/

# Create utils directory and copy utility modules
RUN mkdir -p /app/utils
COPY utils/ /app/utils/

# Create config and cache directories
RUN mkdir -p /app/config /app/logs /app/cache

# Create entrypoint script for PUID/PGID support (LinuxServer.io style)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Default user (can be overridden with PUID/PGID env vars at runtime)
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd -g ${USER_GID} wesense && \
    useradd -m -u ${USER_UID} -g ${USER_GID} wesense && \
    chown -R wesense:wesense /app && \
    chmod -R 755 /app/utils

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "ingester_wesense.py", "--config", "/app/config/config.yaml"]
