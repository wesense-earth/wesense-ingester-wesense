#!/bin/bash
set -e

# LinuxServer.io style PUID/PGID support
PUID=${PUID:-1000}
PGID=${PGID:-1000}

# Update user/group if needed
if [ "$(id -u wesense)" != "$PUID" ] || [ "$(id -g wesense)" != "$PGID" ]; then
    echo "[entrypoint] Updating wesense user to PUID=$PUID, PGID=$PGID"
    groupmod -o -g "$PGID" wesense
    usermod -o -u "$PUID" wesense
fi

# Generate config.yaml from environment variables if it doesn't exist
CONFIG_FILE="/app/config/config.yaml"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "[entrypoint] Generating config.yaml from environment variables"

    # Determine output type (default to none, MQTT output for debugging only)
    OUTPUT_TYPE=${OUTPUT_TYPE:-none}

    cat > "$CONFIG_FILE" <<EOF
# Auto-generated from environment variables
# Edit this file or set environment variables to reconfigure

input:
  type: mqtt
  mqtt:
    broker: ${MQTT_BROKER:-localhost}
    port: ${MQTT_PORT:-1883}
    client_id: ${MQTT_INPUT_CLIENT_ID:-ingester_wesense_input}
    subscribe_topics:
      - "${MQTT_SUBSCRIBE_TOPICS:-wesense/v2/wifi/#}"

output:
  type: ${OUTPUT_TYPE}
  mqtt:
    broker: ${MQTT_BROKER:-localhost}
    port: ${MQTT_PORT:-1883}
    client_id: ${MQTT_OUTPUT_CLIENT_ID:-ingester_wesense_output}
    topic_prefix: ${MQTT_TOPIC_PREFIX:-wesense/decoded}

# ClickHouse is configured via environment variables:
# CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
# CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE

logging:
  level: ${LOG_LEVEL:-INFO}
EOF

    echo "[entrypoint] Config generated at $CONFIG_FILE"
else
    echo "[entrypoint] Using existing config at $CONFIG_FILE"
fi

# Fix permissions on app directory (including utils, config, logs, cache)
chown -R wesense:wesense /app 2>/dev/null || true
chmod -R 755 /app/utils 2>/dev/null || true

# Switch to wesense user and execute command
exec gosu wesense "$@"
