#!/bin/sh
# entrypoint.sh — Fix directory ownership, generate config, then drop to PUID:PGID
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

# Ensure writable directories exist with correct ownership
mkdir -p /app/cache /app/logs /app/data/keys /app/config
chown -R "$PUID:$PGID" /app/cache /app/logs /app/data /app/config

# Generate config.yaml from environment variables if it doesn't exist
CONFIG_FILE="/app/config/config.yaml"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "[entrypoint] Generating config.yaml from environment variables"

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

    chown "$PUID:$PGID" "$CONFIG_FILE"
    echo "[entrypoint] Config generated at $CONFIG_FILE"
else
    echo "[entrypoint] Using existing config at $CONFIG_FILE"
fi

exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
    python -u main.py "$@"
