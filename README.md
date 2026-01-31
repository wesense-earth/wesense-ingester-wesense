# WeSense Ingester - WeSense

Unified ingester for WeSense WiFi, LoRa, and TTN webhook sensor data. Decodes WeSense v2 protobuf, reverse geocodes, and publishes to ClickHouse and MQTT.

> For detailed documentation, see the [Wiki](https://github.com/wesense-earth/wesense-ingester-wesense/wiki).
> Read on for a project overview and quick install instructions.

## Overview

Handles all WeSense-native sensor protocols in a single service:

- **WiFi sensors** — subscribe to `wesense/v2/wifi/#` MQTT topics (full messages with location)
- **LoRa sensors** — subscribe to `wesense/v2/lora/#` MQTT topics (split messages: readings + metadata sent separately to save bandwidth)
- **TTN webhook** — optional HTTP server receiving payloads from The Things Network (`TTN_WEBHOOK_ENABLED=true`)

LoRa sensors send readings and metadata as separate messages. The ingester maintains a `MetadataCache` per device (persisted to disk) and merges metadata with readings when they arrive. WiFi sensors send complete messages so no caching is needed.

Uses [wesense-ingester-core](https://github.com/wesense-earth/wesense-ingester-core) for ClickHouse writing, MQTT publishing, geocoding, and logging.

## Quick Install (Recommended)

Most users should deploy via [wesense-deploy](https://github.com/wesense-earth/wesense-deploy), which orchestrates all WeSense services using Docker Compose profiles:

```bash
# Clone the deploy repo
git clone https://github.com/wesense-earth/wesense-deploy.git
cd wesense-deploy

# Configure
cp .env.sample .env
# Edit .env with your settings

# Start as a contributor (ingesters only, sends to remote hub)
docker compose --profile contributor up -d

# Or as a full station (includes EMQX, ClickHouse, Respiro map)
docker compose --profile station up -d
```

For Unraid or manual deployments, use the docker-run script:

```bash
./scripts/docker-run.sh station
```

See [Deployment Personas](https://github.com/wesense-earth/wesense-deploy) for all options.

## Docker (Standalone)

For running this ingester independently (e.g. on a separate host):

```bash
docker pull ghcr.io/wesense-earth/wesense-ingester-wesense:latest

docker run -d \
  --name wesense-ingester-wesense \
  --restart unless-stopped \
  -e MQTT_BROKER=your-mqtt-broker \
  -e MQTT_PORT=1883 \
  -e WESENSE_OUTPUT_BROKER=mqtt.wesense.earth \
  -e WESENSE_OUTPUT_PORT=1883 \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_DATABASE=wesense \
  -e TTN_WEBHOOK_ENABLED=false \
  -v wesense-cache:/app/cache \
  -v wesense-logs:/app/logs \
  -p 5000:5000 \
  ghcr.io/wesense-earth/wesense-ingester-wesense:latest
```

## Local Development

```bash
# Install core library (from sibling directory)
pip install -e ../wesense-ingester-core

# Install adapter dependencies
pip install -r requirements.txt

# Run (MQTT input only)
python wesense_ingester.py

# Run (MQTT + TTN webhook)
TTN_WEBHOOK_ENABLED=true python wesense_ingester.py
```

## Architecture

```
WeSense Sensors
    ├─ WiFi → MQTT (wesense/v2/wifi/{country}/{subdivision}/{device_id})
    ├─ LoRa → MQTT (wesense/v2/lora/{device_id} + wesense/v2/lora/metadata/{device_id})
    └─ TTN  → HTTP POST /ttn/uplink (optional)
    │
    ▼  Decode protobuf (SensorReadingV2 / DeviceMetadataV2)
    │
    ├─ WiFi: Complete message with all fields
    ├─ LoRa: Readings merged with cached DeviceMetadataV2
    │
    ▼  [wesense-ingester-core pipeline]
    ├─→ ReverseGeocoder (GeoNames → ISO 3166 codes)
    ├─→ BufferedClickHouseWriter (batched inserts, 22 columns)
    └─→ WeSensePublisher (MQTT: wesense/decoded/wesense/{country}/{subdiv}/{device})
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BROKER` | `localhost` | Input MQTT broker |
| `MQTT_PORT` | `1883` | Input MQTT port |
| `MQTT_USERNAME` | | Input MQTT username |
| `MQTT_PASSWORD` | | Input MQTT password |
| `MQTT_SUBSCRIBE_TOPICS` | `wesense/v2/wifi/#,wesense/v2/lora/#` | Comma-separated topics |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse server |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_DATABASE` | `wesense` | Database name |
| `CLICKHOUSE_BATCH_SIZE` | `100` | Rows before flush |
| `CLICKHOUSE_FLUSH_INTERVAL` | `10` | Seconds between flushes |
| `WESENSE_OUTPUT_BROKER` | `localhost` | Output MQTT broker |
| `WESENSE_OUTPUT_PORT` | `1883` | Output MQTT port |
| `TTN_WEBHOOK_ENABLED` | `false` | Enable TTN webhook HTTP server |
| `TTN_WEBHOOK_PORT` | `5000` | TTN webhook listen port |
| `TTN_WEBHOOK_SECRET` | | HMAC-SHA256 webhook verification |
| `DEBUG` | `false` | Enable debug logging |

## Output

Publishes decoded readings to MQTT topic `wesense/decoded/wesense/{country}/{subdivision}/{device_id}` and inserts into ClickHouse `wesense.sensor_readings`.

## Related

- [wesense-ingester-core](https://github.com/wesense-earth/wesense-ingester-core) — Shared library
- [wesense-sensor-firmware](https://github.com/wesense-earth/wesense-sensor-firmware) — ESP32 firmware that produces the data this ingester consumes
- [wesense-deploy](https://github.com/wesense-earth/wesense-deploy) — Docker Compose orchestration
- [wesense-respiro](https://github.com/wesense-earth/wesense-respiro) — Sensor map dashboard

## License

MIT
