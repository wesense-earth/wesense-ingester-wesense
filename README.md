# WeSense Ingester - WeSense

Specialized ingester for WeSense WiFi and LoRa sensors. Decodes WeSense v2 protobuf, performs reverse geocoding, and publishes decoded JSON to a unified MQTT topic.

## Why a Specialized Ingester?

WeSense sensors are simpler than Meshtastic because **every message includes position**:

| Transport | Topic | Position |
|-----------|-------|----------|
| WiFi (ESP32) | `wesense/v2/{country}/{subdivision}/{device_id}` | In protobuf payload |
| LoRa (TTN webhook) | `wesense/v2/lora/{device_id}` | In protobuf payload |

The ingester just needs to:
1. Decode protobuf payload
2. Reverse geocode lat/lon → ISO 3166 country/subdivision
3. Publish decoded JSON

No position caching or correlation required (unlike Meshtastic).

## Architecture

```
WeSense WiFi Sensors        WeSense LoRa Sensors
     (ESP32)                 (via TTN Webhook)
         │                          │
         │  wesense/v2/{loc}/...    │  wesense/v2/lora/...
         │                          │
         └──────────┬───────────────┘
                    │
                    ▼
           ┌───────────────┐
           │   WeSense     │
           │   Ingester    │
           │  (this svc)   │
           └───────┬───────┘
                   │
              ┌────▼────┐
              │ Decode  │
              │Protobuf │
              └────┬────┘
                   │
              ┌────▼────┐
              │Reverse  │
              │Geocode  │
              └────┬────┘
                   │
                   ▼
  wesense/decoded/{country}/{subdivision}/{device_id}
                   │
                   ▼
            ┌──────────┐
            │ Telegraf │ ──► InfluxDB
            └──────────┘
```

## Output Format

Publishes to: `wesense/decoded/{country}/{subdivision}/{device_id}`

```json
{
  "device_id": "wesense_0xABCD1234",
  "timestamp": 1732291200,
  "latitude": -36.848461,
  "longitude": 174.763336,
  "country": "New Zealand",
  "country_code": "nz",
  "subdivision_code": "auk",
  "data_source": "WESENSE",
  "transport_type": "WIFI",
  "measurements": [
    {"reading_type": "TEMPERATURE", "value": 22.5, "sensor_model": "BME280"},
    {"reading_type": "HUMIDITY", "value": 65.3, "sensor_model": "BME280"},
    {"reading_type": "PRESSURE", "value": 1013.25, "sensor_model": "BME280"}
  ]
}
```

## Quick Start

### Docker

```bash
docker build -t wesense-ingester-wesense .
docker run -d \
  -e MQTT_BROKER=your-broker \
  -e MQTT_SUBSCRIBE_TOPICS="wesense/v2/#" \
  wesense-ingester-wesense
```

### Without Docker

```bash
pip install -r requirements.txt
python wesense_decoder.py --config config.yaml
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BROKER` | localhost | MQTT broker address |
| `MQTT_PORT` | 1883 | MQTT broker port |
| `MQTT_USERNAME` | (none) | MQTT username |
| `MQTT_PASSWORD` | (none) | MQTT password |
| `MQTT_SUBSCRIBE_TOPICS` | wesense/v2/# | Comma-separated topics |
| `MQTT_TOPIC_PREFIX` | wesense/decoded | Output topic prefix |
| `LOG_LEVEL` | INFO | Logging level |

### Config File

```yaml
input:
  type: mqtt
  mqtt:
    broker: localhost
    port: 1883
    subscribe_topics:
      - "wesense/v2/#"

output:
  type: mqtt
  mqtt:
    broker: localhost
    port: 1883
    preserve_topics: false
    topic_prefix: "wesense/decoded"
```

## LoRaWAN Support

LoRaWAN sensors connect via The Things Network (TTN):

1. TTN receives LoRaWAN uplinks from WeSense sensors
2. `wesense-ttn-webhook` receives HTTP webhooks from TTN
3. Webhook publishes raw protobuf to `wesense/v2/lora/{device_id}`
4. This ingester decodes and publishes to `wesense/decoded/{country}/{subdivision}/{device_id}`

See the `wesense-ttn-webhook` project for webhook setup.

## Integration with Telegraf

```toml
[[inputs.mqtt_consumer]]
  servers = ["tcp://localhost:1883"]
  topics = ["wesense/decoded/#"]
  data_format = "json"
  tag_keys = ["device_id", "country_code", "subdivision_code", "data_source"]
```

## Related Components

- **wesense-mqtt-hub** - Central MQTT broker for the WeSense network
- **wesense-ingester-meshtastic** - Handles Meshtastic environmental sensors (position caching, correlation)
- **wesense-ttn-webhook** - Receives TTN webhooks for LoRaWAN sensors

## License

MIT
