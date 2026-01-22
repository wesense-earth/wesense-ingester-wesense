#!/usr/bin/env python3
"""
WeSense WiFi Ingester
Specialized ingester for WeSense WiFi sensors.

Architecture:
- Input Layer: MQTT subscriber (wesense/v2/wifi/#)
- Decode Layer: Protobuf parser for WeSense v2.2 format
- Output Layer: ClickHouse (primary) + optional MQTT (debugging)

Message Format:
- WeSense v2.1: SensorReadingV2 with repeated SensorValue measurements
- WiFi sensors send full messages with all fields (no caching needed)

Topic Format:
- wesense/v2/wifi/{country}/{subdivision}/{device_id}

Note: LoRa data is handled by wesense-ingester-lora (with metadata caching)
      Meshtastic data is handled by wesense-ingester-meshtastic

Usage:
    python ingester_wesense.py --config config.yaml
"""

import sys
import os
import json
import logging
import logging.handlers
import argparse
import threading
import time
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone
from pathlib import Path

import yaml
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ClickHouse support
try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    print("Warning: clickhouse_connect not available. Install with: pip install clickhouse-connect")

# Add proto directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'proto'))

# Import WeSense v2 format (consolidated format with repeated SensorValue)
import wesense_homebrew_v2_pb2 as proto

# Add utils directory to path
sys.path.insert(0, str(Path(__file__).parent / 'utils'))
from geocoding import ReverseGeocoder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Create logs directory
os.makedirs('logs', exist_ok=True)

# Future timestamp logger - dedicated log for devices with incorrect RTC
future_timestamp_logger = logging.getLogger('future_timestamps')
future_timestamp_logger.setLevel(logging.WARNING)
_future_ts_handler = logging.handlers.RotatingFileHandler(
    'logs/future_timestamps.log',
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5
)
_future_ts_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
future_timestamp_logger.addHandler(_future_ts_handler)

# =====================================================================
# Configuration from environment (with defaults)
# =====================================================================
DEBUG = os.getenv('DEBUG', 'false').lower() in ('true', '1', 'yes')

# ClickHouse settings
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'wesense')
CLICKHOUSE_TABLE = os.getenv('CLICKHOUSE_TABLE', 'sensor_readings')
CLICKHOUSE_BATCH_SIZE = int(os.getenv('CLICKHOUSE_BATCH_SIZE', '100'))
CLICKHOUSE_FLUSH_INTERVAL = int(os.getenv('CLICKHOUSE_FLUSH_INTERVAL', '5'))

# Ingestion node ID (for tracking which ingester wrote the data)
INGESTION_NODE_ID = os.getenv('INGESTION_NODE_ID', 'wesense-ingester-1')

# Future timestamp tolerance - reject readings more than 30 seconds in the future
FUTURE_TIMESTAMP_TOLERANCE = 30

# =====================================================================
# Reading type mapping (protobuf enum name -> database column name)
# =====================================================================
READING_TYPE_MAP = {
    'READING_UNKNOWN': 'unknown',
    'TEMPERATURE': 'temperature',
    'HUMIDITY': 'humidity',
    'CO2': 'co2',
    'PRESSURE': 'pressure',
    'PM1': 'pm1_0',
    'PM2_5': 'pm2_5',  # v2.1 naming (renamed from PM25)
    'PM25': 'pm2_5',   # v2.0 backward compat
    'PM10': 'pm10',
    'VOC_INDEX': 'voc_index',
    'NOX_INDEX': 'nox_index',
    'VOLTAGE': 'voltage',
    'CURRENT': 'current',
    'POWER': 'power',
    'BATTERY_LEVEL': 'battery_level',
    'GAS_RESISTANCE': 'gas_resistance',  # v2.2 - BME680 gas resistance in kOhm
    # Extended particle counts (v2.1, WiFi only)
    'PC_03UM': 'pc_03um',
    'PC_05UM': 'pc_05um',
    'PC_10UM': 'pc_10um',
    'PC_25UM': 'pc_25um',
    'PC_50UM': 'pc_50um',
    'PC_100UM': 'pc_100um',
}

# Unit mapping for reading types
READING_UNITS = {
    'temperature': '°C',
    'humidity': '%',
    'co2': 'ppm',
    'pressure': 'hPa',
    'pm1_0': 'µg/m³',
    'pm2_5': 'µg/m³',
    'pm10': 'µg/m³',
    'voc_index': '',
    'nox_index': '',
    'voltage': 'V',
    'current': 'mA',
    'power': 'mW',
    'battery_level': '%',
    'gas_resistance': 'kOhm',  # v2.2 - BME680
    # Extended particle counts (v2.1)
    'pc_03um': '/L',
    'pc_05um': '/L',
    'pc_10um': '/L',
    'pc_25um': '/L',
    'pc_50um': '/L',
    'pc_100um': '/L',
}

# Decimal places for value rounding (to avoid float precision noise)
READING_DECIMALS = {
    'temperature': 2,
    'humidity': 1,
    'co2': 0,
    'pressure': 2,  # BMP280/BMP390/BME680: 0.01 hPa resolution
    'pm1_0': 1,
    'pm2_5': 1,
    'pm10': 1,
    'voc_index': 0,
    'nox_index': 0,
    'voltage': 2,
    'current': 2,
    'power': 2,
    'battery_level': 0,
    'gas_resistance': 2,
    'pc_03um': 0,
    'pc_05um': 0,
    'pc_10um': 0,
    'pc_25um': 0,
    'pc_50um': 0,
    'pc_100um': 0,
}

# Sensor model mapping (v2.3)
SENSOR_MODEL_MAP = {
    'SENSOR_UNKNOWN': None,
    'SHT4X': 'SHT4x',
    'AHT20': 'AHT20',
    'SCD4X': 'SCD4x',
    'SCD30': 'SCD30',    # v2.2 - older Sensirion CO2 sensor
    'BMP280': 'BMP280',
    'BMP390': 'BMP390',  # v2.2 - high precision pressure sensor
    'BME280': 'BME280',
    'BME680': 'BME680',
    'SGP41': 'SGP41',
    'PMS5003': 'PMS5003',
    'INA219': 'INA219',
    'CM1106C': 'CM1106-C',  # Cubic NDIR CO2 sensor (I2C or UART)
    'TMP117': 'TMP117',     # v2.3 - TI high-precision temperature (±0.1°C)
    'AXP2101': 'AXP2101',   # v2.3 - X-Powers PMU (battery management)
}

# Board type mapping (v2.3)
BOARD_TYPE_MAP = {
    'BOARD_UNKNOWN': 'UNKNOWN',
    # LilyGo boards (1-19)
    'LILYGO_T3_S3': 'LilyGo T3 S3',
    'LILYGO_T_BEAM': 'LilyGo T-Beam',
    'LILYGO_T_BEAM_S3': 'LilyGo T-Beam S3',
    'LILYGO_T3_LORA32': 'LilyGo T3 LoRa32',
    'LILYGO_T3_S3_V1_3': 'LilyGo T3 S3 v1.3',
    'LILYGO_T3_S3_LR1121': 'LilyGo T3 S3 LR1121',
    'LILYGO_T_BEAM_V0_7': 'LilyGo T-Beam v0.7',
    'LILYGO_T_BEAM_S3_CORE': 'LilyGo T-Beam S3 Core',
    'LILYGO_T_ECHO': 'LilyGo T-Echo',
    'LILYGO_T_DECK': 'LilyGo T-Deck',
    'LILYGO_TLORA_V1': 'LilyGo T-LoRa V1',
    'LILYGO_TLORA_V2_1_1P6': 'LilyGo T-LoRa V2.1 1.6',
    # Heltec boards (20-29)
    'HELTEC_WIFI_LORA_32_V2': 'Heltec WiFi LoRa 32 V2',
    'HELTEC_WIFI_LORA_32_V3': 'Heltec WiFi LoRa 32 V3',
    'HELTEC_WIRELESS_STICK_LITE_V3': 'Heltec Wireless Stick Lite V3',
    'HELTEC_WIRELESS_TRACKER': 'Heltec Wireless Tracker',
    'HELTEC_WIRELESS_PAPER': 'Heltec Wireless Paper',
    'HELTEC_HT62': 'Heltec HT62',
    # RAK Wireless boards (30-39)
    'RAK4631': 'RAK4631',
    'RAK11200': 'RAK11200',
    'RAK11310': 'RAK11310',
    # Generic ESP32 variants (40-59)
    'ESP32_DEVKIT': 'ESP32 DevKit',
    'ESP32_S2_DEVKIT': 'ESP32-S2 DevKit',
    'ESP32_S3_DEVKIT': 'ESP32-S3 DevKit',
    'ESP32_C3_DEVKIT': 'ESP32-C3 DevKit',
    'ESP32_C6_DEVKIT': 'ESP32-C6 DevKit',
    'ESP32_C2_DEVKIT': 'ESP32-C2 DevKit',
    'ESP32_H2_DEVKIT': 'ESP32-H2 DevKit',
    'ESP8266_D1_MINI': 'ESP8266 D1 Mini',
    # DFRobot boards (60-69)
    'DFROBOT_ESP32_C6_BEETLE': 'DFRobot ESP32-C6 Beetle',
    # Other manufacturers (70-79)
    'STATION_G2': 'Station G2',
    'CDEBYTE_EORA_S3': 'CDEBYTE EORA S3',
    'RP2040_LORA': 'RP2040 LoRa',
    'RPI_PICO': 'Raspberry Pi Pico',
    # DIY / Custom (80-89)
    'CUSTOM_HOMEBREW': 'Custom Homebrew',
    'DIY_V1': 'DIY V1',
    'NRF52_PROMICRO_DIY': 'nRF52 ProMicro DIY',
    'PORTDUINO': 'Portduino',
    'PRIVATE_HW': 'Private Hardware',
    # Pre-calibrated commercial (100+)
    'WESENSE_SENTINEL': 'WeSense Sentinel',
    'WESENSE_SCOUT': 'WeSense Scout',
}

# Deployment type mapping
DEPLOYMENT_TYPE_MAP = {
    'DEPLOYMENT_UNKNOWN': 'UNKNOWN',
    'INDOOR': 'INDOOR',
    'OUTDOOR': 'OUTDOOR',
    'MIXED': 'MIXED',
}

# Transport type mapping
TRANSPORT_TYPE_MAP = {
    'TRANSPORT_UNKNOWN': 'UNKNOWN',
    'WIFI_MQTT': 'WIFI',
    'LORAWAN': 'LORA',
    'TRANSPORT_MESHTASTIC': 'MESHTASTIC',
    'CELLULAR': 'CELLULAR',
    'SATELLITE': 'SATELLITE',
}

# =====================================================================
# ClickHouse Client
# =====================================================================
clickhouse_client = None
clickhouse_buffer = []
clickhouse_buffer_lock = threading.Lock()
clickhouse_flush_timer = None


def flush_clickhouse_buffer():
    """Flush the ClickHouse write buffer to the database"""
    global clickhouse_buffer, clickhouse_flush_timer

    with clickhouse_buffer_lock:
        if not clickhouse_buffer or not clickhouse_client:
            return

        rows_to_insert = clickhouse_buffer.copy()
        clickhouse_buffer = []

    try:
        # Column names matching the sensor_readings schema
        columns = [
            'timestamp', 'device_id', 'data_source', 'network_source', 'ingestion_node_id',
            'reading_type', 'value', 'unit',
            'latitude', 'longitude', 'altitude', 'geo_country', 'geo_subdivision',
            'board_model', 'sensor_model', 'deployment_type', 'deployment_type_source',
            'transport_type', 'deployment_location', 'node_name', 'node_info', 'node_info_url'
        ]

        clickhouse_client.insert(
            f'{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}',
            rows_to_insert,
            column_names=columns
        )

        logger.info(f"[ClickHouse] Flushed {len(rows_to_insert)} rows")

    except Exception as e:
        logger.error(f"[ClickHouse] Flush failed: {e}")
        # Put rows back in buffer for retry
        with clickhouse_buffer_lock:
            clickhouse_buffer = rows_to_insert + clickhouse_buffer


def schedule_clickhouse_flush():
    """Schedule the next buffer flush"""
    global clickhouse_flush_timer
    if clickhouse_flush_timer:
        clickhouse_flush_timer.cancel()
    clickhouse_flush_timer = threading.Timer(CLICKHOUSE_FLUSH_INTERVAL, periodic_flush)
    clickhouse_flush_timer.daemon = True
    clickhouse_flush_timer.start()


def periodic_flush():
    """Periodic flush callback"""
    if DEBUG:
        logger.debug(f"[ClickHouse] Periodic flush, buffer size: {len(clickhouse_buffer)}")
    flush_clickhouse_buffer()
    schedule_clickhouse_flush()


def add_to_clickhouse_buffer(row: Tuple):
    """Add a row to the ClickHouse buffer and flush if needed"""
    global clickhouse_buffer

    with clickhouse_buffer_lock:
        clickhouse_buffer.append(row)
        buffer_size = len(clickhouse_buffer)

    if buffer_size >= CLICKHOUSE_BATCH_SIZE:
        flush_clickhouse_buffer()


def init_clickhouse():
    """Initialize ClickHouse connection"""
    global clickhouse_client

    if not CLICKHOUSE_AVAILABLE:
        logger.warning("ClickHouse client not available, data will not be persisted")
        return False

    try:
        clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )

        # Test connection
        clickhouse_client.ping()
        logger.info(f"[ClickHouse] Connected to {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}")

        # Start periodic flush timer
        schedule_clickhouse_flush()
        return True

    except Exception as e:
        logger.error(f"[ClickHouse] Connection failed: {e}")
        clickhouse_client = None
        return False


# =====================================================================
# Protobuf Decoder
# =====================================================================
class ProtobufDecoder:
    """Decode protobuf messages to structured data"""

    @staticmethod
    def decode_sensor_reading(payload: bytes) -> Optional[Dict[str, Any]]:
        """
        Decode a SensorReadingV2 protobuf message

        Args:
            payload: Raw protobuf bytes

        Returns:
            Decoded data as dict with 'measurements' list, or None if decoding fails
        """
        try:
            msg = proto.SensorReadingV2()
            msg.ParseFromString(payload)

            # Extract measurements from repeated SensorValue field
            measurements = []
            for sv in msg.measurements:
                reading_type_name = proto.ReadingType.Name(sv.reading_type)
                sensor_model_name = proto.SensorModel.Name(sv.sensor_model) if sv.sensor_model else None

                measurement = {
                    'reading_type': READING_TYPE_MAP.get(reading_type_name, reading_type_name.lower()),
                    'reading_type_raw': reading_type_name,
                    'value': sv.value,
                    'sensor_model': SENSOR_MODEL_MAP.get(sensor_model_name, sensor_model_name),
                    # v2.1: per-reading timestamps removed, use header timestamp
                    'timestamp': msg.timestamp,
                }
                measurements.append(measurement)

            # Get enum names and map them
            deployment_type_name = proto.DeploymentType.Name(msg.deployment_type)

            # v2.1: board_type replaces vendor/product_line/device_type
            board_type_name = None
            if hasattr(msg, 'board_type') and msg.board_type:
                board_type_name = proto.BoardType.Name(msg.board_type)

            # v2.1: node_name for friendly device names
            node_name = None
            if hasattr(msg, 'node_name') and msg.node_name:
                node_name = msg.node_name

            # v2.2: node_info for physical setup description
            node_info = None
            if hasattr(msg, 'node_info') and msg.node_info:
                node_info = msg.node_info

            # v2.2: node_info_url for detailed documentation link
            node_info_url = None
            if hasattr(msg, 'node_info_url') and msg.node_info_url:
                node_info_url = msg.node_info_url

            # v2.1: altitude_cm
            altitude = None
            if hasattr(msg, 'altitude_cm') and msg.altitude_cm:
                altitude = msg.altitude_cm / 100.0  # Convert cm to meters

            # Extract data into structured dict
            data = {
                # Device info
                'device_id': f"wesense_{msg.device_id:016x}",
                'device_id_hex': f"0x{msg.device_id:016X}",
                'data_source': 'WESENSE',

                # Temporal
                'timestamp': msg.timestamp,

                # Location (scaled integers)
                'latitude': msg.latitude_e5 / 100000.0 if msg.latitude_e5 else None,
                'longitude': msg.longitude_e5 / 100000.0 if msg.longitude_e5 else None,
                'altitude': altitude,

                # Device identification (v2.1 board_type preferred)
                'board_type': BOARD_TYPE_MAP.get(board_type_name, board_type_name) if board_type_name else None,
                'node_name': node_name,
                'node_info': node_info,          # v2.2 - physical setup description
                'node_info_url': node_info_url,  # v2.2 - documentation link

                # Legacy device taxonomy (deprecated in v2.1, kept for backward compat)
                'vendor': proto.Vendor.Name(msg.vendor) if msg.vendor else None,
                'product_line': proto.ProductLine.Name(msg.product_line) if msg.product_line else None,
                'device_type': proto.DeviceType.Name(msg.device_type) if msg.device_type else None,

                # Deployment context (transport_type inferred from topic, not message)
                'deployment_type': DEPLOYMENT_TYPE_MAP.get(deployment_type_name, 'UNKNOWN'),
                'transport_type': 'WIFI',  # This ingester only handles WiFi

                # All sensor measurements
                'measurements': measurements,
            }

            return data

        except Exception as e:
            logger.error(f"Failed to decode protobuf: {e}")
            if DEBUG:
                logger.debug(f"Raw payload (first 50 bytes): {payload[:50].hex()}")
            return None


# =====================================================================
# MQTT Input Handler
# =====================================================================
class MQTTInputHandler:
    """Handle MQTT input from WeSense sensors"""

    def __init__(self, config: Dict[str, Any], decoder: ProtobufDecoder, mqtt_output=None):
        self.config = config
        self.decoder = decoder
        self.mqtt_output = mqtt_output
        self.client = None
        self.geocoder = ReverseGeocoder()

    def connect(self):
        """Connect to MQTT broker"""
        mqtt_config = self.config['input']['mqtt']

        client_id = os.getenv('MQTT_INPUT_CLIENT_ID') or mqtt_config.get('client_id', 'ingester_wesense_input')

        self.client = mqtt.Client(client_id=client_id, clean_session=True)

        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        username = os.getenv('MQTT_USERNAME') or mqtt_config.get('username')
        password = os.getenv('MQTT_PASSWORD') or mqtt_config.get('password')

        if username and password:
            self.client.username_pw_set(username, password)

        broker = os.getenv('MQTT_BROKER') or mqtt_config['broker']
        port = int(os.getenv('MQTT_PORT', mqtt_config.get('port', 1883)))

        logger.info(f"Connecting to MQTT broker {broker}:{port}...")
        self.client.connect(broker, port, keepalive=60)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT broker")

            topics_env = os.getenv('MQTT_SUBSCRIBE_TOPICS')
            if topics_env:
                topics = [t.strip() for t in topics_env.split(',')]
            else:
                topics = self.config['input']['mqtt'].get('subscribe_topics', ['wesense/v2/#'])

            for topic in topics:
                logger.info(f"Subscribing to: {topic}")
                client.subscribe(topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection (code: {rc}), will auto-reconnect")

    def _on_message(self, client, userdata, msg):
        """Process incoming MQTT message"""
        try:
            # Skip non-sensor topics
            topic_parts = msg.topic.split('/')
            if 'diagnostic' in topic_parts or 'status' in topic_parts:
                return

            # Skip Meshtastic topics (handled by meshtastic-ingester)
            if msg.topic.startswith('msh/'):
                return

            # Skip LoRa topics (handled by wesense-ingester-lora)
            if '/lora/' in msg.topic or msg.topic.endswith('/lora'):
                return

            # Check if protobuf (first byte is not { or [)
            if len(msg.payload) > 0 and msg.payload[0] not in (0x7B, 0x5B):
                self._handle_protobuf(msg.topic, msg.payload)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def _handle_protobuf(self, topic: str, payload: bytes):
        """Decode protobuf and write to ClickHouse"""
        decoded = self.decoder.decode_sensor_reading(payload)
        if not decoded:
            return

        latitude = decoded.get('latitude')
        longitude = decoded.get('longitude')

        # Reverse geocode if we have coordinates
        geo_country = None
        geo_subdivision = None

        if latitude and longitude:
            location_info = self.geocoder.reverse_geocode(latitude, longitude)
            if location_info:
                geo_country = location_info.get('country_code', '').upper()
                geo_subdivision = self.geocoder.format_subdivision_code(location_info.get('admin1'))
                decoded['geo_country'] = geo_country
                decoded['geo_subdivision'] = geo_subdivision
                decoded['city'] = location_info.get('city')
                decoded['country'] = location_info.get('country')
                decoded['admin1'] = location_info.get('admin1')

                if DEBUG:
                    logger.debug(f"Geocoded: {latitude}, {longitude} -> {geo_country}/{geo_subdivision}")

        # Parse topic to get device info
        topic_parts = topic.split('/')

        # Format: wesense/v2/wifi/{country}/{subdivision}/{device_id}
        if len(topic_parts) >= 6 and topic_parts[0] == 'wesense' and topic_parts[2] == 'wifi':
            network_source = "wesense/v2/wifi"
            # Use geocoded values if available, otherwise fall back to topic
            if not geo_country:
                geo_country = topic_parts[3].upper()
            if not geo_subdivision:
                geo_subdivision = topic_parts[4]
        # Legacy format: wesense/v2/{country}/{subdivision}/{device_id}
        elif len(topic_parts) >= 5 and topic_parts[0] == 'wesense' and topic_parts[1] in ('v1', 'v2'):
            network_source = f"{topic_parts[0]}/{topic_parts[1]}"
            if not geo_country:
                geo_country = topic_parts[2].upper()
            if not geo_subdivision:
                geo_subdivision = topic_parts[3]
        else:
            network_source = topic

        # Write each measurement to ClickHouse
        if clickhouse_client:
            device_id = decoded['device_id']
            timestamp = decoded['timestamp']

            # Check for future timestamps
            current_time = int(time.time())
            time_delta = timestamp - current_time
            if time_delta > FUTURE_TIMESTAMP_TOLERANCE:
                node_name = decoded.get('node_name') or device_id
                # Format delta for readability
                if time_delta > 86400:
                    delta_str = f"{time_delta / 86400:.1f} days"
                elif time_delta > 3600:
                    delta_str = f"{time_delta / 3600:.1f} hours"
                elif time_delta > 60:
                    delta_str = f"{time_delta / 60:.1f} minutes"
                else:
                    delta_str = f"{time_delta} seconds"

                future_timestamp_logger.warning(
                    f"FUTURE_TIMESTAMP | node_name={node_name} | device_id={device_id} | "
                    f"timestamp={datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"ahead_by={delta_str} | raw_delta_seconds={time_delta}"
                )
                if DEBUG:
                    logger.warning(f"⏰ FUTURE TIMESTAMP {node_name}: {delta_str} ahead - SKIPPING")
                return  # Skip this message

            deployment_type = decoded.get('deployment_type', 'UNKNOWN')
            # WeSense sensors have deployment_type set in firmware config, so source is 'manual'
            # If deployment_type is UNKNOWN, source should also be 'unknown'
            deployment_type_source = 'manual' if deployment_type != 'UNKNOWN' else 'unknown'
            transport_type = decoded.get('transport_type', 'WIFI')
            altitude = decoded.get('altitude')
            board_type = decoded.get('board_type') or ''
            node_name = decoded.get('node_name')
            node_info = decoded.get('node_info')
            node_info_url = decoded.get('node_info_url')

            for measurement in decoded.get('measurements', []):
                reading_type = measurement['reading_type']
                raw_value = measurement['value']
                sensor_model = measurement.get('sensor_model')
                unit = READING_UNITS.get(reading_type, '')

                # Round value to appropriate decimal places (avoid float precision noise)
                decimals = READING_DECIMALS.get(reading_type, 2)
                value = round(float(raw_value), decimals)

                # Use per-reading timestamp if different from header
                reading_timestamp = measurement.get('timestamp', timestamp)

                # Build row tuple matching column order
                # Use empty strings for non-nullable LowCardinality(String) columns
                row = (
                    datetime.fromtimestamp(reading_timestamp, tz=timezone.utc),  # timestamp
                    device_id,                    # device_id
                    'WESENSE',                    # data_source
                    network_source,               # network_source
                    INGESTION_NODE_ID,            # ingestion_node_id
                    reading_type,                 # reading_type
                    value,                        # value (rounded)
                    unit,                         # unit
                    float(latitude) if latitude else None,   # latitude (Nullable)
                    float(longitude) if longitude else None, # longitude (Nullable)
                    float(altitude) if altitude else None,   # altitude (Nullable)
                    geo_country or '',            # geo_country
                    geo_subdivision or '',        # geo_subdivision
                    board_type,                   # board_model
                    sensor_model or '',           # sensor_model
                    deployment_type,              # deployment_type
                    deployment_type_source,       # deployment_type_source
                    transport_type,               # transport_type
                    '',                           # deployment_location
                    node_name,                    # node_name (Nullable)
                    node_info,                    # node_info (Nullable)
                    node_info_url,                # node_info_url (Nullable)
                )

                add_to_clickhouse_buffer(row)

            logger.info(f"[{geo_country}/{geo_subdivision}] Buffered {len(decoded['measurements'])} readings from {device_id}")

        # Optionally publish to MQTT for debugging
        if self.mqtt_output:
            self.mqtt_output.publish(topic, decoded)

    def run(self):
        """Start MQTT client loop"""
        logger.info("Starting MQTT input handler...")
        self.client.loop_forever()


# =====================================================================
# MQTT Output Handler (optional, for debugging)
# =====================================================================
class MQTTOutputHandler:
    """Output decoded data to MQTT (for debugging)"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = None

    def connect(self):
        mqtt_config = self.config['output']['mqtt']

        client_id = os.getenv('MQTT_OUTPUT_CLIENT_ID') or mqtt_config.get('client_id', 'ingester_wesense_output')

        self.client = mqtt.Client(client_id=client_id, clean_session=True)

        username = os.getenv('MQTT_USERNAME') or mqtt_config.get('username')
        password = os.getenv('MQTT_PASSWORD') or mqtt_config.get('password')

        if username and password:
            self.client.username_pw_set(username, password)

        broker = os.getenv('MQTT_BROKER') or mqtt_config['broker']
        port = int(os.getenv('MQTT_PORT', mqtt_config.get('port', 1883)))

        logger.info(f"Connecting to output MQTT broker {broker}:{port}...")
        self.client.connect(broker, port, keepalive=60)
        self.client.loop_start()

    def publish(self, source_topic: str, data: Dict[str, Any]):
        """Publish decoded data as JSON to MQTT"""
        try:
            output_config = self.config['output']['mqtt']
            prefix = os.getenv('MQTT_TOPIC_PREFIX') or output_config.get('topic_prefix', 'wesense/decoded')

            geo_country = data.get('geo_country', 'unknown').lower()
            geo_subdivision = data.get('geo_subdivision', 'unknown').lower()
            device_id = data.get('device_id', 'unknown')

            output_topic = f"{prefix}/wesense/{geo_country}/{geo_subdivision}/{device_id}"

            payload = json.dumps(data, indent=None, default=str)
            self.client.publish(output_topic, payload, qos=0, retain=False)

            if DEBUG:
                logger.debug(f"Published to {output_topic}")

        except Exception as e:
            logger.error(f"Error publishing to MQTT: {e}")


# =====================================================================
# Main
# =====================================================================
def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description='WeSense Ingester')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging
    global DEBUG
    DEBUG = args.debug or os.getenv('DEBUG', 'false').lower() in ('true', '1', 'yes')
    log_level = 'DEBUG' if DEBUG else os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))

    # Load configuration
    logger.info(f"Loading configuration from {args.config}")
    config = load_config(args.config)

    # Initialize ClickHouse
    init_clickhouse()

    # Initialize decoder
    decoder = ProtobufDecoder()

    # Initialize MQTT output (always enabled for debugging)
    mqtt_output = MQTTOutputHandler(config)
    mqtt_output.connect()

    # Initialize MQTT input
    input_handler = MQTTInputHandler(config, decoder, mqtt_output)
    input_handler.connect()

    try:
        input_handler.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Final flush
        flush_clickhouse_buffer()
        if clickhouse_client:
            clickhouse_client.close()
            logger.info("[ClickHouse] Connection closed")


if __name__ == '__main__':
    main()
