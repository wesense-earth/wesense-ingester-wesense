#!/usr/bin/env python3
"""
WeSense Ingester (Unified)

Handles all WeSense sensor protocols in a single adapter:
- WiFi: Full SensorReadingV2 messages via MQTT (wesense/v2/wifi/#)
- LoRa: Split SensorReadingV2 + DeviceMetadataV2 via MQTT (wesense/v2/lora/#)
- TTN Webhook: HTTP POST from The Things Network (optional, TTN_WEBHOOK_ENABLED=true)

Uses wesense-ingester-core for shared infrastructure:
  - BufferedClickHouseWriter for batched database writes
  - WeSensePublisher for MQTT output
  - ReverseGeocoder for coordinate-to-location lookup
  - setup_logging for colored console + rotating file logs

Replaces the separate wesense-ingester-wesense (WiFi),
wesense-ingester-wesense-lora, and wesense-ttn-webhook services.

Usage:
    python wesense_ingester.py
    TTN_WEBHOOK_ENABLED=true python wesense_ingester.py
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

from wesense_ingester import (
    BufferedClickHouseWriter,
    ReverseGeocoder,
    setup_logging,
)
from wesense_ingester.clickhouse.writer import ClickHouseConfig
from wesense_ingester.mqtt.publisher import MQTTPublisherConfig, WeSensePublisher
from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig
from wesense_ingester.signing.signer import ReadingSigner
from wesense_ingester.zenoh.config import ZenohConfig
from wesense_ingester.zenoh.publisher import ZenohPublisher
from wesense_ingester.zenoh.queryable import ZenohQueryable
from wesense_ingester.registry.config import RegistryConfig
from wesense_ingester.registry.client import RegistryClient
from wesense_ingester.signing.trust import TrustStore

# Protobuf support
sys.path.insert(0, str(Path(__file__).parent / "proto"))
import wesense_homebrew_v2_pb2 as proto  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────

INGESTION_NODE_ID = os.getenv("INGESTION_NODE_ID", "wesense-ingester-1")
FUTURE_TIMESTAMP_TOLERANCE = 30  # seconds
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
STATS_INTERVAL = int(os.getenv("STATS_INTERVAL", "60"))

# TTN webhook (optional)
TTN_WEBHOOK_ENABLED = os.getenv("TTN_WEBHOOK_ENABLED", "false").lower() in ("true", "1", "yes")
TTN_WEBHOOK_HOST = os.getenv("TTN_WEBHOOK_HOST", "0.0.0.0")
TTN_WEBHOOK_PORT = int(os.getenv("TTN_WEBHOOK_PORT", "5000"))

# ClickHouse columns (25-column unified schema)
CLICKHOUSE_COLUMNS = [
    "timestamp", "device_id", "data_source", "network_source", "ingestion_node_id",
    "reading_type", "value", "unit",
    "latitude", "longitude", "altitude", "geo_country", "geo_subdivision",
    "board_model", "sensor_model", "deployment_type", "deployment_type_source",
    "transport_type", "deployment_location", "node_name", "node_info", "node_info_url",
    "signature", "ingester_id", "key_version",
]

# ── Mapping tables ────────────────────────────────────────────────────

READING_TYPE_MAP = {
    "READING_UNKNOWN": "unknown",
    "TEMPERATURE": "temperature",
    "HUMIDITY": "humidity",
    "CO2": "co2",
    "PRESSURE": "pressure",
    "PM1": "pm1_0",
    "PM2_5": "pm2_5",
    "PM25": "pm2_5",       # v2.0 backward compat
    "PM10": "pm10",
    "VOC_INDEX": "voc_index",
    "NOX_INDEX": "nox_index",
    "VOLTAGE": "voltage",
    "CURRENT": "current",
    "POWER": "power",
    "BATTERY_LEVEL": "battery_level",
    "GAS_RESISTANCE": "gas_resistance",
    "PC_03UM": "pc_03um",
    "PC_05UM": "pc_05um",
    "PC_10UM": "pc_10um",
    "PC_25UM": "pc_25um",
    "PC_50UM": "pc_50um",
    "PC_100UM": "pc_100um",
}

READING_UNITS = {
    "temperature": "\u00b0C",
    "humidity": "%",
    "co2": "ppm",
    "pressure": "hPa",
    "pm1_0": "\u00b5g/m\u00b3",
    "pm2_5": "\u00b5g/m\u00b3",
    "pm10": "\u00b5g/m\u00b3",
    "voc_index": "",
    "nox_index": "",
    "voltage": "V",
    "current": "mA",
    "power": "mW",
    "battery_level": "%",
    "gas_resistance": "kOhm",
    "pc_03um": "/L",
    "pc_05um": "/L",
    "pc_10um": "/L",
    "pc_25um": "/L",
    "pc_50um": "/L",
    "pc_100um": "/L",
}

READING_DECIMALS = {
    "temperature": 2,
    "humidity": 1,
    "co2": 0,
    "pressure": 2,
    "pm1_0": 1,
    "pm2_5": 1,
    "pm10": 1,
    "voc_index": 0,
    "nox_index": 0,
    "voltage": 2,
    "current": 2,
    "power": 2,
    "battery_level": 0,
    "gas_resistance": 2,
    "pc_03um": 0,
    "pc_05um": 0,
    "pc_10um": 0,
    "pc_25um": 0,
    "pc_50um": 0,
    "pc_100um": 0,
}

SENSOR_MODEL_MAP = {
    "SENSOR_UNKNOWN": None,
    "SHT4X": "SHT4x",
    "AHT20": "AHT20",
    "SCD4X": "SCD4x",
    "SCD30": "SCD30",
    "BMP280": "BMP280",
    "BMP390": "BMP390",
    "BME280": "BME280",
    "BME680": "BME680",
    "SGP41": "SGP41",
    "PMS5003": "PMS5003",
    "INA219": "INA219",
    "CM1106C": "CM1106-C",
    "TMP117": "TMP117",
    "AXP2101": "AXP2101",
    "MS5611": "MS5611",
}

BOARD_TYPE_MAP = {
    "BOARD_UNKNOWN": "UNKNOWN",
    # LilyGo boards (1-19)
    "LILYGO_T3_S3": "LilyGo T3 S3",
    "LILYGO_T_BEAM": "LilyGo T-Beam",
    "LILYGO_T_BEAM_S3": "LilyGo T-Beam S3",
    "LILYGO_T3_LORA32": "LilyGo T3 LoRa32",
    "LILYGO_T3_S3_V1_3": "LilyGo T3 S3 v1.3",
    "LILYGO_T3_S3_LR1121": "LilyGo T3 S3 LR1121",
    "LILYGO_T_BEAM_V0_7": "LilyGo T-Beam v0.7",
    "LILYGO_T_BEAM_S3_CORE": "LilyGo T-Beam S3 Core",
    "LILYGO_T_ECHO": "LilyGo T-Echo",
    "LILYGO_T_DECK": "LilyGo T-Deck",
    "LILYGO_TLORA_V1": "LilyGo T-LoRa V1",
    "LILYGO_TLORA_V2_1_1P6": "LilyGo T-LoRa V2.1 1.6",
    # Heltec boards (20-29)
    "HELTEC_WIFI_LORA_32_V2": "Heltec WiFi LoRa 32 V2",
    "HELTEC_WIFI_LORA_32_V3": "Heltec WiFi LoRa 32 V3",
    "HELTEC_WIRELESS_STICK_LITE_V3": "Heltec Wireless Stick Lite V3",
    "HELTEC_WIRELESS_TRACKER": "Heltec Wireless Tracker",
    "HELTEC_WIRELESS_PAPER": "Heltec Wireless Paper",
    "HELTEC_HT62": "Heltec HT62",
    # RAK Wireless boards (30-39)
    "RAK4631": "RAK4631",
    "RAK11200": "RAK11200",
    "RAK11310": "RAK11310",
    # Generic ESP32 variants (40-59)
    "ESP32_DEVKIT": "ESP32 DevKit",
    "ESP32_S2_DEVKIT": "ESP32-S2 DevKit",
    "ESP32_S3_DEVKIT": "ESP32-S3 DevKit",
    "ESP32_C3_DEVKIT": "ESP32-C3 DevKit",
    "ESP32_C6_DEVKIT": "ESP32-C6 DevKit",
    "ESP32_C2_DEVKIT": "ESP32-C2 DevKit",
    "ESP32_H2_DEVKIT": "ESP32-H2 DevKit",
    "ESP8266_D1_MINI": "ESP8266 D1 Mini",
    # DFRobot boards (60-69)
    "DFROBOT_ESP32_C6_BEETLE": "DFRobot ESP32-C6 Beetle",
    # Other manufacturers (70-79)
    "STATION_G2": "Station G2",
    "CDEBYTE_EORA_S3": "CDEBYTE EORA S3",
    "RP2040_LORA": "RP2040 LoRa",
    "RPI_PICO": "Raspberry Pi Pico",
    # DIY / Custom (80-89)
    "CUSTOM_HOMEBREW": "Custom Homebrew",
    "DIY_V1": "DIY V1",
    "NRF52_PROMICRO_DIY": "nRF52 ProMicro DIY",
    "PORTDUINO": "Portduino",
    "PRIVATE_HW": "Private Hardware",
    # Pre-calibrated commercial (100+)
    "WESENSE_SENTINEL": "WeSense Sentinel",
    "WESENSE_SCOUT": "WeSense Scout",
}

DEPLOYMENT_TYPE_MAP = {
    "DEPLOYMENT_UNKNOWN": "UNKNOWN",
    "INDOOR": "INDOOR",
    "OUTDOOR": "OUTDOOR",
    "MIXED": "MIXED",
}


# ── MetadataCache (LoRa split-message support) ───────────────────────

class MetadataCache:
    """
    In-memory cache for LoRa device metadata with disk persistence.

    LoRa sensors send DeviceMetadataV2 (location, board_type, node_name, etc.)
    separately from SensorReadingV2 (measurements). This cache stores the
    metadata so readings can be enriched when they arrive.
    """

    def __init__(self, cache_file: Optional[str] = None, max_age_hours: int = 72):
        self.cache: Dict[int, Dict[str, Any]] = {}
        self.cache_file = cache_file
        self.max_age_seconds = max_age_hours * 3600
        self.lock = threading.Lock()
        self.logger = logging.getLogger("MetadataCache")

        if cache_file:
            self._load_from_disk()

    def _load_from_disk(self):
        if not self.cache_file or not os.path.exists(self.cache_file):
            return
        try:
            with open(self.cache_file) as f:
                data = json.load(f)
            self.cache = {int(k): v for k, v in data.items()}
            self.logger.info("Loaded %d cached devices from %s", len(self.cache), self.cache_file)
        except Exception as e:
            self.logger.warning("Failed to load metadata cache: %s", e)

    def _save_to_disk(self):
        if not self.cache_file:
            return
        try:
            with open(self.cache_file, "w") as f:
                json.dump({str(k): v for k, v in self.cache.items()}, f)
        except Exception as e:
            self.logger.warning("Failed to save metadata cache: %s", e)

    def update(self, device_id: int, metadata: Dict[str, Any]):
        with self.lock:
            metadata["cached_at"] = time.time()
            self.cache[device_id] = metadata
            self._save_to_disk()

    def get(self, device_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            if device_id not in self.cache:
                return None
            metadata = self.cache[device_id]
            if time.time() - metadata.get("cached_at", 0) > self.max_age_seconds:
                return None
            return metadata

    def get_stats(self) -> Dict[str, int]:
        with self.lock:
            return {"total_devices": len(self.cache)}


# ── WeSenseIngester ───────────────────────────────────────────────────

class WeSenseIngester:
    """
    Unified WeSense ingester handling WiFi, LoRa, and TTN webhook inputs.

    Uses wesense-ingester-core for shared infrastructure:
      - BufferedClickHouseWriter for batched database writes
      - WeSensePublisher for MQTT output
      - ReverseGeocoder for coordinate-to-location lookup
      - setup_logging for colored console + rotating file logs
    """

    def __init__(self):
        # Logging
        self.logger = setup_logging(
            "wesense_ingester",
            enable_future_timestamp_log=True,
        )
        self.ft_logger = logging.getLogger("wesense_ingester.future_timestamps")

        # Core components
        self.geocoder = ReverseGeocoder()

        # ClickHouse writer
        try:
            self.ch_writer = BufferedClickHouseWriter(
                config=ClickHouseConfig.from_env(),
                columns=CLICKHOUSE_COLUMNS,
            )
        except Exception as e:
            self.logger.error("Failed to connect to ClickHouse: %s", e)
            self.logger.warning("Continuing without ClickHouse (MQTT output only)")
            self.ch_writer = None

        # MQTT publisher for decoded output (supports old WESENSE_OUTPUT_* env vars)
        mqtt_config = MQTTPublisherConfig(
            broker=os.getenv("WESENSE_OUTPUT_BROKER", os.getenv("MQTT_BROKER", "localhost")),
            port=int(os.getenv("WESENSE_OUTPUT_PORT", os.getenv("MQTT_PORT", "1883"))),
            username=os.getenv("WESENSE_OUTPUT_USERNAME", os.getenv("MQTT_USERNAME")),
            password=os.getenv("WESENSE_OUTPUT_PASSWORD", os.getenv("MQTT_PASSWORD")),
            client_id="wesense_unified_publisher",
        )
        self.publisher = WeSensePublisher(config=mqtt_config)
        self.publisher.connect()

        # Ed25519 signing
        key_config = KeyConfig.from_env()
        self.key_manager = IngesterKeyManager(config=key_config)
        self.key_manager.load_or_generate()
        self.signer = ReadingSigner(self.key_manager)
        self.logger.info("Ingester ID: %s (key version %d)", self.key_manager.ingester_id, self.key_manager.key_version)

        # OrbitDB registry — node registration + trust sync
        self.trust_store = TrustStore()
        registry_config = RegistryConfig.from_env()
        self.registry_client = RegistryClient(
            config=registry_config,
            trust_store=self.trust_store,
        )
        # Build zenoh_endpoint for node registry (WAN + LAN discovery)
        reg_metadata = {}
        announce_addr = os.getenv("ANNOUNCE_ADDRESS", "")
        zenoh_announce = os.getenv("ZENOH_ANNOUNCE_ADDRESS", "")
        zenoh_port = os.getenv("PORT_ZENOH", "7447")
        if announce_addr:
            reg_metadata["zenoh_endpoint"] = f"tcp/{announce_addr}:{zenoh_port}"
        # LAN endpoint for same-network peers (avoids NAT hairpin).
        # ZENOH_ANNOUNCE_ADDRESS is the host's LAN IP, passed from docker-compose.
        if zenoh_announce and zenoh_announce != announce_addr:
            reg_metadata["zenoh_endpoint_lan"] = f"tcp/{zenoh_announce}:{zenoh_port}"

        try:
            self.registry_client.register_node(
                ingester_id=self.key_manager.ingester_id,
                public_key_bytes=self.key_manager.public_key_bytes,
                key_version=self.key_manager.key_version,
                **reg_metadata,
            )
        except Exception as e:
            self.logger.warning("OrbitDB registration failed (%s), will retry on next trust sync", e)
        self.registry_client.start_trust_sync()
        self.logger.info("OrbitDB registry — trust sync active")

        # Zenoh publisher + queryable (optional, non-blocking)
        zenoh_config = ZenohConfig.from_env()
        if zenoh_config.enabled:
            self.zenoh_publisher = ZenohPublisher(config=zenoh_config, signer=self.signer)
            self.zenoh_publisher.connect()
            self.zenoh_queryable = ZenohQueryable(
                config=zenoh_config,
                clickhouse_config=ClickHouseConfig.from_env(),
            )
            self.zenoh_queryable.connect()
            self.zenoh_queryable.register("wesense/v2/live/**")
        else:
            self.zenoh_publisher = None
            self.zenoh_queryable = None

        # LoRa metadata cache
        cache_file = os.getenv("METADATA_CACHE_FILE", "cache/metadata_cache.json")
        os.makedirs(os.path.dirname(cache_file) or ".", exist_ok=True)
        self.metadata_cache = MetadataCache(cache_file=cache_file)

        # MQTT input client
        self.mqtt_client = None
        self.running = True

        # Stats
        self.stats = {
            "wifi_readings": 0,
            "lora_readings": 0,
            "lora_metadata": 0,
            "ttn_uplinks": 0,
            "future_timestamps": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }

    # ── Protobuf decoding ─────────────────────────────────────────

    def _decode_sensor_reading(self, payload: bytes) -> Optional[Dict[str, Any]]:
        """Decode a SensorReadingV2 protobuf message."""
        try:
            msg = proto.SensorReadingV2()
            msg.ParseFromString(payload)

            measurements = []
            for sv in msg.measurements:
                reading_type_name = proto.ReadingType.Name(sv.reading_type)
                sensor_model_name = (
                    proto.SensorModel.Name(sv.sensor_model) if sv.sensor_model else None
                )

                measurements.append({
                    "reading_type": READING_TYPE_MAP.get(
                        reading_type_name, reading_type_name.lower()
                    ),
                    "reading_type_raw": reading_type_name,
                    "value": sv.value,
                    "sensor_model": SENSOR_MODEL_MAP.get(sensor_model_name, sensor_model_name),
                    "timestamp": msg.timestamp,
                })

            deployment_type_name = proto.DeploymentType.Name(msg.deployment_type)
            board_type_name = (
                proto.BoardType.Name(msg.board_type) if msg.board_type else None
            )
            altitude = (
                msg.altitude_cm / 100.0
                if hasattr(msg, "altitude_cm") and msg.altitude_cm
                else None
            )

            return {
                "device_id": f"wesense_{msg.device_id:016x}",
                "device_id_raw": msg.device_id,
                "timestamp": msg.timestamp,
                "latitude": msg.latitude_e5 / 100000.0 if msg.latitude_e5 else None,
                "longitude": msg.longitude_e5 / 100000.0 if msg.longitude_e5 else None,
                "altitude": altitude,
                "board_type": (
                    BOARD_TYPE_MAP.get(board_type_name, board_type_name)
                    if board_type_name
                    else None
                ),
                "node_name": msg.node_name or None,
                "node_info": msg.node_info or None,
                "node_info_url": msg.node_info_url or None,
                "deployment_type": DEPLOYMENT_TYPE_MAP.get(
                    deployment_type_name, "UNKNOWN"
                ),
                "measurements": measurements,
            }
        except Exception as e:
            self.logger.error("Failed to decode SensorReadingV2: %s", e)
            if DEBUG:
                self.logger.debug("Raw payload (first 50 bytes): %s", payload[:50].hex())
            return None

    def _decode_device_metadata(self, payload: bytes) -> Optional[Dict[str, Any]]:
        """Decode a DeviceMetadataV2 protobuf message."""
        try:
            msg = proto.DeviceMetadataV2()
            msg.ParseFromString(payload)

            metadata = {
                "device_id_raw": msg.device_id,
                "latitude": msg.latitude_e5 / 100000.0 if msg.latitude_e5 else None,
                "longitude": msg.longitude_e5 / 100000.0 if msg.longitude_e5 else None,
                "altitude": msg.altitude_cm / 100.0 if msg.altitude_cm else None,
                "board_type": (
                    proto.BoardType.Name(msg.board_type) if msg.board_type else ""
                ),
                "deployment_type": (
                    proto.DeploymentType.Name(msg.deployment_type)
                    if msg.deployment_type
                    else ""
                ),
                "node_name": msg.node_name or "",
                "node_info": msg.node_info or "",
                "node_info_url": msg.node_info_url or "",
                "firmware_version": msg.firmware_version or "",
                "calibrations": {},
            }

            for cal in msg.calibrations:
                sensor = proto.SensorModel.Name(cal.sensor_model)
                metadata["calibrations"][sensor] = {
                    "status": proto.CalibrationStatus.Name(cal.status),
                    "method": proto.CalibrationMethod.Name(cal.method),
                    "date": cal.calibration_date,
                }

            # Geocode if we have location
            if metadata["latitude"] and metadata["longitude"]:
                geo = self.geocoder.reverse_geocode(
                    metadata["latitude"], metadata["longitude"]
                )
                if geo:
                    metadata["geo_country"] = geo["geo_country"]
                    metadata["geo_subdivision"] = geo["geo_subdivision"]

            return metadata
        except Exception as e:
            self.logger.error("Failed to decode DeviceMetadataV2: %s", e)
            return None

    # ── Future timestamp check ────────────────────────────────────

    def _check_future_timestamp(
        self, timestamp: int, device_id: str, node_name: str = None
    ) -> bool:
        """Check if timestamp is too far in the future. Returns True if rejected."""
        current_time = int(time.time())
        time_delta = timestamp - current_time
        if time_delta <= FUTURE_TIMESTAMP_TOLERANCE:
            return False

        self.stats["future_timestamps"] += 1

        if time_delta > 86400:
            delta_str = f"{time_delta / 86400:.1f} days"
        elif time_delta > 3600:
            delta_str = f"{time_delta / 3600:.1f} hours"
        elif time_delta > 60:
            delta_str = f"{time_delta / 60:.1f} minutes"
        else:
            delta_str = f"{time_delta} seconds"

        self.ft_logger.warning(
            "FUTURE_TIMESTAMP | node_name=%s | device_id=%s | "
            "timestamp=%s | ahead_by=%s | raw_delta_seconds=%d",
            node_name or device_id,
            device_id,
            datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S"),
            delta_str,
            time_delta,
        )
        return True

    # ── Process WiFi reading ──────────────────────────────────────

    def _process_wifi_reading(self, topic: str, payload: bytes):
        """Process a WiFi SensorReadingV2 (full message with all fields)."""
        decoded = self._decode_sensor_reading(payload)
        if not decoded:
            return

        device_id = decoded["device_id"]
        timestamp = decoded["timestamp"]

        if self._check_future_timestamp(timestamp, device_id, decoded.get("node_name")):
            return

        latitude = decoded.get("latitude")
        longitude = decoded.get("longitude")

        # Geocode
        geo_country = ""
        geo_subdivision = ""
        if latitude and longitude:
            geo = self.geocoder.reverse_geocode(latitude, longitude)
            if geo:
                geo_country = geo["geo_country"]
                geo_subdivision = geo["geo_subdivision"]

        # Fall back to topic-based geo if not available from coordinates
        topic_parts = topic.split("/")
        if not geo_country:
            if len(topic_parts) >= 6 and topic_parts[2] == "wifi":
                geo_country = topic_parts[3].lower()
                geo_subdivision = topic_parts[4]
            elif len(topic_parts) >= 5:
                geo_country = topic_parts[2].lower()
                geo_subdivision = topic_parts[3]

        # Determine network source from topic
        if len(topic_parts) >= 3 and topic_parts[2] == "wifi":
            network_source = "wesense/v2/wifi"
        elif len(topic_parts) >= 2:
            network_source = f"{topic_parts[0]}/{topic_parts[1]}"
        else:
            network_source = topic

        deployment_type = decoded.get("deployment_type", "UNKNOWN")
        deployment_type_source = "manual" if deployment_type != "UNKNOWN" else "unknown"

        for measurement in decoded.get("measurements", []):
            reading_type = measurement["reading_type"]
            raw_value = measurement["value"]
            sensor_model = measurement.get("sensor_model")
            unit = READING_UNITS.get(reading_type, "")
            decimals = READING_DECIMALS.get(reading_type, 2)
            value = round(float(raw_value), decimals)
            reading_timestamp = measurement.get("timestamp", timestamp)

            # Sign the reading for ClickHouse persistence
            signing_dict = {
                "device_id": device_id,
                "data_source": "wesense",
                "timestamp": reading_timestamp,
                "reading_type": reading_type,
                "value": value,
                "latitude": latitude,
                "longitude": longitude,
                "transport_type": "WIFI",
            }
            signed = self.signer.sign(json.dumps(signing_dict, sort_keys=True).encode())

            if self.ch_writer:
                row = (
                    datetime.fromtimestamp(reading_timestamp, tz=timezone.utc),
                    device_id,
                    "WESENSE",
                    network_source,
                    INGESTION_NODE_ID,
                    reading_type,
                    value,
                    unit,
                    float(latitude) if latitude else None,
                    float(longitude) if longitude else None,
                    float(decoded["altitude"]) if decoded.get("altitude") else None,
                    geo_country,
                    geo_subdivision,
                    decoded.get("board_type") or "",
                    sensor_model or "",
                    deployment_type,
                    deployment_type_source,
                    "WIFI",
                    "",  # deployment_location
                    decoded.get("node_name"),
                    decoded.get("node_info"),
                    decoded.get("node_info_url"),
                    signed.signature.hex(),
                    self.key_manager.ingester_id,
                    self.key_manager.key_version,
                )
                self.ch_writer.add(row)

            # Publish per-measurement to Zenoh (includes reading_type/value)
            if self.zenoh_publisher:
                self.zenoh_publisher.publish_reading({
                    "device_id": device_id,
                    "data_source": "WESENSE",
                    "ingestion_node_id": INGESTION_NODE_ID,
                    "geo_country": geo_country,
                    "geo_subdivision": geo_subdivision,
                    "timestamp": reading_timestamp,
                    "latitude": latitude,
                    "longitude": longitude,
                    "altitude": decoded.get("altitude"),
                    "transport_type": "WIFI",
                    "reading_type": reading_type,
                    "value": value,
                    "unit": unit,
                    "sensor_model": sensor_model or "",
                    "board_model": decoded.get("board_type") or "",
                    "node_name": decoded.get("node_name"),
                    "deployment_type": deployment_type,
                    "deployment_type_source": deployment_type_source,
                    "network_source": network_source,
                    "node_info": decoded.get("node_info"),
                    "node_info_url": decoded.get("node_info_url"),
                })

        self.stats["wifi_readings"] += 1

        # Publish to MQTT (device-level notification for Respiro map refresh)
        mqtt_dict = {
            "device_id": device_id,
            "data_source": "wesense",
            "geo_country": geo_country,
            "geo_subdivision": geo_subdivision,
            "timestamp": timestamp,
            "latitude": latitude,
            "longitude": longitude,
            "transport_type": "WIFI",
        }
        self.publisher.publish_reading(mqtt_dict)

        self.logger.info(
            "[%s/%s] Buffered %d WiFi readings from %s",
            geo_country, geo_subdivision,
            len(decoded.get("measurements", [])),
            device_id,
        )

    # ── Process LoRa reading ──────────────────────────────────────

    def _process_lora_reading(self, topic: str, payload: bytes, data_source: str = "CHIRPSTACK"):
        """Process a LoRa SensorReadingV2 (merged with cached metadata)."""
        decoded = self._decode_sensor_reading(payload)
        if not decoded:
            return

        device_id = decoded["device_id"]
        device_id_raw = decoded["device_id_raw"]
        timestamp = decoded["timestamp"]

        if self._check_future_timestamp(timestamp, device_id):
            return

        # Get cached metadata for this device
        cached = self.metadata_cache.get(device_id_raw)
        if cached:
            self.stats["cache_hits"] += 1
        else:
            self.stats["cache_misses"] += 1

        # Start with reading values, fill gaps from cache
        latitude = decoded.get("latitude")
        longitude = decoded.get("longitude")
        altitude = decoded.get("altitude")
        board_type = decoded.get("board_type") or ""
        deployment_type = decoded.get("deployment_type", "UNKNOWN")
        node_name = decoded.get("node_name") or ""
        node_info = decoded.get("node_info") or ""
        node_info_url = decoded.get("node_info_url") or ""
        geo_country = ""
        geo_subdivision = ""

        if cached:
            if not latitude:
                latitude = cached.get("latitude")
            if not longitude:
                longitude = cached.get("longitude")
            if not altitude:
                altitude = cached.get("altitude")
            if not board_type or board_type == "UNKNOWN":
                cached_bt = cached.get("board_type", "")
                if cached_bt:
                    board_type = BOARD_TYPE_MAP.get(cached_bt, cached_bt)
            if deployment_type == "UNKNOWN":
                cached_dt = cached.get("deployment_type", "")
                if cached_dt and cached_dt != "DEPLOYMENT_UNKNOWN":
                    deployment_type = DEPLOYMENT_TYPE_MAP.get(cached_dt, cached_dt)
            if not node_name:
                node_name = cached.get("node_name", "")
            if not node_info:
                node_info = cached.get("node_info", "")
            if not node_info_url:
                node_info_url = cached.get("node_info_url", "")
            geo_country = cached.get("geo_country", "")
            geo_subdivision = cached.get("geo_subdivision", "")

        # Geocode if we have coords but no geo data
        if latitude and longitude and not geo_country:
            geo = self.geocoder.reverse_geocode(latitude, longitude)
            if geo:
                geo_country = geo["geo_country"]
                geo_subdivision = geo["geo_subdivision"]

        deployment_type_source = (
            "manual" if deployment_type not in ("UNKNOWN", "DEPLOYMENT_UNKNOWN", "") else "unknown"
        )

        # Map board_type from enum name to display name if still an enum name
        if board_type in BOARD_TYPE_MAP:
            board_type = BOARD_TYPE_MAP[board_type]

        for measurement in decoded.get("measurements", []):
            reading_type = measurement["reading_type"]
            raw_value = measurement["value"]
            sensor_model = measurement.get("sensor_model") or ""
            unit = READING_UNITS.get(reading_type, "")
            decimals = READING_DECIMALS.get(reading_type, 2)
            value = round(float(raw_value), decimals)

            # Sign the reading for ClickHouse persistence
            signing_dict = {
                "device_id": device_id,
                "data_source": data_source.lower(),
                "timestamp": timestamp,
                "reading_type": reading_type,
                "value": value,
                "latitude": latitude,
                "longitude": longitude,
                "transport_type": "LORA",
            }
            signed = self.signer.sign(json.dumps(signing_dict, sort_keys=True).encode())

            if self.ch_writer:
                row = (
                    datetime.fromtimestamp(timestamp, tz=timezone.utc),
                    device_id,
                    data_source,
                    "wesense/v2/lora",
                    INGESTION_NODE_ID,
                    reading_type,
                    value,
                    unit,
                    float(latitude) if latitude else None,   # Fixed: was 0.0 in old LoRa
                    float(longitude) if longitude else None,  # Fixed: was 0.0 in old LoRa
                    float(altitude) if altitude else None,    # Fixed: was 0.0 in old LoRa
                    geo_country or "",
                    geo_subdivision or "",
                    board_type or "",
                    sensor_model,
                    deployment_type if deployment_type != "DEPLOYMENT_UNKNOWN" else "UNKNOWN",
                    deployment_type_source,
                    "LORA",
                    "",  # deployment_location
                    node_name or None,
                    node_info or None,
                    node_info_url or None,
                    signed.signature.hex(),
                    self.key_manager.ingester_id,
                    self.key_manager.key_version,
                )
                self.ch_writer.add(row)

            # Publish per-measurement to Zenoh (includes reading_type/value)
            if self.zenoh_publisher:
                self.zenoh_publisher.publish_reading({
                    "device_id": device_id,
                    "data_source": data_source,
                    "ingestion_node_id": INGESTION_NODE_ID,
                    "geo_country": geo_country or "",
                    "geo_subdivision": geo_subdivision or "",
                    "timestamp": timestamp,
                    "latitude": latitude,
                    "longitude": longitude,
                    "altitude": altitude,
                    "transport_type": "LORA",
                    "reading_type": reading_type,
                    "value": value,
                    "unit": unit,
                    "sensor_model": sensor_model or "",
                    "board_model": board_type or "",
                    "node_name": node_name or None,
                    "deployment_type": deployment_type if deployment_type != "DEPLOYMENT_UNKNOWN" else "UNKNOWN",
                    "deployment_type_source": deployment_type_source,
                    "network_source": "wesense/v2/lora",
                    "node_info": node_info or None,
                    "node_info_url": node_info_url or None,
                })

        self.stats["lora_readings"] += 1

        # Publish to MQTT (device-level notification for Respiro map refresh)
        mqtt_dict = {
            "device_id": device_id,
            "data_source": data_source.lower(),
            "geo_country": geo_country,
            "geo_subdivision": geo_subdivision,
            "timestamp": timestamp,
            "latitude": latitude,
            "longitude": longitude,
            "transport_type": "LORA",
        }
        self.publisher.publish_reading(mqtt_dict)

        self.logger.info(
            "Processed %d LoRa readings from %s (cache %s, source %s)",
            len(decoded.get("measurements", [])),
            device_id,
            "hit" if cached else "miss",
            data_source,
        )

    # ── Process LoRa metadata ─────────────────────────────────────

    def _process_lora_metadata(self, topic: str, payload: bytes):
        """Process a DeviceMetadataV2 message and cache it."""
        metadata = self._decode_device_metadata(payload)
        if not metadata:
            return

        device_id_raw = metadata["device_id_raw"]
        device_id_str = f"wesense_{device_id_raw:016x}"

        self.metadata_cache.update(device_id_raw, metadata)
        self.stats["lora_metadata"] += 1

        self.logger.info(
            "Cached metadata for %s (%s)",
            device_id_str,
            metadata.get("node_name") or "unnamed",
        )

    # ── MQTT callbacks ────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._mqtt_ever_connected = True
            self.logger.info("Connected to MQTT broker")

            # Allow custom topic override
            topics_env = os.getenv("MQTT_SUBSCRIBE_TOPICS")
            if topics_env:
                topics = [(t.strip(), 0) for t in topics_env.split(",")]
            else:
                topics = [
                    ("wesense/v2/wifi/#", 0),
                    ("wesense/v2/lora/#", 0),
                ]

            for topic, qos in topics:
                client.subscribe(topic, qos)
                self.logger.info("Subscribed to %s", topic)
        else:
            self.logger.error("MQTT connection failed (rc=%d)", rc)

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        if rc != 0:
            if getattr(self, '_mqtt_ever_connected', False):
                self.logger.warning("Unexpected MQTT disconnection (rc=%s)", rc)
            else:
                self.logger.debug("MQTT connection attempt failed (rc=%s), will retry", rc)

    def _on_message(self, client, userdata, msg):
        try:
            topic_parts = msg.topic.split("/")

            # Skip non-sensor topics
            if "diagnostic" in topic_parts or "status" in topic_parts:
                return

            # Skip Meshtastic topics
            if msg.topic.startswith("msh/"):
                return

            # Check payload is protobuf (not JSON)
            if len(msg.payload) == 0 or msg.payload[0] in (0x7B, 0x5B):
                return

            # Route by topic
            if "/lora/metadata/" in msg.topic:
                self._process_lora_metadata(msg.topic, msg.payload)
            elif "/lora/" in msg.topic or msg.topic.endswith("/lora"):
                self._process_lora_reading(msg.topic, msg.payload)
            else:
                self._process_wifi_reading(msg.topic, msg.payload)

        except Exception as e:
            self.logger.error("Error processing message on %s: %s", msg.topic, e)

    # ── TTN Webhook ───────────────────────────────────────────────

    def _start_ttn_webhook(self):
        """Start Flask TTN webhook server in a daemon thread."""
        try:
            from flask import Flask, request, jsonify  # noqa: F811
        except ImportError:
            self.logger.error(
                "Flask not installed - TTN webhook disabled. "
                "Install with: pip install flask"
            )
            return

        app = Flask("ttn_webhook")
        app.logger.setLevel(logging.WARNING)

        @app.route("/health", methods=["GET"])
        def health():
            return jsonify({
                "status": "healthy",
                "mqtt_connected": self.publisher.is_connected(),
            }), 200

        @app.route("/ttn/uplink", methods=["POST"])
        def ttn_uplink():
            try:
                # Verify signature if configured
                webhook_secret = os.getenv("TTN_WEBHOOK_SECRET")
                if webhook_secret:
                    signature = request.headers.get("X-Downlink-Apikey", "")
                    expected = hmac.new(
                        webhook_secret.encode(), request.data, hashlib.sha256
                    ).hexdigest()
                    if not hmac.compare_digest(signature, expected):
                        return jsonify({"error": "Invalid signature"}), 401

                data = request.get_json()
                if not data:
                    return jsonify({"error": "Empty payload"}), 400

                end_device_ids = data.get("end_device_ids", {})
                device_id = end_device_ids.get("device_id", "unknown")
                dev_eui = end_device_ids.get("dev_eui", "")

                uplink_message = data.get("uplink_message", {})
                frm_payload_b64 = uplink_message.get("frm_payload")
                f_port = uplink_message.get("f_port", 0)

                if not frm_payload_b64:
                    return jsonify({"status": "ok", "message": "No payload"}), 200

                payload_bytes = base64.b64decode(frm_payload_b64)

                # Build topic-style device ID
                topic_device_id = (
                    f"{device_id}_{dev_eui[-6:].lower()}" if dev_eui else device_id
                )

                # Process directly (skip MQTT round-trip)
                if f_port == 2:
                    self._process_lora_metadata(
                        f"wesense/v2/lora/metadata/{topic_device_id}",
                        payload_bytes,
                    )
                else:
                    self._process_lora_reading(
                        f"wesense/v2/lora/{topic_device_id}",
                        payload_bytes,
                        data_source="TTN",
                    )

                self.stats["ttn_uplinks"] += 1

                rx_meta = uplink_message.get("rx_metadata", [{}])
                rssi = rx_meta[0].get("rssi", "N/A") if rx_meta else "N/A"
                self.logger.info(
                    "[TTN] %s | %d bytes | port=%d | rssi=%s",
                    device_id, len(payload_bytes), f_port, rssi,
                )

                return jsonify({"status": "ok"}), 200

            except Exception as e:
                self.logger.error("Error processing TTN uplink: %s", e)
                return jsonify({"error": str(e)}), 500

        @app.route("/ttn/join", methods=["POST"])
        def ttn_join():
            data = request.get_json() or {}
            device_id = data.get("end_device_ids", {}).get("device_id", "unknown")
            self.logger.info("[TTN] Device %s joined network", device_id)
            return jsonify({"status": "ok"}), 200

        @app.route("/ttn/downlink/ack", methods=["POST"])
        def ttn_downlink_ack():
            data = request.get_json() or {}
            device_id = data.get("end_device_ids", {}).get("device_id", "unknown")
            self.logger.info("[TTN] Device %s acknowledged downlink", device_id)
            return jsonify({"status": "ok"}), 200

        def run_server():
            try:
                from waitress import serve
                self.logger.info(
                    "TTN webhook server starting on %s:%d (waitress)",
                    TTN_WEBHOOK_HOST, TTN_WEBHOOK_PORT,
                )
                serve(app, host=TTN_WEBHOOK_HOST, port=TTN_WEBHOOK_PORT, _quiet=True)
            except ImportError:
                self.logger.info(
                    "TTN webhook server starting on %s:%d (flask dev server)",
                    TTN_WEBHOOK_HOST, TTN_WEBHOOK_PORT,
                )
                app.run(
                    host=TTN_WEBHOOK_HOST, port=TTN_WEBHOOK_PORT, use_reloader=False
                )

        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()

    # ── Stats ─────────────────────────────────────────────────────

    def print_stats(self):
        ch_stats = (
            self.ch_writer.get_stats()
            if self.ch_writer
            else {"total_written": 0, "buffer_size": 0}
        )
        cache_stats = self.metadata_cache.get_stats()

        self.logger.info(
            "STATS | wifi=%d | lora=%d | lora_meta=%d | ttn=%d | "
            "cache_hits=%d | cache_misses=%d | cached_devices=%d | "
            "ch_written=%d | ch_buffer=%d | future_ts=%d",
            self.stats["wifi_readings"],
            self.stats["lora_readings"],
            self.stats["lora_metadata"],
            self.stats["ttn_uplinks"],
            self.stats["cache_hits"],
            self.stats["cache_misses"],
            cache_stats["total_devices"],
            ch_stats["total_written"],
            ch_stats["buffer_size"],
            self.stats["future_timestamps"],
        )

    # ── Lifecycle ─────────────────────────────────────────────────

    def shutdown(self, signum=None, frame=None):
        self.logger.info("Shutting down...")
        self.running = False

        if hasattr(self, 'zenoh_queryable') and self.zenoh_queryable:
            self.zenoh_queryable.close()
        if hasattr(self, 'zenoh_publisher') and self.zenoh_publisher:
            self.zenoh_publisher.close()
        if hasattr(self, 'registry_client'):
            self.registry_client.close()
        if self.ch_writer:
            self.ch_writer.close()
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        self.publisher.close()

        self.logger.info("Shutdown complete")

    def run(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        self.logger.info("=" * 60)
        self.logger.info("WeSense Ingester (Unified: WiFi + LoRa + TTN)")
        self.logger.info("=" * 60)

        # Connect MQTT input
        input_broker = os.getenv("MQTT_BROKER", "localhost")
        input_port = int(os.getenv("MQTT_PORT", "1883"))
        client_id = os.getenv("MQTT_INPUT_CLIENT_ID", "wesense_unified_input")

        self.mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )

        username = os.getenv("MQTT_USERNAME")
        password = os.getenv("MQTT_PASSWORD")
        if username:
            self.mqtt_client.username_pw_set(username, password)

        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message

        self.logger.info("Connecting to MQTT broker %s:%d", input_broker, input_port)
        retry_delay = 5
        while self.running:
            try:
                self.mqtt_client.connect(input_broker, input_port, keepalive=60)
                break
            except (ConnectionRefusedError, OSError) as e:
                self.logger.warning(
                    "MQTT broker not available (%s), retrying in %ds", e, retry_delay,
                )
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
        self.mqtt_client.loop_start()

        # Start TTN webhook if enabled
        if TTN_WEBHOOK_ENABLED:
            self._start_ttn_webhook()
        else:
            self.logger.info("TTN webhook disabled (set TTN_WEBHOOK_ENABLED=true to enable)")

        # Main loop with periodic stats
        try:
            while self.running:
                time.sleep(STATS_INTERVAL)
                self.print_stats()
        except KeyboardInterrupt:
            self.shutdown()
            sys.exit(0)


def main():
    ingester = WeSenseIngester()
    ingester.run()


if __name__ == "__main__":
    main()
