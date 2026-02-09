#!/usr/bin/env python3
"""
WeSense Ingester - Meshtastic (Unified)

Monitors Meshtastic MQTT regions, decodes protobuf telemetry, correlates
position + telemetry, and writes decoded data to ClickHouse + MQTT.

Replaces the previous separate public and community ingesters
with a single codebase.

Set MESHTASTIC_MODE=community (default) or MESHTASTIC_MODE=downlink
to control data_source labelling and MQTT source routing.
"""

import atexit
import base64
import hashlib
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from meshtastic import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2

from wesense_ingester import (
    BufferedClickHouseWriter,
    DeduplicationCache,
    ReverseGeocoder,
    setup_logging,
)
from wesense_ingester.clickhouse.writer import ClickHouseConfig
from wesense_ingester.mqtt.publisher import MQTTPublisherConfig, WeSensePublisher

# ── AES decryption (adapter-specific) ────────────────────────────────
try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend

    _DECRYPTION_AVAILABLE = True
except ImportError:
    _DECRYPTION_AVAILABLE = False

# ── Configuration ────────────────────────────────────────────────────
MESHTASTIC_MODE = os.getenv("MESHTASTIC_MODE", "community").lower()
# Backwards compatibility: treat "public" as "downlink"
if MESHTASTIC_MODE == "public":
    MESHTASTIC_MODE = "downlink"
INGESTION_NODE_ID = os.getenv("INGESTION_NODE_ID", socket.gethostname())
MESHTASTIC_CHANNEL_KEY = os.getenv("MESHTASTIC_CHANNEL_KEY", "AQ==")

DATA_SOURCE = (
    "MESHTASTIC_COMMUNITY" if MESHTASTIC_MODE == "community" else "MESHTASTIC_DOWNLINK"
)

PENDING_TELEMETRY_MAX_AGE = 7 * 24 * 3600  # 7 days
FUTURE_TIMESTAMP_TOLERANCE = 30  # seconds
STATS_INTERVAL = int(os.getenv("STATS_INTERVAL", "10"))

# ClickHouse column schema for Meshtastic readings (18 columns)
CLICKHOUSE_COLUMNS = [
    "timestamp", "device_id", "data_source", "network_source", "ingestion_node_id",
    "reading_type", "value", "unit",
    "latitude", "longitude", "altitude", "geo_country", "geo_subdivision",
    "board_model", "deployment_type", "transport_type", "location_source", "node_name",
]

# ── AES decryption keys ──────────────────────────────────────────────
DEFAULT_KEYS = {
    0: bytes([
        0xD4, 0xF1, 0xBB, 0x3A, 0x20, 0x29, 0x07, 0x59,
        0xF0, 0xBC, 0xFF, 0xAB, 0xCF, 0x4E, 0x69, 0x01,
    ]),
    1: bytes([
        0xD4, 0xF1, 0xBB, 0x3A, 0x20, 0x29, 0x07, 0x59,
        0xF0, 0xBC, 0xFF, 0xAB, 0xCF, 0x4E, 0x69, 0x01,
    ]),
}


def get_encryption_key(channel_key_b64: str) -> bytes:
    """Derive AES key from Meshtastic channel PSK (base64-encoded)."""
    try:
        key_bytes = base64.b64decode(channel_key_b64)
    except Exception:
        return hashlib.sha256(channel_key_b64.encode()).digest()[:16]

    if len(key_bytes) == 0:
        return DEFAULT_KEYS[0]
    elif len(key_bytes) == 1:
        return DEFAULT_KEYS.get(key_bytes[0], DEFAULT_KEYS[0])
    elif len(key_bytes) in (16, 32):
        return key_bytes
    else:
        return hashlib.sha256(key_bytes).digest()[:16]


CHANNEL_KEY = get_encryption_key(MESHTASTIC_CHANNEL_KEY) if _DECRYPTION_AVAILABLE else None


def decrypt_packet(encrypted_data: bytes, packet_id: int, from_node: int, key: bytes) -> bytes | None:
    """Decrypt a Meshtastic packet using AES-CTR."""
    try:
        nonce = packet_id.to_bytes(8, "little") + from_node.to_bytes(8, "little")
        cipher = Cipher(algorithms.AES(key), modes.CTR(nonce), backend=default_backend())
        decryptor = cipher.decryptor()
        return decryptor.update(encrypted_data) + decryptor.finalize()
    except Exception:
        return None


# ── Region config loader ─────────────────────────────────────────────

def load_regions_config(config_file: str = "config/mqtt_regions.json") -> dict:
    """Load MQTT regions from JSON config file."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_file)
    try:
        with open(config_path) as f:
            regions = json.load(f)
        cleaned = {}
        for key, value in regions.items():
            cleaned[key.replace("untested_", "")] = value
        return cleaned
    except FileNotFoundError:
        print(f"ERROR: Configuration file {config_file} not found at {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in {config_file}: {e}")
        sys.exit(1)


def get_deployment_type_from_node_name(node_name: str | None) -> str:
    """Return deployment type from node name. WS- prefix = OUTDOOR, else blank."""
    if not node_name:
        return ""
    if node_name.upper().startswith("WS-"):
        return "OUTDOOR"
    return ""


# ── MeshtasticIngester ───────────────────────────────────────────────

class MeshtasticIngester:
    """
    Unified Meshtastic ingester handling both community and downlink networks.

    Uses wesense-ingester-core for shared infrastructure:
      - DeduplicationCache for mesh flooding protection
      - BufferedClickHouseWriter for batched database writes
      - WeSensePublisher for MQTT output
      - ReverseGeocoder for coordinate-to-location lookup
      - setup_logging for colored console + rotating file logs
    """

    def __init__(self):
        # Logging
        self.logger = setup_logging(
            "meshtastic_ingester",
            enable_future_timestamp_log=True,
        )
        self.ft_logger = logging.getLogger("meshtastic_ingester.future_timestamps")

        # Core components
        self.dedup = DeduplicationCache()
        self.geocoder = ReverseGeocoder()

        # ClickHouse writer
        try:
            self.ch_writer = BufferedClickHouseWriter(
                config=ClickHouseConfig.from_env(),
                columns=CLICKHOUSE_COLUMNS,
            )
            print(f"Connected to ClickHouse at {os.getenv('CLICKHOUSE_HOST', 'localhost')}:"
                  f"{os.getenv('CLICKHOUSE_PORT', '8123')}")
        except Exception as e:
            print(f"Failed to connect to ClickHouse: {e}")
            print("  Continuing without ClickHouse (MQTT only)")
            self.ch_writer = None

        # MQTT publisher for decoded output (supports old WESENSE_OUTPUT_* env vars)
        mqtt_config = MQTTPublisherConfig(
            broker=os.getenv("WESENSE_OUTPUT_BROKER", os.getenv("MQTT_BROKER", "localhost")),
            port=int(os.getenv("WESENSE_OUTPUT_PORT", os.getenv("MQTT_PORT", "1883"))),
            username=os.getenv("WESENSE_OUTPUT_USERNAME", os.getenv("MQTT_USERNAME")),
            password=os.getenv("WESENSE_OUTPUT_PASSWORD", os.getenv("MQTT_PASSWORD")),
            client_id=f"meshtastic_{MESHTASTIC_MODE}_publisher",
        )
        self.publisher = WeSensePublisher(config=mqtt_config)
        self.publisher.connect()

        # Region state
        if MESHTASTIC_MODE != "downlink":
            # Community mode: single local MQTT source (env vars), no regions config needed
            self.regions = {
                "LOCAL": {
                    "broker": os.getenv("MQTT_BROKER", os.getenv("LOCAL_MQTT_HOST", "localhost")),
                    "port": int(os.getenv("MQTT_PORT", os.getenv("LOCAL_MQTT_PORT", "1883"))),
                    "username": os.getenv("MQTT_USERNAME", os.getenv("LOCAL_MQTT_USER", "")),
                    "password": os.getenv("MQTT_PASSWORD", os.getenv("LOCAL_MQTT_PASSWORD", "")),
                    "topic": os.getenv("MQTT_SUBSCRIBE_TOPIC", "msh/+/2/e/#"),
                    "cache_file": "cache/meshtastic_cache_local.json",
                    "enabled": True,
                    "publish_to_wesense": True,
                }
            }
        else:
            # Downlink mode: multiple regions from config file
            self.regions = load_regions_config()
        self.stats = {
            region: {
                "messages": 0,
                "nodes": set(),
                "positions": {},
                "environmental": 0,
                "device_telemetry": 0,
                "start_time": datetime.now(),
            }
            for region in self.regions
        }
        self.pending_telemetry: dict[str, dict[str, list]] = {
            region: {} for region in self.regions
        }
        self.pending_node_info: dict[str, dict[str, dict]] = {
            region: {} for region in self.regions
        }

        # Track cache save frequency
        self._save_counter: dict[str, int] = {}
        self._source_clients: list = []

    # ── Cache I/O (backwards-compatible format) ──────────────────────

    def _load_cache(self, cache_file: str) -> dict:
        """Load position cache from disk."""
        try:
            if not os.path.exists(cache_file):
                return {}
            with open(cache_file) as f:
                data = json.load(f)
            positions = data.get("nodes_with_position", {})
            saved_at = data.get("saved_at", 0)
            age = int(time.time()) - saved_at
            print(f"  Loaded cache from {cache_file} (age: {age}s, {len(positions)} nodes)")
            return positions
        except Exception as e:
            print(f"  Warning: Failed to load cache {cache_file}: {e}")
            return {}

    def _save_cache(self, region: str, positions: dict) -> None:
        """Save position cache to disk."""
        try:
            cache_file = self.regions[region]["cache_file"]
            data = {"nodes_with_position": positions, "saved_at": int(time.time())}
            with open(cache_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    def _load_pending_telemetry(self, region: str) -> dict:
        """Load pending telemetry cache from disk, filtering expired entries."""
        try:
            cache_file = self.regions[region]["cache_file"].replace(".json", "_pending.json")
            if not os.path.exists(cache_file):
                return {}
            with open(cache_file) as f:
                data = json.load(f)
            pending = data.get("pending_telemetry", {})
            saved_at = data.get("saved_at", 0)
            age = int(time.time()) - saved_at
            current_time = int(time.time())

            filtered = {}
            total_readings = expired_readings = 0
            for node_id, readings in pending.items():
                valid = [
                    (rt, v, u, ts) for rt, v, u, ts in readings
                    if (current_time - ts) <= PENDING_TELEMETRY_MAX_AGE
                    and (ts - current_time) <= FUTURE_TIMESTAMP_TOLERANCE
                ]
                if valid:
                    filtered[node_id] = valid
                    total_readings += len(valid)
                expired_readings += len(readings) - len(valid)

            if total_readings > 0 or expired_readings > 0:
                print(f"  Loaded pending telemetry (age: {age}s)")
                print(f"    Valid: {total_readings}, Expired: {expired_readings}, Nodes: {len(filtered)}")
            return filtered
        except Exception as e:
            print(f"  Warning: Failed to load pending telemetry: {e}")
            return {}

    def _save_pending_telemetry(self, region: str, pending: dict) -> None:
        """Save pending telemetry cache to disk."""
        try:
            cache_file = self.regions[region]["cache_file"].replace(".json", "_pending.json")
            data = {"pending_telemetry": pending, "saved_at": int(time.time())}
            with open(cache_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # ── Helpers ───────────────────────────────────────────────────────

    def _get_mqtt_source(self, region: str) -> str:
        """Derive MQTT data source label for topic routing."""
        if MESHTASTIC_MODE == "community":
            return "meshtastic-community"
        return "meshtastic-community" if region == "LOCAL" else "meshtastic-downlink"

    def _should_publish_telemetry(self, region: str) -> bool:
        """Check if this region is configured to publish environment telemetry."""
        config = self.regions[region]
        return config.get("publish_to_wesense", True)

    # ── Core processing pipeline ─────────────────────────────────────

    def process_reading(
        self, region: str, node_id: str,
        reading_type: str, value: float, unit: str, timestamp: int,
    ) -> None:
        """
        Process a single environmental reading: dedup -> geocode -> ClickHouse + MQTT.

        If position is unknown, caches reading for later processing.
        """
        # Dedup check
        if self.dedup.is_duplicate(node_id, reading_type, timestamp):
            self.logger.debug(
                "DUPLICATE_SKIPPED | region=%s | node=%s | type=%s | value=%s",
                region, node_id, reading_type, value,
            )
            return

        position = self.stats[region]["positions"].get(node_id)
        if not position:
            # Cache for later when position arrives
            if node_id not in self.pending_telemetry[region]:
                self.pending_telemetry[region][node_id] = []
            self.pending_telemetry[region][node_id].append((reading_type, value, unit, timestamp))
            self._save_pending_telemetry(region, self.pending_telemetry[region])

            self.logger.warning(
                "NO_CLICKHOUSE_WRITE_WAITING_FOR_POSITION | region=%s | node=%s | type=%s | "
                "pending_count=%d",
                region, node_id, reading_type,
                len(self.pending_telemetry[region][node_id]),
            )
            return

        # Validate position
        if not position.get("lat") or not position.get("lon"):
            return

        # Track last environmental reading time
        cache_updated = False
        if "last_env_time" not in position or timestamp > position.get("last_env_time", 0):
            position["last_env_time"] = timestamp
            cache_updated = True
            if region not in self._save_counter:
                self._save_counter[region] = 0
            self._save_counter[region] += 1
            if self._save_counter[region] >= 10:
                self._save_cache(region, self.stats[region]["positions"])
                self._save_counter[region] = 0

        # Geocode
        geo = self.geocoder.reverse_geocode(position["lat"], position["lon"])
        country_code = geo["geo_country"] if geo else "unknown"
        subdivision_code = geo["geo_subdivision"] if geo else "unknown"

        mqtt_source = self._get_mqtt_source(region)

        # Publish to MQTT
        self.publisher.publish_reading({
            "timestamp": timestamp,
            "device_id": node_id,
            "name": position.get("name"),
            "latitude": position["lat"],
            "longitude": position["lon"],
            "altitude": position.get("alt"),
            "country": country_code,
            "subdivision": subdivision_code,
            "data_source": mqtt_source,
            "geo_country": country_code,
            "geo_subdivision": subdivision_code,
            "reading_type": reading_type,
            "value": value,
            "unit": unit,
            "board_model": position.get("hardware"),
        })

        # Write to ClickHouse
        if self.ch_writer:
            row = (
                datetime.fromtimestamp(timestamp, tz=timezone.utc),
                node_id,
                DATA_SOURCE,
                region,
                INGESTION_NODE_ID,
                reading_type,
                float(value),
                unit or "",
                float(position["lat"]),
                float(position["lon"]),
                float(position["alt"]) if position.get("alt") else None,
                country_code,
                subdivision_code,
                position.get("hardware") or "",
                get_deployment_type_from_node_name(position.get("name")),
                "LORA",
                "gps",
                position.get("name"),
            )
            self.ch_writer.add(row)

            status = "CACHE_UPDATED" if cache_updated else "CACHE_NOT_UPDATED"
            self.logger.info(
                "CLICKHOUSE_BUFFERED_%s | region=%s | node=%s | type=%s | value=%s | "
                "lat=%s | lon=%s",
                status, region, node_id, reading_type, value,
                position["lat"], position["lon"],
            )

    # ── Protobuf handlers ────────────────────────────────────────────

    def _handle_position(self, region: str, node_id: str, decoded) -> None:
        """Handle POSITION_APP message: update position cache, process pending."""
        try:
            position = mesh_pb2.Position()
            position.ParseFromString(decoded.payload)

            lat = position.latitude_i / 1e7 if position.latitude_i != 0 else None
            lon = position.longitude_i / 1e7 if position.longitude_i != 0 else None
            alt = position.altitude if hasattr(position, "altitude") and position.altitude != 0 else None

            if not lat or not lon:
                return

            existing = self.stats[region]["positions"].get(node_id, {})
            is_new = node_id not in self.stats[region]["positions"]

            # Preserve existing metadata
            existing_hw = existing.get("hardware")
            existing_name = existing.get("name")
            existing_last_env_time = existing.get("last_env_time")

            # Apply pending node info if available
            if node_id in self.pending_node_info[region]:
                pending_info = self.pending_node_info[region].pop(node_id)
                existing_hw = existing_hw or pending_info.get("hardware")
                existing_name = existing_name or pending_info.get("name")

            position_changed = False
            if not is_new:
                position_changed = (
                    existing.get("lat") != lat
                    or existing.get("lon") != lon
                    or existing.get("alt") != alt
                )

            if is_new or position_changed:
                new_entry = {
                    "lat": lat, "lon": lon, "alt": alt,
                    "hardware": existing_hw, "name": existing_name,
                }
                if existing_last_env_time is not None:
                    new_entry["last_env_time"] = existing_last_env_time

                self.stats[region]["positions"][node_id] = new_entry
                self._save_cache(region, self.stats[region]["positions"])

            action = "NEW" if is_new else ("CHANGED" if position_changed else "UNCHANGED")
            self.logger.info(
                "POSITION_BROADCAST | node=%s | region=%s | action=%s | lat=%s | lon=%s",
                existing_name or node_id, region, action, lat, lon,
            )

            # Process pending telemetry
            if node_id in self.pending_telemetry[region]:
                pending = self.pending_telemetry[region][node_id]
                current_time = int(time.time())

                valid = [
                    (rt, v, u, ts) for rt, v, u, ts in pending
                    if (current_time - ts) <= PENDING_TELEMETRY_MAX_AGE
                    and (ts - current_time) <= FUTURE_TIMESTAMP_TOLERANCE
                ]

                if valid:
                    self.logger.info(
                        "POSITION_ARRIVED_PROCESSING_PENDING | region=%s | node=%s | count=%d",
                        region, node_id, len(valid),
                    )
                    for reading_type, value, unit, ts in valid:
                        self.process_reading(region, node_id, reading_type, value, unit, ts)

                    self._save_cache(region, self.stats[region]["positions"])

                del self.pending_telemetry[region][node_id]
                self._save_pending_telemetry(region, self.pending_telemetry[region])

        except Exception as e:
            self.logger.error("Error parsing position for %s: %s", node_id, e)

    def _handle_nodeinfo(self, region: str, node_id: str, decoded) -> None:
        """Handle NODEINFO_APP message: store name/hardware in position cache."""
        try:
            user = mesh_pb2.User()
            user.ParseFromString(decoded.payload)

            name = user.long_name if user.long_name else None
            try:
                hw_model = mesh_pb2.HardwareModel.Name(user.hw_model) if user.hw_model else None
            except ValueError:
                hw_model = f"UNKNOWN_{user.hw_model}"

            if node_id in self.stats[region]["positions"]:
                pos = self.stats[region]["positions"][node_id]
                if name:
                    pos["name"] = name
                if hw_model:
                    pos["hardware"] = hw_model
                if name or hw_model:
                    self._save_cache(region, self.stats[region]["positions"])
            else:
                if node_id not in self.pending_node_info[region]:
                    self.pending_node_info[region][node_id] = {}
                if name:
                    self.pending_node_info[region][node_id]["name"] = name
                if hw_model:
                    self.pending_node_info[region][node_id]["hardware"] = hw_model

        except Exception as e:
            self.logger.error("Error parsing nodeinfo for %s: %s", node_id, e)

    def _handle_telemetry(self, region: str, node_id: str, decoded) -> None:
        """Handle TELEMETRY_APP message: extract readings and dispatch."""
        try:
            telemetry = telemetry_pb2.Telemetry()
            telemetry.ParseFromString(decoded.payload)

            if telemetry.HasField("device_metrics"):
                self.stats[region]["device_telemetry"] += 1
                dm = telemetry.device_metrics
                node_name = node_id
                if node_id in self.stats[region]["positions"]:
                    node_name = self.stats[region]["positions"][node_id].get("name") or node_id
                self.logger.info(
                    "DEVICE_TELEMETRY | node=%s | region=%s | battery=%s%% | voltage=%sV",
                    node_name, region, dm.battery_level, dm.voltage,
                )

            if telemetry.HasField("environment_metrics") and self._should_publish_telemetry(region):
                self.stats[region]["environmental"] += 1
                em = telemetry.environment_metrics

                if not (hasattr(telemetry, "time") and telemetry.time):
                    return

                timestamp = telemetry.time
                current_time = int(time.time())
                time_delta = timestamp - current_time

                # Future timestamp check
                if time_delta > FUTURE_TIMESTAMP_TOLERANCE:
                    if time_delta > 86400:
                        delta_str = f"{time_delta / 86400:.1f} days"
                    elif time_delta > 3600:
                        delta_str = f"{time_delta / 3600:.1f} hours"
                    elif time_delta > 60:
                        delta_str = f"{time_delta / 60:.1f} minutes"
                    else:
                        delta_str = f"{time_delta} seconds"

                    node_name = node_id
                    if node_id in self.stats[region]["positions"]:
                        node_name = self.stats[region]["positions"][node_id].get("name") or node_id

                    self.ft_logger.warning(
                        "FUTURE_TIMESTAMP | node=%s | node_id=%s | region=%s | ahead=%s",
                        node_name, node_id, region, delta_str,
                    )
                    return

                has_position = node_id in self.stats[region]["positions"]
                self.logger.info(
                    "ENVIRONMENT_TELEMETRY | node=%s | region=%s | temp=%s | humidity=%s | "
                    "pressure=%s | has_position=%s",
                    node_id, region, em.temperature, em.relative_humidity,
                    em.barometric_pressure, has_position,
                )

                if em.temperature != 0:
                    self.process_reading(region, node_id, "temperature", em.temperature, "°C", timestamp)
                if em.relative_humidity != 0:
                    self.process_reading(region, node_id, "humidity", em.relative_humidity, "%", timestamp)
                if em.barometric_pressure != 0:
                    self.process_reading(region, node_id, "pressure", em.barometric_pressure, "hPa", timestamp)

        except Exception as e:
            self.logger.error("Error parsing telemetry for %s: %s", node_id, e)

    # ── MQTT callbacks ───────────────────────────────────────────────

    def create_message_callback(self, region: str):
        """Create MQTT on_message callback for a specific region."""
        def on_message(client, userdata, msg):
            try:
                self.stats[region]["messages"] += 1

                envelope = mqtt_pb2.ServiceEnvelope()
                envelope.ParseFromString(msg.payload)

                if not envelope.HasField("packet"):
                    return

                packet = envelope.packet
                try:
                    from_id = getattr(packet, "from")
                except AttributeError:
                    from_id = packet.from_

                node_id = f"!{from_id:08x}"
                self.stats[region]["nodes"].add(node_id)

                # Handle encrypted vs decoded packets
                if packet.HasField("decoded"):
                    decoded = packet.decoded
                elif packet.HasField("encrypted") and _DECRYPTION_AVAILABLE and CHANNEL_KEY:
                    decrypted_bytes = decrypt_packet(
                        packet.encrypted, packet.id, from_id, CHANNEL_KEY,
                    )
                    if decrypted_bytes is None:
                        return
                    decoded = mesh_pb2.Data()
                    decoded.ParseFromString(decrypted_bytes)
                else:
                    return

                portnum = decoded.portnum

                if portnum == portnums_pb2.PortNum.POSITION_APP:
                    self._handle_position(region, node_id, decoded)
                elif portnum == portnums_pb2.PortNum.NODEINFO_APP:
                    self._handle_nodeinfo(region, node_id, decoded)
                elif portnum == portnums_pb2.PortNum.TELEMETRY_APP:
                    self._handle_telemetry(region, node_id, decoded)

            except Exception:
                pass  # Silent fail to avoid spamming console

        return on_message

    def create_connect_callback(self, region: str):
        """Create MQTT on_connect callback for a specific region."""
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                print(f"[{region}] Connected, subscribing to {self.regions[region]['topic']}")
                client.subscribe(self.regions[region]["topic"])
            else:
                print(f"[{region}] Connection failed (code {rc})")

        return on_connect

    # ── Stats ────────────────────────────────────────────────────────

    def print_stats(self) -> None:
        """Print statistics for all active regions."""
        current_timestamp = int(time.time())
        current_time = datetime.now()

        print("\n" + "=" * 80)
        total_nodes_last_hour = 0
        seen_nodes: set[str] = set()

        for region, data in self.stats.items():
            if not self.regions[region].get("enabled", False):
                continue

            elapsed = (current_time - data["start_time"]).total_seconds()
            rate = data["messages"] / elapsed if elapsed > 0 else 0

            nodes_last_hour = 0
            for node_id, pos_data in data["positions"].items():
                if pos_data.get("last_env_time"):
                    age = current_timestamp - pos_data["last_env_time"]
                    if 0 <= age <= 3600 and node_id not in seen_nodes:
                        nodes_last_hour += 1
                        seen_nodes.add(node_id)

            total_nodes_last_hour += nodes_last_hour
            names = sum(1 for pos in data["positions"].values() if pos.get("name"))

            print(
                f"[{region:6}] Msgs: {data['messages']:6} | Nodes: {len(data['nodes']):4} | "
                f"Pos: {len(data['positions']):4} | Names: {names:3} | "
                f"Env: {data['environmental']:3} | Env/hr: {nodes_last_hour:3} | "
                f"Dev: {data['device_telemetry']:4} | Rate: {rate:5.1f}/s"
            )

        print("=" * 80)
        print(f"TOTAL Unique nodes with env data + position in last hour: {total_nodes_last_hour}")

        dedup_stats = self.dedup.get_stats()
        ch_stats = self.ch_writer.get_stats() if self.ch_writer else {"total_written": 0}
        total = dedup_stats["duplicates_blocked"] + dedup_stats["unique_processed"]
        block_rate = (
            dedup_stats["duplicates_blocked"] / total * 100 if total > 0 else 0
        )
        print(
            f"\nDEDUP: Total: {total} | Dups: {dedup_stats['duplicates_blocked']} "
            f"({block_rate:.1f}%) | Unique: {dedup_stats['unique_processed']} | "
            f"CH writes: {ch_stats['total_written']} | Cache: {dedup_stats['cache_size']}"
        )
        print("=" * 80)

    # ── Lifecycle ────────────────────────────────────────────────────

    def shutdown(self, signum=None, frame=None) -> None:
        """Graceful shutdown: save caches, flush ClickHouse, disconnect."""
        print("\n" + "=" * 60)
        print("Shutting down gracefully...")

        for region in self.regions:
            if self.regions[region].get("enabled", False):
                positions = self.stats[region]["positions"]
                if positions:
                    print(f"  Saving {region} cache ({len(positions)} nodes)...")
                    self._save_cache(region, positions)
                if region in self.pending_telemetry:
                    self._save_pending_telemetry(region, self.pending_telemetry[region])

        if self.ch_writer:
            print("  Flushing ClickHouse buffer...")
            self.ch_writer.close()

        for client in self._source_clients:
            client.loop_stop()
            client.disconnect()

        self.publisher.close()
        print("Shutdown complete.")
        print("=" * 60)

    def run(self) -> None:
        """Main entry point: connect to all regions and run the main loop."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        atexit.register(self.shutdown)

        print("=" * 60)
        print(f"Meshtastic Ingester (mode={MESHTASTIC_MODE}, source={DATA_SOURCE})")
        print("=" * 60)

        if _DECRYPTION_AVAILABLE and CHANNEL_KEY:
            key_preview = MESHTASTIC_CHANNEL_KEY[:8] + "..." if len(MESHTASTIC_CHANNEL_KEY) > 8 else MESHTASTIC_CHANNEL_KEY
            print(f"Decryption enabled (key: {key_preview})")
        else:
            print("Decryption disabled - encrypted packets will be skipped")
        print()

        failed = []

        for region, config in self.regions.items():
            if not config.get("enabled", False):
                continue

            # Load caches
            self.stats[region]["positions"] = self._load_cache(config["cache_file"])
            self.pending_telemetry[region] = self._load_pending_telemetry(region)

            # Create MQTT client for this region
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"meshtastic_{region.lower()}",
            )
            client.username_pw_set(config.get("username", ""), config.get("password", ""))
            client.on_connect = self.create_connect_callback(region)
            client.on_message = self.create_message_callback(region)

            try:
                client.connect(config["broker"], config.get("port", 1883), 60)
                client.loop_start()
                self._source_clients.append(client)
                print(f"[{region}] Connected to {config['broker']}")
            except Exception as e:
                print(f"[{region}] Failed to connect: {e}")
                failed.append({"region": region, "error": str(e)})

        if failed:
            print(f"\nFailed connections: {len(failed)}")
            for f in failed:
                print(f"  {f['region']}: {f['error']}")

        print(f"\nAll decoders running. Press Ctrl+C to stop.")

        try:
            while True:
                time.sleep(STATS_INTERVAL)
                self.print_stats()
        except KeyboardInterrupt:
            self.shutdown()
            sys.exit(0)


def main():
    ingester = MeshtasticIngester()
    ingester.run()


if __name__ == "__main__":
    main()
