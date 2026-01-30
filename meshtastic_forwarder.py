#!/usr/bin/env python3
"""
Meshtastic Raw Protobuf Forwarder

This script connects to the public Meshtastic MQTT server and forwards
raw protobuf messages to the local MQTT broker for decoding by
wesense-protobuf-decoder.

Architecture:
  Meshtastic MQTT (msh/#) → This Forwarder → Local MQTT (msh/#) → Decoder

The decoder handles:
  - Protobuf decoding
  - Geocoding
  - Publishing to wesense/decoded/#

This script does NOT:
  - Decode protobuf
  - Write to InfluxDB
  - Do geocoding
"""

import paho.mqtt.client as mqtt
import sys
import os
import json
import time
import signal
import atexit
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from collections import defaultdict

# Configuration
def load_regions_config(config_file='config/mqtt_regions.json'):
    """Load MQTT regions configuration from external JSON file"""
    config_path = os.path.join(os.path.dirname(__file__), config_file)
    try:
        with open(config_path, 'r') as f:
            regions = json.load(f)
        # Remove 'untested_' prefix if present
        cleaned_regions = {}
        for key, value in regions.items():
            clean_key = key.replace('untested_', '')
            cleaned_regions[clean_key] = value
        print(f"Loaded {len(cleaned_regions)} regions from {config_file}")
        return cleaned_regions
    except FileNotFoundError:
        print(f"ERROR: Configuration file {config_file} not found!")
        print(f"Looking in: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in {config_file}: {e}")
        sys.exit(1)

# Load regions from config
REGIONS = load_regions_config('config/mqtt_regions.json')

# Local MQTT broker configuration (where decoder listens)
LOCAL_BROKER = os.getenv('LOCAL_MQTT_BROKER', 'localhost')
LOCAL_PORT = int(os.getenv('LOCAL_MQTT_PORT', '1883'))
LOCAL_USERNAME = os.getenv('LOCAL_MQTT_USERNAME', '')
LOCAL_PASSWORD = os.getenv('LOCAL_MQTT_PASSWORD', '')

# Debug settings
DEBUG = os.getenv('DEBUG', 'true').lower() in ('true', '1', 'yes')
LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', str(10*1024*1024)))  # 10MB
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# Stats tracking
stats = defaultdict(lambda: {
    'messages': 0,
    'forwarded': 0,
    'start_time': datetime.now()
})

failed_connections = []

# Local MQTT client for publishing
local_client = None

# Setup logging
def setup_logging():
    """Configure logging with rotation"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    handler = RotatingFileHandler(
        os.path.join(log_dir, 'forwarder.log'),
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT
    )
    handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s'
    ))

    logger = logging.getLogger('forwarder')
    logger.setLevel(getattr(logging, LOG_LEVEL))
    logger.addHandler(handler)

    # Also log to stdout
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()


def create_message_callback(region):
    """Create message callback for a specific region - forwards raw protobuf"""
    def on_message(client, userdata, msg):
        global local_client

        stats[region]['messages'] += 1

        # Forward raw protobuf to local MQTT (same topic)
        if local_client and local_client.is_connected():
            try:
                # Publish raw bytes to local broker on same topic
                local_client.publish(msg.topic, msg.payload)
                stats[region]['forwarded'] += 1

                if DEBUG:
                    print(f"[{region}] → {msg.topic} ({len(msg.payload)} bytes)")

            except Exception as e:
                logger.error(f"[{region}] Failed to forward: {e}")
        else:
            if DEBUG:
                print(f"[{region}] ⚠ Local MQTT not connected, dropping message")

    return on_message


def create_connect_callback(region):
    """Create connect callback for a specific region"""
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            topic = REGIONS[region]['topic']
            print(f"[{region}] Connected, subscribing to {topic}")
            client.subscribe(topic)
        else:
            print(f"[{region}] Connection failed (code {rc})")

    return on_connect


def print_stats():
    """Print statistics for all regions"""
    current_time = datetime.now()

    print("\n" + "=" * 70)
    total_msgs = 0
    total_fwd = 0

    for region, data in stats.items():
        if region not in REGIONS or not REGIONS[region].get('enabled', True):
            continue

        elapsed = (current_time - data['start_time']).total_seconds()
        rate = data['messages'] / elapsed if elapsed > 0 else 0

        total_msgs += data['messages']
        total_fwd += data['forwarded']

        print(f"[{region:8}] Received: {data['messages']:6} | "
              f"Forwarded: {data['forwarded']:6} | Rate: {rate:5.1f}/s")

    print("=" * 70)
    print(f"TOTAL: Received: {total_msgs} | Forwarded: {total_fwd}")
    print("=" * 70)


def shutdown_handler(signum=None, frame=None):
    """Handle graceful shutdown"""
    print("\n" + "=" * 60)
    print("Shutting down gracefully...")
    print("=" * 60)
    print("Shutdown complete.")
    sys.exit(0)


def main():
    global local_client

    # Register signal handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    atexit.register(shutdown_handler)

    print("=" * 60)
    print("Meshtastic Raw Protobuf Forwarder")
    print("=" * 60)
    print()
    print(f"Forwarding to local MQTT: {LOCAL_BROKER}:{LOCAL_PORT}")
    print()

    # Connect to local MQTT broker
    local_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="meshtastic_forwarder"
    )
    local_client.username_pw_set(LOCAL_USERNAME, LOCAL_PASSWORD)

    try:
        local_client.connect(LOCAL_BROKER, LOCAL_PORT, 60)
        local_client.loop_start()
        print(f"✓ Connected to local MQTT at {LOCAL_BROKER}")
    except Exception as e:
        print(f"✗ Failed to connect to local MQTT: {e}")
        print("  Cannot forward messages without local broker!")
        sys.exit(1)

    # Create MQTT clients for each Meshtastic region
    clients = []

    for region, config in REGIONS.items():
        if not config.get('enabled', True):
            print(f"⊘ [{region}] Disabled")
            continue

        # Create client for this region
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"meshtastic_fwd_{region.lower()}"
        )
        client.username_pw_set(config['username'], config['password'])
        client.on_connect = create_connect_callback(region)
        client.on_message = create_message_callback(region)

        try:
            client.connect(config['broker'], config['port'], 60)
            client.loop_start()
            clients.append(client)
            print(f"✓ [{region}] Connected to {config['broker']}")
        except Exception as e:
            print(f"✗ [{region}] Failed to connect: {e}")
            failed_connections.append({'region': region, 'error': str(e)})

    print()

    # Print failed connections summary
    if failed_connections:
        print("\n" + "=" * 60)
        print(f"FAILED CONNECTIONS ({len(failed_connections)} regions)")
        print("=" * 60)
        for fail in failed_connections:
            print(f"  {fail['region']}: {fail['error']}")
        print("=" * 60)

    print("\n" + "=" * 60)
    print("Forwarder running. Press Ctrl+C to stop.")
    print("=" * 60)

    # Main loop - print stats periodically
    try:
        while True:
            time.sleep(30)
            print_stats()

    except KeyboardInterrupt:
        print("\n\nStopping forwarder...")

        # Disconnect all clients
        for client in clients:
            client.loop_stop()
            client.disconnect()

        if local_client:
            local_client.loop_stop()
            local_client.disconnect()

        print("Forwarder stopped.")


if __name__ == '__main__':
    main()
