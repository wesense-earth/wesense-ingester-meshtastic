#!/usr/bin/env python3
"""
WeSense Ingester - Meshtastic
Monitors multiple Meshtastic MQTT regions, decodes telemetry, and writes to ClickHouse.
Also publishes decoded JSON to local MQTT for real-time consumers (e.g., wesense-respiro).
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
from datetime import datetime, timezone
from collections import defaultdict
from meshtastic import mesh_pb2, mqtt_pb2, telemetry_pb2, portnums_pb2
import clickhouse_connect
import threading
import socket
import base64
import hashlib

# Cryptography for Meshtastic packet decryption
try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    decryption_enabled = True
except ImportError as e:
    print(f"Warning: cryptography not available, encrypted packets will be skipped: {e}")
    decryption_enabled = False

# Import reverse geocoder for offline country/subdivision lookup
try:
    import reverse_geocoder as rg
    geocoding_enabled = True
except ImportError as e:
    print(f"Warning: reverse_geocoder not available: {e}")
    geocoding_enabled = False


# Meshtastic Decryption Configuration
# Default Meshtastic channel key (base64 encoded). "AQ==" is the default public key.
# Can be overridden with MESHTASTIC_CHANNEL_KEY environment variable.
MESHTASTIC_CHANNEL_KEY = os.environ.get('MESHTASTIC_CHANNEL_KEY', 'AQ==')

# Expanded default keys for key indices 0-7 (from Meshtastic source)
# These are the actual AES keys used when the PSK is a single byte (key index)
DEFAULT_KEYS = {
    0: bytes([0xd4, 0xf1, 0xbb, 0x3a, 0x20, 0x29, 0x07, 0x59,
              0xf0, 0xbc, 0xff, 0xab, 0xcf, 0x4e, 0x69, 0x01]),  # Default key index 0
    1: bytes([0xd4, 0xf1, 0xbb, 0x3a, 0x20, 0x29, 0x07, 0x59,
              0xf0, 0xbc, 0xff, 0xab, 0xcf, 0x4e, 0x69, 0x01]),  # Same as 0 for "AQ=="
}


def get_encryption_key(channel_key_b64: str) -> bytes:
    """
    Derive the AES encryption key from the channel PSK.

    Meshtastic key derivation:
    - If key is 0 bytes: use default key index 0
    - If key is 1 byte: use as index into default keys (0-7)
    - If key is 16 bytes: use directly as AES-128 key
    - If key is 32 bytes: use directly as AES-256 key
    - Otherwise: use SHA-256 hash of the key, truncated to 16 bytes
    """
    try:
        key_bytes = base64.b64decode(channel_key_b64)
    except Exception:
        # If not valid base64, hash it
        key_bytes = hashlib.sha256(channel_key_b64.encode()).digest()[:16]
        return key_bytes

    if len(key_bytes) == 0:
        return DEFAULT_KEYS.get(0, DEFAULT_KEYS[0])
    elif len(key_bytes) == 1:
        key_index = key_bytes[0]
        return DEFAULT_KEYS.get(key_index, DEFAULT_KEYS[0])
    elif len(key_bytes) == 16:
        return key_bytes
    elif len(key_bytes) == 32:
        return key_bytes
    else:
        # Hash and truncate to 16 bytes
        return hashlib.sha256(key_bytes).digest()[:16]


def decrypt_packet(encrypted_data: bytes, packet_id: int, from_node: int, channel_key: bytes) -> bytes:
    """
    Decrypt a Meshtastic packet using AES-CTR.

    Args:
        encrypted_data: The encrypted payload bytes
        packet_id: The packet ID from the MeshPacket
        from_node: The sender's node number
        channel_key: The AES key (16 or 32 bytes)

    Returns:
        Decrypted payload bytes, or None if decryption fails
    """
    if not decryption_enabled:
        return None

    try:
        # Build the nonce (16 bytes for AES-CTR)
        # Meshtastic nonce format: packet_id (8 bytes LE) + from_node (4 bytes LE) + zeros (4 bytes)
        nonce = packet_id.to_bytes(8, 'little') + from_node.to_bytes(4, 'little') + bytes(4)

        # Create AES-CTR cipher
        cipher = Cipher(
            algorithms.AES(channel_key),
            modes.CTR(nonce),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()

        # Decrypt
        decrypted = decryptor.update(encrypted_data) + decryptor.finalize()
        return decrypted
    except Exception as e:
        return None


# Pre-compute the channel key at startup
CHANNEL_KEY = get_encryption_key(MESHTASTIC_CHANNEL_KEY) if decryption_enabled else None


# Configuration for each region
# Official Meshtastic region codes: https://meshtastic.org/docs/configuration/region
# Load REGIONS from external JSON file for easier Docker management
def load_regions_config(config_file='config/mqtt_regions.json'):
    """Load MQTT regions configuration from external JSON file"""
    config_path = os.path.join(os.path.dirname(__file__), config_file)
    try:
        with open(config_path, 'r') as f:
            regions = json.load(f)
        # Remove 'untested_' prefix if present (for part2 configs)
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

REGIONS_ORIG = {
    'US': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/US/#',
        'cache_file': 'cache/meshtastic_cache_us.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'EU_433': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/EU_433/#',
        'cache_file': 'cache/meshtastic_cache_eu433.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'EU_868': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/EU_868/#',
        'cache_file': 'cache/meshtastic_cache_eu868.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'CN': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/CN/#',
        'cache_file': 'cache/meshtastic_cache_cn.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'JP': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/JP/#',
        'cache_file': 'cache/meshtastic_cache_jp.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'ANZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ANZ/#',
        'cache_file': 'cache/meshtastic_cache_anz.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'KR': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/KR/#',
    # 'cache_file': 'cache/meshtastic_cache_kr.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'TW': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/TW/#',
        'cache_file': 'cache/meshtastic_cache_tw.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'RU': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/RU/#',
        'cache_file': 'cache/meshtastic_cache_ru.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'IN': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/IN/#',
    # 'cache_file': 'cache/meshtastic_cache_in.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'NZ_865': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NZ_865/#',
        'cache_file': 'cache/meshtastic_cache_nz865.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'TH': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/TH/#',
    # 'cache_file': 'cache/meshtastic_cache_th.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'UA_433': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/UA_433/#',
        'cache_file': 'cache/meshtastic_cache_ua433.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'UA_868': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/UA_868/#',
        'cache_file': 'cache/meshtastic_cache_ua868.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MY_433': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MY_433/#',
        'cache_file': 'cache/meshtastic_cache_my433.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MY_919': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MY_919/#',
        'cache_file': 'cache/meshtastic_cache_my919.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'SG_923': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SG_923/#',
        'cache_file': 'cache/meshtastic_cache_sg923.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LORA_24': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LORA_24/#',
        'cache_file': 'cache/meshtastic_cache_lora24.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'AF': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/AF/#',
    # 'cache_file': 'cache/meshtastic_cache_af.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'AL': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/AL/#',
    # 'cache_file': 'cache/meshtastic_cache_al.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'DZ': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/DZ/#',
    # 'cache_file': 'cache/meshtastic_cache_dz.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'AR': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/AR/#',
        'cache_file': 'cache/meshtastic_cache_ar.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'AM': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/AM/#',
    # 'cache_file': 'cache/meshtastic_cache_am.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'AU': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/AU/#',
    # 'cache_file': 'cache/meshtastic_cache_au.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'AT': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/AT/#',
    # 'cache_file': 'cache/meshtastic_cache_at.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'AZ': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/AZ/#',
    # 'cache_file': 'cache/meshtastic_cache_az.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BS': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BS/#',
    # 'cache_file': 'cache/meshtastic_cache_bs.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BH': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BH/#',
    # 'cache_file': 'cache/meshtastic_cache_bh.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BD': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BD/#',
    # 'cache_file': 'cache/meshtastic_cache_bd.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BB': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BB/#',
    # 'cache_file': 'cache/meshtastic_cache_bb.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'BY': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/BY/#',
        'cache_file': 'cache/meshtastic_cache_by.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BE': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BE/#',
    # 'cache_file': 'cache/meshtastic_cache_be.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BZ': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BZ/#',
    # 'cache_file': 'cache/meshtastic_cache_bz.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BO': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BO/#',
    # 'cache_file': 'cache/meshtastic_cache_bo.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BA': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BA/#',
    # 'cache_file': 'cache/meshtastic_cache_ba.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BW': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BW/#',
    # 'cache_file': 'cache/meshtastic_cache_bw.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'BR': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/BR/#',
        'cache_file': 'cache/meshtastic_cache_br.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BN': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BN/#',
    # 'cache_file': 'cache/meshtastic_cache_bn.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'BG': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/BG/#',
    # 'cache_file': 'cache/meshtastic_cache_bg.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'KH': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/KH/#',
    # 'cache_file': 'cache/meshtastic_cache_kh.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'CM': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/CM/#',
    # 'cache_file': 'cache/meshtastic_cache_cm.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'CA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/CA/#',
        'cache_file': 'cache/meshtastic_cache_ca.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'CL': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/CL/#',
    # 'cache_file': 'cache/meshtastic_cache_cl.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'CO': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/CO/#',
    # 'cache_file': 'cache/meshtastic_cache_co.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'CR': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/CR/#',
    # 'cache_file': 'cache/meshtastic_cache_cr.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'CI': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/CI/#',
    # 'cache_file': 'cache/meshtastic_cache_ci.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'HR': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/HR/#',
    # 'cache_file': 'cache/meshtastic_cache_hr.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'CU': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/CU/#',
    # 'cache_file': 'cache/meshtastic_cache_cu.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'CY': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/CY/#',
    # 'cache_file': 'cache/meshtastic_cache_cy.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'CZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/CZ/#',
        'cache_file': 'cache/meshtastic_cache_cz.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # COMMENTED OUT - No data received (checked all-time)
    # 'DK': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/DK/#',
    # 'cache_file': 'cache/meshtastic_cache_dk.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'DO': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/DO/#',
    # 'cache_file': 'cache/meshtastic_cache_do.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'EC': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/EC/#',
    # 'cache_file': 'cache/meshtastic_cache_ec.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'EG': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/EG/#',
    # 'cache_file': 'cache/meshtastic_cache_eg.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'SV': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/SV/#',
    # 'cache_file': 'cache/meshtastic_cache_sv.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'EE': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/EE/#',
    # 'cache_file': 'cache/meshtastic_cache_ee.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'ET': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/ET/#',
    # 'cache_file': 'cache/meshtastic_cache_et.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'FI': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/FI/#',
    # 'cache_file': 'cache/meshtastic_cache_fi.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'FR': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/FR/#',
    # 'cache_file': 'cache/meshtastic_cache_fr.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'GE': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/GE/#',
    # 'cache_file': 'cache/meshtastic_cache_ge.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'DE': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/DE/#',
    # 'cache_file': 'cache/meshtastic_cache_de.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'GH': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/GH/#',
    # 'cache_file': 'cache/meshtastic_cache_gh.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'GR': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/GR/#',
    # 'cache_file': 'cache/meshtastic_cache_gr.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'GT': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/GT/#',
    # 'cache_file': 'cache/meshtastic_cache_gt.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'HN': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/HN/#',
    # 'cache_file': 'cache/meshtastic_cache_hn.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'HK': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/HK/#',
    # 'cache_file': 'cache/meshtastic_cache_hk.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'HU': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/HU/#',
    # 'cache_file': 'cache/meshtastic_cache_hu.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    # COMMENTED OUT - No data received (checked all-time)
    # 'IS': {
    # 'broker': 'mqtt.meshtastic.org',
    # 'port': 1883,
    # 'username': 'meshdev',
    # 'password': 'large4cats',
    # 'topic': 'msh/IS/#',
    # 'cache_file': 'cache/meshtastic_cache_is.json',
    # 'enabled': True,
    # 'publish_to_wesense': True
    # },
    'ID': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ID/#',
        'cache_file': 'cache/meshtastic_cache_id.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'IR': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IR/#',
        'cache_file': 'cache/meshtastic_cache_ir.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'IQ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IQ/#',
        'cache_file': 'cache/meshtastic_cache_iq.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'IE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IE/#',
        'cache_file': 'cache/meshtastic_cache_ie.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'IL': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IL/#',
        'cache_file': 'cache/meshtastic_cache_il.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'IT': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IT/#',
        'cache_file': 'cache/meshtastic_cache_it.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'JM': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/JM/#',
        'cache_file': 'cache/meshtastic_cache_jm.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'JO': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/JO/#',
        'cache_file': 'cache/meshtastic_cache_jo.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'KZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KZ/#',
        'cache_file': 'cache/meshtastic_cache_kz.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'KE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KE/#',
        'cache_file': 'cache/meshtastic_cache_ke.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'KW': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KW/#',
        'cache_file': 'cache/meshtastic_cache_kw.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'KG': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KG/#',
        'cache_file': 'cache/meshtastic_cache_kg.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LA/#',
        'cache_file': 'cache/meshtastic_cache_la.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LV': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LV/#',
        'cache_file': 'cache/meshtastic_cache_lv.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LB': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LB/#',
        'cache_file': 'cache/meshtastic_cache_lb.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LY': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LY/#',
        'cache_file': 'cache/meshtastic_cache_ly.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LT': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LT/#',
        'cache_file': 'cache/meshtastic_cache_lt.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LU': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LU/#',
        'cache_file': 'cache/meshtastic_cache_lu.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'MK': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MK/#',
        'cache_file': 'cache/meshtastic_cache_mk.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'MX': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MX/#',
        'cache_file': 'cache/meshtastic_cache_mx.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'MD': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MD/#',
        'cache_file': 'cache/meshtastic_cache_md.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'MN': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MN/#',
        'cache_file': 'cache/meshtastic_cache_mn.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'ME': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ME/#',
        'cache_file': 'cache/meshtastic_cache_me.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'MA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MA/#',
        'cache_file': 'cache/meshtastic_cache_ma.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'MZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MZ/#',
        'cache_file': 'cache/meshtastic_cache_mz.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'MM': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MM/#',
        'cache_file': 'cache/meshtastic_cache_mm.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'NP': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NP/#',
        'cache_file': 'cache/meshtastic_cache_np.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'NL': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NL/#',
        'cache_file': 'cache/meshtastic_cache_nl.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'NZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NZ/#',
        'cache_file': 'cache/meshtastic_cache_nz.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'NI': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NI/#',
        'cache_file': 'cache/meshtastic_cache_ni.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'NG': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NG/#',
        'cache_file': 'cache/meshtastic_cache_ng.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'NO': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NO/#',
        'cache_file': 'cache/meshtastic_cache_no.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'OM': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/OM/#',
        'cache_file': 'cache/meshtastic_cache_om.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'PK': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PK/#',
        'cache_file': 'cache/meshtastic_cache_pk.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'PA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PA/#',
        'cache_file': 'cache/meshtastic_cache_pa.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'PY': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PY/#',
        'cache_file': 'cache/meshtastic_cache_py.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'PE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PE/#',
        'cache_file': 'cache/meshtastic_cache_pe.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'PH': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PH/#',
        'cache_file': 'cache/meshtastic_cache_ph.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'PL': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PL/#',
        'cache_file': 'cache/meshtastic_cache_pl.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'PT': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PT/#',
        'cache_file': 'cache/meshtastic_cache_pt.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'QA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/QA/#',
        'cache_file': 'cache/meshtastic_cache_qa.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'RO': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/RO/#',
        'cache_file': 'cache/meshtastic_cache_ro.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'SA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SA/#',
        'cache_file': 'cache/meshtastic_cache_sa.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'SN': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SN/#',
        'cache_file': 'cache/meshtastic_cache_sn.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'RS': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/RS/#',
        'cache_file': 'cache/meshtastic_cache_rs.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'SK': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SK/#',
        'cache_file': 'cache/meshtastic_cache_sk.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'SI': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SI/#',
        'cache_file': 'cache/meshtastic_cache_si.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'ZA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ZA/#',
        'cache_file': 'cache/meshtastic_cache_za.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'ES': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ES/#',
        'cache_file': 'cache/meshtastic_cache_es.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LK': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LK/#',
        'cache_file': 'cache/meshtastic_cache_lk.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'SD': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SD/#',
        'cache_file': 'cache/meshtastic_cache_sd.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'SE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SE/#',
        'cache_file': 'cache/meshtastic_cache_se.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'CH': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/CH/#',
        'cache_file': 'cache/meshtastic_cache_ch.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'SY': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/SY/#',
        'cache_file': 'cache/meshtastic_cache_sy.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'TZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/TZ/#',
        'cache_file': 'cache/meshtastic_cache_tz.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'TN': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/TN/#',
        'cache_file': 'cache/meshtastic_cache_tn.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'TR': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/TR/#',
        'cache_file': 'cache/meshtastic_cache_tr.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'UG': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/UG/#',
        'cache_file': 'cache/meshtastic_cache_ug.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'UA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/UA/#',
        'cache_file': 'cache/meshtastic_cache_ua.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'AE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/AE/#',
        'cache_file': 'cache/meshtastic_cache_ae.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'GB': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/GB/#',
        'cache_file': 'cache/meshtastic_cache_gb.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'UY': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/UY/#',
        'cache_file': 'cache/meshtastic_cache_uy.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'UZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/UZ/#',
        'cache_file': 'cache/meshtastic_cache_uz.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'VE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/VE/#',
        'cache_file': 'cache/meshtastic_cache_ve.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'VN': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/VN/#',
        'cache_file': 'cache/meshtastic_cache_vn.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'YE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/YE/#',
        'cache_file': 'cache/meshtastic_cache_ye.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'ZM': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ZM/#',
        'cache_file': 'cache/meshtastic_cache_zm.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'ZW': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ZW/#',
        'cache_file': 'cache/meshtastic_cache_zw.json',
        'enabled': False,
        'publish_to_wesense': True
    },
    'LOCAL': {
        'broker': '192.168.43.11',
        'port': 1883,
        'username': 'mqttuser',
        'password': 'rubadub32',
        'topic': 'msh/ANZ/2/e/#',
        'cache_file': 'cache/meshtastic_cache_local.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    # ADDED from part2 - regions to monitor for potential data
    'PL': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PL/#',
        'cache_file': 'cache/meshtastic_cache_pl.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'ZA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ZA/#',
        'cache_file': 'cache/meshtastic_cache_za.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'AE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/AE/#',
        'cache_file': 'cache/meshtastic_cache_ae.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'CH': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/CH/#',
        'cache_file': 'cache/meshtastic_cache_ch.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'ES': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ES/#',
        'cache_file': 'cache/meshtastic_cache_es.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'GB': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/GB/#',
        'cache_file': 'cache/meshtastic_cache_gb.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'ID': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ID/#',
        'cache_file': 'cache/meshtastic_cache_id.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'IE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IE/#',
        'cache_file': 'cache/meshtastic_cache_ie.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'IL': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IL/#',
        'cache_file': 'cache/meshtastic_cache_il.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'IQ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IQ/#',
        'cache_file': 'cache/meshtastic_cache_iq.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'IR': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IR/#',
        'cache_file': 'cache/meshtastic_cache_ir.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'IT': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/IT/#',
        'cache_file': 'cache/meshtastic_cache_it.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'JM': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/JM/#',
        'cache_file': 'cache/meshtastic_cache_jm.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'JO': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/JO/#',
        'cache_file': 'cache/meshtastic_cache_jo.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'KE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KE/#',
        'cache_file': 'cache/meshtastic_cache_ke.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'KG': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KG/#',
        'cache_file': 'cache/meshtastic_cache_kg.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'KW': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KW/#',
        'cache_file': 'cache/meshtastic_cache_kw.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'KZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/KZ/#',
        'cache_file': 'cache/meshtastic_cache_kz.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LA/#',
        'cache_file': 'cache/meshtastic_cache_la.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LB': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LB/#',
        'cache_file': 'cache/meshtastic_cache_lb.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LK': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LK/#',
        'cache_file': 'cache/meshtastic_cache_lk.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LT': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LT/#',
        'cache_file': 'cache/meshtastic_cache_lt.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LU': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LU/#',
        'cache_file': 'cache/meshtastic_cache_lu.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LV': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LV/#',
        'cache_file': 'cache/meshtastic_cache_lv.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'LY': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/LY/#',
        'cache_file': 'cache/meshtastic_cache_ly.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MA/#',
        'cache_file': 'cache/meshtastic_cache_ma.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MD': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MD/#',
        'cache_file': 'cache/meshtastic_cache_md.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'ME': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/ME/#',
        'cache_file': 'cache/meshtastic_cache_me.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MK': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MK/#',
        'cache_file': 'cache/meshtastic_cache_mk.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MM': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MM/#',
        'cache_file': 'cache/meshtastic_cache_mm.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MN': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MN/#',
        'cache_file': 'cache/meshtastic_cache_mn.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MX': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MX/#',
        'cache_file': 'cache/meshtastic_cache_mx.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'MZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/MZ/#',
        'cache_file': 'cache/meshtastic_cache_mz.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'NG': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NG/#',
        'cache_file': 'cache/meshtastic_cache_ng.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'NI': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NI/#',
        'cache_file': 'cache/meshtastic_cache_ni.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'NL': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NL/#',
        'cache_file': 'cache/meshtastic_cache_nl.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'NO': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NO/#',
        'cache_file': 'cache/meshtastic_cache_no.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'NP': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NP/#',
        'cache_file': 'cache/meshtastic_cache_np.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'NZ': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/NZ/#',
        'cache_file': 'cache/meshtastic_cache_nz.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'OM': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/OM/#',
        'cache_file': 'cache/meshtastic_cache_om.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'PA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PA/#',
        'cache_file': 'cache/meshtastic_cache_pa.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'PE': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PE/#',
        'cache_file': 'cache/meshtastic_cache_pe.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'PH': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PH/#',
        'cache_file': 'cache/meshtastic_cache_ph.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'PK': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PK/#',
        'cache_file': 'cache/meshtastic_cache_pk.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'PT': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PT/#',
        'cache_file': 'cache/meshtastic_cache_pt.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'PY': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/PY/#',
        'cache_file': 'cache/meshtastic_cache_py.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'QA': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/QA/#',
        'cache_file': 'cache/meshtastic_cache_qa.json',
        'enabled': True,
        'publish_to_wesense': True
    },
    'RO': {
        'broker': 'mqtt.meshtastic.org',
        'port': 1883,
        'username': 'meshdev',
        'password': 'large4cats',
        'topic': 'msh/RO/#',
        'cache_file': 'cache/meshtastic_cache_ro.json',
        'enabled': True,
        'publish_to_wesense': True
    },
}

# Load REGIONS from external configuration file
# This allows easy management in Docker via volume mounts
REGIONS = load_regions_config('config/mqtt_regions.json')

# WeSense output MQTT configuration (local broker for decoded data)
WESENSE_OUTPUT_BROKER = os.getenv('WESENSE_OUTPUT_BROKER', 'localhost')
WESENSE_OUTPUT_PORT = int(os.getenv('WESENSE_OUTPUT_PORT', '1883'))
WESENSE_OUTPUT_USERNAME = os.getenv('WESENSE_OUTPUT_USERNAME', '')
WESENSE_OUTPUT_PASSWORD = os.getenv('WESENSE_OUTPUT_PASSWORD', '')

# ClickHouse configuration
# Load from environment variables with defaults for backwards compatibility
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'wesense')
CLICKHOUSE_TABLE = os.getenv('CLICKHOUSE_TABLE', 'sensor_readings')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

# ClickHouse batching configuration
CLICKHOUSE_BATCH_SIZE = int(os.getenv('CLICKHOUSE_BATCH_SIZE', '100'))
CLICKHOUSE_FLUSH_INTERVAL = int(os.getenv('CLICKHOUSE_FLUSH_INTERVAL', '10'))  # seconds

# Ingestion node ID (for provenance tracking)
INGESTION_NODE_ID = os.getenv('INGESTION_NODE_ID', socket.gethostname())

# Debug settings
# Load from environment variables with defaults
DEBUG = os.getenv('DEBUG', 'true').lower() in ('true', '1', 'yes')
LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', str(10*1024*1024)))  # Default: 10MB
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))  # Default: 5 backup files
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG').upper()  # Default: DEBUG

# ANSI color codes for terminal output
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for different log types"""

    # Color codes
    GREEN = '\033[92m'    # Successful operations (data written with location)
    YELLOW = '\033[93m'   # Warnings (cached pending, cache not updated)
    RED = '\033[91m'      # Errors
    CYAN = '\033[96m'     # Info (position broadcasts)
    MAGENTA = '\033[95m'  # Environment telemetry (with position)
    BLUE = '\033[94m'     # Device telemetry (battery/voltage)
    RESET = '\033[0m'     # Reset color

    def format(self, record):
        log_message = record.getMessage()

        # Color based on content
        if 'CLICKHOUSE_BUFFERED_CACHE_UPDATED' in log_message:
            # Green: ClickHouse write with cache updated (matched data with location)
            color = self.GREEN
        elif 'CLICKHOUSE_WRITE_COMPLETE_ALL_CACHED_DATA_WRITTEN' in log_message or 'POSITION_ARRIVED_NOW_WRITING_CACHED_DATA' in log_message:
            # Green: Pending data matched with location and written
            color = self.GREEN
        elif 'NO_CLICKHOUSE_WRITE_WAITING_FOR_POSITION' in log_message:
            # Yellow: Data cached waiting for position (not written to ClickHouse yet)
            color = self.YELLOW
        elif 'CACHE_NOT_UPDATED_TIMESTAMP_NOT_NEWER' in log_message:
            # Yellow: Written to ClickHouse but cache not updated (out of order timestamp)
            color = self.YELLOW
        elif 'POSITION_BROADCAST' in log_message:
            # Cyan for new/changed positions, no color for ignored (to reduce noise)
            if 'IN_CACHE_NO_CHANGE_IGNORED' in log_message:
                color = ''  # No color for duplicate positions (reduce log noise)
            else:
                color = self.CYAN
        elif 'ENVIRONMENT_TELEMETRY_BROADCAST' in log_message:
            # Magenta for environment telemetry (temperature, humidity, pressure)
            if 'has_position=True' in log_message:
                color = self.MAGENTA  # Has position - will be written
            else:
                color = self.YELLOW  # No position yet - will be cached
        elif 'DEVICE_TELEMETRY_BROADCAST' in log_message:
            # Blue for device telemetry (battery/voltage)
            color = self.BLUE
        elif record.levelname == 'ERROR':
            # Red: Errors
            color = self.RED
        else:
            # Default: no color
            color = ''

        # Format the log
        formatted = super().format(record)

        if color:
            return f"{color}{formatted}{self.RESET}"
        return formatted

# Set up debug logger with file rotation AND console output
debug_logger = logging.getLogger('ingester_debug')
debug_logger.setLevel(logging.DEBUG)

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# 1. Rotating file handler: configurable size and backup count
file_handler = RotatingFileHandler(
    'logs/ingester_debug.log',
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT
)
file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))
formatter = ColoredFormatter('%(asctime)s | %(levelname)s | %(message)s')
file_handler.setFormatter(formatter)
debug_logger.addHandler(file_handler)

# 2. Set up logging for ALL console output (including print statements)
# Save original stdout first
original_stdout = sys.stdout

class TeeStream:
    """Stream that writes to both stdout and a file"""
    def __init__(self, terminal, file_path):
        self.terminal = terminal
        self.log_file = open(file_path, 'a')

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

# Redirect stdout to write to both console and full log file
sys.stdout = TeeStream(original_stdout, 'logs/ingester_full.log')

# 3. Console handler - show logs in terminal based on configured level
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))
console_handler.setFormatter(formatter)
debug_logger.addHandler(console_handler)

# 4. Future timestamp logger - dedicated log for nodes with incorrect RTC
future_timestamp_logger = logging.getLogger('future_timestamps')
future_timestamp_logger.setLevel(logging.WARNING)
future_ts_handler = RotatingFileHandler(
    'logs/future_timestamps.log',
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT
)
future_ts_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
future_timestamp_logger.addHandler(future_ts_handler)

# Global statistics per region
stats = {region: {
    'messages': 0,
    'nodes': set(),
    'positions': {},
    'environmental': 0,
    'device_telemetry': 0,
    'start_time': datetime.now()
} for region in REGIONS.keys()}

# Pending telemetry cache: {region: {node_id: [(reading_type, value, unit, timestamp), ...]}}
pending_telemetry = {region: {} for region in REGIONS.keys()}
PENDING_TELEMETRY_MAX_AGE = 7 * 24 * 3600  # 7 days in seconds
FUTURE_TIMESTAMP_TOLERANCE = 30  # Allow timestamps up to 30 seconds in the future (minor clock skew)

# Deduplication cache: tracks recently seen readings to prevent duplicates
# Key: (node_id, reading_type, timestamp) -> Value: time.time() when first seen
# Duplicates occur when the same packet is received via multiple mesh hops or gateways
dedup_cache = {}
DEDUP_CACHE_MAX_AGE = 3600  # Keep entries for 1 hour (duplicates arrive within seconds)
DEDUP_CACHE_CLEANUP_INTERVAL = 300  # Clean up old entries every 5 minutes
dedup_last_cleanup = time.time()
dedup_stats = {'duplicates_blocked': 0, 'unique_processed': 0}

# Batch stats tracking - stores previous values for calculating deltas
batch_stats_last = {
    'duplicates_blocked': 0,
    'unique_processed': 0,
    'messages': 0,
    'environmental': 0,
    'clickhouse_writes': 0
}
clickhouse_writes_total = 0  # Track total ClickHouse rows written

def is_duplicate_reading(node_id, reading_type, timestamp):
    """
    Check if this reading has already been processed.
    Returns True if duplicate (should skip), False if new (should process).

    Also performs periodic cleanup of old cache entries.
    """
    global dedup_last_cleanup

    # Create unique key for this reading
    key = (node_id, reading_type, timestamp)
    current_time = time.time()

    # Periodic cleanup of old entries
    if current_time - dedup_last_cleanup > DEDUP_CACHE_CLEANUP_INTERVAL:
        cleanup_dedup_cache(current_time)
        dedup_last_cleanup = current_time

    # Check if we've seen this exact reading before
    if key in dedup_cache:
        dedup_stats['duplicates_blocked'] += 1
        return True  # Duplicate - skip it

    # New reading - add to cache
    dedup_cache[key] = current_time
    dedup_stats['unique_processed'] += 1
    return False  # Not a duplicate - process it

def cleanup_dedup_cache(current_time):
    """Remove entries older than DEDUP_CACHE_MAX_AGE from the dedup cache."""
    global dedup_cache

    cutoff_time = current_time - DEDUP_CACHE_MAX_AGE
    old_size = len(dedup_cache)

    # Create new dict with only recent entries (more efficient than deleting)
    dedup_cache = {k: v for k, v in dedup_cache.items() if v > cutoff_time}

    new_size = len(dedup_cache)
    if old_size != new_size and DEBUG:
        print(f"[DEDUP] Cleaned cache: {old_size} -> {new_size} entries "
              f"(removed {old_size - new_size} old entries)")

def get_batch_stats():
    """Return batch statistics (per-batch deltas since last call)."""
    global batch_stats_last

    # Calculate total messages and environmental across all regions
    total_messages = sum(data['messages'] for data in stats.values())
    total_environmental = sum(data['environmental'] for data in stats.values())

    # Calculate deltas since last call
    messages_delta = total_messages - batch_stats_last['messages']
    env_delta = total_environmental - batch_stats_last['environmental']
    blocked_delta = dedup_stats['duplicates_blocked'] - batch_stats_last['duplicates_blocked']
    unique_delta = dedup_stats['unique_processed'] - batch_stats_last['unique_processed']
    writes_delta = clickhouse_writes_total - batch_stats_last['clickhouse_writes']

    # Total readings = duplicates + unique (at reading level, not packet level)
    total_readings = blocked_delta + unique_delta
    block_rate = (blocked_delta / total_readings * 100) if total_readings > 0 else 0

    # Update last values for next call
    batch_stats_last['messages'] = total_messages
    batch_stats_last['environmental'] = total_environmental
    batch_stats_last['duplicates_blocked'] = dedup_stats['duplicates_blocked']
    batch_stats_last['unique_processed'] = dedup_stats['unique_processed']
    batch_stats_last['clickhouse_writes'] = clickhouse_writes_total

    return {
        'messages': messages_delta,
        'environmental': env_delta,
        'duplicates_blocked': blocked_delta,
        'unique_processed': unique_delta,
        'clickhouse_writes': writes_delta,
        'block_rate': block_rate,
        'cache_size': len(dedup_cache)
    }

# Pending node info cache: {region: {node_id: {'name': str, 'hardware': str}}}
pending_node_info = {region: {} for region in REGIONS.keys()}

# WeSense output MQTT client (publishes decoded data to local broker)
wesense_output_client = None

# ClickHouse client and write buffer
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
        # Column names matching the schema
        columns = [
            'timestamp', 'device_id', 'data_source', 'network_source', 'ingestion_node_id',
            'reading_type', 'value', 'unit',
            'latitude', 'longitude', 'altitude', 'geo_country', 'geo_subdivision',
            'board_model', 'deployment_type', 'transport_type', 'location_source', 'node_name'
        ]

        clickhouse_client.insert(
            f'{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}',
            rows_to_insert,
            column_names=columns
        )

        # Track successful writes for batch stats
        global clickhouse_writes_total
        clickhouse_writes_total += len(rows_to_insert)

        if DEBUG:
            print(f"[ClickHouse] ✓ Flushed {len(rows_to_insert)} rows")
        debug_logger.info(f"CLICKHOUSE_FLUSH | rows={len(rows_to_insert)}")

    except Exception as e:
        debug_logger.error(f"CLICKHOUSE_FLUSH_FAILED | rows={len(rows_to_insert)} | error={e}")
        if DEBUG:
            print(f"[ClickHouse] ✗ Flush failed: {e}")
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
        print(f"[ClickHouse] Periodic flush triggered, buffer size: {len(clickhouse_buffer)}, client: {clickhouse_client is not None}")
    flush_clickhouse_buffer()
    schedule_clickhouse_flush()

def add_to_clickhouse_buffer(row):
    """Add a row to the ClickHouse buffer and flush if needed"""
    global clickhouse_buffer

    with clickhouse_buffer_lock:
        clickhouse_buffer.append(row)
        buffer_size = len(clickhouse_buffer)

    if buffer_size >= CLICKHOUSE_BATCH_SIZE:
        flush_clickhouse_buffer()

# Track failed connections
failed_connections = []

def load_cache(cache_file):
    """Load position cache from disk"""
    try:
        if not os.path.exists(cache_file):
            return {}
        
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)
        
        positions = cache_data.get('nodes_with_position', {})
        saved_at = cache_data.get('saved_at', 0)
        age = int(time.time()) - saved_at
        print(f"  Loaded cache from {cache_file} (age: {age}s, {len(positions)} nodes)")
        return positions
    except Exception as e:
        print(f"  Warning: Failed to load cache {cache_file}: {e}")
        return {}

def save_cache(region, positions):
    """Save position cache to disk"""
    try:
        cache_file = REGIONS[region]['cache_file']
        cache_data = {
            'nodes_with_position': positions,
            'saved_at': int(time.time())
        }
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        pass  # Silent fail for performance

def load_pending_telemetry(region):
    """Load pending telemetry cache from disk"""
    try:
        cache_file = REGIONS[region]['cache_file'].replace('.json', '_pending.json')
        if not os.path.exists(cache_file):
            return {}

        with open(cache_file, 'r') as f:
            cache_data = json.load(f)

        pending = cache_data.get('pending_telemetry', {})
        saved_at = cache_data.get('saved_at', 0)
        age = int(time.time()) - saved_at

        # Filter out expired telemetry (older than 7 days)
        current_time = int(time.time())
        filtered_pending = {}
        total_readings = 0
        expired_readings = 0

        for node_id, readings in pending.items():
            # Filter out expired telemetry (older than 7 days) AND future timestamps
            valid_readings = [(rt, v, u, ts) for rt, v, u, ts in readings
                            if (current_time - ts) <= PENDING_TELEMETRY_MAX_AGE
                            and (ts - current_time) <= FUTURE_TIMESTAMP_TOLERANCE]
            if valid_readings:
                filtered_pending[node_id] = valid_readings
                total_readings += len(valid_readings)
            expired_readings += len(readings) - len(valid_readings)

        if total_readings > 0 or expired_readings > 0:
            print(f"  Loaded pending telemetry from {cache_file} (age: {age}s)")
            print(f"    Valid readings: {total_readings}, Expired: {expired_readings}, Nodes: {len(filtered_pending)}")

        return filtered_pending
    except Exception as e:
        print(f"  Warning: Failed to load pending telemetry {cache_file}: {e}")
        return {}

def save_pending_telemetry(region, pending):
    """Save pending telemetry cache to disk"""
    try:
        cache_file = REGIONS[region]['cache_file'].replace('.json', '_pending.json')
        cache_data = {
            'pending_telemetry': pending,
            'saved_at': int(time.time())
        }
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        if DEBUG:
            print(f"  Warning: Failed to save pending telemetry: {e}")

def get_deployment_type_from_node_name(node_name: str) -> str:
    """
    Get deployment type from node name - only for official WeSense devices.

    All other devices return blank ('') to be classified by the
    wesense-deployment-classifier service which uses temperature variance,
    weather correlation, and other data analysis to infer deployment type.

    Deployment types:
        'OUTDOOR' - fixed outdoor sensors
        'INDOOR' - indoor sensors (excluded from regional averages)
        'PORTABLE' - mobile sensors (excluded from regional averages)
        'MIXED' - semi-outdoor like balconies
        '' - unclassified, to be determined by classifier
    """
    if not node_name:
        return ''

    # WS- prefix is official WeSense outdoor convention
    if node_name.upper().startswith('WS-'):
        return 'OUTDOOR'

    # Everything else - leave blank for classifier
    return ''

def publish_to_wesense(region, node_id, reading_type, value, unit, timestamp):
    """Publish environmental reading to WeSense MQTT and ClickHouse"""
    # Check for duplicate readings (same device, type, timestamp seen before)
    # This catches duplicates from mesh network flooding or gateway rebroadcasts
    if is_duplicate_reading(node_id, reading_type, timestamp):
        if DEBUG:
            print(f"[{region}] 🔄 Duplicate skipped: {node_id} {reading_type}={value} @ {timestamp}")
        debug_logger.debug(f"DUPLICATE_SKIPPED | region={region} | node={node_id} | type={reading_type} | value={value} | timestamp={timestamp}")
        return

    position = stats[region]['positions'].get(node_id)
    if not position:
        # Cache this reading for future processing when position arrives
        if node_id not in pending_telemetry[region]:
            pending_telemetry[region][node_id] = []
        pending_telemetry[region][node_id].append((reading_type, value, unit, timestamp))

        # FIXED: Save pending telemetry to disk (survives restarts)
        save_pending_telemetry(region, pending_telemetry[region])

        debug_logger.warning(f"NO_CLICKHOUSE_WRITE_WAITING_FOR_POSITION | region={region} | node={node_id} | type={reading_type} | timestamp={timestamp} | sensor_time={datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')} | cached_on_disk=YES | pending_readings_count={len(pending_telemetry[region][node_id])} | will_write_when_position_arrives=YES")

        if DEBUG:
            cached_count = len(stats[region]['positions'])
            pending_count = len(pending_telemetry[region][node_id])
            print(f"[{region}] 📦 Cached telemetry for {node_id}: {reading_type}={value}{unit} (pending: {pending_count} readings, saved to disk)")
        return

    # Validate position has valid lat/lon
    if not position.get('lat') or not position.get('lon'):
        if DEBUG:
            print(f"[{region}] ⚠ Skipped publish for {node_id}: Invalid position (lat={position.get('lat')}, lon={position.get('lon')})")
        return

    # FIXED: Track last environmental reading time for this node (for hourly stats)
    # Store the sensor reading timestamp so we can calculate rolling hourly unique nodes
    # Update to the most recent timestamp
    old_last_env_time = position.get('last_env_time')
    cache_updated = False
    if 'last_env_time' not in position or timestamp > position.get('last_env_time', 0):
        position['last_env_time'] = timestamp
        cache_updated = True
        # Periodically save cache (every 10th update to reduce I/O)
        if not hasattr(publish_to_wesense, 'save_counter'):
            publish_to_wesense.save_counter = {}
        if region not in publish_to_wesense.save_counter:
            publish_to_wesense.save_counter[region] = 0
        publish_to_wesense.save_counter[region] += 1
        if publish_to_wesense.save_counter[region] >= 10:
            save_cache(region, stats[region]['positions'])
            publish_to_wesense.save_counter[region] = 0

    # Get country and subdivision from reverse geocoder (instant offline lookup)
    country_code = "unknown"
    subdivision_code = "unknown"
    if geocoding_enabled and position['lat'] and position['lon']:
        try:
            results = rg.search([(position['lat'], position['lon'])], mode=1)
            if results and len(results) > 0:
                geo = results[0]
                # Country code (already ISO format like "US", "PL")
                cc = geo.get('cc')
                if cc:
                    country_code = cc.lower()
                # Subdivision name (admin1), sanitized for topic
                admin1 = geo.get('admin1')
                if admin1:
                    subdivision_code = admin1.lower().replace(' ', '-').replace("'", "")
        except Exception as e:
            if DEBUG:
                print(f"[{region}] Geocoding error: {e}")

    # Determine MQTT source based on region
    # LOCAL region = meshtastic-community, all others = meshtastic-public
    mqtt_source = "meshtastic-community" if region == "LOCAL" else "meshtastic-public"

    # Publish to MQTT
    if wesense_output_client and wesense_output_client.is_connected():
        topic = f"wesense/decoded/{mqtt_source}/{country_code}/{subdivision_code}/{node_id}"

        payload = {
            "timestamp": timestamp,
            "device_id": node_id,
            "name": position.get('name'),
            "latitude": position['lat'],
            "longitude": position['lon'],
            "altitude": position.get('alt'),
            "country": country_code,
            "subdivision": subdivision_code,
            "data_source": mqtt_source,
            "reading_type": reading_type,
            "value": value,
            "unit": unit,
            "board_model": position.get('hardware'),
        }

        wesense_output_client.publish(topic, json.dumps(payload))

    # Write to ClickHouse (batched)
    if clickhouse_client:
        try:
            # Build row tuple matching column order in flush_clickhouse_buffer
            # columns: timestamp, device_id, data_source, network_source, ingestion_node_id,
            #          reading_type, value, unit,
            #          latitude, longitude, altitude, geo_country, geo_subdivision,
            #          board_model, deployment_type, transport_type, location_source, node_name
            row = (
                datetime.fromtimestamp(timestamp, tz=timezone.utc),  # timestamp (UTC)
                node_id,                               # device_id
                'MESHTASTIC_PUBLIC',                   # data_source
                region,                                # network_source (e.g., msh/ANZ/2/json)
                INGESTION_NODE_ID,                     # ingestion_node_id
                reading_type,                          # reading_type
                float(value),                          # value
                unit or '',                            # unit
                float(position['lat']),                # latitude
                float(position['lon']),                # longitude
                float(position['alt']) if position.get('alt') else None,  # altitude
                country_code,                          # geo_country
                subdivision_code,                      # geo_subdivision
                position.get('hardware') or '',        # board_model (use empty string if None)
                get_deployment_type_from_node_name(position.get('name')),  # deployment_type
                'LORA',                                # transport_type
                'gps',                                 # location_source
                position.get('name'),                  # node_name (Nullable, None is OK)
            )

            add_to_clickhouse_buffer(row)

            # Log write + cache update status
            if cache_updated:
                log_msg = f"CLICKHOUSE_BUFFERED_CACHE_UPDATED | region={region} | node={node_id} | type={reading_type} | value={value} | sensor_timestamp={timestamp} | sensor_time={datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')} | lat={position['lat']} | lon={position['lon']} | old_last_env_time={old_last_env_time} | new_last_env_time={position.get('last_env_time')}"
            else:
                log_msg = f"CLICKHOUSE_BUFFERED_CACHE_NOT_UPDATED | region={region} | node={node_id} | type={reading_type} | value={value} | sensor_timestamp={timestamp} | sensor_time={datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')} | lat={position['lat']} | lon={position['lon']} | old_last_env_time={old_last_env_time} | incoming_timestamp={timestamp}"
            debug_logger.info(log_msg)

            if DEBUG:
                print(f"[{country_code}/{subdivision_code}] ✓ Buffered {reading_type}={value}{unit} for {node_id}")

        except Exception as e:
            debug_logger.error(f"CLICKHOUSE_BUFFER_FAILED | country={country_code} | subdivision={subdivision_code} | node={node_id} | type={reading_type} | error={e}")
            if DEBUG:
                print(f"[{country_code}/{subdivision_code}] ClickHouse buffer error: {e}")

def create_message_callback(region):
    """Create message callback for a specific region"""
    def on_message(client, userdata, msg):
        try:
            stats[region]['messages'] += 1
            
            # Decode ServiceEnvelope
            envelope = mqtt_pb2.ServiceEnvelope()
            envelope.ParseFromString(msg.payload)
            
            if not envelope.HasField('packet'):
                return
            
            packet = envelope.packet
            
            # Get node ID
            try:
                from_id = getattr(packet, 'from')
            except AttributeError:
                from_id = packet.from_

            # Native Meshtastic node ID format (topic includes source identification)
            node_id = f"!{from_id:08x}"
            stats[region]['nodes'].add(node_id)

            # Handle encrypted vs decoded packets
            if packet.HasField('decoded'):
                # Already decrypted (e.g., from public MQTT gateway)
                decoded = packet.decoded
            elif packet.HasField('encrypted') and decryption_enabled and CHANNEL_KEY:
                # Decrypt the packet
                try:
                    decrypted_bytes = decrypt_packet(
                        packet.encrypted,
                        packet.id,
                        from_id,
                        CHANNEL_KEY
                    )
                    if decrypted_bytes is None:
                        return

                    # Parse as Data message
                    decoded = mesh_pb2.Data()
                    decoded.ParseFromString(decrypted_bytes)
                except Exception as e:
                    if DEBUG:
                        print(f"[{region}] Decryption failed for {node_id}: {e}")
                    return
            else:
                # No decoded data and can't decrypt
                return

            portnum = decoded.portnum
            
            # Handle POSITION
            if portnum == portnums_pb2.PortNum.POSITION_APP:
                try:
                    position = mesh_pb2.Position()
                    position.ParseFromString(decoded.payload)
                    
                    lat = position.latitude_i / 1e7 if position.latitude_i != 0 else None
                    lon = position.longitude_i / 1e7 if position.longitude_i != 0 else None
                    alt = position.altitude if hasattr(position, 'altitude') and position.altitude != 0 else None
                    
                    if lat and lon:
                        # Check if this is a new position (not in cache)
                        is_new = node_id not in stats[region]['positions']

                        # Preserve existing hardware, name, and last_env_time if already known
                        existing_entry = stats[region]['positions'].get(node_id, {})
                        existing_hw = existing_entry.get('hardware')
                        existing_name = existing_entry.get('name')
                        existing_last_env_time = existing_entry.get('last_env_time')  # CRITICAL: Preserve this!

                        # Check if position actually changed
                        position_changed = False
                        if not is_new:
                            existing_lat = existing_entry.get('lat')
                            existing_lon = existing_entry.get('lon')
                            existing_alt = existing_entry.get('alt')
                            # Position changed if lat/lon/alt are different
                            position_changed = (existing_lat != lat or existing_lon != lon or existing_alt != alt)

                        # Check if we have pending node info (name/hardware)
                        if node_id in pending_node_info[region]:
                            pending = pending_node_info[region][node_id]
                            existing_hw = existing_hw or pending.get('hardware')
                            existing_name = existing_name or pending.get('name')
                            if DEBUG:
                                print(f"[{region}] ✓ Applied pending node info for {node_id}")
                            del pending_node_info[region][node_id]

                        # Determine action
                        if is_new:
                            action = "NOT_IN_CACHE_ADDED"
                        elif position_changed:
                            action = "IN_CACHE_POSITION_CHANGED_UPDATED"
                        else:
                            action = "IN_CACHE_NO_CHANGE_IGNORED"

                        # Only update cache if new or position changed
                        if is_new or position_changed:
                            # Create new position entry, preserving last_env_time
                            new_entry = {
                                'lat': lat,
                                'lon': lon,
                                'alt': alt,
                                'hardware': existing_hw,
                                'name': existing_name
                            }
                            # CRITICAL: Preserve last_env_time so position updates don't wipe it out
                            if existing_last_env_time is not None:
                                new_entry['last_env_time'] = existing_last_env_time

                            stats[region]['positions'][node_id] = new_entry
                            save_cache(region, stats[region]['positions'])

                        # Log position broadcast with action taken
                        node_name = existing_name or node_id
                        preserved_status = "PRESERVED" if existing_last_env_time is not None else "NOT_SET"
                        debug_logger.info(f"POSITION_BROADCAST | node={node_name} | node_id={node_id} | region={region} | action={action} | lat={lat} | lon={lon} | alt={alt} | last_env_time={preserved_status}")

                        if DEBUG:
                            if is_new:
                                # Green bold text for new positions
                                print(f"\033[1;32m[{region}] 🆕 NEW POSITION {node_id}: {lat:.6f}, {lon:.6f}, alt={alt}m\033[0m")
                            else:
                                print(f"[{region}] POSITION {node_id}: {lat:.6f}, {lon:.6f}, alt={alt}m")
                        
                        # Process any pending telemetry for this node
                        if node_id in pending_telemetry[region]:
                            pending_readings = pending_telemetry[region][node_id]
                            current_time = int(time.time())

                            # Filter out readings older than 7 days AND future timestamps
                            valid_readings = [(rt, v, u, ts) for rt, v, u, ts in pending_readings
                                            if (current_time - ts) <= PENDING_TELEMETRY_MAX_AGE
                                            and (ts - current_time) <= FUTURE_TIMESTAMP_TOLERANCE]

                            if DEBUG and len(pending_readings) != len(valid_readings):
                                expired = len(pending_readings) - len(valid_readings)
                                print(f"[{region}] 🗑️  Filtered {expired} invalid readings for {node_id} (expired or future)")
                            
                            if valid_readings:
                                debug_logger.info(f"POSITION_ARRIVED_NOW_WRITING_CACHED_DATA | region={region} | node={node_id} | cached_readings_count={len(valid_readings)} | expired_readings={len(pending_readings) - len(valid_readings)}")

                                if DEBUG:
                                    print(f"[{region}] 🚀 Processing {len(valid_readings)} pending readings for {node_id}")

                                for reading_type, value, unit, ts in valid_readings:
                                    # Each call to publish_to_wesense will write to ClickHouse with position + sensor data
                                    publish_to_wesense(region, node_id, reading_type, value, unit, ts)

                                # Save cache after processing pending telemetry (updates last_env_time)
                                save_cache(region, stats[region]['positions'])
                                del pending_telemetry[region][node_id]
                                # Also save pending telemetry cache after removing processed node
                                save_pending_telemetry(region, pending_telemetry[region])

                                debug_logger.info(f"CLICKHOUSE_WRITE_COMPLETE_ALL_CACHED_DATA_WRITTEN | region={region} | node={node_id} | total_readings_written={len(valid_readings)} | final_last_env_time={stats[region]['positions'][node_id].get('last_env_time')}")
                except Exception as e:
                    if DEBUG:
                        print(f"[{region}] Error parsing position: {e}")
            
            # Handle NODEINFO
            elif portnum == portnums_pb2.PortNum.NODEINFO_APP:
                try:
                    user = mesh_pb2.User()
                    user.ParseFromString(decoded.payload)
                    
                    if DEBUG:
                        long_name = user.long_name if user.long_name else 'Unknown'
                        hw_model = mesh_pb2.HardwareModel.Name(user.hw_model) if user.hw_model else 'Unknown'
                        print(f"[{region}] NODEINFO {node_id}: {long_name}, hw={hw_model}")
                    
                    # Store node name if available
                    if user.long_name:
                        if node_id in stats[region]['positions']:
                            stats[region]['positions'][node_id]['name'] = user.long_name
                            if DEBUG:
                                print(f"[{region}] ✓ Stored name '{user.long_name}' for {node_id}")
                        else:
                            # Cache name for when position arrives
                            if node_id not in pending_node_info[region]:
                                pending_node_info[region][node_id] = {}
                            pending_node_info[region][node_id]['name'] = user.long_name
                            if DEBUG:
                                print(f"[{region}] 📦 Cached name '{user.long_name}' for {node_id} (waiting for position)")
                    
                    # Store hardware info - update position entry if it exists
                    if user.hw_model:
                        hw_name = mesh_pb2.HardwareModel.Name(user.hw_model)
                        if node_id in stats[region]['positions']:
                            # Update existing position entry with hardware
                            stats[region]['positions'][node_id]['hardware'] = hw_name
                            if DEBUG:
                                print(f"[{region}] ✓ Stored hardware {hw_name} for {node_id}")
                        else:
                            # Cache hardware for when position arrives
                            if node_id not in pending_node_info[region]:
                                pending_node_info[region][node_id] = {}
                            pending_node_info[region][node_id]['hardware'] = hw_name
                            if DEBUG:
                                print(f"[{region}] 📦 Cached hardware {hw_name} for {node_id} (waiting for position)")
                    
                    # Save cache after updating name/hardware
                    if (user.long_name or user.hw_model) and node_id in stats[region]['positions']:
                        save_cache(region, stats[region]['positions'])
                except Exception as e:
                    if DEBUG:
                        print(f"[{region}] Error parsing nodeinfo: {e}")
            
            # Handle TELEMETRY
            elif portnum == portnums_pb2.PortNum.TELEMETRY_APP:
                try:
                    telemetry = telemetry_pb2.Telemetry()
                    telemetry.ParseFromString(decoded.payload)
                    
                    if telemetry.HasField('device_metrics'):
                        stats[region]['device_telemetry'] += 1
                        dm = telemetry.device_metrics

                        # Get node name from cache if available
                        node_name = node_id
                        if node_id in stats[region]['positions']:
                            node_name = stats[region]['positions'][node_id].get('name') or node_id

                        debug_logger.info(f"DEVICE_TELEMETRY_BROADCAST | node={node_name} | node_id={node_id} | region={region} | battery={dm.battery_level}% | voltage={dm.voltage}V")

                        if DEBUG:
                            print(f"[{region}] DEVICE {node_id}: batt={dm.battery_level}%, volt={dm.voltage}V")
                    
                    if telemetry.HasField('environment_metrics') and REGIONS[region]['publish_to_wesense']:
                        stats[region]['environmental'] += 1
                        em = telemetry.environment_metrics
                        
                        # Only use telemetry timestamp - skip data if not present
                        if not (hasattr(telemetry, 'time') and telemetry.time):
                            if DEBUG:
                                print(f"[{region}] ⚠ Skipping telemetry from {node_id}: No timestamp")
                            return
                        
                        timestamp = telemetry.time

                        # Get node name from cache if available (needed for logging)
                        node_name = node_id
                        has_position = node_id in stats[region]['positions']
                        if has_position:
                            node_name = stats[region]['positions'][node_id].get('name') or node_id

                        # Check for future timestamps (indicates incorrect RTC on device)
                        current_time = int(time.time())
                        time_delta = timestamp - current_time
                        if time_delta > FUTURE_TIMESTAMP_TOLERANCE:
                            # Format the delta for human readability
                            if time_delta > 86400:  # More than a day
                                delta_str = f"{time_delta / 86400:.1f} days"
                            elif time_delta > 3600:  # More than an hour
                                delta_str = f"{time_delta / 3600:.1f} hours"
                            elif time_delta > 60:  # More than a minute
                                delta_str = f"{time_delta / 60:.1f} minutes"
                            else:
                                delta_str = f"{time_delta} seconds"

                            # Log to dedicated future timestamp log
                            future_timestamp_logger.warning(
                                f"FUTURE_TIMESTAMP | node_name={node_name} | node_id={node_id} | region={region} | "
                                f"timestamp={datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')} | "
                                f"ahead_by={delta_str} | raw_delta_seconds={time_delta}"
                            )

                            if DEBUG:
                                print(f"[{region}] ⏰ FUTURE TIMESTAMP {node_name} ({node_id}): {delta_str} ahead - SKIPPING")

                            return  # Skip this telemetry data

                        # Log environment telemetry broadcast
                        debug_logger.info(f"ENVIRONMENT_TELEMETRY_BROADCAST | node={node_name} | node_id={node_id} | region={region} | temp={em.temperature}°C | humidity={em.relative_humidity}% | pressure={em.barometric_pressure}hPa | has_position={has_position}")

                        if DEBUG:
                            print(f"[{region}] 🌡️  ENVIRONMENT {node_id}: temp={em.temperature}°C, hum={em.relative_humidity}%, press={em.barometric_pressure}hPa (ts={timestamp})")

                        if em.temperature != 0:
                            publish_to_wesense(region, node_id, "temperature", em.temperature, "°C", timestamp)
                        if em.relative_humidity != 0:
                            publish_to_wesense(region, node_id, "humidity", em.relative_humidity, "%", timestamp)
                        if em.barometric_pressure != 0:
                            publish_to_wesense(region, node_id, "pressure", em.barometric_pressure, "hPa", timestamp)
                except Exception as e:
                    if DEBUG:
                        print(f"[{region}] Error parsing telemetry: {e}")
        
        except Exception as e:
            pass  # Silent fail to not spam console
    
    return on_message

def create_connect_callback(region):
    """Create connect callback for a specific region"""
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[{region}] Connected, subscribing to {REGIONS[region]['topic']}")
            client.subscribe(REGIONS[region]['topic'])
        else:
            print(f"[{region}] Connection failed (code {rc})")
    
    return on_connect

def print_stats():
    """Print statistics for all regions"""
    current_time = datetime.now()
    current_timestamp = int(time.time())

    print("\n" + "=" * 80)
    total_nodes_last_hour = 0
    all_nodes_list = []  # DEBUG: Track which nodes are counted
    seen_nodes = set()  # Track unique nodes across ALL regions to avoid duplicates

    for region, data in stats.items():
        if not REGIONS[region]['enabled']:
            continue

        elapsed = (current_time - data['start_time']).total_seconds()
        rate = data['messages'] / elapsed if elapsed > 0 else 0

        # FIXED: Count unique nodes with environmental data AND position in last hour (rolling)
        # Check positions cache for nodes with last_env_time within last hour
        nodes_last_hour = 0
        for node_id, pos_data in data['positions'].items():
            if pos_data.get('last_env_time'):
                # Check if sensor reading timestamp is within last hour
                age_seconds = current_timestamp - pos_data['last_env_time']
                # Filter out invalid timestamps (negative = future, > 1 hour = too old)
                if 0 <= age_seconds <= 3600 and node_id not in seen_nodes:
                    nodes_last_hour += 1
                    seen_nodes.add(node_id)
                    # DEBUG: Track this node with its age and timestamp
                    age_minutes = int(age_seconds / 60)
                    timestamp_str = datetime.fromtimestamp(pos_data['last_env_time']).strftime('%Y-%m-%d %H:%M:%S')
                    all_nodes_list.append((node_id, age_minutes, timestamp_str))

        total_nodes_last_hour += nodes_last_hour

        # Count nodes with friendly names
        names_count = sum(1 for pos in data['positions'].values() if pos.get('name'))

        print(f"[{region:6}] Msgs: {data['messages']:6} | Nodes: {len(data['nodes']):4} | "
              f"Pos: {len(data['positions']):4} | Names: {names_count:3} | Env: {data['environmental']:3} | "
              f"Env/hr: {nodes_last_hour:3} | Dev: {data['device_telemetry']:4} | Rate: {rate:5.1f}/s")
    print("=" * 80)
    print(f"TOTAL Unique nodes with environmental data + position in last hour: {total_nodes_last_hour}")

    # DEBUG: Print sorted list of nodes counted
    if all_nodes_list:
        print("\nDEBUG - Nodes counted (sorted by age):")
        all_nodes_list.sort(key=lambda x: x[1])  # Sort by age
        for node_id, age_mins, timestamp_str in all_nodes_list:
            print(f"  {node_id}: {age_mins}m ago (sensor time: {timestamp_str})")

    # Print batch summary
    bs = get_batch_stats()
    total_readings = bs['duplicates_blocked'] + bs['unique_processed']
    print(f"\nBATCH: Msgs: {total_readings} | Dups: {bs['duplicates_blocked']} ({bs['block_rate']:.1f}%) | "
          f"Processed: {bs['unique_processed']} | Writes: {bs['clickhouse_writes']} | Cache: {bs['cache_size']}")
    print("=" * 80)

def shutdown_handler(signum=None, frame=None):
    """Handle graceful shutdown - save all caches before exiting"""
    print("\n" + "=" * 60)
    print("Shutting down gracefully...")
    print("=" * 60)

    # Save all position caches
    for region, data in stats.items():
        if REGIONS[region]['enabled'] and data['positions']:
            print(f"Saving {region} position cache ({len(data['positions'])} nodes)...")
            save_cache(region, data['positions'])
            # Save pending telemetry
            if region in pending_telemetry:
                save_pending_telemetry(region, pending_telemetry[region])

    # Flush remaining ClickHouse buffer and close connection
    if clickhouse_client:
        print("Flushing ClickHouse buffer...")
        flush_clickhouse_buffer()
        if clickhouse_flush_timer:
            clickhouse_flush_timer.cancel()
        print("Closing ClickHouse connection...")
        clickhouse_client.close()

    # Close log file
    if hasattr(sys.stdout, 'log_file'):
        print("Closing log file...")
        sys.stdout.log_file.close()

    print("Shutdown complete.")
    sys.exit(0)

def main():
    global wesense_output_client, clickhouse_client

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    atexit.register(shutdown_handler)

    print("=" * 60)
    print("Unified Meshtastic Decoder")
    print("=" * 60)
    print()
    
    # Connect to ClickHouse
    try:
        clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DATABASE,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        # Test connection
        clickhouse_client.ping()
        print(f"✓ Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        print(f"  Database: {CLICKHOUSE_DATABASE}, Table: {CLICKHOUSE_TABLE}")
        print(f"  Batch size: {CLICKHOUSE_BATCH_SIZE}, Flush interval: {CLICKHOUSE_FLUSH_INTERVAL}s")
        # Start periodic flush timer
        schedule_clickhouse_flush()
    except Exception as e:
        print(f"✗ Failed to connect to ClickHouse: {e}")
        print("  Continuing without ClickHouse (MQTT only)\n")
        clickhouse_client = None
    
    # Connect to WeSense output MQTT (local broker for decoded data)
    wesense_output_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="meshtastic_unified_publisher")
    wesense_output_client.username_pw_set(WESENSE_OUTPUT_USERNAME, WESENSE_OUTPUT_PASSWORD)
    try:
        wesense_output_client.connect(WESENSE_OUTPUT_BROKER, WESENSE_OUTPUT_PORT, 60)
        wesense_output_client.loop_start()
        print(f"✓ Connected to WeSense output MQTT at {WESENSE_OUTPUT_BROKER}")
    except Exception as e:
        print(f"✗ Failed to connect to WeSense output MQTT: {e}")

    # Show decryption status
    if decryption_enabled and CHANNEL_KEY:
        key_preview = MESHTASTIC_CHANNEL_KEY[:8] + "..." if len(MESHTASTIC_CHANNEL_KEY) > 8 else MESHTASTIC_CHANNEL_KEY
        print(f"✓ Decryption enabled (key: {key_preview})\n")
    else:
        print("⚠ Decryption disabled - encrypted packets will be skipped\n")

    # Create MQTT clients for each region
    clients = []
    
    for region, config in REGIONS.items():
        if not config['enabled']:
            print(f"⊘ [{region}] Disabled")
            continue

        # Load position cache
        stats[region]['positions'] = load_cache(config['cache_file'])

        # FIXED: Load pending telemetry from disk (survives restarts)
        pending_telemetry[region] = load_pending_telemetry(region)

        # Create client
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"meshtastic_{region.lower()}")
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
        print("\nNote: Connection refused errors likely indicate MQTT server")
        print("connection limit reached. These regions may exist but are")
        print("inaccessible with current connection count.")
        print("=" * 60)
    
    print("\n" + "=" * 60)
    print("All decoders running. Press Ctrl+C to stop.")
    print("=" * 60)
    
    # Main loop - print stats every 10 seconds
    try:
        while True:
            time.sleep(10)
            print_stats()
    
    except KeyboardInterrupt:
        print("\n\nStopping all decoders...")

        # Save caches
        for region, config in REGIONS.items():
            if config['enabled']:
                save_cache(region, stats[region]['positions'])
                # FIXED: Save pending telemetry to disk on shutdown
                save_pending_telemetry(region, pending_telemetry[region])

        # Disconnect all clients
        for client in clients:
            client.loop_stop()
            client.disconnect()
        
        if wesense_output_client:
            wesense_output_client.loop_stop()
            wesense_output_client.disconnect()
        
        # Flush and close ClickHouse connection
        if clickhouse_client:
            flush_clickhouse_buffer()
            if clickhouse_flush_timer:
                clickhouse_flush_timer.cancel()
            clickhouse_client.close()
            print("✓ ClickHouse connection closed")
        
        print("All decoders stopped.")

if __name__ == '__main__':
    main()
