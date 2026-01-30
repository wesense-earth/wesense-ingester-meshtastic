#!/usr/bin/env python3
"""
Monitor Liam Cottle's public Meshtastic MQTT gateway to see available data
"""

import paho.mqtt.client as mqtt
import sys
from datetime import datetime
from collections import defaultdict

# Track statistics
topic_counts = defaultdict(int)
topics_seen = set()
start_time = datetime.now()

def on_connect(client, userdata, flags, rc):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Connected to mqtt.meshtastic.liamcottle.net")
    print(f"Connection result code: {rc}")
    
    if rc == 0:
        print("\nSubscribing to msh/CN/# topics...")
        client.subscribe("msh/CN/#", qos=0)
        print("Listening for messages... (Press Ctrl+C to stop)\n")
    else:
        print(f"Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    topic = msg.topic
    topics_seen.add(topic)
    
    # Parse topic structure
    parts = topic.split('/')
    if len(parts) >= 3:
        topic_pattern = f"{parts[0]}/{parts[1]}/{parts[2]}"
        topic_counts[topic_pattern] += 1
    
    # Show first few messages of each topic type
    if topic_counts[topic_pattern] <= 3:
        payload_size = len(msg.payload)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Topic: {topic}")
        print(f"  Payload size: {payload_size} bytes")
        
        # Try to identify message type from topic structure
        if len(parts) >= 4:
            msg_type = parts[3]
            print(f"  Message type: {msg_type}")
        
        # Show hex preview of payload
        hex_preview = msg.payload[:32].hex()
        print(f"  Payload preview: {hex_preview}...")
        print()

def on_disconnect(client, userdata, rc):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Disconnected from broker (code: {rc})")

def print_statistics():
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "="*60)
    print("STATISTICS")
    print("="*60)
    print(f"Duration: {elapsed:.1f} seconds")
    print(f"Unique topics seen: {len(topics_seen)}")
    print(f"\nMessage counts by topic pattern:")
    for topic, count in sorted(topic_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {topic}: {count} messages")
    print("="*60)

if __name__ == "__main__":
    # mqtt.mess.host public gateway settings
    BROKER = "mqtt.mess.host"
    PORT = 1883
    USERNAME = "meshdev"
    PASSWORD = "large4cats"
    
    print("="*60)
    print("Meshtastic Public Gateway Monitor")
    print("="*60)
    print(f"Broker: {BROKER}")
    print(f"Port: {PORT}")
    print(f"Username: {USERNAME}")
    print("="*60)
    print()
    
    client = mqtt.Client()
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    try:
        client.connect(BROKER, PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n\nStopping monitor...")
        print_statistics()
        client.disconnect()
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
