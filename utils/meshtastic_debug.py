#!/usr/bin/env python3
"""
Debug decoder to analyze raw Meshtastic messages from mqtt.mess.host
"""

import paho.mqtt.client as mqtt
import sys
from datetime import datetime
from meshtastic import mesh_pb2, mqtt_pb2

def analyze_message(topic, payload):
    """Try different ways to decode the message"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Topic: {topic}")
    print(f"  Payload size: {len(payload)} bytes")
    print(f"  Hex: {payload[:64].hex()}...")
    
    # Try decoding as ServiceEnvelope
    try:
        envelope = mqtt_pb2.ServiceEnvelope()
        envelope.ParseFromString(payload)
        print(f"  ✓ Decoded as ServiceEnvelope")
        print(f"    Channel: {envelope.channel_id}")
        print(f"    Gateway: {envelope.gateway_id}")
        
        # Try decoding the packet inside
        if envelope.packet:
            try:
                packet = mesh_pb2.MeshPacket()
                packet.ParseFromString(envelope.packet)
                print(f"    ✓ Contains MeshPacket")
                print(f"      From: !{packet.from_:08x}")
                print(f"      To: !{packet.to:08x}")
                if packet.HasField('decoded'):
                    print(f"      Port: {packet.decoded.portnum}")
                    print(f"      Payload: {len(packet.decoded.payload)} bytes")
            except Exception as e:
                print(f"    ✗ MeshPacket decode failed: {e}")
                
    except Exception as e:
        print(f"  ✗ ServiceEnvelope decode failed: {e}")
    
    # Try decoding directly as MeshPacket
    try:
        packet = mesh_pb2.MeshPacket()
        packet.ParseFromString(payload)
        print(f"  ✓ Direct MeshPacket decode worked")
        print(f"    From: !{packet.from_:08x}")
        print(f"    To: !{packet.to:08x}")
    except Exception as e:
        print(f"  ✗ Direct MeshPacket decode failed: {e}")
    
    print()

def on_connect(client, userdata, flags, rc):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Connected to mqtt.mess.host")
    if rc == 0:
        client.subscribe("msh/CN/#", qos=0)
        print("Analyzing first 5 messages...\n")

message_count = 0

def on_message(client, userdata, msg):
    global message_count
    message_count += 1
    
    if message_count <= 5:
        analyze_message(msg.topic, msg.payload)
    elif message_count == 6:
        print("Analyzed 5 messages. Stopping...")
        client.disconnect()

if __name__ == "__main__":
    client = mqtt.Client()
    client.username_pw_set("meshdev", "large4cats")
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect("mqtt.mess.host", 1883, 60)
        client.loop_forever()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)