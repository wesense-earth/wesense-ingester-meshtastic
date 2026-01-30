#!/usr/bin/env python3
"""
Backfill Geocoding for Existing Sensor Locations
Reads unique coordinates from InfluxDB and geocodes them
"""

import asyncio
import os
from influxdb_client import InfluxDBClient
from utils.geocoder import get_geocoder
import paho.mqtt.client as mqtt
import json

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "opensensors")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensordata")

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")


async def get_unique_locations():
    """Query InfluxDB for all unique sensor locations"""
    print("Connecting to InfluxDB...")
    
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    
    query_api = client.query_api()
    
    # Query to get unique lat/lon combinations
    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -30d)
          |> filter(fn: (r) => r._measurement == "environment")
          |> filter(fn: (r) => exists r.latitude and exists r.longitude)
          |> keep(columns: ["latitude", "longitude"])
          |> distinct()
          |> limit(n: 1000)
    '''
    
    print(f"Querying unique locations from last 30 days...")
    result = query_api.query(query)
    
    locations = set()
    for table in result:
        for record in table.records:
            lat = record.values.get("latitude")
            lon = record.values.get("longitude")
            if lat and lon:
                locations.add((float(lat), float(lon)))
    
    client.close()
    print(f"Found {len(locations)} unique locations")
    return list(locations)


async def geocode_and_publish(locations):
    """Geocode all locations and publish to MQTT"""
    geocoder = get_geocoder()
    
    # Connect to MQTT
    mqtt_client = mqtt.Client(client_id="geocoding_backfill")
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        print(f"Connected to MQTT broker at {MQTT_BROKER}\n")
    except Exception as e:
        print(f"Failed to connect to MQTT: {e}")
        return
    
    geocoded_count = 0
    cached_count = 0
    
    for i, (lat, lon) in enumerate(locations, 1):
        print(f"[{i}/{len(locations)}] Processing ({lat:.6f}, {lon:.6f})...")
        
        # Check cache first
        cached = geocoder.get_cached_location(lat, lon)
        if cached:
            print(f"  ✓ Already cached")
            cached_count += 1
            location_data = cached
        else:
            # Geocode it
            location_data = await geocoder.reverse_geocode(lat, lon)
            if location_data:
                geocoded_count += 1
                location_str = geocoder.format_location_string(location_data)
                print(f"  ✓ Geocoded: {location_str}")
            else:
                print(f"  ✗ Failed to geocode")
                continue
        
        # Publish to MQTT
        topic = f"wesense/location/{lat:.3f},{lon:.3f}"
        payload = {
            "latitude": lat,
            "longitude": lon,
            "locality": location_data.get('locality'),
            "city": location_data.get('city'),
            "country": location_data.get('country'),
            "country_code": location_data.get('country_code'),
            "admin1": location_data.get('admin1'),
            "cached_at": location_data.get('cached_at')
        }
        mqtt_client.publish(topic, json.dumps(payload))
    
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    await geocoder.close()
    
    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Total locations: {len(locations)}")
    print(f"  Already cached: {cached_count}")
    print(f"  Newly geocoded: {geocoded_count}")
    print(f"  Failed: {len(locations) - cached_count - geocoded_count}")
    print(f"{'='*60}")


async def main():
    print("="*60)
    print("Geocoding Backfill Script")
    print("="*60)
    print()
    
    # Get unique locations from InfluxDB
    locations = await get_unique_locations()
    
    if not locations:
        print("No locations found in InfluxDB")
        return
    
    # Geocode and publish
    await geocode_and_publish(locations)


if __name__ == "__main__":
    asyncio.run(main())
