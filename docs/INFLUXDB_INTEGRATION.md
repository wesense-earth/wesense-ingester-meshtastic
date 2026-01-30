# InfluxDB Integration with Embedded Location Data

## Overview

The data ingester now writes sensor readings directly to InfluxDB with **embedded location fields** (city, state, country) instead of publishing them separately via MQTT.

This implements **Phase 1** of the [Location Storage Architecture](docs/location-storage-architecture.md).

## Changes Made

### 1. InfluxDB Connection Added
- Connects to InfluxDB at startup (remote host: `192.168.43.11:8086`)
- Falls back gracefully to MQTT-only if InfluxDB is unavailable
- Automatically reconnects and handles errors

### 2. Location Data Embedded in Measurements
Each sensor reading now includes location fields:

```python
Point("environment")
  .tag("device_id", "!abc123")
  .tag("reading_type", "temperature")
  .tag("data_source", "MESHTASTIC")
  .tag("board_model", "TBEAM")
  .tag("deployment_region", "ANZ")
  .field("value", 22.5)                  # Sensor value
  .field("latitude", -36.848)            # GPS coordinates
  .field("longitude", 174.763)
  .field("altitude", 10.0)               # Optional
  .field("city", "Auckland")             # Geocoded (if available)
  .field("state", "Auckland")            # Geocoded (if available)
  .field("country", "New Zealand")       # Geocoded (if available)
```

### 3. MQTT Location Publishing Removed
- No longer publishes to `skytrace/location/{lat},{lon}` topic
- Reduces MQTT traffic and simplifies architecture
- Location data flows: Geocoder cache → InfluxDB fields

### 4. Non-Blocking Geocoding
- Checks geocoding cache synchronously (instant)
- Queues uncached coordinates for async geocoding
- Sensor data written immediately (doesn't wait for geocoding)
- Future readings will include location once cached

## Configuration

Connection details in `data_ingester_part1.py`:

```python
INFLUXDB_URL = 'http://192.168.43.11:8086'
INFLUXDB_TOKEN = 'svIx1nmVHoMju8mefW4pw755Q7fX9ec3KSnQE9sdDIGGHIZa4cefOb8gvYRdR+lJ'
INFLUXDB_ORG = 'opensensors'
INFLUXDB_BUCKET = 'sensordata'
```

**Note:** Move these to environment variables for production deployment.

## Testing

### Test InfluxDB Connection

```bash
python3 test_influxdb_connection.py
```

This will:
1. Connect to InfluxDB
2. Write a test point with location data
3. Query it back to verify
4. Display results

Expected output:
```
✓ InfluxDB is reachable
✓ Test point written successfully
✓ Query successful! Found test data:
  Field: city
  Value: Auckland
  ...
✅ All tests passed! InfluxDB is ready.
```

### Run the Ingester

```bash
python3 data_ingester_part1.py
```

Look for:
```
✓ Connected to InfluxDB at http://192.168.43.11:8086
✓ Connected to SkyTrace MQTT at 192.168.43.11
```

When sensor data arrives with location:
```
[ANZ] 📍 !abc12345 @ Auckland, Auckland, New Zealand
[ANZ] ✓ Published temperature=22.5°C for !abc12345
```

## Querying Location Data

### Flux Query (InfluxDB UI)

Find all sensors in New Zealand:
```flux
from(bucket: "sensordata")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "environment")
  |> filter(fn: (r) => r._field == "country")
  |> filter(fn: (r) => r._value == "New Zealand")
```

Get temperature readings with location:
```flux
from(bucket: "sensordata")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "environment")
  |> filter(fn: (r) => r.reading_type == "temperature")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
```

### Python Query API

```python
from influxdb_client import InfluxDBClient

client = InfluxDBClient(url="http://192.168.43.11:8086", token="...", org="opensensors")
query_api = client.query_api()

query = '''
from(bucket: "sensordata")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "environment")
  |> filter(fn: (r) => r._field == "value" or r._field == "city" or r._field == "country")
  |> pivot(rowKey:["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
'''

tables = query_api.query(query, org="opensensors")

for table in tables:
    for record in table.records:
        device = record.values.get('device_id')
        value = record.values.get('value')
        city = record.values.get('city')
        country = record.values.get('country')
        print(f"{device}: {value}°C in {city}, {country}")
```

## Data Flow

```
Meshtastic MQTT → Data Ingester
                    ↓
        ┌───────────┴────────────┐
        ↓                        ↓
  Geocoding Cache         SkyTrace MQTT
  (check for location)    (publish reading)
        ↓
    InfluxDB
    (write with embedded location)
```

## Troubleshooting

### InfluxDB Connection Failed

```
✗ Failed to connect to InfluxDB: [Errno 61] Connection refused
  Continuing without InfluxDB (MQTT only)
```

**Solutions:**
1. Check InfluxDB is running: `docker ps | grep influxdb`
2. Verify host is reachable: `ping 192.168.43.11`
3. Test port: `telnet 192.168.43.11 8086`
4. Check Docker InfluxDB logs: `docker logs influxdb`

### No Location Data in InfluxDB

Readings written without city/state/country fields:

**Cause:** Coordinates not yet geocoded

**Solution:** Location will appear after first geocoding:
1. Check geocoding cache: `cat cache/geocoding_cache.json`
2. Geocoding happens async (1 call/sec rate limit)
3. Wait for next reading from same location
4. Run backfill script: `python3 backfill_geocoding.py` (to be created)

### Location Data Not Updating

**Cause:** Cache is persistent (locations don't change often)

**Solution:** This is expected behaviour. To force refresh:
1. Delete cache entry: Edit `cache/geocoding_cache.json`
2. Or clear entire cache: `rm cache/geocoding_cache.json`
3. Restart ingester

## Next Steps

1. **Test the connection**: Run `test_influxdb_connection.py`
2. **Run the ingester**: Verify location data is being written
3. **Update Map UI**: Query InfluxDB location fields instead of separate lookups
4. **Backfill historical data**: Create script to add location fields to existing measurements
5. **Production deployment**: Move to Docker, use environment variables for config

## Migration to ClickHouse (Future)

When migrating to ClickHouse:
- Export InfluxDB data **with location fields**
- Import to ClickHouse (locations included)
- Export to Parquet (locations embedded)
- Publish to IPFS (self-contained datasets)

See [Location Storage Architecture](docs/location-storage-architecture.md) for full roadmap.

## Related Files

- `data_ingester_part1.py` - Main ingestion service (updated)
- `test_influxdb_connection.py` - Connection test script (new)
- `utils/geocoder.py` - Geocoding with cache
- `utils/geocoding_worker.py` - Async geocoding worker
- `docs/location-storage-architecture.md` - Full architecture documentation
