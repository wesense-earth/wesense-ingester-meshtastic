# Location Data Storage Architecture

## Overview

This document describes the scalable approach for storing and retrieving geocoded location names for sensor nodes in the SkyTrace system.

**Architecture Evolution:**
- **Phase 1 (Current - InfluxDB):** Embedded location fields with file-based cache
- **Phase 2 (Future - IPFS):** Location data embedded in Parquet exports for self-contained datasets

## Problem Statement

With potentially millions of sensor nodes, each generating multiple readings per hour, we need an efficient way to store human-readable location names (city, state, country) without:
- Duplicating location data millions of times
- Wasting storage and bandwidth
- Making database migrations difficult
- Creating external dependencies for data consumers

## Solution: Embedded Location Data with Efficient Caching

### Core Principle

**Embed location data with sensor readings, deduplicate through caching and compression**

- **Geocoding cache**: Local file cache prevents redundant API calls (~100K unique locations)
- **InfluxDB (current)**: Location fields embedded in sensor measurements
- **Parquet exports (future)**: Location data included in columnar format with compression
- **IPFS distribution**: Self-contained datasets requiring no external lookups

### Data Model

#### Phase 1: InfluxDB (Current Implementation)

**Sensor Measurement with Embedded Location:**
```python
{
  "measurement": "environment",
  "tags": {
    "device_id": "!abc123",
    "reading_type": "temperature",
    "data_source": "MESHTASTIC",
    "board_model": "TBEAM"
  },
  "fields": {
    "value": 22.5,
    "latitude": -36.848,
    "longitude": 174.763,
    "city": "Auckland",           # Geocoded from cache
    "suburb": "City Centre",      # Geocoded from cache
    "state": "Auckland",          # Geocoded from cache
    "country": "New Zealand"      # Geocoded from cache
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Geocoding Cache (`cache/geocoding_cache.json`):**
```json
{
  "-36.848,174.763": {
    "city": "Auckland",
    "suburb": "City Centre",
    "state": "Auckland",
    "country": "New Zealand",
    "display_name": "City Centre, Auckland, New Zealand",
    "cached_at": 1705315800
  }
}
```

#### Phase 2: Parquet/IPFS (Future Implementation)

**Daily Aggregate Parquet Schema:**
```
timestamp           DateTime
device_id           String
latitude            Float32
longitude           Float32
city                String        # Embedded from InfluxDB
state               String        # Embedded from InfluxDB  
country             String        # Embedded from InfluxDB
reading_type        String
value_mean          Float32
value_min           Float32
value_max           Float32
reading_count       UInt32
```

**Storage Efficiency with Parquet Compression:**
- Columnar format: Location strings compressed together
- Dictionary encoding: "Auckland, New Zealand" stored once, referenced by index
- Result: ~98% compression on repeated location strings

## Architecture Diagram

### Phase 1: Current Architecture (InfluxDB)

```
┌─────────────────────────────────────────────────────────────┐
│                  Meshtastic MQTT Broker                      │
│                  (mqtt.meshtastic.org)                       │
└────────────────────────┬────────────────────────────────────┘
                         │ msh/ANZ/#, msh/US/#, etc.
                         ▼
              ┌──────────────────────┐
              │  Data Ingester       │
              │  (data_ingester.py)  │
              └──────────┬───────────┘
                         │
                ┌────────┴────────┐
                ▼                 ▼
     ┌──────────────────┐   ┌─────────────────┐
     │  Geocoding Cache │   │  MQTT Publish   │
     │  (JSON file)     │   │  skytrace/      │
     │                  │   │  decoded/env/#  │
     │  lat,lon → name  │   │  + location     │
     │  ~100K entries   │   │    fields       │
     └──────────────────┘   └────────┬────────┘
                                     │
                                     ▼
                            ┌─────────────────┐
                            │   InfluxDB      │
                            │  (Measurements  │
                            │   with embedded │
                            │   location data)│
                            └────────┬────────┘
                                     │
                                     ▼
                            ┌─────────────────┐
                            │    Map UI       │
                            │  (Query sensor  │
                            │   data with     │
                            │   locations)    │
                            └─────────────────┘
```

### Phase 2: Future Architecture (IPFS + ClickHouse)

```
┌─────────────────────────────────────────────────────────────┐
│                    InfluxDB (Live Data)                      │
│              (sensor readings + locations)                   │
└────────────────────────┬────────────────────────────────────┘
                         │ Daily aggregation
                         ▼
              ┌──────────────────────┐
              │   ClickHouse         │
              │   (Aggregates)       │
              └──────────┬───────────┘
                         │ Export daily
                         ▼
              ┌──────────────────────┐
              │  Parquet Files       │
              │  (with embedded      │
              │   location data)     │
              │                      │
              │  nz_2025-01-15.parq  │
              │  au_2025-01-15.parq  │
              └──────────┬───────────┘
                         │ ipfs add
                         ▼
              ┌──────────────────────┐
              │      IPFS            │
              │  (Content-addressed  │
              │   permanent storage) │
              │                      │
              │  QmXxxx... (CID)     │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │     OrbitDB          │
              │  (Metadata index)    │
              │                      │
              │  {cid, date, region} │
              └──────────┬───────────┘
                         │ P2P replication
                         ▼
              ┌──────────────────────┐
              │  Global Community    │
              │  (Download Parquet   │
              │   → Import local     │
              │   ClickHouse         │
              │   → Query w/         │
              │     locations)       │
              └──────────────────────┘
```

## Storage Efficiency

### Phase 1: InfluxDB with Embedded Locations

**Geocoding cache overhead:**
```
~100K unique locations × 200 bytes avg = 20 MB total
Stored once in cache/geocoding_cache.json
Rate limited: 1 API call/sec, ~86K calls/day max
```

**InfluxDB storage:**
```
Location fields stored as strings in measurements
InfluxDB compression handles repeated values efficiently
Storage overhead: ~2-5% increase vs coordinates-only
```

### Phase 2: Parquet with Columnar Compression

**Without compression (naive approach):**
```
1M sensors × 24 readings/day × 365 days = 8.76B records
Location string "Auckland, New Zealand" = 24 bytes
Total: 8.76B × 24 bytes = 210 GB for location names
```

**With Parquet dictionary encoding:**
```
100K unique location strings = 2.4 MB
Dictionary indices (4 bytes per row) = 35 GB
Total: 37.4 MB for all location data
```

**Savings: 99.98% reduction vs naive storage**

**Key insight:** Columnar storage with dictionary encoding means "Auckland, New Zealand" is stored once and referenced 87,600 times (1 year of readings) using just a 4-byte index.

## Query Patterns

### Phase 1: InfluxDB (Current)

**Query sensors with location filtering:**
```flux
// Get all NZ temperature sensors from last 24h
from(bucket: "sensordata")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "environment")
  |> filter(fn: (r) => r._field == "value")
  |> filter(fn: (r) => r.country == "New Zealand")
  |> filter(fn: (r) => r.reading_type == "temperature")
```

**Aggregate by location:**
```flux
// Average temperature by city
from(bucket: "sensordata")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "environment")
  |> filter(fn: (r) => r.reading_type == "temperature")
  |> group(columns: ["city", "country"])
  |> mean()
```

### Phase 2: ClickHouse (Future)

**Direct queries on embedded location data:**
```sql
-- No dictionary needed - locations already in table
SELECT 
    city,
    country,
    avg(value_mean) AS avg_temp,
    count() AS reading_count
FROM sensors_daily
WHERE date >= today() - INTERVAL 7 DAY
  AND reading_type = 'temperature'
GROUP BY city, country
ORDER BY avg_temp DESC;
```

**Spatial aggregation with H3:**
```sql
-- Aggregate by H3 cell with location context
SELECT 
    geoToH3(latitude, longitude, 8) AS h3_cell,
    any(city) AS cell_city,        -- Pick any location name in cell
    any(country) AS cell_country,
    avg(value_mean) AS avg_value,
    uniq(device_id) AS device_count
FROM sensors_daily
WHERE date = today()
GROUP BY h3_cell;
```

## Implementation

### Phase 1: InfluxDB with Embedded Locations (Current)

**Status:** ✅ Geocoding infrastructure complete, awaiting InfluxDB integration

**Components:**
1. **Geocoding cache** (`cache/geocoding_cache.json`)
   - File-based persistent cache
   - Coordinate rounding (3 decimals ≈ 100m)
   - Rate-limited Nominatim API calls (1/sec)

2. **Geocoding worker** (`utils/geocoding_worker.py`)
   - Background thread for async geocoding
   - Non-blocking sensor data ingestion
   - Callback-based result handling

3. **Data ingester** (`data_ingester_part1.py`)
   - Geocodes coordinates on sensor readings
   - Publishes to MQTT with location fields
   - **TODO:** Add location fields to InfluxDB writes

**Next steps:**
- [x] Modify InfluxDB write to include city, state, country fields
- [x] Remove MQTT location publishing (no longer needed)
- [ ] Test InfluxDB connection and data writes
- [ ] Update Map UI to query location fields directly
- [ ] Backfill existing InfluxDB data with location fields

### Phase 2: ClickHouse Migration (Future)

**Daily aggregation pipeline:**
```sql
-- ClickHouse materialised view
CREATE MATERIALIZED VIEW sensors_daily_mv AS
SELECT
    toStartOfDay(timestamp) AS date,
    device_id,
    latitude,
    longitude,
    city,           -- Carried over from InfluxDB import
    state,          -- Carried over from InfluxDB import
    country,        -- Carried over from InfluxDB import
    reading_type,
    avg(value) AS value_mean,
    min(value) AS value_min,
    max(value) AS value_max,
    count() AS reading_count
FROM sensors_raw
GROUP BY date, device_id, latitude, longitude, city, state, country, reading_type;
```

**Parquet export:**
```bash
# Export daily aggregates
clickhouse-client --query "
    SELECT * FROM sensors_daily_mv
    WHERE date = yesterday()
    INTO OUTFILE 'nz_$(date -d yesterday +%Y-%m-%d).parquet'
    FORMAT Parquet
    SETTINGS output_format_parquet_compression_method='snappy';
"
```

### Phase 3: IPFS Publishing (Future)

**Automated daily publish:**
```bash
#!/bin/bash
# Daily cron job: Aggregate, export, publish to IPFS

DATE=$(date -d yesterday +%Y-%m-%d)
FILE="nz_${DATE}.parquet"

# Export from ClickHouse
clickhouse-client --query "..."

# Add to IPFS
CID=$(ipfs add $FILE | awk '{print $2}')

# Publish metadata to OrbitDB
curl -X POST http://localhost:3000/orbitdb/bundles \
    -d "{\"ipfs_cid\": \"$CID\", \"date\": \"$DATE\", \"region\": \"NZ\"}"

# Pin to IPFS Cluster
ipfs-cluster-ctl pin add $CID
```

## Database Migration Path

### Phase 1: InfluxDB Schema

**Measurement: `environment` (with embedded locations)**
```
Tags:
  - device_id
  - reading_type
  - data_source
  - board_model
  - deployment_region

Fields:
  - value (float)
  - latitude (float)
  - longitude (float)
  - city (string)           # NEW
  - state (string)          # NEW
  - country (string)        # NEW
  - altitude (float)
```

**Geocoding cache: `cache/geocoding_cache.json`**
```json
{
  "-36.848,174.763": {
    "city": "Auckland",
    "suburb": "City Centre",
    "state": "Auckland",
    "country": "New Zealand",
    "display_name": "City Centre, Auckland, New Zealand",
    "cached_at": 1705315800
  }
}
```

### Phase 2: ClickHouse Schema

**Raw sensor table:**
```sql
CREATE TABLE sensors_raw (
    timestamp DateTime,
    device_id String,
    reading_type String,
    value Float32,
    latitude Float32,
    longitude Float32,
    city String,              -- Imported from InfluxDB
    state String,             -- Imported from InfluxDB
    country String,           -- Imported from InfluxDB
    data_source String,
    board_model String
) ENGINE = MergeTree()
ORDER BY (timestamp, device_id, reading_type);
```

**Aggregated table with ReplacingMergeTree for deduplication:**
```sql
CREATE TABLE sensors_daily (
    date Date,
    device_id String,
    latitude Float32,
    longitude Float32,
    city String,
    state String,
    country String,
    reading_type String,
    value_mean Float32,
    value_min Float32,
    value_max Float32,
    reading_count UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (date, device_id, reading_type);
```

### Migration Steps: InfluxDB → ClickHouse

```bash
# 1. Export from InfluxDB
influx query '
  from(bucket: "sensordata")
  |> range(start: 2024-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "environment")
' --format csv > influx_export.csv

# 2. Import to ClickHouse
clickhouse-client --query "
  INSERT INTO sensors_raw FORMAT CSV
" < influx_export.csv

# 3. Verify location data
clickhouse-client --query "
  SELECT 
    country, 
    city, 
    count() AS sensor_count
  FROM sensors_raw
  WHERE city IS NOT NULL
  GROUP BY country, city
  ORDER BY sensor_count DESC
  LIMIT 20;
"
```

## Performance Characteristics

### Write Path
- **Frequency**: 1 write per unique location (one-time)
- **Volume**: ~100K locations total
- **Rate**: Limited by geocoding API (1/sec)

### Read Path (ClickHouse with Dictionary)
- **Latency**: <1ms (in-memory dictionary lookup)
- **Throughput**: Millions of queries/second
- **Cache hit rate**: >99% (location names don't change)

## Best Practices

### Coordinate Rounding
- Round to **3 decimal places** (~100m precision)
- Reduces unique locations by 100x
- Acceptable for city/suburb level geocoding

### Cache Strategy
- **L1**: In-memory cache in application (instant)
- **L2**: File cache `geocoding_cache.json` (local disk)
- **L3**: Database dimension table (shared across services)
- **L4**: Nominatim API (external, rate-limited)

### Data Freshness
- Location names rarely change
- TTL: 30 days (or manual invalidation)
- Re-geocode on coordinate updates only

## Migration Checklist

### Phase 1: Add Locations to InfluxDB (Current Priority)

- [ ] Modify `data_ingester.py` to write city/state/country fields to InfluxDB
- [ ] Test geocoding cache hit rate (should be >95% after initial population)
- [ ] Backfill existing InfluxDB measurements with location data
- [ ] Update Map UI to query location fields directly
- [ ] Remove MQTT `skytrace/location/#` publishing (no longer needed)
- [ ] Verify query performance with location filtering

### Phase 2: Migrate to ClickHouse

- [ ] Set up ClickHouse server
- [ ] Create `sensors_raw` table with location columns
- [ ] Export InfluxDB data to CSV (including location fields)
- [ ] Import CSV to ClickHouse
- [ ] Create materialised views for aggregations
- [ ] Verify location data completeness
- [ ] Test query performance vs InfluxDB
- [ ] Run parallel for 1 week before cutover

### Phase 3: IPFS Publishing

- [ ] Implement daily Parquet export from ClickHouse
- [ ] Set up IPFS node and IPFS Cluster
- [ ] Create OrbitDB metadata index
- [ ] Automate daily publish pipeline
- [ ] Test IPFS retrieval and ClickHouse import
- [ ] Document consumer workflow
- [ ] Onboard first community replication node

## Future Enhancements

1. **Hierarchical locations**: Store city → state → country relationships
2. **Multiple languages**: Support i18n location names
3. **Custom regions**: User-defined location boundaries
4. **Batch geocoding**: Process multiple coordinates in parallel
5. **Alternative providers**: Fallback to Google/Mapbox if Nominatim fails

## Why Embedded Location Data?

This architecture embeds location names directly with sensor data rather than using a separate dimension table. Here's why:

### Advantages

1. **Self-contained exports**: Parquet files on IPFS include everything needed
   - Researchers don't need external lookup tables
   - No broken references if dimension table becomes unavailable
   - Simplifies data distribution and consumption

2. **Efficient with modern compression**: 
   - Parquet dictionary encoding handles repeated strings efficiently
   - InfluxDB compression reduces overhead to ~2-5%
   - Storage cost is negligible compared to query simplicity

3. **Simpler queries**:
   - No joins required (faster, easier to write)
   - Filter directly on `WHERE country = 'New Zealand'`
   - Aggregation by location is trivial

4. **Decentralised-friendly**:
   - Aligns with "data is the database" philosophy
   - IPFS files are complete datasets, not partial references
   - Works offline after download

### When to Use Dimension Tables Instead

 Dimension tables make sense when:
- Data changes frequently (locations don't)
- Storage is extremely constrained (not the case with Parquet)
- You need ACID updates across references (not needed here)
- Working with traditional OLTP databases (we're using time-series/analytics DBs)

For SkyTrace's decentralised data commons architecture, embedded location data is the right choice.

## Related Files

- `utils/geocoder.py` - Geocoding module with caching
- `utils/geocoding_worker.py` - Background worker  
- `backfill_geocoding.py` - Backfill existing coordinates
- `GEOCODING_README.md` - Setup and usage guide
- `data_ingester_part1.py` - Main ingestion service

## References

- [Apache Parquet Dictionary Encoding](https://parquet.apache.org/docs/file-format/data-pages/encodings/)
- [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
- [Nominatim Usage Policy](https://operations.osmfoundation.org/policies/nominatim/)
- [Decentralised Data Commons Architecture](../../wesense-respiro/docs/DECENTRALISED_DATA_COMMONS_ARCHITECTURE.md)
