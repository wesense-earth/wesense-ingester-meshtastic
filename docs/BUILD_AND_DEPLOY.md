# WeSense Ingester (Meshtastic) - Build & Deploy Guide

**Consolidated guide for building on Mac and deploying to Docker hosts.**

The data ingester processes Meshtastic sensor data from public MQTT servers and writes to ClickHouse. It also publishes decoded JSON to local MQTT for real-time consumers (e.g., wesense-respiro).

---

## Prerequisites

Before deploying the ingester, ensure ClickHouse is running:

1. **Deploy wesense-clickhouse-live** (see [ClickHouse deployment](#clickhouse-setup))
2. **Create database and table** using the SQL files provided

---

## Quick Reference

**Build on Mac → Deploy to TrueNAS:**

```bash
# On Mac
cd wesense-ingester-meshtastic
docker buildx build --platform linux/amd64 -t wesense-ingester-meshtastic:latest --load .
docker save wesense-ingester-meshtastic:latest | gzip > wesense-ingester-meshtastic.tar.gz
scp wesense-ingester-meshtastic.tar.gz root@your-docker-host:/tmp/

# On TrueNAS
ssh root@your-docker-host
gunzip -c /tmp/wesense-ingester-meshtastic.tar.gz | docker load
rm /tmp/wesense-ingester-meshtastic.tar.gz
docker run -d --name=wesense-ingester-meshtastic ... wesense-ingester-meshtastic:latest
```

---

## ClickHouse Setup

The ingester writes to ClickHouse. Ensure it's running before starting the ingester.

### Start ClickHouse

```bash
cd /path/to/wesense-clickhouse-live
docker-compose up -d
```

### Create Database and Table

```bash
# Connect to ClickHouse
docker exec -it wesense-clickhouse-live clickhouse-client

# Run these SQL commands
CREATE DATABASE IF NOT EXISTS wesense;

# Then run the contents of create_sensors_raw.sql to create the sensor_readings table
```

### Verify ClickHouse is Ready

```bash
# Test connection
docker exec -it wesense-clickhouse-live clickhouse-client -q "SELECT 1"

# Check database exists
docker exec -it wesense-clickhouse-live clickhouse-client -q "SHOW DATABASES"
```

---

## Method 1: Build on Mac, Transfer Image (Recommended)

**Use this when:** You want to develop and build on your Mac, then deploy the pre-built image to your Docker host without copying source code.

### Step 1: Build on Mac

```bash
cd wesense-ingester-meshtastic

# Build the Docker image for x86_64/amd64 (TrueNAS architecture)
docker buildx build --platform linux/amd64 -t wesense-ingester-meshtastic:latest --load .
```

**Important:**

- On Apple Silicon Macs (M1/M2/M3), the `--platform linux/amd64` flag is **required** to build x86_64 images for TrueNAS
- On Intel Macs, this flag is optional but recommended for consistency
- The `--load` flag loads the image into your local Docker daemon

**Build output should show:**

```
[+] Building 45.2s (12/12) FINISHED
 => [internal] load build definition from Dockerfile
 => => transferring dockerfile: 1.26kB
 ...
 => => naming to docker.io/library/wesense-ingester-meshtastic:latest
```

### Step 2: Save Image to File

```bash
# Save as compressed tar file (much smaller for transfer)
docker save wesense-ingester-meshtastic:latest | gzip > wesense-ingester-meshtastic.tar.gz

# Check file size (should be ~100-150 MB)
ls -lh wesense-ingester-meshtastic.tar.gz
```

**Alternative (uncompressed, faster but larger):**

```bash
docker save wesense-ingester-meshtastic:latest > wesense-ingester-meshtastic.tar
```

### Step 3: Transfer to TrueNAS

```bash
# Copy image file to TrueNAS
scp wesense-ingester-meshtastic.tar.gz root@your-docker-host:/tmp/

# Or if using your non-root user:
scp wesense-ingester-meshtastic.tar.gz your_user@your-docker-host:/tmp/
```

**Transfer time:** ~30 seconds to 2 minutes depending on network and file size.

### Step 4: Load Image on TrueNAS

```bash
# SSH to TrueNAS
ssh root@your-docker-host

# Load the image
gunzip -c /tmp/wesense-ingester-meshtastic.tar.gz | docker load

# Or if you transferred uncompressed:
docker load < /tmp/wesense-ingester-meshtastic.tar

# Clean up transfer file
rm /tmp/wesense-ingester-meshtastic.tar.gz

# Verify image loaded
docker images | grep wesense-ingester-meshtastic
```

**Expected output:**

```
REPOSITORY                    TAG       IMAGE ID       CREATED          SIZE
wesense-ingester-meshtastic   latest    a1b2c3d4e5f6   5 minutes ago    285MB
```

### Step 5: Prepare Directories and Config

```bash
# Create required directories
mkdir -p /opt/wesense/ingester-meshtastic/{cache,config,logs}
cd /opt/wesense/ingester-meshtastic

# Copy config file (mqtt_regions.json)
# Transfer from Mac or create manually
```

### Step 6: Run Container on Docker Host

```bash
docker run -d \
  --name=wesense-ingester-meshtastic \
  --restart unless-stopped \
  -v /opt/wesense/ingester-meshtastic/cache:/app/cache \
  -v /opt/wesense/ingester-meshtastic/config:/app/config:ro \
  -v /opt/wesense/ingester-meshtastic/logs:/app/logs \
  -e CLICKHOUSE_HOST=your-docker-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_DATABASE=wesense \
  -e CLICKHOUSE_TABLE=sensor_readings \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD= \
  -e CLICKHOUSE_BATCH_SIZE=100 \
  -e CLICKHOUSE_FLUSH_INTERVAL=10 \
  -e TZ=Pacific/Auckland \
  -e DEBUG=false \
  -e LOG_LEVEL=INFO \
  -e LOG_MAX_BYTES=10485760 \
  -e LOG_BACKUP_COUNT=5 \
  wesense-ingester-meshtastic:latest
```

### Step 7: Verify

```bash
# Check it's running
docker ps | grep wesense-ingester-meshtastic

# View logs
docker logs -f wesense-ingester-meshtastic
```

**Expected log output:**

```
============================================================
WeSense Ingester - Meshtastic
============================================================

✓ Connected to ClickHouse at your-docker-host:8123
  Database: wesense, Table: sensor_readings
  Batch size: 100, Flush interval: 10s
✓ Connected to WeSense output MQTT at your-docker-host

[ANZ] Connecting to mqtt.meshtastic.org:1883...
[ANZ] Connected successfully
[US] Connecting to mqtt.meshtastic.org:1883...
[US] Connected successfully
...
```

---

## Method 2: Build Directly on Docker Host

**Use this when:** You want everything built and running on the Docker host (simpler but requires copying source code).

### Step 1: Copy Source Code

```bash
# From Mac
cd /Users/quentinj/projects/wesense
scp -r wesense-ingester-meshtastic root@your-docker-host:/opt/wesense/
```

### Step 2: Build on Docker Host

```bash
# SSH to Docker host
ssh root@your-docker-host
cd /opt/wesense/wesense-ingester-meshtastic

# Build
docker build -t wesense-ingester-meshtastic:latest .
```

### Step 3: Run Container

Same as Method 1, Step 6 above.

---

## WeSense v1 Topic Structure

The data ingester publishes decoded JSON to local MQTT using the WeSense v1 topic format:

### Output Topics

```
wesense/v1/{country}/{subdivision}/{device_id}/{reading_type}
```

**Examples:**

```
wesense/v1/us/ca/meshtastic_a1b2c3d4/temperature
wesense/v1/nz/auk/meshtastic_12345678/humidity
wesense/v1/au/qld/meshtastic_9abcdef0/pressure
```

### Device ID Format

Meshtastic devices use the prefix `meshtastic_` followed by the node ID:

- Old format: `!a1b2c3d4`
- New format: `meshtastic_a1b2c3d4`

### Geographic Codes

Uses ISO 3166 codes:

- **Country**: ISO 3166-1 alpha-2 (e.g., `us`, `nz`, `au`)
- **Subdivision**: ISO 3166-2 (e.g., `ca` for California, `auk` for Auckland)
- **Unknown locations**: `wesense/v1/unknown/unknown/meshtastic_xxx/temperature`

---

## Environment Variables Reference

### ClickHouse Configuration

| Variable                    | Default           | Description                       |
| --------------------------- | ----------------- | --------------------------------- |
| `CLICKHOUSE_HOST`           | `your-docker-host`   | ClickHouse server hostname/IP     |
| `CLICKHOUSE_PORT`           | `8123`            | ClickHouse HTTP port              |
| `CLICKHOUSE_DATABASE`       | `wesense`         | ClickHouse database name          |
| `CLICKHOUSE_TABLE`          | `sensor_readings` | ClickHouse table name             |
| `CLICKHOUSE_USER`           | `default`         | ClickHouse username               |
| `CLICKHOUSE_PASSWORD`       | (empty)           | ClickHouse password               |
| `CLICKHOUSE_BATCH_SIZE`     | `100`             | Rows to buffer before flushing    |
| `CLICKHOUSE_FLUSH_INTERVAL` | `10`              | Seconds between automatic flushes |

### Logging Configuration

| Variable           | Default           | Description                             |
| ------------------ | ----------------- | --------------------------------------- |
| `TZ`               | `UTC`             | Timezone (e.g., `Pacific/Auckland`)     |
| `DEBUG`            | `false`           | Enable/disable debug mode (true/false)  |
| `LOG_LEVEL`        | `INFO`            | Log level (DEBUG, INFO, WARNING, ERROR) |
| `LOG_MAX_BYTES`    | `10485760` (10MB) | Maximum size per log file               |
| `LOG_BACKUP_COUNT` | `5`               | Number of backup log files              |

### Ingestion Metadata

| Variable            | Default    | Description                           |
| ------------------- | ---------- | ------------------------------------- |
| `INGESTION_NODE_ID` | (hostname) | Identifier for this ingester instance |

---

## Quick Update Workflow

**When you make code changes and need to redeploy:**

```bash
# On Mac - build and transfer
cd wesense-ingester-meshtastic
docker buildx build --platform linux/amd64 -t wesense-ingester-meshtastic:latest --load .
docker save wesense-ingester-meshtastic:latest | gzip > wesense-ingester-meshtastic.tar.gz
scp wesense-ingester-meshtastic.tar.gz root@your-docker-host:/tmp/

# On TrueNAS - stop, load, restart
ssh root@your-docker-host
docker stop wesense-ingester-meshtastic
docker rm wesense-ingester-meshtastic
gunzip -c /tmp/wesense-ingester-meshtastic.tar.gz | docker load
rm /tmp/wesense-ingester-meshtastic.tar.gz

# Run with same command as initial deployment
docker run -d --name=wesense-ingester-meshtastic ... wesense-ingester-meshtastic:latest
```

---

## Management Commands

### View Logs

```bash
# Follow logs in real-time
docker logs -f wesense-ingester-meshtastic

# Last 100 lines
docker logs --tail 100 wesense-ingester-meshtastic

# Last hour
docker logs --since 1h wesense-ingester-meshtastic
```

### Restart Container

```bash
docker restart wesense-ingester-meshtastic
```

### Stop Container

```bash
docker stop wesense-ingester-meshtastic
```

### Remove Container

```bash
docker stop wesense-ingester-meshtastic
docker rm wesense-ingester-meshtastic
```

### Check Status

```bash
# Is it running?
docker ps | grep wesense-ingester-meshtastic

# Resource usage
docker stats wesense-ingester-meshtastic --no-stream
```

### Execute Commands Inside Container

```bash
# Interactive shell
docker exec -it wesense-ingester-meshtastic /bin/bash

# Check environment
docker exec wesense-ingester-meshtastic env | grep CLICKHOUSE

# View cache directory
docker exec wesense-ingester-meshtastic ls -la /app/cache

# Tail internal logs
docker exec wesense-ingester-meshtastic tail -f /app/logs/ingester_debug.log
```

---

## Docker Compose Deployment

**Alternative deployment using docker-compose:**

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  ingester-meshtastic:
    image: wesense-ingester-meshtastic:latest
    container_name: wesense-ingester-meshtastic
    restart: unless-stopped
    volumes:
      - ./cache:/app/cache
      - ./config:/app/config:ro
      - ./logs:/app/logs
    environment:
      CLICKHOUSE_HOST: your-docker-host
      CLICKHOUSE_PORT: 8123
      CLICKHOUSE_DATABASE: wesense
      CLICKHOUSE_TABLE: sensor_readings
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
      CLICKHOUSE_BATCH_SIZE: 100
      CLICKHOUSE_FLUSH_INTERVAL: 10
      TZ: Pacific/Auckland
      DEBUG: "false"
      LOG_LEVEL: INFO
      LOG_MAX_BYTES: "10485760"
      LOG_BACKUP_COUNT: "5"
```

**Usage:**

```bash
# Start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## Verification Commands

### Check Container Status

```bash
docker ps | grep wesense-ingester-meshtastic
docker inspect wesense-ingester-meshtastic | grep Status
```

### Monitor MQTT Traffic

```bash
# Watch WeSense v1 topics
mosquitto_sub -h your-docker-host -t "wesense/v1/#" -v

# Watch specific country
mosquitto_sub -h your-docker-host -t "wesense/v1/us/#" -v

# Watch specific subdivision
mosquitto_sub -h your-docker-host -t "wesense/v1/nz/auk/#" -v
```

**Expected output:**

```json
wesense/v1/us/ca/meshtastic_a1b2c3d4/temperature {
  "value": 72.5,
  "timestamp": 1701234567,
  "device_id": "meshtastic_a1b2c3d4",
  "latitude": 37.7749,
  "longitude": -122.4194,
  "country": "us",
  "subdivision": "ca",
  "data_source": "MESHTASTIC",
  "board_model": "TBEAM",
  "reading_type": "temperature",
  "unit": "°F"
}
```

### Check ClickHouse Data

```bash
# Connect to ClickHouse
docker exec -it wesense-clickhouse-live clickhouse-client

# Check recent data
SELECT * FROM wesense.sensor_readings ORDER BY timestamp DESC LIMIT 10;

# Count by data source
SELECT data_source, count() FROM wesense.sensor_readings GROUP BY data_source;

# Count by reading type
SELECT reading_type, count() FROM wesense.sensor_readings GROUP BY reading_type;
```

---

## Troubleshooting

### Container Won't Start

**Check logs:**

```bash
docker logs wesense-ingester-meshtastic
```

**Common issues:**

- Missing config file: Ensure `config/mqtt_regions.json` exists
- ClickHouse not running: Start wesense-clickhouse-live first
- Network issues: Check ClickHouse is accessible

### No Data in ClickHouse

**Check ClickHouse connectivity:**

```bash
docker exec wesense-ingester-meshtastic ping your-docker-host
```

**Verify environment variables:**

```bash
docker exec wesense-ingester-meshtastic env | grep CLICKHOUSE
```

**Check buffer is flushing:**

```bash
# Look for flush messages in logs
docker logs wesense-ingester-meshtastic | grep "Flushed"
```

**Enable debug logging:**

```bash
docker stop wesense-ingester-meshtastic
docker rm wesense-ingester-meshtastic
# Re-run with DEBUG=true and LOG_LEVEL=DEBUG
```

### No MQTT Messages

**Check Meshtastic connectivity:**

- Verify mqtt.meshtastic.org is accessible
- Check region configuration in config/mqtt_regions.json
- Monitor logs for connection errors

**Test MQTT subscription manually:**

```bash
mosquitto_sub -h mqtt.meshtastic.org -p 1883 -u meshdev -P large4cats -t "msh/US/#" -v
```

### Unknown Geographic Codes

If you see topics like `wesense/v1/unknown/unknown/...`:

- The geocoder couldn't find ISO codes for the location
- Check `/app/logs/ingester_debug.log` for warnings about unknown countries/subdivisions
- Add missing codes to `utils/iso3166_mapper.py`

---

## Performance Tuning

### ClickHouse Batching

Adjust batch settings based on your data volume:

**High volume (many messages/sec):**

```bash
-e CLICKHOUSE_BATCH_SIZE=500 -e CLICKHOUSE_FLUSH_INTERVAL=5
```

**Low volume (few messages/min):**

```bash
-e CLICKHOUSE_BATCH_SIZE=50 -e CLICKHOUSE_FLUSH_INTERVAL=30
```

### Log Rotation Settings

**Production (minimal logging):**

```bash
-e DEBUG=false -e LOG_LEVEL=WARNING -e LOG_MAX_BYTES=5242880 -e LOG_BACKUP_COUNT=3
```

**Development (verbose logging):**

```bash
-e DEBUG=true -e LOG_LEVEL=DEBUG -e LOG_MAX_BYTES=20971520 -e LOG_BACKUP_COUNT=10
```

**Balanced:**

```bash
-e DEBUG=false -e LOG_LEVEL=INFO -e LOG_MAX_BYTES=10485760 -e LOG_BACKUP_COUNT=5
```

### Cache Management

The cache directory stores:

- Position data for Meshtastic nodes
- Geocoding results
- Pending telemetry

**Cache location:** `/app/cache/` (mapped to host volume)

To clear cache:

```bash
docker exec wesense-ingester-meshtastic rm -f /app/cache/*.json
docker restart wesense-ingester-meshtastic
```

---

## Data Persistence

- **Cache**: `./cache` directory persists between container restarts
- **Logs**: `./logs` directory persists between container restarts
- **Config**: `./config` directory is mounted read-only

**Backup strategy:**

```bash
# Backup cache and config
tar -czf wesense-ingester-backup-$(date +%Y%m%d).tar.gz cache/ config/ logs/
```

---

## Advanced: Multi-Region Monitoring

To monitor multiple Meshtastic regions, edit `config/mqtt_regions.json`:

```json
{
  "US": {
    "broker": "mqtt.meshtastic.org",
    "port": 1883,
    "username": "meshdev",
    "password": "large4cats",
    "topic": "msh/US/#",
    "enabled": true
  },
  "EU_868": {
    "broker": "mqtt.meshtastic.org",
    "port": 1883,
    "username": "meshdev",
    "password": "large4cats",
    "topic": "msh/EU_868/#",
    "enabled": true
  }
}
```

The container will automatically connect to all enabled regions.

---

## Summary

**Recommended workflow:**

1. Ensure ClickHouse is running (`wesense-clickhouse-live`)
2. Develop on Mac
3. Build image on Mac: `docker buildx build --platform linux/amd64 -t wesense-ingester-meshtastic:latest --load .`
4. Save to file: `docker save wesense-ingester-meshtastic:latest | gzip > wesense-ingester-meshtastic.tar.gz`
5. Transfer to TrueNAS: `scp wesense-ingester-meshtastic.tar.gz root@your-docker-host:/tmp/`
6. Load on TrueNAS: `gunzip -c /tmp/wesense-ingester-meshtastic.tar.gz | docker load`
7. Run with environment variables

**Advantages:**

- ✅ No source code on Docker host
- ✅ Build once, deploy anywhere
- ✅ Faster updates (just transfer image)
- ✅ Consistent builds (Mac build environment)
- ✅ WeSense v1 standardized topics with ISO 3166 geographic codes
- ✅ ClickHouse for high-performance time-series storage
- ✅ Batched writes for efficiency
