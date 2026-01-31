# WeSense Ingester - Meshtastic

Decodes environmental sensor data from Meshtastic mesh networks. Subscribes to public and community Meshtastic MQTT brokers, decodes protobuf, correlates position + telemetry, reverse geocodes, and publishes to ClickHouse and MQTT.

> For detailed documentation, see the [Wiki](https://github.com/wesense-earth/wesense-ingester-meshtastic/wiki).
> Read on for a project overview and quick install instructions.

## Overview

Meshtastic environmental data is challenging because position and telemetry arrive as **separate messages**, often minutes apart. This ingester caches positions per node, holds pending telemetry until a position is known, and correlates data before publishing. It also handles AES-CTR decryption, deduplication (mesh flooding sends the same packet multiple times), and offline reverse geocoding via GeoNames.

Supports two modes via the `MESHTASTIC_MODE` environment variable:
- **`public`** (default) — subscribes to `mqtt.meshtastic.org`
- **`community`** — subscribes to a private community gateway

Uses [wesense-ingester-core](https://github.com/wesense-earth/wesense-ingester-core) for ClickHouse writing, MQTT publishing, geocoding, deduplication, and logging.

## Quick Install (Recommended)

Most users should deploy via [wesense-deploy](https://github.com/wesense-earth/wesense-deploy), which orchestrates all WeSense services using Docker Compose profiles:

```bash
# Clone the deploy repo
git clone https://github.com/wesense-earth/wesense-deploy.git
cd wesense-deploy

# Configure
cp .env.sample .env
# Edit .env with your settings

# Start as a contributor (ingesters only, sends to remote hub)
docker compose --profile contributor up -d

# Or as a full station (includes EMQX, ClickHouse, Respiro map)
docker compose --profile station up -d
```

For Unraid or manual deployments, use the docker-run script:

```bash
./scripts/docker-run.sh station
```

See [Deployment Personas](https://github.com/wesense-earth/wesense-deploy) for all options.

## Docker (Standalone)

For running this ingester independently (e.g. on a separate host):

```bash
docker pull ghcr.io/wesense-earth/wesense-ingester-meshtastic:latest

docker run -d \
  --name wesense-ingester-meshtastic \
  --restart unless-stopped \
  -e MESHTASTIC_MODE=public \
  -e WESENSE_OUTPUT_BROKER=mqtt.wesense.earth \
  -e WESENSE_OUTPUT_PORT=1883 \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_DATABASE=wesense \
  -v ./cache:/app/cache \
  -v ./config:/app/config:ro \
  -v ./logs:/app/logs \
  ghcr.io/wesense-earth/wesense-ingester-meshtastic:latest
```

## Local Development

```bash
# Install core library (from sibling directory)
pip install -e ../wesense-ingester-core

# Install adapter dependencies
pip install -r requirements.txt

# Run (public mode)
python meshtastic_ingester.py

# Run (community mode)
MESHTASTIC_MODE=community python meshtastic_ingester.py
```

## Architecture

```
Meshtastic MQTT (mqtt.meshtastic.org or community)
    │
    ▼  Subscribe to msh/{region}/#
ServiceEnvelope (protobuf)
    │
    ▼  Decrypt (AES-CTR) → MeshPacket → Position / Telemetry / NodeInfo
    │
    ▼  Cache position per node, correlate with telemetry
    │
    ▼  [wesense-ingester-core pipeline]
    ├─→ DeduplicationCache (mesh flooding protection)
    ├─→ ReverseGeocoder (GeoNames → ISO 3166 codes)
    ├─→ BufferedClickHouseWriter (batched inserts)
    └─→ WeSensePublisher (MQTT: wesense/decoded/meshtastic-public/{country}/{subdiv}/{device})
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MESHTASTIC_MODE` | `public` | `public` or `community` |
| `MESHTASTIC_CHANNEL_KEY` | `AQ==` | AES channel key (base64) |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse server |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_DATABASE` | `wesense` | Database name |
| `CLICKHOUSE_BATCH_SIZE` | `100` | Rows before flush |
| `CLICKHOUSE_FLUSH_INTERVAL` | `10` | Seconds between flushes |
| `WESENSE_OUTPUT_BROKER` | `localhost` | Output MQTT broker |
| `WESENSE_OUTPUT_PORT` | `1883` | Output MQTT port |
| `WESENSE_OUTPUT_USERNAME` | | Output MQTT username |
| `WESENSE_OUTPUT_PASSWORD` | | Output MQTT password |
| `DEBUG` | `false` | Enable debug logging |
| `LOG_LEVEL` | `INFO` | Log level |

Region subscriptions are configured in `config/mqtt_regions.json`.

## Output

Publishes decoded readings to MQTT topic `wesense/decoded/{source}/{country}/{subdivision}/{device_id}` and inserts into ClickHouse `wesense.sensor_readings`.

## Related

- [wesense-ingester-core](https://github.com/wesense-earth/wesense-ingester-core) — Shared library
- [wesense-deploy](https://github.com/wesense-earth/wesense-deploy) — Docker Compose orchestration
- [wesense-respiro](https://github.com/wesense-earth/wesense-respiro) — Sensor map dashboard

## License

MIT
