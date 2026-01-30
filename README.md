# WeSense Ingester - Meshtastic

Specialized ingester for Meshtastic environmental sensor data. Subscribes to the public Meshtastic MQTT broker, decodes Meshtastic protobuf, caches positions, correlates position and telemetry data, performs reverse geocoding, and publishes decoded JSON to a unified MQTT topic.

## Why a Specialized Ingester?

Meshtastic data requires special handling because position and telemetry arrive in **separate messages**, often minutes or hours apart:

| Message Type  | Content                     | Timing                                        |
| ------------- | --------------------------- | --------------------------------------------- |
| POSITION_APP  | lat/lon, altitude           | Sent periodically (e.g., every 30 min)        |
| TELEMETRY_APP | temperature, humidity, etc. | Sent with sensor readings (e.g., every 5 min) |

The ingester must:

1. **Cache positions** per node (persisted to disk, 7-day TTL, survives restarts)
2. **Hold pending telemetry** until a position is known
3. **Correlate** position + telemetry before publishing
4. **Filter** to only environmental telemetry (ignore chat, device metrics, etc.)

## Architecture

```
Meshtastic Public MQTT              Meshtastic Ingester
(mqtt.meshtastic.org)               (this service)

    msh/{region}/...                     │
         │                               │
         └──────────────────────────────►│
                                         │
                                    ┌────▼────┐
                                    │ Decode  │
                                    │ Protobuf│
                                    └────┬────┘
                                         │
                                    ┌────▼────┐
                                    │  Cache  │
                                    │Position │
                                    │ (7 day) │
                                    └────┬────┘
                                         │
                                    ┌────▼────┐
                                    │Correlate│
                                    │Pos+Tele │
                                    └────┬────┘
                                         │
                                    ┌────▼────┐
                                    │Reverse  │
                                    │Geocode  │
                                    └────┬────┘
                                         │
                                         ▼
               wesense/decoded/{country}/{subdivision}/{device_id}
                                         │
                                         ▼
                                    ┌──────────┐
                                    │ Telegraf │ ──► InfluxDB
                                    └──────────┘
```

## Output Format

Publishes to: `wesense/decoded/{country}/{subdivision}/{device_id}`

```json
{
  "device_id": "meshtastic_e4cc140c",
  "timestamp": 1732291200,
  "latitude": -36.848461,
  "longitude": 174.763336,
  "country": "New Zealand",
  "country_code": "nz",
  "subdivision_code": "auk",
  "data_source": "MESHTASTIC",
  "measurements": [
    {"reading_type": "temperature", "value": 22.5, "unit": "°C"},
    {"reading_type": "humidity", "value": 65.3, "unit": "%"}
  ]
}
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the ingester
python data_ingester_part1.py
```

## Configuration

Regions are configured in `config/mqtt_regions.json`. Key environment variables:

| Variable            | Default             | Description                                |
| ------------------- | ------------------- | ------------------------------------------ |
| `MQTT_BROKER`       | mqtt.meshtastic.org | Source MQTT broker                         |
| `LOCAL_MQTT_BROKER` | (required)          | Destination MQTT broker for decoded output |
| `LOCAL_MQTT_PORT`   | 1883                | Destination MQTT port                      |
| `INFLUX_URL`        | (optional)          | InfluxDB URL for direct writes             |
| `DEBUG`             | false               | Enable debug logging                       |

## Files

| File                        | Description                     |
| --------------------------- | ------------------------------- |
| `data_ingester_part1.py`    | Main ingester script            |
| `config/mqtt_regions.json`  | Meshtastic region configuration |
| `utils/geocoding_worker.py` | Background reverse geocoding    |
| `utils/geocoder.py`         | Geocoding implementation        |

## Related Components

- **wesense-mqtt-hub** - Central MQTT broker for the WeSense network
- **wesense-ingester-wesense** - Handles WeSense WiFi/LoRa sensors (simple decode + geocode)
- **wesense-ttn-webhook** - Receives TTN webhooks for LoRaWAN sensors

## License

MIT
