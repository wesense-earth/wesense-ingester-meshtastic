# WeSense Ingester - Data Quality & Classification TODO

## Indoor/Outdoor Sensor Classification

### Correlation-Based Detection

The key insight: outdoor sensors in the same region should correlate highly with each other, while indoor sensors will show dampened, lagged, or uncorrelated patterns.

#### Temperature Correlation Analysis

```sql
-- Compare each sensor's temperature pattern against regional outdoor median
WITH hourly_readings AS (
    SELECT 
        device_id,
        toStartOfHour(timestamp) as hour,
        avg(value) as avg_temp
    FROM sensor_readings
    WHERE reading_type = 'temperature'
      AND timestamp > now() - INTERVAL 7 DAY
    GROUP BY device_id, hour
),
regional_median AS (
    SELECT 
        hour,
        median(avg_temp) as median_temp
    FROM hourly_readings
    GROUP BY hour
),
sensor_correlation AS (
    SELECT 
        h.device_id,
        corr(h.avg_temp, r.median_temp) as temp_correlation,
        stddevPop(h.avg_temp) as temp_stddev,
        max(h.avg_temp) - min(h.avg_temp) as temp_range
    FROM hourly_readings h
    JOIN regional_median r ON h.hour = r.hour
    GROUP BY h.device_id
)
SELECT 
    device_id,
    temp_correlation,
    temp_stddev,
    temp_range,
    CASE 
        WHEN temp_correlation > 0.85 AND temp_range > 8 THEN 'outdoor_high_confidence'
        WHEN temp_correlation > 0.7 AND temp_range > 5 THEN 'outdoor_likely'
        WHEN temp_correlation < 0.5 AND temp_range < 5 THEN 'indoor_likely'
        WHEN temp_correlation < 0.3 THEN 'indoor_high_confidence'
        ELSE 'unknown'
    END as environment_inference
FROM sensor_correlation
```

#### CO2 Pattern Analysis

```sql
-- Indoor CO2 shows occupancy patterns, outdoor is stable
SELECT 
    device_id,
    stddevPop(value) as co2_stddev,
    max(value) - min(value) as co2_range,
    avg(value) as co2_avg,
    CASE
        WHEN co2_stddev > 100 AND co2_avg > 500 THEN 'indoor_high_confidence'
        WHEN co2_stddev < 20 AND co2_avg < 450 THEN 'outdoor_high_confidence'
        WHEN co2_stddev > 50 THEN 'indoor_likely'
        ELSE 'outdoor_likely'
    END as environment_inference
FROM sensor_readings
WHERE reading_type = 'co2'
  AND timestamp > now() - INTERVAL 7 DAY
GROUP BY device_id
```

#### Multi-Signal Scoring

Combine signals for confidence scoring:

| Signal                       | Outdoor Indicator | Indoor Indicator |
| ---------------------------- | ----------------- | ---------------- |
| Temp correlation with region | > 0.85            | < 0.5            |
| Temp daily range             | > 8°C             | < 4°C            |
| CO2 variance                 | < 20 ppm σ        | > 100 ppm σ      |
| CO2 average                  | ~420 ppm          | > 500 ppm        |
| Humidity follows outdoor     | High correlation  | Low correlation  |
| Light level range            | 0 to 100k+ lux    | 0 to 500 lux     |

```sql
-- Composite environment score (-100 to +100, positive = outdoor)
SELECT 
    device_id,
    (temp_outdoor_score * 30) +
    (co2_outdoor_score * 40) +
    (humidity_outdoor_score * 15) +
    (light_outdoor_score * 15) as environment_score,
    CASE
        WHEN environment_score > 50 THEN 'outdoor'
        WHEN environment_score < -50 THEN 'indoor'
        ELSE 'uncertain'
    END as classification
FROM sensor_environment_signals
```

---

## Suspicious Reading Detection

### Enclosure Heat Buildup

Sensors in unventilated enclosures (perspex cases in sun) show:

- Temperature spikes during daylight hours
- Temperature significantly above regional median (>5°C)
- Rapid temperature changes not matching weather patterns

```sql
-- Detect sensors reading hot due to enclosure
WITH sensor_vs_regional AS (
    SELECT 
        s.device_id,
        s.timestamp,
        s.value as sensor_temp,
        r.median_temp as regional_temp,
        s.value - r.median_temp as temp_delta
    FROM sensor_readings s
    JOIN regional_hourly_median r 
        ON toStartOfHour(s.timestamp) = r.hour
        AND s.geo_subdivision = r.geo_subdivision
    WHERE s.reading_type = 'temperature'
)
SELECT 
    device_id,
    avg(temp_delta) as avg_delta,
    max(temp_delta) as max_delta,
    countIf(temp_delta > 5) / count() as pct_readings_hot,
    CASE
        WHEN avg_delta > 3 AND max_delta > 10 THEN 'enclosure_heat_suspected'
        WHEN pct_readings_hot > 0.3 THEN 'enclosure_heat_possible'
        ELSE 'normal'
    END as enclosure_status
FROM sensor_vs_regional
GROUP BY device_id
HAVING count() > 100
```

### Indoor Sensor in Winter (Heating Detection)

Indoor sensors should show stable temps in winter while outdoor freezes:

```sql
-- Flag sensors claiming outdoor but never dropping below ~15°C in winter
SELECT 
    device_id,
    deployment_environment,
    min(value) as min_temp,
    max(value) as max_temp,
    CASE
        WHEN deployment_environment = 'outdoor' 
             AND min_temp > 12 
             AND regional_min_temp < 5 
        THEN 'indoor_misclassified'
        ELSE 'ok'
    END as classification_check
FROM sensor_readings
WHERE reading_type = 'temperature'
  AND timestamp > now() - INTERVAL 30 DAY
GROUP BY device_id, deployment_environment
```

### Stale/Stuck Readings

Sensors reporting identical values repeatedly:

```sql
-- Detect sensors with suspiciously stable readings
SELECT 
    device_id,
    reading_type,
    count() as reading_count,
    uniqExact(value) as unique_values,
    reading_count / unique_values as repetition_ratio,
    CASE
        WHEN repetition_ratio > 10 THEN 'stuck_sensor_suspected'
        WHEN repetition_ratio > 5 THEN 'low_resolution_sensor'
        ELSE 'normal'
    END as sensor_status
FROM sensor_readings
WHERE timestamp > now() - INTERVAL 24 HOUR
GROUP BY device_id, reading_type
HAVING reading_count > 20
```

### Impossible Values

```sql
-- Flag physically impossible readings
SELECT 
    device_id,
    reading_type,
    value,
    timestamp,
    CASE
        WHEN reading_type = 'temperature' AND (value < -60 OR value > 60) THEN 'impossible'
        WHEN reading_type = 'humidity' AND (value < 0 OR value > 100) THEN 'impossible'
        WHEN reading_type = 'pressure' AND (value < 870 OR value > 1084) THEN 'impossible'
        WHEN reading_type = 'co2' AND value < 300 THEN 'impossible'
        WHEN reading_type = 'pm2_5' AND value < 0 THEN 'impossible'
        ELSE 'valid'
    END as validity
FROM sensor_readings
WHERE validity = 'impossible'
```

---

## Implementation Tasks

### Phase 1: Data Collection

- [ ] Add `environment_inference` column to device metadata table
- [ ] Add `data_quality_flags` column for suspicious reading markers
- [ ] Create materialized view for hourly sensor statistics
- [ ] Create regional median temperature table (updated hourly)
- [ ] Need to think through how timezones should work, from the sensor, throught the ingester, into the database and into respiro

### Phase 2: Classification Pipeline

- [ ] Implement temperature correlation analysis (runs daily)
- [ ] Implement CO2 pattern analysis (runs daily)  
- [ ] Create composite environment scoring function
- [ ] Add confidence levels (high/medium/low) to classifications

### Phase 3: Quality Detection

- [ ] Implement enclosure heat detection
- [ ] Implement stuck sensor detection
- [ ] Implement impossible value flagging
- [ ] Create alert system for newly suspicious sensors

### Phase 4: Feedback Loop

- [ ] Store user overrides of automatic classification
- [ ] Use overrides to improve classification algorithm
- [ ] Track classification accuracy over time

---

## Database Schema Additions

```sql
-- Device environment classification
ALTER TABLE device_metadata 
ADD COLUMN environment_auto String DEFAULT 'unknown',      -- Auto-detected
ADD COLUMN environment_override String DEFAULT '',         -- User override
ADD COLUMN environment_confidence Float32 DEFAULT 0,       -- 0-1 confidence
ADD COLUMN environment_last_analyzed DateTime DEFAULT now();

-- Data quality flags
ALTER TABLE sensor_readings
ADD COLUMN quality_flags Array(String) DEFAULT [];
-- Possible flags: 'enclosure_heat', 'stuck_value', 'impossible', 'outlier'

-- Or use a separate quality table
CREATE TABLE sensor_quality_events (
    device_id String,
    reading_type String,
    event_type String,  -- 'enclosure_heat', 'stuck', 'outlier', etc.
    detected_at DateTime,
    details String,     -- JSON with specifics
    resolved Boolean DEFAULT false
) ENGINE = MergeTree()
ORDER BY (device_id, detected_at);
```

---

## Notes

- Run classification weekly (sensor behavior is stable)
- Run quality checks hourly (catch issues quickly)
- Store historical classifications to track sensor moves
- Consider seasonal adjustment (summer vs winter patterns differ)

---

## Board Name Normalization (TODO)

Currently, Meshtastic hardware names are stored as raw enum names (e.g., `HELTEC_V3`, `TBEAM`, `RAK4631`) while the WeSense WiFi ingester stores friendly names (e.g., `Heltec WiFi LoRa 32 V3`, `LilyGo T-Beam`).

### Task
Add a board name mapping similar to `BOARD_TYPE_MAP` in `wesense-ingester-wesense/ingester_wesense.py` to normalize Meshtastic hardware names to friendly display names.

### Mapping needed
```python
BOARD_NAME_MAP = {
    'HELTEC_V3': 'Heltec WiFi LoRa 32 V3',
    'HELTEC_V2_0': 'Heltec WiFi LoRa 32 V2',
    'HELTEC_WSL_V3': 'Heltec Wireless Stick Lite V3',
    'HELTEC_WIRELESS_TRACKER': 'Heltec Wireless Tracker',
    'TBEAM': 'LilyGo T-Beam',
    'T_ECHO': 'LilyGo T-Echo',
    'T_DECK': 'LilyGo T-Deck',
    'TLORA_V1': 'LilyGo T-LoRa V1',
    'TLORA_V1_1P3': 'LilyGo T-LoRa V1 1.3',
    'TLORA_V2': 'LilyGo T-LoRa V2',
    'TLORA_V2_1_1P6': 'LilyGo T-LoRa V2.1 1.6',
    'TLORA_T3_S3': 'LilyGo T3 S3',
    'LILYGO_TBEAM_S3_CORE': 'LilyGo T-Beam S3 Core',
    'LILYGO_T_BEAM_S3': 'LilyGo T-Beam S3',
    'RAK4631': 'RAK4631',
    'RAK11200': 'RAK11200',
    'RAK11310': 'RAK11310',
    'STATION_G2': 'Station G2',
    'NANO_G2_ULTRA': 'Nano G2 Ultra',
    'NRF52_PROMICRO_DIY': 'nRF52 ProMicro DIY',
    'DIY_V1': 'DIY V1',
    'PORTDUINO': 'Portduino',
    'PRIVATE_HW': 'Private Hardware',
    'RP2040_LORA': 'RP2040 LoRa',
    'RPI_PICO': 'Raspberry Pi Pico',
    'T_WATCH_S3': 'LilyGo T-Watch S3',
    'EBYTE_ESP32_S3': 'EBYTE ESP32-S3',
}
```

### Database cleanup (optional, after ingester update)
```sql
-- Normalize existing raw Meshtastic names to friendly names
ALTER TABLE wesense.sensor_readings UPDATE board_model = 'Heltec WiFi LoRa 32 V3' WHERE board_model = 'HELTEC_V3';
ALTER TABLE wesense.sensor_readings UPDATE board_model = 'LilyGo T-Beam' WHERE board_model = 'TBEAM';
-- ... etc for other mappings
```
