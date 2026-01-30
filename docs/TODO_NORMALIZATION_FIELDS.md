# Normalization Fields - Implementation Plan

**Status:** Planned
**Priority:** High
**Related Document:** `wesense-general-docs/Decentralised_Data_Commons_Architecture.md`

---

## Overview

The MQTT Hub now performs rate normalization, buffering sensor readings and outputting 5-minute averaged readings. The ingester needs to handle the new payload fields that the hub adds.

## New Payload Fields

The hub adds these fields to normalized readings:

| Field                 | Type    | Description                                        |
| --------------------- | ------- | -------------------------------------------------- |
| `sample_count`        | integer | Number of readings averaged in the 5-minute window |
| `sample_interval_avg` | integer | Average interval between samples in seconds        |
| `min`                 | float   | Minimum value in the window                        |
| `max`                 | float   | Maximum value in the window                        |

**Example payload from hub:**

```json
{
  "value": 23.45,
  "timestamp": 1732291500,
  "latitude": -36.848461,
  "longitude": 174.763336,
  "unit": "°C",
  "country": "nz",
  "subdivision": "auk",
  "device_id": "office_301274c0e8fc",
  "sensor_model": "SHT4x",
  "sample_count": 5,
  "sample_interval_avg": 62,
  "min": 23.21,
  "max": 23.68
}
```

---

## Implementation Tasks

### Task 1: Update ClickHouse Schema

Add columns for normalization fields:

```sql
ALTER TABLE sensor_readings ADD COLUMN sample_count UInt16 DEFAULT 1;
ALTER TABLE sensor_readings ADD COLUMN sample_interval_avg UInt16 DEFAULT 300;
ALTER TABLE sensor_readings ADD COLUMN value_min Float32;
ALTER TABLE sensor_readings ADD COLUMN value_max Float32;
```

### Task 2: Update Decoder/Processor

- Parse new fields from incoming payload
- Default `sample_count` to 1 if not present (for backward compatibility)
- Default `sample_interval_avg` to 300 if not present
- Set `value_min` and `value_max` to `value` if not present

### Task 3: Update ClickHouse Insert

Include new fields in INSERT statements:

```sql
INSERT INTO sensor_readings (
  ...,
  sample_count,
  sample_interval_avg,
  value_min,
  value_max
) VALUES (
  ...,
  :sample_count,
  :sample_interval_avg,
  :min,
  :max
)
```

### Task 4: Update libp2p Broadcast

Include normalization fields in the payload broadcast to libp2p pub/sub topics.

### Task 5: Update Parquet Export

Include normalization fields in archived Parquet files.

---

## Statistical Analysis Use Cases

These fields enable:

1. **Data quality assessment** - Low `sample_count` indicates sensor sending at expected 5-min intervals; high count indicates over-frequent sending
2. **Variability analysis** - `min`/`max` range shows reading stability within windows
3. **Timing consistency** - `sample_interval_avg` helps identify sensors with irregular timing (especially Meshtastic)

---

## Backward Compatibility

- Existing payloads without these fields should still work
- Default values ensure old data can be processed
- Consumer apps should handle missing fields gracefully

---

## Testing Checklist

- [ ] ClickHouse schema migration runs successfully
- [ ] Payloads with normalization fields are stored correctly
- [ ] Payloads without normalization fields use defaults
- [ ] libp2p broadcasts include normalization fields
- [ ] Parquet exports include normalization fields
- [ ] Consumer apps display/use the new fields correctly

---

## References

- Architecture Document: `wesense-general-docs/Decentralised_Data_Commons_Architecture.md` (Section 3.1, 7.5)
- MQTT Hub Architecture: `wesense-mqtt-hub/ARCHITECTURE.md`
