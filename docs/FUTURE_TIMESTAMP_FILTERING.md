# Future Timestamp Filtering

## Problem

Some Meshtastic devices have incorrect RTC (Real-Time Clock) settings, causing them to report timestamps in the future - sometimes by seconds, sometimes by years (e.g., 2080). This causes several issues:

1. **Dashboard pollution**: Devices with future timestamps always appear at the top of "recent" lists since they're perpetually "just now"
2. **Data integrity**: Time-series analysis (e.g., rush hour CO2 correlation) is impossible with inaccurate timestamps
3. **Discrepancies**: The "Active Now" count differed from the "Node Activity Distribution" chart because they handled future timestamps differently

## Architecture Decision

The WeSense architecture uses **sensor reading time** (not ingestion time) as the canonical timestamp because:
- Mesh network delays mean readings may arrive seconds/minutes/hours after being taken
- The sensor's timestamp is the only way to know when the measurement actually occurred

**Consequence**: If a sensor's clock is wrong, the data is fundamentally untraceable in time. We cannot "fix" it by offsetting because we don't know the true reading time.

**Decision**: Reject data from devices with timestamps >30 seconds in the future. This threshold balances:
- Allowing minor clock skew from network delays
- Rejecting clearly broken clocks
- Maintaining data accuracy for time-correlated analysis

## Code Changes

### File: `data_ingester_part1.py`

#### 1. Added constant (line ~2207)
```python
FUTURE_TIMESTAMP_TOLERANCE = 30  # Allow timestamps up to 30 seconds in the future
```

#### 2. Added dedicated logger (lines ~2194-2203)
```python
# Future timestamp logger - dedicated log for nodes with incorrect RTC
future_timestamp_logger = logging.getLogger('future_timestamps')
future_timestamp_logger.setLevel(logging.WARNING)
future_ts_handler = RotatingFileHandler(
    'logs/future_timestamps.log',
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT
)
future_ts_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
future_timestamp_logger.addHandler(future_ts_handler)
```

#### 3. Added filtering in telemetry handler (lines ~2767-2791)
After extracting the timestamp from telemetry, we check if it's too far in the future:
```python
# Check for future timestamps (indicates incorrect RTC on device)
current_time = int(time.time())
time_delta = timestamp - current_time
if time_delta > FUTURE_TIMESTAMP_TOLERANCE:
    # Format delta for human readability
    if time_delta > 86400:
        delta_str = f"{time_delta / 86400:.1f} days"
    elif time_delta > 3600:
        delta_str = f"{time_delta / 3600:.1f} hours"
    elif time_delta > 60:
        delta_str = f"{time_delta / 60:.1f} minutes"
    else:
        delta_str = f"{time_delta} seconds"

    # Log to dedicated future timestamp log
    future_timestamp_logger.warning(
        f"FUTURE_TIMESTAMP | node_name={node_name} | node_id={node_id} | region={region} | "
        f"timestamp={datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')} | "
        f"ahead_by={delta_str} | raw_delta_seconds={time_delta}"
    )

    if DEBUG:
        print(f"[{region}] ⏰ FUTURE TIMESTAMP {node_name} ({node_id}): {delta_str} ahead - SKIPPING")

    return  # Skip this telemetry data
```

#### 4. Added filtering in pending telemetry cache (lines ~2352-2355)
When loading cached pending telemetry:
```python
valid_readings = [(rt, v, u, ts) for rt, v, u, ts in readings
                if (current_time - ts) <= PENDING_TELEMETRY_MAX_AGE
                and (ts - current_time) <= FUTURE_TIMESTAMP_TOLERANCE]
```

#### 5. Added filtering when position arrives (lines ~2654-2657)
When processing pending telemetry after position data arrives:
```python
valid_readings = [(rt, v, u, ts) for rt, v, u, ts in pending_readings
                if (current_time - ts) <= PENDING_TELEMETRY_MAX_AGE
                and (ts - current_time) <= FUTURE_TIMESTAMP_TOLERANCE]
```

## Log Output

The `logs/future_timestamps.log` file contains entries like:
```
2025-12-05 09:40:00,636 | FUTURE_TIMESTAMP | node_name=!17e89f42 | node_id=!17e89f42 | region=US | timestamp=2080-01-13 05:21:48 | ahead_by=19761.8 days | raw_delta_seconds=1707421308
2025-12-05 09:41:21,837 | FUTURE_TIMESTAMP | node_name=PBNZ 🥝 Sensor (T-Echo) | node_id=!0b9ca93f | region=LOCAL | timestamp=2025-12-05 09:45:11 | ahead_by=3.8 minutes | raw_delta_seconds=230
```

## Maintenance: Cleaning Up Bad Devices

### 1. Monitor the log
```bash
tail -f logs/future_timestamps.log
```

### 2. Identify repeat offenders
Look for devices that consistently appear with >30 second drift.

### 3. Delete their historical data from ClickHouse
```sql
-- Check what data exists for suspect devices
SELECT
    device_id,
    any(node_name) as node_name,
    count(*) as record_count,
    min(timestamp) as earliest,
    max(timestamp) as latest
FROM wesense.sensor_readings
WHERE device_id IN ('!device1', '!device2', '!device3')
GROUP BY device_id
ORDER BY record_count DESC;

-- Delete all history for those devices
ALTER TABLE wesense.sensor_readings DELETE
WHERE device_id IN ('!device1', '!device2', '!device3');

-- Verify deletion
SELECT count(*) FROM wesense.sensor_readings
WHERE device_id IN ('!device1', '!device2', '!device3');
```

### 4. Delete all future-dated records (catch-all)
```sql
-- Preview
SELECT count(*) FROM wesense.sensor_readings WHERE timestamp > now();

-- Delete
ALTER TABLE wesense.sensor_readings DELETE WHERE timestamp > now();
```

## Initial Cleanup Performed (2025-12-05)

Deleted ~11,000 records from devices with broken clocks including:
- Devices with timestamps in 2080 (55 years ahead)
- Devices with timestamps in 2028 (826 days ahead)
- Devices with consistent >30 second drift

Remaining dataset: 898,725 records from 1,575 devices.

## ClickHouse Connection Details

```bash
curl -s "http://192.168.43.11:8123/?user=wesense&password=YOUR_PASSWORD&database=wesense" \
  --data-binary "YOUR SQL QUERY"
```
