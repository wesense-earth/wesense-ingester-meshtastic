# Data Ingester - ISO 3166 Topic Structure Update

**Status:** Planned
**Priority:** High
**Related Document:** `wesense-general-docs/Decentralised_Data_Commons_Architecture.md`

---

## Overview

Update the data ingester to translate all incoming data sources (Meshtastic, future third-party sensors) into the unified WeSense topic structure using ISO 3166 geographic codes.

The ingester acts as a **protocol adapter** - it speaks multiple "languages" on input but outputs a single unified format.

## Current vs New Output Format

|                | Current                                         | New                                                             |
| -------------- | ----------------------------------------------- | --------------------------------------------------------------- |
| **MQTT Topic** | `skytrace/decoded/env/{node_id}/{reading_type}` | `wesense/v1/{country}/{subdivision}/{device_id}/{reading_type}` |
| **Example**    | `skytrace/decoded/env/!a1b2c3d4/temperature`    | `wesense/v1/nz/auk/meshtastic_a1b2c3d4/temperature`             |

---

## Implementation Tasks

### Task 1: Create ISO 3166 Mapping Module

**Create new file:** `utils/iso3166_mapper.py`

```python
"""
ISO 3166 Geographic Code Mapper

Maps country names and state/region names to standardized ISO codes.
Used to translate geocoded locations to topic-compatible identifiers.
"""

# ISO 3166-1 alpha-2 country codes (lowercase)
COUNTRY_NAME_TO_ISO = {
    "New Zealand": "nz",
    "Australia": "au",
    "United States": "us",
    "United Kingdom": "gb",
    "Canada": "ca",
    "Germany": "de",
    "France": "fr",
    "Japan": "jp",
    "China": "cn",
    "Brazil": "br",
    "Mexico": "mx",
    "South Africa": "za",
    "India": "in",
    "Russia": "ru",
    "Singapore": "sg",
    "Malaysia": "my",
    "Taiwan": "tw",
    "Poland": "pl",
    "Czech Republic": "cz",
    "Ukraine": "ua",
    "Argentina": "ar",
    "Belarus": "by",
    # Add more as needed
}

# ISO 3166-2 subdivision codes (lowercase, without country prefix)
# Key: (country_code, state/region name)
# Value: subdivision code
SUBDIVISION_NAME_TO_ISO = {
    # New Zealand
    ("nz", "Auckland"): "auk",
    ("nz", "Wellington"): "wgn",
    ("nz", "Canterbury"): "can",
    ("nz", "Otago"): "ota",
    ("nz", "Waikato"): "wai",
    ("nz", "Bay of Plenty"): "bop",
    ("nz", "Hawke's Bay"): "hkb",
    ("nz", "Manawatu-Wanganui"): "mwt",
    ("nz", "Northland"): "ntl",
    ("nz", "Taranaki"): "tki",
    ("nz", "Southland"): "stl",
    ("nz", "Tasman"): "tas",
    ("nz", "Nelson"): "nsn",
    ("nz", "Marlborough"): "mbh",
    ("nz", "West Coast"): "wtc",
    ("nz", "Gisborne"): "gis",

    # Australia
    ("au", "New South Wales"): "nsw",
    ("au", "Queensland"): "qld",
    ("au", "Victoria"): "vic",
    ("au", "Western Australia"): "wa",
    ("au", "South Australia"): "sa",
    ("au", "Tasmania"): "tas",
    ("au", "Northern Territory"): "nt",
    ("au", "Australian Capital Territory"): "act",

    # United States (all 50 states)
    ("us", "Alabama"): "al",
    ("us", "Alaska"): "ak",
    ("us", "Arizona"): "az",
    ("us", "Arkansas"): "ar",
    ("us", "California"): "ca",
    ("us", "Colorado"): "co",
    ("us", "Connecticut"): "ct",
    ("us", "Delaware"): "de",
    ("us", "Florida"): "fl",
    ("us", "Georgia"): "ga",
    ("us", "Hawaii"): "hi",
    ("us", "Idaho"): "id",
    ("us", "Illinois"): "il",
    ("us", "Indiana"): "in",
    ("us", "Iowa"): "ia",
    ("us", "Kansas"): "ks",
    ("us", "Kentucky"): "ky",
    ("us", "Louisiana"): "la",
    ("us", "Maine"): "me",
    ("us", "Maryland"): "md",
    ("us", "Massachusetts"): "ma",
    ("us", "Michigan"): "mi",
    ("us", "Minnesota"): "mn",
    ("us", "Mississippi"): "ms",
    ("us", "Missouri"): "mo",
    ("us", "Montana"): "mt",
    ("us", "Nebraska"): "ne",
    ("us", "Nevada"): "nv",
    ("us", "New Hampshire"): "nh",
    ("us", "New Jersey"): "nj",
    ("us", "New Mexico"): "nm",
    ("us", "New York"): "ny",
    ("us", "North Carolina"): "nc",
    ("us", "North Dakota"): "nd",
    ("us", "Ohio"): "oh",
    ("us", "Oklahoma"): "ok",
    ("us", "Oregon"): "or",
    ("us", "Pennsylvania"): "pa",
    ("us", "Rhode Island"): "ri",
    ("us", "South Carolina"): "sc",
    ("us", "South Dakota"): "sd",
    ("us", "Tennessee"): "tn",
    ("us", "Texas"): "tx",
    ("us", "Utah"): "ut",
    ("us", "Vermont"): "vt",
    ("us", "Virginia"): "va",
    ("us", "Washington"): "wa",
    ("us", "West Virginia"): "wv",
    ("us", "Wisconsin"): "wi",
    ("us", "Wyoming"): "wy",
    ("us", "District of Columbia"): "dc",

    # United Kingdom
    ("gb", "England"): "eng",
    ("gb", "Scotland"): "sct",
    ("gb", "Wales"): "wls",
    ("gb", "Northern Ireland"): "nir",

    # Canada
    ("ca", "Ontario"): "on",
    ("ca", "Quebec"): "qc",
    ("ca", "British Columbia"): "bc",
    ("ca", "Alberta"): "ab",
    ("ca", "Manitoba"): "mb",
    ("ca", "Saskatchewan"): "sk",
    ("ca", "Nova Scotia"): "ns",
    ("ca", "New Brunswick"): "nb",
    ("ca", "Newfoundland and Labrador"): "nl",
    ("ca", "Prince Edward Island"): "pe",
    ("ca", "Northwest Territories"): "nt",
    ("ca", "Yukon"): "yt",
    ("ca", "Nunavut"): "nu",

    # Germany (example states)
    ("de", "Bavaria"): "by",
    ("de", "Berlin"): "be",
    ("de", "Hamburg"): "hh",
    ("de", "Hesse"): "he",
    ("de", "North Rhine-Westphalia"): "nw",
    ("de", "Saxony"): "sn",

    # Add more countries/subdivisions as needed
}

def get_country_code(country_name: str) -> str:
    """
    Convert country name to ISO 3166-1 alpha-2 code.
    Returns 'unknown' if not found.
    """
    if not country_name:
        return "unknown"
    return COUNTRY_NAME_TO_ISO.get(country_name, "unknown")

def get_subdivision_code(country_code: str, state_name: str) -> str:
    """
    Convert state/region name to ISO 3166-2 subdivision code.
    Returns 'unknown' if not found.
    """
    if not state_name or not country_code:
        return "unknown"
    return SUBDIVISION_NAME_TO_ISO.get((country_code, state_name), "unknown")

def get_iso_codes(country_name: str, state_name: str) -> tuple:
    """
    Get both country and subdivision ISO codes from names.
    Returns (country_code, subdivision_code).
    """
    country_code = get_country_code(country_name)
    subdivision_code = get_subdivision_code(country_code, state_name)
    return (country_code, subdivision_code)
```

---

### Task 2: Update Device ID Format for Meshtastic

**File:** `data_ingester_part1.py`

Meshtastic node IDs need to be prefixed to distinguish from WeSense native sensors:

**Current:**

```python
node_id = f"!{from_id:08x}"  # e.g., !a1b2c3d4
```

**New:**

```python
device_id = f"meshtastic_{from_id:08x}"  # e.g., meshtastic_a1b2c3d4
```

This ensures:

- Clear identification of data source
- No collision with WeSense native device IDs
- Consistent lowercase format

---

### Task 3: Update publish_to_skytrace Function

**File:** `data_ingester_part1.py`
**Current location:** Lines 2282-2303

**Current signature:**

```python
def publish_to_skytrace(region, node_id, reading_type, value, unit, timestamp):
    topic = f"skytrace/decoded/env/{node_id}/{reading_type}"
    # ...
```

**New signature:**

```python
def publish_to_wesense(device_id, reading_type, value, unit, timestamp,
                       latitude, longitude, country_code, subdivision_code,
                       data_source="MESHTASTIC", board_model="unknown"):
    """
    Publish normalized reading to WeSense MQTT topic.

    Topic format: wesense/v1/{country}/{subdivision}/{device_id}/{reading_type}
    """
    topic = f"wesense/v1/{country_code}/{subdivision_code}/{device_id}/{reading_type}"

    payload = {
        "value": value,
        "timestamp": timestamp,
        "device_id": device_id,
        "latitude": latitude,
        "longitude": longitude,
        "country": country_code,
        "subdivision": subdivision_code,
        "unit": unit,
        "data_source": data_source,
        "board_model": board_model,
        "location_source": "gps",
        "reading_type": reading_type
    }

    wesense_output_client.publish(topic, json.dumps(payload))
```

---

### Task 4: Update Telemetry Handler to Use Geocoded Location

**File:** `data_ingester_part1.py`
**Location:** Lines 2550-2585 (TELEMETRY_APP handler)

When publishing telemetry, use the geocoded country/state to get ISO codes:

```python
from utils.iso3166_mapper import get_iso_codes

# In telemetry handler, after getting position from cache:
if node_id in stats[region]['positions']:
    pos = stats[region]['positions'][node_id]
    lat = pos.get('latitude')
    lon = pos.get('longitude')

    # Get geocoded location (already cached)
    geo = geocoder.get_cached_location(lat, lon)
    if geo:
        country_code, subdivision_code = get_iso_codes(
            geo.get('country'),
            geo.get('state')
        )
    else:
        country_code = "unknown"
        subdivision_code = "unknown"

    # Generate device ID
    device_id = f"meshtastic_{from_id:08x}"

    # Publish with ISO codes
    if em.temperature != 0:
        publish_to_wesense(
            device_id=device_id,
            reading_type="temperature",
            value=em.temperature,
            unit="°C",
            timestamp=telemetry.time,
            latitude=lat,
            longitude=lon,
            country_code=country_code,
            subdivision_code=subdivision_code,
            data_source="MESHTASTIC",
            board_model=pos.get('hardware', 'unknown')
        )
```

---

### Task 5: Update InfluxDB Write Points

**File:** `data_ingester_part1.py`
**Location:** Lines 2306-2342

Add `country` and `subdivision` as tags for efficient querying:

**Current:**

```python
Point("environment")
    .tag("device_id", node_id)
    .tag("reading_type", reading_type)
    .tag("data_source", "MESHTASTIC")
    .tag("board_model", hardware)
    .tag("deployment_region", region)  # Meshtastic region code
    # ...
```

**New:**

```python
Point("environment")
    .tag("device_id", device_id)           # meshtastic_a1b2c3d4
    .tag("reading_type", reading_type)
    .tag("data_source", "MESHTASTIC")
    .tag("board_model", hardware)
    .tag("country", country_code)          # ISO 3166-1: nz, au, us
    .tag("subdivision", subdivision_code)  # ISO 3166-2: auk, qld, ca
    .field("value", value)
    .field("latitude", lat)
    .field("longitude", lon)
    # ... rest of fields
```

**Note:** Remove `deployment_region` tag (Meshtastic-specific) in favor of standard ISO codes.

---

### Task 6: Update Pending Telemetry Cache Structure

**File:** `data_ingester_part1.py`

When caching pending telemetry (readings that arrive before position), ensure the cache structure supports the new format:

```python
# Current cache entry
pending_telemetry[region][node_id] = [
    (reading_type, value, unit, timestamp),
    ...
]

# No change needed - ISO codes are derived from position when it arrives
# The geocoding happens when position is received, not when telemetry is cached
```

---

### Task 7: Handle Unknown Locations Gracefully

When geocoding fails or returns incomplete data:

```python
def get_iso_codes_safe(country_name: str, state_name: str) -> tuple:
    """
    Get ISO codes with fallback for unknown locations.
    """
    country_code = get_country_code(country_name)
    subdivision_code = get_subdivision_code(country_code, state_name)

    # Log unknown mappings for future addition
    if country_code == "unknown":
        logger.warning(f"Unknown country: {country_name}")
    if subdivision_code == "unknown" and country_code != "unknown":
        logger.warning(f"Unknown subdivision: {country_code}/{state_name}")

    return (country_code, subdivision_code)
```

**Topic for unknown locations:**

```
wesense/v1/unknown/unknown/meshtastic_a1b2c3d4/temperature
```

These can be filtered or flagged in the consumer for manual review.

---

### Task 8: Update Statistics and Logging

**File:** `data_ingester_part1.py`

Update logging to show ISO codes instead of Meshtastic region codes:

**Current:**

```python
logger.info(f"[{region}] Published temperature: {value}°C for {node_id}")
```

**New:**

```python
logger.info(f"[{country_code}/{subdivision_code}] Published temperature: {value}°C for {device_id}")
```

---

### Task 9: Add libp2p Pub/Sub Publishing (Future)

When libp2p integration is added, publish aggregates to partitioned topics:

```python
def publish_to_libp2p(country_code, subdivision_code, reading_type, aggregate_payload):
    """
    Publish aggregated readings to libp2p pub/sub topic.

    Topic format: /wesense/v1/live/{country}/{subdivision}/{reading_type}
    """
    topic = f"/wesense/v1/live/{country_code}/{subdivision_code}/{reading_type}"
    libp2p_client.publish(topic, json.dumps(aggregate_payload))
```

This is for Phase 2 when the aggregator component is built.

---

### Task 10: Create Unit Tests for ISO Mapper

**Create new file:** `tests/test_iso3166_mapper.py`

```python
import pytest
from utils.iso3166_mapper import get_country_code, get_subdivision_code, get_iso_codes

def test_known_country():
    assert get_country_code("New Zealand") == "nz"
    assert get_country_code("Australia") == "au"
    assert get_country_code("United States") == "us"

def test_unknown_country():
    assert get_country_code("Narnia") == "unknown"
    assert get_country_code("") == "unknown"
    assert get_country_code(None) == "unknown"

def test_known_subdivision():
    assert get_subdivision_code("nz", "Auckland") == "auk"
    assert get_subdivision_code("au", "Queensland") == "qld"
    assert get_subdivision_code("us", "California") == "ca"

def test_unknown_subdivision():
    assert get_subdivision_code("nz", "Mordor") == "unknown"
    assert get_subdivision_code("xx", "Auckland") == "unknown"

def test_combined_lookup():
    country, subdiv = get_iso_codes("New Zealand", "Auckland")
    assert country == "nz"
    assert subdiv == "auk"
```

---

## Testing Checklist

- [ ] ISO 3166 mapper returns correct codes for all test countries
- [ ] Unknown countries/subdivisions return "unknown" gracefully
- [ ] MQTT topics use new format: `wesense/v1/{country}/{subdivision}/{device_id}/{reading_type}`
- [ ] Device IDs prefixed with source: `meshtastic_a1b2c3d4`
- [ ] InfluxDB points include `country` and `subdivision` tags
- [ ] Pending telemetry still works (position arrives after readings)
- [ ] Geocoding cache integrates with ISO mapping
- [ ] Logging shows ISO codes clearly
- [ ] Test with real Meshtastic data from multiple regions

---

## Migration Notes

### Breaking Changes

1. **MQTT topic structure changed** - Subscribers must update
2. **Device ID format changed** - `!a1b2c3d4` → `meshtastic_a1b2c3d4`
3. **InfluxDB tags changed** - `deployment_region` → `country` + `subdivision`

### Backward Compatibility

Consider a transition period where both old and new topics are published:

```python
# Transition period: publish to both formats
publish_to_skytrace_legacy(region, node_id, reading_type, value, unit, timestamp)
publish_to_wesense(device_id, reading_type, value, unit, timestamp, ...)
```

### Downstream Updates Required

1. **wesense-respiro** - Update MQTT subscription to new topic format
2. **wesense-local-decoder** - Update topic parsing (if still used)
3. **InfluxDB queries** - Update to use `country`/`subdivision` tags instead of `deployment_region`
4. **Grafana dashboards** - Update queries for new tag names

---

## Future Enhancements

### Additional Data Sources

The same pattern can be used for other sources:

| Source         | Device ID Prefix | Example               |
| -------------- | ---------------- | --------------------- |
| Meshtastic     | `meshtastic_`    | `meshtastic_a1b2c3d4` |
| PurpleAir      | `purpleair_`     | `purpleair_12345`     |
| OpenAQ         | `openaq_`        | `openaq_us_ca_001`    |
| WeSense Native | (none)           | `office_301274c0e8fc` |

### Automatic Subdivision Detection

For sources that only provide coordinates (no region info), the geocoder already provides state/country which can be mapped to ISO codes automatically.

---

## References

- Architecture Document: `wesense-general-docs/Decentralised_Data_Commons_Architecture.md`
- Firmware TODO: `wesense-esp32_sensorarray_automatic/TODO_MQTT_TOPIC_UPDATE.md`
- ISO 3166-1 Country Codes: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
- ISO 3166-2 Subdivision Codes: https://en.wikipedia.org/wiki/ISO_3166-2
