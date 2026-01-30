#!/usr/bin/env python3
"""
Migrate geocoding cache from old format to new format.

Old format:
{
    "city": "Auckland",
    "suburb": "City Centre",
    "state": "Auckland",
    "country": "New Zealand",
    "display_name": "...",
    "cached_at": 1761982177
}

New format:
{
    "locality": "City Centre",
    "city": "Auckland",
    "country": "New Zealand",
    "country_code": "NZ",
    "admin1": "Auckland",
    "cached_at": 1761982177
}

Uses reverse_geocoder library to get country codes from coordinates.
"""

import json
import sys

try:
    import reverse_geocoder as rg
    RG_AVAILABLE = True
except ImportError:
    print("Warning: reverse_geocoder not available, will skip country_code lookup")
    RG_AVAILABLE = False


def get_country_code_from_coords(lat, lon):
    """Use reverse_geocoder to get country code from coordinates"""
    if not RG_AVAILABLE:
        return None
    try:
        results = rg.search([(lat, lon)], mode=1)
        if results and len(results) > 0:
            return results[0].get('cc')
    except Exception as e:
        print(f"  Warning: reverse_geocoder failed for ({lat}, {lon}): {e}")
    return None


def migrate_entry(key, old_entry):
    """Convert old cache entry to new format"""
    # Parse coordinates from key
    try:
        lat, lon = map(float, key.split(','))
        country_code = get_country_code_from_coords(lat, lon)
    except:
        country_code = None

    return {
        'locality': old_entry.get('suburb'),
        'city': old_entry.get('city'),
        'country': old_entry.get('country'),
        'country_code': country_code,
        'admin1': old_entry.get('state'),
        'cached_at': old_entry.get('cached_at')
    }


def is_old_format(entry):
    """Check if entry is in old format"""
    # Old format has 'state' and 'suburb', new format has 'admin1' and 'locality'
    return 'state' in entry or 'suburb' in entry


def migrate_cache(input_file, output_file=None):
    """Migrate entire cache file"""
    if output_file is None:
        output_file = input_file

    with open(input_file, 'r') as f:
        cache = json.load(f)

    migrated = 0
    skipped = 0

    new_cache = {}
    total = len(cache)
    for i, (key, entry) in enumerate(cache.items()):
        if is_old_format(entry):
            new_entry = migrate_entry(key, entry)
            new_cache[key] = new_entry
            migrated += 1
            if (migrated % 100) == 0:
                print(f"  Progress: {migrated}/{total} entries migrated...")
        else:
            # Already in new format
            new_cache[key] = entry
            skipped += 1

    # Backup original
    if output_file == input_file:
        backup_file = input_file + '.backup'
        with open(backup_file, 'w') as f:
            json.dump(cache, f, indent=2)
        print(f"Backed up original to {backup_file}")

    # Write migrated cache
    with open(output_file, 'w') as f:
        json.dump(new_cache, f, indent=2)

    print(f"Migration complete:")
    print(f"  Migrated: {migrated}")
    print(f"  Already new format: {skipped}")
    print(f"  Total entries: {len(new_cache)}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python migrate_geocache.py <cache_file> [output_file]")
        print("If output_file is not specified, input file will be overwritten (with backup)")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    migrate_cache(input_file, output_file)
