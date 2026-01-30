#!/usr/bin/env python3
"""
Migrate Geocoding Cache to New Hybrid Format

Converts old cache format (Nominatim-only) to new hybrid format (GeoNames + Nominatim).

Old format:
{
    "city": "Waitematā",        # Actually a local board, not a city
    "suburb": "City Centre",
    "state": "Auckland",
    "country": "New Zealand",
    "display_name": "...",
    "cached_at": timestamp
}

New format:
{
    "locality": "Waitematā",    # Local area from old city/suburb
    "city": "Auckland",         # Actual city from GeoNames
    "country": "New Zealand",
    "country_code": "NZ",
    "admin1": "Auckland",       # State/region from GeoNames
    "cached_at": timestamp
}
"""

import json
import os
import shutil
from datetime import datetime

# Import reverse_geocoder for GeoNames lookups
try:
    import reverse_geocoder as rg
    print("Loading GeoNames data (this may take a moment on first run)...")
    # Warm up the library by doing a test lookup
    rg.search([(-36.848, 174.763)], mode=1)
    print("GeoNames data loaded.\n")
except ImportError:
    print("ERROR: reverse_geocoder not installed.")
    print("Run: pip install reverse_geocoder==1.5.1")
    exit(1)


# Country code to name mapping
COUNTRY_NAMES = {
    'NZ': 'New Zealand',
    'AU': 'Australia',
    'US': 'United States',
    'GB': 'United Kingdom',
    'DE': 'Germany',
    'FR': 'France',
    'NL': 'Netherlands',
    'CA': 'Canada',
    'JP': 'Japan',
    'CN': 'China',
    'IN': 'India',
    'BR': 'Brazil',
    'MX': 'Mexico',
    'ES': 'Spain',
    'IT': 'Italy',
    'PL': 'Poland',
    'SE': 'Sweden',
    'NO': 'Norway',
    'DK': 'Denmark',
    'FI': 'Finland',
    'CH': 'Switzerland',
    'AT': 'Austria',
    'BE': 'Belgium',
    'IE': 'Ireland',
    'PT': 'Portugal',
    'GR': 'Greece',
    'CZ': 'Czechia',
    'RU': 'Russia',
    'UA': 'Ukraine',
    'ZA': 'South Africa',
    'SG': 'Singapore',
    'MY': 'Malaysia',
    'TH': 'Thailand',
    'ID': 'Indonesia',
    'PH': 'Philippines',
    'VN': 'Vietnam',
    'KR': 'South Korea',
    'TW': 'Taiwan',
    'HK': 'Hong Kong',
    'AE': 'United Arab Emirates',
    'SA': 'Saudi Arabia',
    'IL': 'Israel',
    'EG': 'Egypt',
    'AR': 'Argentina',
    'CL': 'Chile',
    'CO': 'Colombia',
    'PE': 'Peru',
}


def get_country_name(country_code):
    """Convert ISO country code to full country name"""
    if not country_code:
        return None
    return COUNTRY_NAMES.get(country_code, country_code)


def is_old_format(entry):
    """Check if cache entry is in old format"""
    # Old format has 'state' and/or 'display_name', new format has 'admin1' and 'country_code'
    return 'state' in entry or 'display_name' in entry or 'suburb' in entry


def is_new_format(entry):
    """Check if cache entry is already in new format"""
    return 'admin1' in entry and 'country_code' in entry


def migrate_entry(coord_key, old_entry):
    """
    Migrate a single cache entry from old to new format.

    Args:
        coord_key: String key like "-36.848,174.763"
        old_entry: Dict with old format fields

    Returns:
        Dict with new format fields
    """
    # Parse coordinates from key
    lat, lon = map(float, coord_key.split(','))

    # Get GeoNames data for accurate city/admin1/country
    try:
        results = rg.search([(lat, lon)], mode=1)
        geonames = results[0] if results else {}
    except Exception as e:
        print(f"  Warning: GeoNames lookup failed for {coord_key}: {e}")
        geonames = {}

    # Determine locality from old data
    # Prefer suburb if available, otherwise use old "city" (which was often a local board)
    locality = old_entry.get('suburb') or old_entry.get('city')

    # Build new format entry
    new_entry = {
        'locality': locality,
        'city': geonames.get('name'),
        'country': get_country_name(geonames.get('cc')) or old_entry.get('country'),
        'country_code': geonames.get('cc'),
        'admin1': geonames.get('admin1'),
        'cached_at': old_entry.get('cached_at', int(datetime.now().timestamp()))
    }

    return new_entry


def migrate_cache(cache_file='cache/geocoding_cache.json', dry_run=False):
    """
    Migrate the geocoding cache to new format.

    Args:
        cache_file: Path to cache file
        dry_run: If True, don't write changes, just show what would happen
    """
    print("=" * 60)
    print("Geocoding Cache Migration")
    print("=" * 60)
    print()

    if not os.path.exists(cache_file):
        print(f"Cache file not found: {cache_file}")
        return

    # Load existing cache
    print(f"Loading cache from {cache_file}...")
    with open(cache_file, 'r') as f:
        cache = json.load(f)

    total = len(cache)
    print(f"Found {total} entries\n")

    # Analyze cache
    old_format_count = 0
    new_format_count = 0
    unknown_format_count = 0

    for key, entry in cache.items():
        if is_new_format(entry):
            new_format_count += 1
        elif is_old_format(entry):
            old_format_count += 1
        else:
            unknown_format_count += 1

    print(f"Cache analysis:")
    print(f"  Already migrated (new format): {new_format_count}")
    print(f"  Need migration (old format):   {old_format_count}")
    print(f"  Unknown format:                {unknown_format_count}")
    print()

    if old_format_count == 0:
        print("Nothing to migrate - cache is already in new format!")
        return

    if dry_run:
        print("DRY RUN - showing first 5 entries that would be migrated:\n")
        count = 0
        for key, entry in cache.items():
            if is_old_format(entry) and count < 5:
                new_entry = migrate_entry(key, entry)
                print(f"  {key}:")
                print(f"    OLD: city={entry.get('city')}, state={entry.get('state')}, country={entry.get('country')}")
                print(f"    NEW: locality={new_entry.get('locality')}, city={new_entry.get('city')}, country={new_entry.get('country')}")
                print()
                count += 1
        print("Run without --dry-run to apply migration.")
        return

    # Backup original cache
    backup_file = cache_file + '.backup.' + datetime.now().strftime('%Y%m%d_%H%M%S')
    print(f"Creating backup: {backup_file}")
    shutil.copy2(cache_file, backup_file)

    # Migrate entries
    print(f"\nMigrating {old_format_count} entries...")
    migrated = 0
    errors = 0

    for key, entry in cache.items():
        if is_old_format(entry):
            try:
                cache[key] = migrate_entry(key, entry)
                migrated += 1

                # Progress indicator
                if migrated % 100 == 0:
                    print(f"  Migrated {migrated}/{old_format_count}...")

            except Exception as e:
                print(f"  Error migrating {key}: {e}")
                errors += 1

    # Save migrated cache
    print(f"\nSaving migrated cache...")
    with open(cache_file, 'w') as f:
        json.dump(cache, f, indent=2)

    print()
    print("=" * 60)
    print("Migration complete!")
    print(f"  Total entries:    {total}")
    print(f"  Migrated:         {migrated}")
    print(f"  Errors:           {errors}")
    print(f"  Already new:      {new_format_count}")
    print(f"  Backup saved to:  {backup_file}")
    print("=" * 60)


def show_sample(cache_file='cache/geocoding_cache.json', count=5):
    """Show sample entries from the cache"""
    if not os.path.exists(cache_file):
        print(f"Cache file not found: {cache_file}")
        return

    with open(cache_file, 'r') as f:
        cache = json.load(f)

    print(f"Sample of {count} entries from cache:\n")
    for i, (key, entry) in enumerate(cache.items()):
        if i >= count:
            break
        print(f"{key}:")
        for k, v in entry.items():
            print(f"  {k}: {v}")
        print()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Migrate geocoding cache to new hybrid format")
    parser.add_argument('--cache', default='cache/geocoding_cache.json', help='Path to cache file')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without making changes')
    parser.add_argument('--sample', action='store_true', help='Show sample entries from cache')
    parser.add_argument('--count', type=int, default=5, help='Number of sample entries to show')

    args = parser.parse_args()

    if args.sample:
        show_sample(args.cache, args.count)
    else:
        migrate_cache(args.cache, args.dry_run)
