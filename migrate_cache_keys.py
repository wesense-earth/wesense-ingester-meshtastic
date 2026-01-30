#!/usr/bin/env python3
"""
Migrate cache files from old device ID format to new WeSense v1 format

Old format: !a1b2c3d4
New format: meshtastic_a1b2c3d4

This script converts all cache files in the cache directory.
Run this ONCE before starting the new data ingester.
"""

import json
import os
import sys
from pathlib import Path
import shutil
from datetime import datetime

def convert_device_id(old_id):
    """
    Convert old format (!a1b2c3d4) to new format (meshtastic_a1b2c3d4)
    """
    if old_id.startswith('!'):
        # Remove ! and add meshtastic_ prefix
        return f"meshtastic_{old_id[1:]}"
    elif old_id.startswith('meshtastic_'):
        # Already converted
        return old_id
    else:
        # Unknown format, leave as-is
        print(f"  ⚠️  Warning: Unknown device ID format: {old_id}")
        return old_id

def migrate_cache_file(cache_file_path):
    """
    Migrate a single cache file
    """
    print(f"\nProcessing: {cache_file_path.name}")

    # Create backup
    backup_path = cache_file_path.with_suffix('.json.backup')
    shutil.copy2(cache_file_path, backup_path)
    print(f"  ✓ Backup created: {backup_path.name}")

    # Load cache
    try:
        with open(cache_file_path, 'r') as f:
            cache_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  ✗ Error reading JSON: {e}")
        return False

    # Convert keys
    converted_data = {}
    conversion_count = 0

    for old_key, value in cache_data.items():
        new_key = convert_device_id(old_key)
        converted_data[new_key] = value

        if old_key != new_key:
            conversion_count += 1
            print(f"  {old_key} → {new_key}")

    # Save converted data
    with open(cache_file_path, 'w') as f:
        json.dump(converted_data, f, indent=2)

    print(f"  ✓ Converted {conversion_count} device IDs")
    print(f"  ✓ Saved to: {cache_file_path.name}")

    return True

def migrate_pending_telemetry_file(cache_file_path):
    """
    Migrate pending telemetry cache file
    Format: {region: {node_id: [(reading_type, value, unit, timestamp), ...]}}
    """
    print(f"\nProcessing: {cache_file_path.name}")

    # Create backup
    backup_path = cache_file_path.with_suffix('.json.backup')
    shutil.copy2(cache_file_path, backup_path)
    print(f"  ✓ Backup created: {backup_path.name}")

    # Load cache
    try:
        with open(cache_file_path, 'r') as f:
            cache_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  ✗ Error reading JSON: {e}")
        return False

    # Convert keys (nested structure)
    converted_data = {}
    total_conversions = 0

    for region, nodes in cache_data.items():
        converted_data[region] = {}
        conversion_count = 0

        for old_key, telemetry_list in nodes.items():
            new_key = convert_device_id(old_key)
            converted_data[region][new_key] = telemetry_list

            if old_key != new_key:
                conversion_count += 1
                total_conversions += 1

        if conversion_count > 0:
            print(f"  {region}: Converted {conversion_count} device IDs")

    # Save converted data
    with open(cache_file_path, 'w') as f:
        json.dump(converted_data, f, indent=2)

    print(f"  ✓ Total converted: {total_conversions} device IDs")
    print(f"  ✓ Saved to: {cache_file_path.name}")

    return True

def main():
    print("=" * 60)
    print("WeSense Data Ingester - Cache Migration Tool")
    print("=" * 60)
    print()
    print("This script converts device IDs in cache files from:")
    print("  Old format: !a1b2c3d4")
    print("  New format: meshtastic_a1b2c3d4")
    print()

    # Find cache directory
    cache_dir = Path(__file__).parent / 'cache'

    if not cache_dir.exists():
        print(f"✗ Error: Cache directory not found: {cache_dir}")
        print()
        print("Please run this script from the data-ingester directory")
        print("or specify the cache directory as an argument:")
        print(f"  {sys.argv[0]} /path/to/cache")
        sys.exit(1)

    # Check if custom cache directory specified
    if len(sys.argv) > 1:
        cache_dir = Path(sys.argv[1])
        if not cache_dir.exists():
            print(f"✗ Error: Specified cache directory not found: {cache_dir}")
            sys.exit(1)

    print(f"Cache directory: {cache_dir}")
    print()

    # Find all cache files
    position_cache_files = list(cache_dir.glob('meshtastic_cache_*.json'))
    pending_telemetry_files = list(cache_dir.glob('pending_telemetry_*.json'))

    if not position_cache_files and not pending_telemetry_files:
        print("✓ No cache files found - nothing to migrate")
        sys.exit(0)

    print(f"Found {len(position_cache_files)} position cache file(s)")
    print(f"Found {len(pending_telemetry_files)} pending telemetry file(s)")
    print()

    # Ask for confirmation
    response = input("Proceed with migration? Backups will be created. [y/N]: ")
    if response.lower() != 'y':
        print("Migration cancelled")
        sys.exit(0)

    print()
    print("Starting migration...")
    print("-" * 60)

    success_count = 0
    error_count = 0

    # Migrate position cache files
    for cache_file in position_cache_files:
        try:
            if migrate_cache_file(cache_file):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"  ✗ Error: {e}")
            error_count += 1

    # Migrate pending telemetry files
    for cache_file in pending_telemetry_files:
        try:
            if migrate_pending_telemetry_file(cache_file):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"  ✗ Error: {e}")
            error_count += 1

    # Summary
    print()
    print("=" * 60)
    print("Migration Complete!")
    print("=" * 60)
    print(f"  Successfully migrated: {success_count} file(s)")
    print(f"  Errors: {error_count} file(s)")
    print()

    if success_count > 0:
        print("✓ Cache files have been converted to WeSense v1 format")
        print("✓ Backups saved with .backup extension")
        print()
        print("You can now start the WeSense data ingester:")
        print("  docker run -d --name=wesense-data-ingester ...")
        print()
        print("If you need to rollback, restore from .backup files:")
        for cache_file in cache_dir.glob('*.json.backup'):
            original = cache_file.with_suffix('')
            print(f"  mv {cache_file} {original}")

    sys.exit(0 if error_count == 0 else 1)

if __name__ == '__main__':
    main()
