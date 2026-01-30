#!/usr/bin/env python3
"""
Test geocoding functionality
"""

import asyncio
from utils.geocoder import get_geocoder

async def test_geocoding():
    geocoder = get_geocoder()
    
    # Test coordinates (Auckland, NZ)
    test_coords = [
        (-36.848, 174.763, "Auckland CBD"),
        (-37.787, 175.279, "Hamilton"),
        (-43.532, 172.636, "Christchurch"),
    ]
    
    print("Testing reverse geocoding...\n")
    
    for lat, lon, expected in test_coords:
        print(f"Testing: {expected} ({lat}, {lon})")
        result = await geocoder.reverse_geocode(lat, lon)
        
        if result:
            location_str = geocoder.format_location_string(result)
            print(f"  ✓ Result: {location_str}")
            print(f"  - Locality: {result.get('locality')}")
            print(f"  - City: {result.get('city')}")
            print(f"  - Country: {result.get('country')}")
            print(f"  - Admin1: {result.get('admin1')}")
            print(f"  - Country Code: {result.get('country_code')}")
        else:
            print(f"  ✗ Failed to geocode")
        
        print()
    
    # Test cache
    print("Testing cache...")
    result = await geocoder.reverse_geocode(-36.848, 174.763)
    print(f"  ✓ Cache hit: {result is not None}")
    
    await geocoder.close()

if __name__ == "__main__":
    asyncio.run(test_geocoding())
