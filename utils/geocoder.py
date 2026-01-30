#!/usr/bin/env python3
"""
Hybrid Reverse Geocoding Module

Combines two data sources for accurate location naming:
- GeoNames (via reverse_geocoder): Reliable city/admin1/country data
- Nominatim (OpenStreetMap): Local area detail (suburb/neighbourhood)

This solves the problem where Nominatim's "city" field often returns
administrative subdivisions (like local boards) rather than actual city names.
"""

import aiohttp
import asyncio
import json
import os
import reverse_geocoder as rg
from datetime import datetime
from typing import Optional, Dict, Tuple


class ReverseGeocoder:
    def __init__(self, cache_file='cache/geocoding_cache.json', user_agent='WeSense/1.0'):
        self.cache_file = cache_file
        self.user_agent = user_agent
        self.cache: Dict[Tuple[float, float], Dict] = {}
        self.last_request_time = 0
        self.min_request_interval = 1.0  # Nominatim requires 1 req/sec max
        self.session: Optional[aiohttp.ClientSession] = None

        # Load cache
        self._load_cache()

        # Ensure cache directory exists
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)

    def _load_cache(self):
        """Load geocoding cache from disk"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    # Convert string keys back to tuple
                    self.cache = {
                        (float(k.split(',')[0]), float(k.split(',')[1])): v
                        for k, v in data.items()
                    }
                print(f"Loaded {len(self.cache)} cached geocoding entries")
            except Exception as e:
                print(f"Warning: Could not load geocoding cache: {e}")
                self.cache = {}

    def _save_cache(self):
        """Save geocoding cache to disk"""
        try:
            # Convert tuple keys to strings for JSON
            data = {f"{lat},{lon}": v for (lat, lon), v in self.cache.items()}
            with open(self.cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save geocoding cache: {e}")

    def _round_coordinates(self, lat: float, lon: float, precision: int = 3) -> Tuple[float, float]:
        """Round coordinates to reduce cache size (3 decimals ≈ 100m precision)"""
        return (round(lat, precision), round(lon, precision))

    def _get_geonames_data(self, lat: float, lon: float) -> Dict:
        """
        Get city/admin/country data from GeoNames (offline, instant).
        Returns dict with: name, admin1, admin2, cc
        """
        try:
            results = rg.search([(lat, lon)], mode=1)  # mode=1 for single-threaded
            if results and len(results) > 0:
                return results[0]
        except Exception as e:
            print(f"GeoNames lookup error for ({lat}, {lon}): {e}")
        return {}

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def _get_nominatim_locality(self, lat: float, lon: float) -> Optional[str]:
        """
        Get local area detail from Nominatim (suburb/neighbourhood/local area).
        This provides finer detail than GeoNames city-level data.
        """
        # Rate limiting
        now = asyncio.get_event_loop().time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)

        try:
            await self._ensure_session()

            url = 'https://nominatim.openstreetmap.org/reverse'
            params = {
                'lat': lat,
                'lon': lon,
                'format': 'json',
                'addressdetails': 1,
                'accept-language': 'en'
            }
            headers = {
                'User-Agent': self.user_agent
            }

            async with self.session.get(url, params=params, headers=headers) as response:
                self.last_request_time = asyncio.get_event_loop().time()

                if response.status == 200:
                    data = await response.json()
                    address = data.get('address', {})

                    # Extract local area - try most specific first
                    # This is what Nominatim calls "city" but is often a local board/ward
                    locality = (
                        address.get('suburb') or
                        address.get('neighbourhood') or
                        address.get('city') or
                        address.get('town') or
                        address.get('village') or
                        address.get('hamlet')
                    )
                    return locality
                else:
                    print(f"Nominatim API error: HTTP {response.status}")
                    return None

        except Exception as e:
            print(f"Nominatim error for ({lat}, {lon}): {e}")
            return None

    async def reverse_geocode(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Reverse geocode coordinates using hybrid approach.

        Returns dict with:
            'locality': str   - Local area (suburb/neighbourhood) from Nominatim
            'city': str       - Actual city name from GeoNames
            'country': str    - Country name from GeoNames
            'country_code': str - ISO 2-letter country code
            'admin1': str     - State/region from GeoNames (for ISO subdivision lookup)
            'cached_at': int  - Cache timestamp
        """
        # Round coordinates for caching
        cache_key = self._round_coordinates(lat, lon)

        # Check cache first
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Get city/country from GeoNames (offline, instant)
        geonames = self._get_geonames_data(lat, lon)

        # Get local area from Nominatim (online, rate-limited)
        locality = await self._get_nominatim_locality(lat, lon)

        # Build result combining both sources
        result = {
            'locality': locality,
            'city': geonames.get('name'),
            'country': self._get_country_name(geonames.get('cc')),
            'country_code': geonames.get('cc'),
            'admin1': geonames.get('admin1'),
            'cached_at': int(datetime.now().timestamp())
        }

        # Cache the result
        self.cache[cache_key] = result
        self._save_cache()

        return result

    def _get_country_name(self, country_code: Optional[str]) -> Optional[str]:
        """Convert ISO country code to full country name"""
        if not country_code:
            return None

        # Common country code mappings
        countries = {
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
        return countries.get(country_code, country_code)

    async def geocode_batch(self, coordinates: list) -> Dict[Tuple[float, float], Optional[Dict]]:
        """
        Geocode multiple coordinates efficiently
        Returns dict mapping (lat, lon) -> location data
        """
        results = {}

        for lat, lon in coordinates:
            result = await self.reverse_geocode(lat, lon)
            results[(lat, lon)] = result

        return results

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()

    def get_cached_location(self, lat: float, lon: float) -> Optional[Dict]:
        """Get location from cache only (no API call)"""
        cache_key = self._round_coordinates(lat, lon)
        return self.cache.get(cache_key)

    def format_location_string(self, location_data: Optional[Dict]) -> str:
        """Format location data into a readable string: Locality, City, Country"""
        if not location_data:
            return "Unknown"

        parts = []
        if location_data.get('locality'):
            parts.append(location_data['locality'])
        if location_data.get('city'):
            parts.append(location_data['city'])
        if location_data.get('country'):
            parts.append(location_data['country'])

        return ', '.join(parts) if parts else "Unknown"


# Global instance
_geocoder_instance = None

def get_geocoder() -> ReverseGeocoder:
    """Get or create global geocoder instance"""
    global _geocoder_instance
    if _geocoder_instance is None:
        _geocoder_instance = ReverseGeocoder()
    return _geocoder_instance
