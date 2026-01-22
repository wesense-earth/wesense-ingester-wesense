"""
Reverse Geocoding Utility
Converts lat/lon coordinates to location information using GeoNames (offline)

Uses the reverse_geocoder library which provides a local GeoNames database.
This ensures consistent results across all ingesters and doesn't require
rate limiting or network access.
"""

import logging
from typing import Optional, Dict, Any

try:
    import reverse_geocoder as rg
    RG_AVAILABLE = True
except ImportError:
    RG_AVAILABLE = False
    print("Warning: reverse_geocoder not available. Install with: pip install reverse_geocoder==1.5.1")

logger = logging.getLogger(__name__)

# Country code to name mapping (consistent with meshtastic ingester)
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


class ReverseGeocoder:
    """
    Reverse geocoder using GeoNames database (offline).

    Uses the reverse_geocoder library which downloads and caches
    the GeoNames database locally on first use.
    """

    def __init__(self):
        if not RG_AVAILABLE:
            raise ImportError("reverse_geocoder library not available")

        # Pre-warm the GeoNames database (loads on first query)
        logger.info("Initializing GeoNames reverse geocoder...")

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
        """
        Reverse geocode coordinates to location information.

        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees

        Returns:
            Dict with location info:
                - city: City/town name
                - admin1: State/province/region (admin1 level)
                - country: Full country name
                - country_code: ISO 2-letter country code (e.g., 'NZ')
            Returns None if geocoding fails
        """
        if latitude is None or longitude is None:
            return None

        # Validate coordinates
        if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
            logger.warning(f"Invalid coordinates: {latitude}, {longitude}")
            return None

        try:
            # GeoNames lookup (offline, instant)
            results = rg.search([(latitude, longitude)], mode=1)  # mode=1 for single-threaded

            if not results or len(results) == 0:
                logger.warning(f"No GeoNames result for {latitude}, {longitude}")
                return None

            result = results[0]
            country_code = result.get('cc', '')

            return {
                'city': result.get('name'),
                'admin1': result.get('admin1'),  # State/province name
                'country': self._get_country_name(country_code),
                'country_code': country_code,
            }

        except Exception as e:
            logger.error(f"Geocoding error for {latitude}, {longitude}: {e}")
            return None

    def _get_country_name(self, country_code: Optional[str]) -> Optional[str]:
        """Convert ISO country code to full country name"""
        if not country_code:
            return None
        return COUNTRY_NAMES.get(country_code, country_code)

    def format_subdivision_code(self, admin1: Optional[str]) -> str:
        """
        Format admin1 name as a URL-safe subdivision code.

        Examples:
            'Auckland' -> 'auckland'
            'New South Wales' -> 'new-south-wales'
            'Subcarpathian Voivodeship' -> 'subcarpathian-voivodeship'
        """
        if not admin1:
            return 'unknown'
        return admin1.lower().replace(' ', '-').replace("'", '')
