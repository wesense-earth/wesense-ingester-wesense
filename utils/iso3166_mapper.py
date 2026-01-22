"""
ISO 3166 Mapper
Converts country and state/region names to ISO 3166-1 alpha-2 and ISO 3166-2 codes
"""

# Country name to ISO 3166-1 alpha-2
COUNTRY_NAME_TO_ISO = {
    "New Zealand": "nz",
    "Australia": "au",
    "United States": "us",
    "United Kingdom": "uk",
    "Canada": "ca",
}

# (country_code, state/region name) to subdivision code
SUBDIVISION_NAME_TO_ISO = {
    # New Zealand regions
    ("nz", "Auckland"): "auk",
    ("nz", "Auckland Region"): "auk",
    ("nz", "Bay of Plenty"): "bop",
    ("nz", "Bay of Plenty Region"): "bop",
    ("nz", "Canterbury"): "can",
    ("nz", "Canterbury Region"): "can",
    ("nz", "Gisborne"): "gis",
    ("nz", "Gisborne District"): "gis",
    ("nz", "Hawke's Bay"): "hkb",
    ("nz", "Hawke's Bay Region"): "hkb",
    ("nz", "Manawatū-Whanganui"): "mwt",
    ("nz", "Manawatu-Wanganui"): "mwt",
    ("nz", "Marlborough"): "mbh",
    ("nz", "Marlborough Region"): "mbh",
    ("nz", "Nelson"): "nsn",
    ("nz", "Nelson Region"): "nsn",
    ("nz", "Northland"): "ntl",
    ("nz", "Northland Region"): "ntl",
    ("nz", "Otago"): "ota",
    ("nz", "Otago Region"): "ota",
    ("nz", "Southland"): "stl",
    ("nz", "Southland Region"): "stl",
    ("nz", "Taranaki"): "tki",
    ("nz", "Taranaki Region"): "tki",
    ("nz", "Tasman"): "tas",
    ("nz", "Tasman District"): "tas",
    ("nz", "Waikato"): "wko",
    ("nz", "Waikato Region"): "wko",
    ("nz", "Wellington"): "wgn",
    ("nz", "Wellington Region"): "wgn",
    ("nz", "West Coast"): "wtc",
    ("nz", "West Coast Region"): "wtc",

    # Australia states
    ("au", "New South Wales"): "nsw",
    ("au", "Victoria"): "vic",
    ("au", "Queensland"): "qld",
    ("au", "Western Australia"): "wa",
    ("au", "South Australia"): "sa",
    ("au", "Tasmania"): "tas",
    ("au", "Australian Capital Territory"): "act",
    ("au", "Northern Territory"): "nt",

    # US states (common ones)
    ("us", "California"): "ca",
    ("us", "New York"): "ny",
    ("us", "Texas"): "tx",
    ("us", "Florida"): "fl",
    ("us", "Washington"): "wa",
    ("us", "Oregon"): "or",
    ("us", "Colorado"): "co",
    ("us", "Massachusetts"): "ma",

    # Canada provinces
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
}


def get_country_code(country_name: str) -> str:
    """
    Convert country name to ISO 3166-1 alpha-2 code

    Args:
        country_name: Full country name

    Returns:
        Two-letter country code, or "unknown" if not found
    """
    if not country_name:
        return "unknown"

    return COUNTRY_NAME_TO_ISO.get(country_name, "unknown")


def get_subdivision_code(country_code: str, state_name: str) -> str:
    """
    Convert state/region name to subdivision code

    Args:
        country_code: Two-letter country code
        state_name: State or region name

    Returns:
        Subdivision code, or "unknown" if not found
    """
    if not country_code or not state_name:
        return "unknown"

    country_code = country_code.lower()

    # Try exact match first
    key = (country_code, state_name)
    if key in SUBDIVISION_NAME_TO_ISO:
        return SUBDIVISION_NAME_TO_ISO[key]

    # Try case-insensitive match
    for (cc, sn), code in SUBDIVISION_NAME_TO_ISO.items():
        if cc == country_code and sn.lower() == state_name.lower():
            return code

    return "unknown"


def get_iso_codes(country_name: str, state_name: str) -> tuple:
    """
    Get both country and subdivision ISO codes from names

    Args:
        country_name: Full country name
        state_name: State or region name

    Returns:
        Tuple of (country_code, subdivision_code)
    """
    country_code = get_country_code(country_name)
    subdivision_code = get_subdivision_code(country_code, state_name)

    return (country_code, subdivision_code)
