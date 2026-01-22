"""
ISO 3166 Reverse Mapper
Converts ISO 3166-1 alpha-2 country codes and ISO 3166-2 subdivision codes
back to human-readable names for display purposes.
"""

# ISO 3166-1 alpha-2 to country name
ISO_TO_COUNTRY_NAME = {
    "nz": "New Zealand",
    "au": "Australia",
    "us": "United States",
    "uk": "United Kingdom",
    "ca": "Canada",
}

# ISO 3166-2 subdivision code to subdivision name (by country)
ISO_TO_SUBDIVISION_NAME = {
    # New Zealand regions
    "nz": {
        "auk": "Auckland",
        "bop": "Bay of Plenty",
        "can": "Canterbury",
        "gis": "Gisborne",
        "hkb": "Hawke's Bay",
        "mwt": "Manawatū-Whanganui",
        "mbh": "Marlborough",
        "nsn": "Nelson",
        "ntl": "Northland",
        "ota": "Otago",
        "stl": "Southland",
        "tki": "Taranaki",
        "tas": "Tasman",
        "wko": "Waikato",
        "wgn": "Wellington",
        "wtc": "West Coast",
    },
    # Australia states
    "au": {
        "nsw": "New South Wales",
        "vic": "Victoria",
        "qld": "Queensland",
        "wa": "Western Australia",
        "sa": "South Australia",
        "tas": "Tasmania",
        "act": "Australian Capital Territory",
        "nt": "Northern Territory",
    },
    # US states (common ones)
    "us": {
        "ca": "California",
        "ny": "New York",
        "tx": "Texas",
        "fl": "Florida",
        "wa": "Washington",
        "or": "Oregon",
        "co": "Colorado",
        "ma": "Massachusetts",
    },
}


def get_country_name(country_code: str) -> str:
    """
    Convert ISO 3166-1 alpha-2 country code to full country name.

    Args:
        country_code: Two-letter country code (e.g., 'nz', 'au')

    Returns:
        Full country name, or the code itself if not found
    """
    if not country_code:
        return None

    return ISO_TO_COUNTRY_NAME.get(country_code.lower(), country_code.upper())


def get_subdivision_name(country_code: str, subdivision_code: str) -> str:
    """
    Convert ISO 3166-2 subdivision code to full subdivision name.

    Args:
        country_code: Two-letter country code (e.g., 'nz')
        subdivision_code: Subdivision code (e.g., 'auk')

    Returns:
        Full subdivision name, or the code itself if not found
    """
    if not country_code or not subdivision_code:
        return None

    country_code = country_code.lower()
    subdivision_code = subdivision_code.lower()

    if country_code in ISO_TO_SUBDIVISION_NAME:
        return ISO_TO_SUBDIVISION_NAME[country_code].get(
            subdivision_code,
            subdivision_code.upper()
        )

    return subdivision_code.upper()


def get_location_names(country_code: str, subdivision_code: str) -> tuple:
    """
    Get human-readable location names from ISO codes.

    Args:
        country_code: Two-letter country code
        subdivision_code: Subdivision code

    Returns:
        Tuple of (country_name, subdivision_name)
    """
    country_name = get_country_name(country_code)
    subdivision_name = get_subdivision_name(country_code, subdivision_code)

    return (country_name, subdivision_name)
