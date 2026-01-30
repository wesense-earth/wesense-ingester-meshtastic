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
