"""Complete ISO 3166-1 alpha-2 country code mapping."""

# Comprehensive country code to full name mapping
# Includes all major markets and safari source countries
COUNTRY_CODES = {
    # Africa
    "ZA": "South Africa",
    "KE": "Kenya",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "RW": "Rwanda",
    "ET": "Ethiopia",
    "GH": "Ghana",
    "NG": "Nigeria",
    "EG": "Egypt",
    "MA": "Morocco",
    "ZW": "Zimbabwe",
    "BW": "Botswana",
    "NA": "Namibia",
    "MZ": "Mozambique",
    "ZM": "Zambia",
    "MW": "Malawi",
    "AO": "Angola",
    "SN": "Senegal",
    "CI": "Ivory Coast",
    "CM": "Cameroon",
    "TN": "Tunisia",
    "DZ": "Algeria",
    "LY": "Libya",
    "SD": "Sudan",
    "MU": "Mauritius",
    "SC": "Seychelles",
    "MG": "Madagascar",
    "RE": "Reunion",

    # Europe - Western
    "UK": "United Kingdom",
    "GB": "United Kingdom",
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
    "NL": "Netherlands",
    "BE": "Belgium",
    "CH": "Switzerland",
    "AT": "Austria",
    "LU": "Luxembourg",
    "MC": "Monaco",
    "LI": "Liechtenstein",
    "AD": "Andorra",

    # Europe - Nordic
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "IS": "Iceland",

    # Europe - Eastern
    "PL": "Poland",
    "CZ": "Czech Republic",
    "HU": "Hungary",
    "RO": "Romania",
    "BG": "Bulgaria",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "HR": "Croatia",
    "RS": "Serbia",
    "BA": "Bosnia and Herzegovina",
    "ME": "Montenegro",
    "MK": "North Macedonia",
    "AL": "Albania",
    "XK": "Kosovo",
    "UA": "Ukraine",
    "BY": "Belarus",
    "MD": "Moldova",
    "LT": "Lithuania",
    "LV": "Latvia",
    "EE": "Estonia",
    "RU": "Russia",

    # Europe - Southern
    "PT": "Portugal",
    "GR": "Greece",
    "CY": "Cyprus",
    "MT": "Malta",

    # Europe - British Isles
    "IE": "Ireland",
    "IM": "Isle of Man",
    "JE": "Jersey",
    "GG": "Guernsey",

    # North America
    "US": "United States",
    "CA": "Canada",
    "MX": "Mexico",

    # Central America & Caribbean
    "GT": "Guatemala",
    "BZ": "Belize",
    "HN": "Honduras",
    "SV": "El Salvador",
    "NI": "Nicaragua",
    "CR": "Costa Rica",
    "PA": "Panama",
    "CU": "Cuba",
    "JM": "Jamaica",
    "HT": "Haiti",
    "DO": "Dominican Republic",
    "PR": "Puerto Rico",
    "TT": "Trinidad and Tobago",
    "BB": "Barbados",
    "BS": "Bahamas",

    # South America
    "BR": "Brazil",
    "AR": "Argentina",
    "CL": "Chile",
    "CO": "Colombia",
    "PE": "Peru",
    "VE": "Venezuela",
    "EC": "Ecuador",
    "UY": "Uruguay",
    "PY": "Paraguay",
    "BO": "Bolivia",
    "GY": "Guyana",
    "SR": "Suriname",

    # Asia - East
    "JP": "Japan",
    "CN": "China",
    "KR": "South Korea",
    "TW": "Taiwan",
    "HK": "Hong Kong",
    "MO": "Macau",
    "MN": "Mongolia",

    # Asia - Southeast
    "SG": "Singapore",
    "MY": "Malaysia",
    "TH": "Thailand",
    "ID": "Indonesia",
    "PH": "Philippines",
    "VN": "Vietnam",
    "MM": "Myanmar",
    "KH": "Cambodia",
    "LA": "Laos",
    "BN": "Brunei",

    # Asia - South
    "IN": "India",
    "PK": "Pakistan",
    "BD": "Bangladesh",
    "LK": "Sri Lanka",
    "NP": "Nepal",
    "BT": "Bhutan",
    "MV": "Maldives",

    # Middle East
    "AE": "United Arab Emirates",
    "IL": "Israel",
    "SA": "Saudi Arabia",
    "QA": "Qatar",
    "KW": "Kuwait",
    "OM": "Oman",
    "BH": "Bahrain",
    "JO": "Jordan",
    "LB": "Lebanon",
    "SY": "Syria",
    "IQ": "Iraq",
    "IR": "Iran",
    "YE": "Yemen",
    "PS": "Palestine",

    # Central Asia
    "KZ": "Kazakhstan",
    "UZ": "Uzbekistan",
    "TM": "Turkmenistan",
    "KG": "Kyrgyzstan",
    "TJ": "Tajikistan",
    "AF": "Afghanistan",

    # Caucasus
    "TR": "Turkey",
    "GE": "Georgia",
    "AM": "Armenia",
    "AZ": "Azerbaijan",

    # Oceania
    "AU": "Australia",
    "NZ": "New Zealand",
    "FJ": "Fiji",
    "PG": "Papua New Guinea",
    "NC": "New Caledonia",
    "PF": "French Polynesia",
    "WS": "Samoa",
    "TO": "Tonga",
    "VU": "Vanuatu",
    "SB": "Solomon Islands",
    "GU": "Guam",
}

# Region classification for demographics analysis
REGION_MAPPING = {
    # North America
    "US": "NA",
    "CA": "NA",
    "MX": "NA",

    # United Kingdom
    "UK": "UK",
    "GB": "UK",

    # Europe
    "DE": "EU",
    "FR": "EU",
    "IT": "EU",
    "ES": "EU",
    "NL": "EU",
    "BE": "EU",
    "CH": "EU",
    "AT": "EU",
    "SE": "EU",
    "NO": "EU",
    "DK": "EU",
    "FI": "EU",
    "PL": "EU",
    "CZ": "EU",
    "PT": "EU",
    "GR": "EU",
    "HU": "EU",
    "RO": "EU",
    "IE": "EU",
    "LU": "EU",
    "SK": "EU",
    "SI": "EU",
    "HR": "EU",
    "BG": "EU",
    "LT": "EU",
    "LV": "EU",
    "EE": "EU",
    "MT": "EU",
    "CY": "EU",
    "IS": "EU",

    # Australia/New Zealand (often grouped with Western markets)
    "AU": "ANZ",
    "NZ": "ANZ",
}


def get_country_name(code: str) -> str:
    """Get full country name from ISO code."""
    if not code:
        return ""
    return COUNTRY_CODES.get(code.upper(), code)


def get_region(code: str) -> str:
    """Get region classification from country code."""
    if not code:
        return "Other"
    return REGION_MAPPING.get(code.upper(), "Other")


def normalize_country_code(code: str) -> str:
    """Normalize country code to uppercase."""
    if not code:
        return ""
    code = code.strip().upper()
    # Handle common variations
    if code == "UK":
        return "GB"
    return code if code in COUNTRY_CODES else ""
