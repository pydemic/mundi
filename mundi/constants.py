REGION_DESCRIPTIONS = {
    "north-america": "North America",
    "south-asia": "South Asia",
    "sub-saharan-africa": "Sub-Saharan Africa",
    "europe": "Europe & Central Asia",
    "latin-america": "Latin America & Caribbean",
    "middle-east": "Middle East & North Africa",
    "east-asia": "East Asia & Pacific",
}
INCOME_GROUP_DESCRIPTIONS = {
    "low": "Low income",
    "lower-middle": "Lower middle income",
    "upper-middle": "Upper middle income",
    "high": "High income",
}
DATA_COLUMNS = {
    "mundi": [
        "name",
        "type",
        "subtype",
        "short_code",
        "numeric_code",
        "long_code",
        "country_code",
        "parent_id",
        "alt_parents",
    ],
    "un": ["region", "income_group"],
}
DATA_TYPES = {
    "name": "string",
    "type": "category",
    "subtype": "category",
    "short_code": "string",
    "numeric_code": "string",
    "long_code": "string",
    "country_code": "string",
    "parent_id": "string",
    "alt_parents": "string",
    "region": "category",
    "income_group": "category",
}
