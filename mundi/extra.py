from pandas import CategoricalDtype

from . import loader

REGIONS = {
    "north-america": "North America",
    "south-asia": "South Asia",
    "sub-saharan-africa": "Sub-Saharan Africa",
    "europe": "Europe & Central Asia",
    "latin-america": "Latin America & Caribbean",
    "middle-east": "Middle East & North Africa",
    "east-asia": "East Asia & Pacific",
}
INCOME_GROUPS = {
    "low": "Low income",
    "lower-middle": "Lower middle income",
    "upper-middle": "Upper middle income",
    "high": "High income",
}

IncomeGroup = CategoricalDtype(categories=INCOME_GROUPS, ordered=True)
Region = CategoricalDtype(categories=REGIONS, ordered=False)


@loader.filtering_from_data(["region"])
def load_region():
    return loader.load_database("un.pkl.gz").astype(Region)


@loader.filtering_from_data(["income_group"])
def load_income_group():
    return loader.load_database("un.pkl.gz").astype(IncomeGroup)
