from pathlib import Path

import pandas as pd
import pycountry

import mundi

inv = lambda d: {v: k for k, v in d.items()}
PATH = Path(__file__).parent.resolve()
COL_RENAME = {"alpha_2": "short_code", "alpha_3": "long_code", "numeric": "numeric_code"}
REGION_MAP = inv(mundi.REGION_DESCRIPTIONS)
INCOME_MAP = inv(mundi.INCOME_GROUP_DESCRIPTIONS)

#
# This script uses pycountry (https://pypi.org/project/pycountry/) as the main
# source of information about countries and sub-divisions.
#
df = pd.DataFrame([c._fields for c in pycountry.countries])
df = (
    df.rename(COL_RENAME, axis=1)
    .drop(columns=["official_name", "common_name"])
    .astype("string")
    .set_index("short_code", drop=False)
)
df.index.name = "id"

df["type"] = "country"
df["subtype"] = pd.NA
df["country_code"] = pd.NA
df = df.astype("string")

#
# Load UN classifications from world bank data
#
un_classes = pd.read_csv(PATH / "world-bank-summary.csv", index_col=0, dtype="string")
un_classes = un_classes[["region", "income_group"]]
un_classes["id"] = df["long_code"].reset_index().set_index("long_code")
un_classes = un_classes.dropna().set_index("id")

# Save to dataframe
un_classes["region"] = un_classes["region"].apply(REGION_MAP.get)
un_classes["income_group"] = un_classes["income_group"].apply(INCOME_MAP.get)

path = PATH / "processed" / "un.pkl"
un_classes.astype("category").to_pickle(path)
print(f"UN data saved to {path}")


#
# Loading continents
#

# Load continent codes
# https://en.wikipedia.org/wiki/List_of_sovereign_states_and_dependent_territories_by_continent_(data_file)#Data_file
data = pd.read_csv(PATH / "countries-to-continents.csv", index_col=0, dtype="string")
df["parent_id"] = "X" + data["continent_id"]

# Load continents
data = pd.read_csv(PATH / "continents.csv").fillna("NA").set_index("id")
data["type"] = "continent"
data["parent_id"] = "XX"
data["short_code"] = data.index
data.loc["XX", "parent_id"] = pd.NA
df = pd.concat([df, data]).astype("string")

# Transcontinental countries are assigned to their secondary continent in the
# alt_parent column.
# https://en.wikipedia.org/wiki/List_of_transcontinental_countries
df["alt_parents"] = pd.Series(
    {
        "AZ": "XEU",
        "AM": "XAS",
        "CY": "XAS",
        "GE": "XAS",
        "KZ": "XEU",
        "UM": "XOC",
        "RU": "XAS",
        "TR": "XEU",
        "EG": "XAS",
    },
    dtype="string",
)
df["alt_parents"] = ";" + df["alt_parents"]

#
# Saving results
#
df = df.astype("string")[mundi.DATA_COLUMNS["mundi"]]
assert len(set(df.index)) == len(df)

path = PATH / "processed" / "mundi-A1-countries.pkl"
df.to_pickle(path)
print(f"Country data saved to {path}")
