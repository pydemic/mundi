#
# This script creates the sus-cities.csv and sus-macros.csv scripts
#

import json
from collections import defaultdict
from pathlib import Path

import pandas as pd

import mundi

COLUMNS = mundi.DATA_COLUMNS["mundi"]
PATH = Path(__file__).parent
STATE_CODES = json.load(open(PATH / "state_codes.json"))
CITY_CODES = json.load(open(PATH / "city-codes.json"))

df = pd.read_csv(PATH / "regionais-de-saude-macro.csv", index_col=0).drop(
    columns=["macro_id"]
)
df["region_id"] = df["region_id"].apply(lambda x: f"BR-SUS:{x}").astype("string")
df["city_id"] = df["city_id"].apply(lambda x: f"BR-{CITY_CODES[str(x)]}").astype("string")
df["uf_id"] = df["region_id"].str[7:9]
df["state_id"] = "BR-" + df["uf"]


#
# SUS macro healthcare regions
#
macro_ids = defaultdict(dict)
for _, (uf, uf_id, name) in df[["uf", "uf_id", "macro_name"]].iterrows():
    db = macro_ids[uf]
    if name not in db:
        db[name] = f"BR-SUS:{uf_id}{len(db) + 1:02};BR-{uf}"

macros = (
    pd.concat(pd.Series(rs) for rs in macro_ids.values())
    .reset_index()
    .astype("string")
    .rename({"index": "name"}, axis=1)
)
macros = pd.concat(
    [
        macros,
        macros.pop(0)
        .str.partition(";")
        .drop(columns=1)
        .rename({0: "id", 2: "state_id"}, axis=1),
    ],
    axis=1,
).set_index("id")


#
# Merge macro-regions with cities
#
cities = (
    pd.merge(
        df[["state_id", "macro_name", "city_id"]].rename(
            {"macro_name": "name", "city_id": "id"}, axis=1
        ),
        macros.reset_index().rename({"id": "sus_id"}, axis=1),
        on=["state_id", "name"],
    )
    .drop(columns="state_id")
    .set_index("id")
)

macros = macros.rename({"state_id": "parent_id"}, axis=1)

print(macros)

macros.to_csv(PATH / "sus-macros.csv")
cities.to_csv(PATH / "sus-cities.csv")
