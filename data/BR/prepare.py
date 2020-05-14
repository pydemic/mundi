import json
from collections import namedtuple
from functools import lru_cache
from pathlib import Path

import pandas as pd

import mundi

COLUMNS = mundi.DATA_COLUMNS["mundi"]
PATH = Path(__file__).parent
STATE_CODES = json.load(open(PATH / "state_codes.json"))


@lru_cache(1)
def territory():
    """
    Full data from IBGE about the division of territories up to the level of
    districts.
    """

    # Read raw data from IBGE
    out = pd.read_csv(PATH / "divisao-do-territorio-distritos.csv")
    out.columns = [
        "state",
        "state_name",
        "meso_code",
        "meso_name",
        "micro_code",
        "micro_name",
        "city_short_code",
        "city_code",
        "city_name",
        "district_short_code",
        "district_code",
        "district_name",
    ]
    out["state_code"] = out["state"]
    out["state"] = out["state_name"].apply(STATE_CODES.get)
    return out


@lru_cache(1)
def macro_regions():
    """
    Macro regions, i.e., Norte, Nordeste, Sudeste, Sul e Centro-Oeste.
    """

    out = pd.DataFrame(
        [
            ["Norte", "1"],
            ["Nordeste", "2"],
            ["Sudeste", "3"],
            ["Sul", "5"],
            ["Centro-Oeste", "5"],
        ],
        index=[f"BR-{i}" for i in range(1, 6)],
        columns=["name", "short_code"],
    )
    out["type"] = "region"
    out["subtype"] = "macro-region"
    out["numeric_code"] = out["short_code"]
    out["country_code"] = "BR"
    out["parent_id"] = "BR"
    out["alt_parents"] = out["long_code"] = pd.NA
    return out.astype("string")[COLUMNS]


@lru_cache(1)
def states():
    """
    Dataframe with all Brazillian states.
    """
    # Load states from prepared pycountry database
    out = (
        pd.Series(STATE_CODES, name="short_code")
        .reset_index()
        .astype("string")
        .rename({"index": "name"}, axis=1)
    )
    out.index = "BR-" + out["short_code"]

    # Save numeric codes for states
    codes = territory()[["state", "state_code"]].drop_duplicates()
    codes.index = "BR-" + codes.pop("state").astype("string")

    # Assign region numbers
    out["numeric_code"] = codes["state_code"].apply(str).astype("string")
    out["parent_id"] = "BR-" + out["numeric_code"].str[:1]
    out["type"] = "state"
    out["country_code"] = "BR"
    out["alt_parents"] = out["subtype"] = out["long_code"] = pd.NA

    out.loc["BR-DF", "subtype"] = "federal district"
    return out.astype("string")[COLUMNS]


@lru_cache(1)
def SUS():
    """
    SUS healthcare regions.
    """

    out = pd.read_csv(PATH / "sus-macros.csv", dtype="string")
    out["type"] = "region"
    out["subtype"] = "healthcare region"
    out["country_code"] = "BR"
    out["short_code"] = out["id"].str[7:]
    out["numeric_code"] = out["id"].str[7:]
    out["long_code"] = out["id"].str[3:]
    out["alt_parents"] = pd.NA
    out = out.set_index("id")
    return out.astype("string")


@lru_cache(1)
def subdivisions():
    """
    Subdivisions

    Include SUS regions as parent_extra after loading raw _subdivisions
    """
    out = _subdivisions()
    sus = pd.read_csv(PATH / "sus-cities.csv", index_col=0, dtype="string")
    out["alt_parents"] = ";" + sus["sus_id"]
    print(out[out["type"] == "city"])
    return out


def _subdivisions():
    """
    Subdivisions.

    Do not fix SUS healthcare regions.
    """
    # Collect dataframes
    cols = ["short_code", "name", "type", "subtype", "parent_id", "long_code"]
    Row = namedtuple("Row", cols)

    # Regions
    meso = set()
    micro = set()
    city = set()
    district = set()

    for _, row in territory().iterrows():
        meso_id = f"{row.state_code}{row.meso_code:02}"
        micro_id = f"{meso_id}{row.micro_code:02}"
        city_long_id = str(row.city_code)
        city_id = str(row.city_code)[:-1]  # strip unused last digit
        district_id = str(row.district_code)

        meso.add(
            Row(meso_id, row.meso_name, "region", "meso-region", str(row.state), pd.NA)
        )
        micro.add(Row(micro_id, row.micro_name, "region", "micro-region", meso_id, pd.NA))
        city.add(
            Row(city_id, row.city_name, "city", "municipality", micro_id, city_long_id)
        )
        district.add(
            Row(district_id, row.district_name, "district", pd.NA, city_long_id, pd.NA)
        )

    def mk_table(data):
        df = pd.DataFrame(list(data), columns=cols)
        df["numeric_code"] = df["long_code"].fillna(df["short_code"])
        df["parent_id"] = "BR-" + df["parent_id"]
        df["country_code"] = "BR"
        df["alt_parents"] = pd.NA

        df.index = "BR-" + df["numeric_code"]
        df.index.name = "id"
        return df.astype("string")

    br_meso = mk_table(meso)
    br_micro = mk_table(micro)
    br_city = mk_table(city)
    br_district = mk_table(district)

    df = pd.concat([br_meso, br_micro, br_city, br_district])
    df["type"] = df["type"]

    return df.astype("string")[COLUMNS]


# Save tables
for i, fn in enumerate([macro_regions, states, SUS, subdivisions], start=1):
    name = fn.__name__.rstrip("s")
    path = PATH / "processed" / f"mundi-C{i}-{name}.pkl"
    df = fn()
    assert len(set(df.index)) == len(df)
    df.to_pickle(path)
    print(f"BR data saved to {path}")
