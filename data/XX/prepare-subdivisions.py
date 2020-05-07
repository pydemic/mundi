import pycountry
import pandas as pd
from pathlib import Path
import numpy as np
import mundi

COLUMNS = mundi.DATA_COLUMNS["mundi"]
COL_RENAME = {"code": "id", "parent_code": "parent_id"}
BLACKLIST = {"BR-FN"}
PATH = Path(__file__).parent
SUBDIVISION_TYPES = {
    # 'prefecture', 'indigenous region', 'administrative region', 'autonomous
    # district', 'borough', 'london borough', 'canton', 'department', 'oblast',
    # 'republic', 'overseas territorial collectivity', 'geographical unit',
    # 'metropolitan region', 'municipalities', 'popularates', 'entity', 'metropolitan
    # district', 'geographical region', 'autonomous territorial
    # unit', 'geographical entity', 'autonomous republic', 'district', 'metropolitan
    # department', 'council area', 'parish', 'development region', 'chains (of
    # islands)', 'economic prefecture', 'territory', 'unitary authority', 'special
    # island authority', 'overseas region', 'administrative territory',
    # 'capital territory', 'capital district', 'union territory', 'special zone',
    # 'self-governed part', 'constitutional province', 'administration', 'autonomous
    # sector', 'nation', 'zone', 'autonomous community',
    # 'quarter', 'local council', 'capital metropolitan city', 'province',
    #  'overseas department',  'island group',
    # 'commune', 'state', 'territorial unit', 'two-tier county',
    # 'special district', 'division', 'area', 'administrative atoll',
    # 'autonomous region', 'federal territories', 'autonomous
    # province', 'dependency', 'emirate',  'arctic region', 'rayon', 'regional
    # council', 'island', 'town council', 'metropolitan cities',
    # 'island council', 'county', 'special region', 'outlying area', 'governorate',
    # 'region', 'special administrative region',  'federal
    # dependency'
    #
    # Primitive types
    # 'country'
    #
    # Derived types with corresponding sub-type
    # State-like
    "state": ("state", "federal district"),
    # City-like
    "city": ("city", pd.NA),
    "city with county rights": ("city", "county rights"),
    "republican city": ("city", "republican city"),
    "special city": ("city", "special"),
    "capital city": ("city", "capital"),
    "city corporation": ("city", "city corporation"),
    "autonomous city": ("city", "autonomous"),
    "municipality": ("city", "municipality"),
    "autonomous municipality": ("city", "autonomous municipality"),
    "special municipality": ("city", "special municipality"),
}

#
# This script uses pycountry (https://pypi.org/project/pycountry/) as the source
# for information about countries and the main sub-divisions.
#
df = pd.DataFrame([s._fields for s in pycountry.subdivisions if s.code not in BLACKLIST])
df = df.rename(COL_RENAME, axis=1).set_index("id").drop(columns=["parent"])

df["short_code"] = pd.Series(df.index).astype("string").str.partition("-")[2].values
df["short_code"] = codes = df["short_code"].astype("string")
df["numeric_code"] = np.where(codes.str.isdigit(), codes, pd.NA)
df["long_code"] = df["alt_parents"] = pd.NA

# Fix region types/subtypes
data = df["type"].apply(lambda x: SUBDIVISION_TYPES.get(x.lower(), (x.lower(), pd.NA)))
types_df = pd.DataFrame(list(data), columns=["type", "subtype"], index=df.index)
df["type"] = types_df["type"]
df["subtype"] = types_df["subtype"]

df = df.astype("string")[COLUMNS]
assert len(set(df.index)) == len(df)

path = PATH / "processed" / "mundi-A1-subdivisions.pkl"
df.to_pickle(path)
print(f"Subdivisions data saved to {path}")
