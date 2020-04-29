"""
This module implements functions to import data from the pycountry module.
"""

import numpy as np
import pandas as pd
import pycountry

from .region import Region

SUBDIVISIONS_BLACKLIST = {"BR-FN"}


class Country(Region):
    """
    Populate the region db using pycountry countries
    """

    def load(self):
        df = pd.DataFrame([c._fields for c in pycountry.countries])
        col_names = {"alpha_2": "code", "alpha_3": "long_code", "numeric": "numeric_code"}
        df = (
            df.rename(col_names, axis=1)
            .drop(columns=["official_name", "common_name"])
            .astype("string")
        )

        df.index = df["code"]
        df.index.name = "id"

        df["type"] = "country"
        df["parent_id"] = pd.NA
        df["country_code"] = pd.NA
        return df[self.cols].astype("string")


class Subdivisions(Region):
    """
    Populate the region db using pycountry countries
    """

    def __init__(self, *args, keep=None, **kwargs):
        self.keep = keep or (lambda x: x.code not in SUBDIVISIONS_BLACKLIST)
        super().__init__(*args, **kwargs)

    def load(self):
        fn = self.keep
        df = pd.DataFrame([s._fields for s in pycountry.subdivisions if fn(s)])

        cols = {"code": "id", "parent_code": "parent_id"}
        df = df.rename(cols, axis=1)
        if "parent" in df:
            del df["parent"]
        df.index = id_col = df.pop("id")

        df["long_code"] = pd.NA
        df["code"] = id_col.str.partition("-")[2].values
        df["code"] = codes = df["code"].astype("string")
        df["numeric_code"] = np.where(codes.str.isdigit(), codes, pd.NA)
        df["type"] = df["type"].astype("string").str.lower()

        return df[self.cols].astype("string")


#
# Functional interface
#
def countries():
    """
    Return dataframe with pycountry countries.
    """
    return Country().load_cached()


def sub_divisions():
    """
    Return dataframe with pycountry sub-divisions.
    """
    return Subdivisions().load_cached()


if __name__ == "__main__":
    print(countries())
    print(sub_divisions())
