"""
This script uses pycountry (https://pypi.org/project/pycountry/) as the main
source of information about countries and sub-divisions.

Pycountry itself seems to be based in an outdated version of ISO ???, so in the
future we should mine data directly from the ISO website.
"""
import warnings
from typing import Dict

import numpy as np
import pandas as pd
import pycountry
import sidekick.api as sk

import mundi
import mundi.utils
from mundi.plugins.region import RegionData

inv = lambda d: {v: k for k, v in d.items()}


class Countries(RegionData):
    """
    Collect basic country information.
    """

    COUNTRY_COLUMNS = {
        "alpha_2": "short_code",
        "alpha_3": "long_code",
        "numeric": "numeric_code",
    }
    REGION_NAMES = inv(mundi.REGION_DESCRIPTIONS)
    SUBDIVISION_TYPES = {
        # 'prefecture', 'indigenous region', 'administrative region', 'autonomous
        # district', 'borough', 'london borough', 'canton', 'department', 'oblast',
        # 'republic', 'overseas territorial collectivity', 'geographical unit',
        # 'metropolitan region', 'municipalities', 'popularates', 'entity',
        # 'metropolitan
        # district', 'geographical region', 'autonomous territorial
        # unit', 'geographical entity', 'autonomous republic', 'district',
        # 'metropolitan
        # department', 'council area', 'parish', 'development region', 'chains (of
        # islands)', 'economic prefecture', 'territory', 'unitary authority', 'special
        # island authority', 'overseas region', 'administrative territory',
        # 'capital territory', 'capital district', 'union territory', 'special zone',
        # 'self-governed part', 'constitutional province', 'administration',
        # 'autonomous
        # sector', 'nation', 'zone', 'autonomous community',
        # 'quarter', 'local council', 'capital metropolitan city', 'province',
        #  'overseas department',  'island group',
        # 'commune', 'state', 'territorial unit', 'two-tier county',
        # 'special district', 'division', 'area', 'administrative atoll',
        # 'autonomous region', 'federal territories', 'autonomous
        # province', 'dependency', 'emirate',  'arctic region', 'rayon', 'regional
        # council', 'island', 'town council', 'metropolitan cities',
        # 'island council', 'county', 'special region', 'outlying area',
        # 'governorate',
        # 'region', 'special administrative region',  'federal
        # dependency'
        #
        # Primitive types
        # 'country'
        #
        # Derived types with corresponding sub-type
        # State-like
        "state": ("state", "federal_district"),
        # City-like
        "city": ("city", pd.NA),
        "city with county rights": ("city", "county_rights"),
        "republican city": ("city", "republican_city"),
        "special city": ("city", "special"),
        "capital city": ("city", "capital"),
        "city corporation": ("city", "corporation"),
        "autonomous city": ("city", "autonomous"),
        "municipality": ("city", "municipality"),
        "autonomous municipality": ("city", "autonomous_municipality"),
        "special municipality": ("city", "special_municipality"),
    }

    @sk.lazy
    def region_pycountry(self):
        """
        Dataframe with raw information from pycountry.
        """
        df = (
            pd.DataFrame([c._fields for c in pycountry.countries])
            .rename(self.COUNTRY_COLUMNS, axis=1)
            .drop(columns=["official_name", "common_name"])
            .set_index("short_code", drop=False)
            .assign(type="country", subtype=pd.NA, country_id=pd.NA, region=pd.NA)
            .astype("string")
            .assign(level=2)
            .astype({"level": "uint8"})
        )
        df.index.name = "id"
        return self.check_unique_index(df)

    @sk.lazy
    def region_pycountry_and_continents(self):
        """
        Include continents into the dataframe and assign countries to their
        respective continent. We added an X prefix to continent codes to avoid
        collisions with country codes.

        See Also:
            - https://en.wikipedia.org/wiki
            /List_of_sovereign_states_and_dependent_territories_by_continent_(
            data_file)#Data_file
        """
        # Load country continent codes
        df = self.region_pycountry.copy()
        data = self.read_csv("countries-to-continents.csv", dtype="string")
        df["parent_id"] = "X" + data["continent_id"]

        # Load continents
        data = self.fill_region(
            self.read_csv("continents.csv")
            .assign(type="continent", parent_id="XX")
            .astype("string")
            .assign(level=1)
            .astype({"level": "uint8"})
        )
        data.loc[:, "short_code"] = data.index
        data.loc["XX", "parent_id"] = pd.NA
        data.loc["XX", "level"] = 0
        data = data.astype({"short_code": "string"})

        return self.check_unique_index(self.safe_concat(df, data))

    @sk.lazy
    def region_world_bank(self):
        """
        Load country classifications from world bank data.
        """

        df = self.region_pycountry_and_continents.copy()
        un = self.read_csv("world-bank-summary.csv", dtype="string")
        un = un[["region"]]
        un["id"] = (
            df.loc[df["type"] == "country", "long_code"]
            .reset_index()
            .set_index("long_code")
        )
        un = un.dropna().set_index("id")

        # Normalize data
        fill_na = lambda dic: lambda x: dic.get(x, pd.NA)
        un["region"] = un["region"].apply(fill_na(self.REGION_NAMES))

        # Merge two dataframes
        df = df.assign(region=un["region"]).fillna(pd.NA).astype({"region": "string"})

        return self.check_unique_index(df)

    @sk.lazy
    def region_subdivisions(self):
        """
        This script uses pycountry (https://pypi.org/project/pycountry/) as the
        source for information about countries and the main sub-divisions.
        """

        # Pycountry have some inconsistent parent_codes for Morroccan provinces
        # We fix that with this mapping
        fix_parent_codes = {f"MA-MA-{i:02}": f"MA-{i:02}" for i in range(1, 13)}

        def to_map(sub):
            data = {k: v or pd.NA for k, v in sub._fields.items()}
            if data.get("parent_code", pd.NA) is pd.NA:
                data["parent_code"] = data["country_code"]
                data["level"] = 3

            # Fix errors in pycountry data
            x = data["parent_code"]
            data["parent_code"] = fix_parent_codes.get(x, x)
            return data

        blacklist = {"BR-FN"}
        cols = {"code": "id", "parent_code": "parent_id", "country_code": "country_id"}
        src = [to_map(s) for s in pycountry.subdivisions if s.code not in blacklist]
        df = (
            pd.DataFrame(src)
            .rename(columns=cols)
            .drop(columns=["parent"])
            .assign(long_code=pd.NA, region=pd.NA)
            .set_index("id")
        )
        df["level"] = self.fill_levels(df[["level", "parent_id"]])

        df["short_code"] = codes = (
            pd.Series(df.index).astype("string").str.partition("-")[2].astype("string")
        )
        df["numeric_code"] = np.where(codes.str.isdigit(), codes, pd.NA)

        # Fix region types/subtypes
        cols = ["type", "subtype"]
        data = df["type"].apply(self.subdivision_type)
        df[cols] = pd.DataFrame(list(data), columns=cols, index=df.index)
        df = df.astype({c: "string" for c in df.columns if c != "level"}).astype(
            {"level": "uint8"}
        )

        return self.check_unique_index(df)

    @sk.lazy
    def region(self):
        return self.safe_concat(self.region_world_bank, self.region_subdivisions)

    @sk.lazy
    def region_m2m(self):
        """
        Transcontinental countries are assigned to their secondary continent.
        https://en.wikipedia.org/wiki/List_of_transcontinental_countries
        """
        return pd.DataFrame(
            [
                ["AZ", "XEU", "continent"],
                ["AM", "XAS", "continent"],
                ["CY", "XAS", "continent"],
                ["GE", "XAS", "continent"],
                ["KZ", "XEU", "continent"],
                ["UM", "XOC", "continent"],
                ["RU", "XAS", "continent"],
                ["TR", "XEU", "continent"],
                ["EG", "XAS", "continent"],
            ],
            columns=["child_id", "parent_id", "relation"],
            dtype="string",
        )

    def subdivision_type(self, x):
        """
        Return a normalized tuple with subdivision types.
        """
        try:
            return self.SUBDIVISION_TYPES[x.lower()]
        except KeyError:
            return x.lower().replace(" ", "_"), pd.NA

    def fill_levels(self, data):
        """
        Fill missing data in the level column using data from level/parent_id columns.
        """

        def fill(x):
            parent = data.loc[x, "parent_id"]
            try:
                return data.loc[parent, "level"] + 1
            except KeyError:
                pass

            depth = 2
            for _ in range(100):
                try:
                    parent = data.loc[parent, "parent_id"]
                except KeyError:
                    msg = f"Invalid parent_id chain: {x}, ..., {parent}"
                    warnings.warn(msg)
                    return None
                try:
                    return data.loc[parent, "level"] + depth
                except KeyError:
                    depth += 1
            else:
                msg = f"maximum level reached for {x}. Circular parent chain?"
                raise ValueError(msg)

        keys = data.index[data["level"].isna().values]
        missing = pd.Series(list(map(fill, keys)), index=keys)
        return pd.concat([data["level"].dropna(), missing]).sort_index().astype("int64")

    def collect(self) -> Dict[str, pd.DataFrame]:
        return {
            "region": self.region,
            "region_m2m": self.region_m2m,
        }


if __name__ == "__main__":
    Countries.cli()
