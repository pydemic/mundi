import warnings
from abc import ABC

import numpy as np
import pandas as pd

from .. import db
from ..pipeline import DataIO, Collector
from ..utils import read_file


class RegionData(DataIO, ABC):
    """
    Validates data for the main plugin.
    """

    REGION_DATA_TYPES = {
        "name": "string",
        "type": "string",
        "subtype": "string",
        "short_code": "string",
        "numeric_code": "string",
        "long_code": "string",
        "country_id": "string",
        "parent_id": "string",
        "level": np.dtype("uint8"),
        "region": "string",
    }
    REGION_M2M_DATA_TYPES = {
        "child_id": "string",
        "parent_id": "string",
        "relation": "string",
    }

    def fill_region(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Fill optional columns with empty values.
        """
        defaults = {
            "short_code": (pd.NA, "string"),
            "numeric_code": (pd.NA, "string"),
            "long_code": (pd.NA, "string"),
            "parent_id": (pd.NA, "string"),
            "region": (pd.NA, "string"),
            "subtype": (pd.NA, "string"),
            "type": (pd.NA, "string"),
            "country_id": (pd.NA, "string"),
        }
        types = {k: "string" for k in defaults}
        types["level"] = "uint8"
        kwargs = {**defaults, **kwargs}
        return self.assign(data, **kwargs).astype(types)


@Collector.register("region_m2m")
class RegionM2MCollector(Collector):
    """
    Specialized collector for the region_m2m table.
    """

    duplicate_indexes = ["child_id", "parent_id", "relation"]
    auto_index = True

    def prepare_table(self, chunks):
        table = super().prepare_table(chunks)
        regions = self.path / "databases" / "region.pkl.gz"

        if regions.exists():
            extra = self.prepare_default_m2m(read_file(regions))
            table = pd.concat([table, extra]).drop_duplicates().reset_index(drop=True)
        else:
            warnings.warn(f"no region.pkl.gz found at {regions}")

        return table

    def prepare_default_m2m(self, data):
        return (
            data[["parent_id"]]
            .reset_index()
            .rename(columns={"id": "child_id"})
            .assign(relation="default")
            .astype("string")
            .dropna()
        )[["child_id", "parent_id", "relation"]]


#
# Plugin
#
class RegionPlugin(db.Plugin):
    """
    Main data
    """

    name = "region"
    tables = {
        "region": db.Region,
        "region_m2m": db.RegionM2M,
    }
    data_tables = {"region"}
    data_url = "http://github.com/pydemic/mundi-data/{table}.pkl.gz"


if __name__ == "__main__":
    RegionPlugin.cli()
