from abc import ABC

import numpy as np
import pandas as pd

from .plugin import Plugin
from .. import db
from ..pipeline import DataIO


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


#
# Plugin
#
class RegionPlugin(Plugin):
    """
    Main data
    """

    name = "region"
    tables = {
        "region": db.Region,
        "region_m2m": db.RegionM2M,
    }
    collectors = {
        "region": RegionCollector,
        "region_m2m": RegionM2MCollector,
    }
    data_url = "http://github.com/pydemic/mundi-data/{table}.pkl.gz"


if __name__ == "__main__":
    RegionPlugin.cli()
