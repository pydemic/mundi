from abc import ABC

import pandas as pd

from .loader import Loader
from ..db import RegionsDB


class Region(Loader, ABC):
    """
    Base class for region importers.
    """

    FORMAT = property(lambda self: "sql" if type(self) is Region else "pickle")
    cols = RegionsDB.columns
    col_types = RegionsDB.column_types


class RegionGroup(Region):
    """
    An aggregate of regions.
    """

    FORMAT = "sql"
    KEY = "region"

    def __init__(self, importers=(), *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaders = importers

    def load(self):
        data = pd.concat(loader.load() for loader in self.loaders)
        return self.fix_result(data)

    def load_cached(self):
        data = pd.concat(loader.load_cached() for loader in self.loaders)
        return self.fix_result(data)

    def fix_result(self, data):
        data["id"] = data.index
        data.drop_duplicates("id", keep="last", inplace=True)
        data.index = data.pop("id")
        data = data.sort_index()[self.cols]
        for k, v in self.col_types.items():
            if v == "string":
                col = data[k].astype(str)
                col[col == "<NA>"] = None  # FIXME: this should not be necessary!
                data[k] = col
        return data
