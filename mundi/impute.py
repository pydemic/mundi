from typing import Mapping, Union, TYPE_CHECKING, cast

import pandas as pd
import numpy as np
from collections import defaultdict
from .types import Region
from .functions import regions_dataframe, code

RegionLike = Union[Region, str]


def impute_parents(data: pd.DataFrame, parents: Mapping, by, agg="sum"):
    """
    Simple imputation strategy for flattened data. This function considers the
    following tabular layout

    +------+-------+-------+-----+-------+
    | id   | <by>  | col_a | ... | col_n |
    +------+-------+-------+-----+-------+
    | ref  | v     | x1    | ... | xn    |
    +------+-------+-------+-----+-------+
    | ...  | ...   | ...   | ... | ...   |
    +------+-------+-------+-----+-------+
    | ref' | v'    | x1_m  | ... | xn_m  |
    +------+-------+-------+-----+-------+

    It is assumed that (id, by) pairs are unique. Given this data and a mapping
    from children in the id column to their respective parents, this function
    produces a similar table for the parent keys by the specified aggregation
    method.
    """
    imputed = []

    groups = data.groupby("id").groups
    parent_groups = defaultdict(list)

    for child, idxs in groups.items():
        parent_groups[parents[child]].append(idxs)

    for k, v in parent_groups.items():
        idxs = np.concatenate(v)
        df = data.iloc[idxs]
        df = df.groupby(by).agg(agg).reset_index()
        df["id"] = k
        imputed.append(df)

    return pd.concat(imputed).reset_index()[data.columns]


def impute_subregions(data: pd.DataFrame, country: RegionLike, by, agg="sum", level=None):
    """
    Like impute parents, but assume that all children are a sub-region with a
    given level within a country.

    Join all results up to the country level.
    """

    if len(data) == 0:
        return data.copy()
    if level is None:
        level = Region(data["id"].iloc[0]).level

    parts = []
    for level in range(level, 2, -1):
        parents = regions_dataframe(["parent_id"], country_id=code(country), level=level)
        parents_map = parents["parent_id"]
        data = impute_parents(data, parents_map, by=by, agg=agg)
        parts.append(data)

    return pd.concat(parts).reset_index()[data.columns]
