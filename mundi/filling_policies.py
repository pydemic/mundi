import enum
from functools import lru_cache

import pandas as pd

from . import db


class FillPolicy(enum.IntEnum):
    """
    Define strategy of filling columns.
    """

    NONE = 0
    INHERIT = 1
    SUM_CHILDREN = 2
    MAX_CHILDREN = 4
    MIN_CHILDREN = 8


POLICY_FUNCTIONS = {
    FillPolicy.NONE: lambda df: df.copy(),
    FillPolicy.SUM_CHILDREN: lambda df: sum_children(df),
    FillPolicy.MAX_CHILDREN: lambda df: agg_children(df, "max"),
    FillPolicy.MIN_CHILDREN: lambda df: agg_children(df, "min"),
}


@lru_cache(10)
def region_m2m(relation) -> pd.DataFrame:
    m = db.RegionM2M
    return pd.DataFrame(
        db.session()
            .query(m)
            .filter(m.relation == relation)
            .values(m.child_id, m.parent_id)
    ).rename(columns={'child_id': 'child', 'parent_id': 'parent'})


def apply_filling_policy(table: pd.DataFrame, policy: FillPolicy) -> pd.DataFrame:
    """
    Apply filling policy to table.
    """
    try:
        fn = POLICY_FUNCTIONS[policy]
    except KeyError:
        raise ValueError(f"invalid policy: {policy}")
    return fn(table)


def sum_children(data: pd.DataFrame, relation="default") -> pd.DataFrame:
    """
    Fill values by summing the contents of children.
    """
    return agg_children(data, "sum", relation).astype(data.dtypes)


def agg_children(data: pd.DataFrame, agg="sum", relation="default") -> pd.DataFrame:
    """
    Fill all NA values aggregating children by aggregation function.
    """
    if relation in ("*", "all"):
        for relation in ("default", "continent", "sus_region"):
            data = agg_children(data, agg, relation)
        return data

    skip = data.index
    parts = [data]

    while True:
        m2m = pd.DataFrame(
            list(
                db.session()
                    .query(db.RegionM2M)
                    .filter(db.RegionM2M.relation == relation)
                    .filter(db.RegionM2M.child_id.in_(data.index))
                    .values(db.RegionM2M.child_id, db.RegionM2M.parent_id)
            )
        )
        if len(m2m) == 0:
            return pd.concat(parts)

        m2m = m2m.rename(columns={"child_id": "id"})
        m2m = m2m[~m2m["parent_id"].isin(skip).values]

        out = (
            pd.merge(m2m, data.reset_index(), on="id")
                .drop(columns="id")
                .rename(columns={"parent_id": "id"})
                .groupby("id")
                .agg(agg)
        )
        if len(out) == 0:
            return pd.concat(parts)
        parts.append(out)
        skip = skip.union(out.index)
        data = out


def agg_children(data: pd.DataFrame, agg="sum", relation="default") -> pd.DataFrame:
    print(agg)
    print(data)
    m2m = region_m2m(relation)

    parents = pd.Index(m2m['parent'].unique())
    missing = parents.difference(data.index)
    print(parents)
    return data


def agg_scalar(ref, data, column, relation="default", agg="sum"):
    ...
