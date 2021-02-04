import pandas as pd

from . import db


def sum_children(data, relation="default"):
    """
    Fill values by summing the contents of children.
    """
    return agg_children(data, "sum", relation)


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
                .values("child_id", "parent_id")
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
