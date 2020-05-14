import pandas as pd


def sum_children(data, which="both"):
    """
    Fill values by summing the contents of children.
    """
    return agg_children(data, "sum", which)


def agg_children(data: pd.DataFrame, agg="sum", which="both") -> pd.DataFrame:
    """
    Fill all NA values aggregating children by summation.
    """
    if which == "both":
        data = agg_children(data, agg, "primary")
        return agg_children(data, agg, "secondary")
    elif which == "primary":
        m2m_callback = primary_child_parent_m2m
    elif which == "secondary":
        m2m_callback = secondary_child_parent_m2m
    else:
        msg = 'which must be in {"primary", "secondary", "both"}, got %r'
        raise ValueError(msg % which)

    skip = data.index
    parts = [data]

    while True:
        m2m = m2m_callback(data).rename({"child_id": "id"}, axis=1)
        m2m = m2m[~m2m["parent_id"].isin(skip).values]

        out = (
            pd.merge(m2m, data.reset_index(), on="id")
            .drop(columns="id")
            .rename({"parent_id": "id"}, axis=1)
            .groupby("id")
            .agg(agg)
        )
        if len(out) == 0:
            return pd.concat(parts)
        parts.append(out)
        skip |= out.index
        data = out


def primary_child_parent_m2m(data):
    """
    Return a m2m table with (child_id, parent_id) pairs.
    """
    return (
        data.mundi["parent_id"].dropna().reset_index().rename({"id": "child_id"}, axis=1)
    )


def secondary_child_parent_m2m(data):
    """
    Return a m2m table with (child_id, parent_id) pairs.
    """

    parents = (
        data.mundi["alt_parents"]["alt_parents"]
        .dropna()
        .reset_index()
        .rename({"alt_parents": "parent_id", "id": "child_id"}, axis=1)
    )
    return pd.DataFrame(
        list(
            (idx, p)
            for _, (idx, parents) in parents.iterrows()
            for p in parents[1:].split(";")
        ),
        columns=["child_id", "parent_id"],
    ).reset_index(drop=True)
