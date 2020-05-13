import importlib

import pandas as pd
import sidekick as sk

from .functions import regions
from .types import PandasT

db = sk.deferred(regions)


def fix_types(package, key, data: PandasT) -> PandasT:
    """
    Fix types of input dataframe using information for the given package.
    """
    mod = importlib.import_module(package)
    try:
        types = mod.DATA_TYPES
    except AttributeError:
        msg = f'module "{package}" does not define a DATA_TYPES constant.'
        raise RuntimeError(msg)

    # Filter types to the desired database
    if isinstance(types, dict):
        try:
            columns = set(mod.DATA_COLUMNS[key])
        except AttributeError:
            msg = f'module "{package}" does not define a DATA_COLUMNS constant.'
            raise RuntimeError(msg)
        types = {k: v for k, v in types.items() if k in columns}

    return data.astype(types)


def sum_children(package, key, data: PandasT, cols=None) -> PandasT:
    """
    Aggregate children by sum.
    """

    id_name = data.index.name

    for _ in range(100):
        # Extract m2m relations to data
        m2m = child_parent_m2m(data).rename({"child_id": id_name}, axis=1)

        # Fill parents with aggregate
        new = (
            pd.merge(m2m, data.reset_index(), on=id_name)
            .set_index("parent_id")
            .dropna()
            .groupby("parent_id")
            .agg("sum")
            .reset_index()
            .rename({"parent_id": id_name}, axis=1)
            .set_index(id_name)
        )
        new = new[~new.index.isin(data.index)]

        # Join and continue or stop process
        if len(new) > 0:
            data = pd.concat([data, new], verify_integrity=False)
        else:
            break

    else:
        raise RuntimeError("maximum number of iterations")

    print(data.loc["BR-1"])
    return data


def mean_children(package, key, data: PandasT, cols=None) -> PandasT:
    """
    Aggregate children
    """
    data = _aggregate_primary(data, "mean")
    return data


def max_children(package, key, data: PandasT, cols=None) -> PandasT:
    """
    Aggregate children
    """
    raise NotImplementedError


def min_children(package, key, data: PandasT, cols=None) -> PandasT:
    """
    Aggregate children
    """
    raise NotImplementedError


def fill_from_children_population_weighted_mean(
    package, key, data: PandasT, cols=None
) -> PandasT:
    """
    Fill children
    """
    raise NotImplementedError


def as_parent(package, key, data: PandasT, cols=None) -> PandasT:
    """
    Fill missing data using the same value as the parent.
    """


def _aggregate_primary(data, agg, maxiter=500) -> PandasT:
    """
    Worker function for sum_children, mean_children, etc.
    """
    parents = db.mundi["parent_id"]
    id_name = data.index.name

    for _ in range(maxiter):
        # Join parents and data and groupby
        join = pd.concat([parents, data], axis=1)
        df = (
            join.dropna(subset=["parent_id"])
            .set_index("parent_id")
            .groupby("parent_id")
            .agg(agg)
        )
        df = df.groupby("parent_id").sum()
        df.index.name = id_name

        if agg == "sum":
            df = df[df.sum(1) > 0]
            df = df[~df.index.isin(data.index)]
        else:
            df = df.dropna()

        parents[parents["parent_id"].isin(df.index)] = pd.NA
        if len(df) == 0:
            break

        data = pd.concat([data, df], verify_integrity=True)
    else:
        raise RuntimeError("maximum number of iterations")

    return data


def _aggregate_secondary(data, agg="sum"):
    """
    Aggregate children of secondary parents relations.
    """
    id_name = data.index.name

    parents = db.mundi["alt_parents"].dropna()
    parents = parents["alt_parents"].str[1:].str.split(";").reset_index()

    parents = pd.DataFrame(
        list((idx, p) for _, (idx, parents) in parents.iterrows() for p in parents),
        columns=[id_name, "parent_id"],
    )

    return (
        pd.merge(parents, data.reset_index(), on=id_name)
        .drop(columns=id_name)
        .set_index("parent_id")
        .groupby("parent_id")
        .agg(agg)
    )


def child_parent_m2m(data, names=["child_id", "parent_id"], primary=True, secondary=True):
    """
    Return a m2m table with (child_id, parent_id) pairs.
    """
    out = []
    if primary:
        out.append(
            data.mundi["parent_id"]
            .dropna()
            .reset_index()
            .rename({"id": "child_id"}, axis=1)
        )

    if secondary:
        parents = (
            db.mundi["alt_parents"]["alt_parents"]
            .dropna()
            .reset_index()
            .rename({"alt_parents": "parent_id", "id": "child_id"}, axis=1)
        )
        out.append(
            pd.DataFrame(
                list(
                    (idx, p)
                    for _, (idx, parents) in parents.iterrows()
                    for p in parents[1:].split(";")
                ),
                columns=["child_id", "parent_id"],
            )
        )

    if not out:
        raise ValueError("must provide primary or secondary children")
    elif len(out) == 1:
        return out[0]
    else:
        return pd.concat(out).drop_duplicates().reset_index(drop=True)
