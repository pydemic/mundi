import importlib

from . import transforms
from .types import PandasT


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
    return transforms.sum_children(data)


def mean_children(package, key, data: PandasT, cols=None) -> PandasT:
    """
    Aggregate children
    """
    raise NotImplementedError


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
