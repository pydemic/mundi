from collections.abc import Set
from typing import (Optional, Dict, FrozenSet, Any, Callable, Union, List, TypeVar,
                    Sequence)

import pandas as pd

from .region import as_region, Region
from .. import db

REGION_UNIVERSE = db.Universe.REGION
T = TypeVar('T')


def inplace_sum(data):
    """
    A sum function that is also efficient when dealing with sequence of
    dataframes over the same indexes.
    """
    head, tail = uncons(data)  #FIXME: sidekick.seq?
    if hasattr(head, 'copy'):
        head = head.copy()
    else:
        return head + sum(tail)
    for x in tail:
        head += x
    return head


class RegionSet(Set):
    """
    Represent a set of regions in mundi db.
    """

    name: str
    year: int
    _cache: Dict[str, Any]
    _regions: FrozenSet[Region]
    __slots__ = "name", "year", "_regions", "_cache"

    @property
    def id(self):
        try:
            return self.__dict__["id"]
        except KeyError:
            out = "|".join(sorted(map(str, self._regions)))
            self.__dict__["id"] = out
        return out

    def __init__(self, regions, name=None, year=None):
        self.name = name
        self.year = year
        self._cache = {}
        self._regions = frozenset(map(as_region, regions))
        if not self._regions:
            raise ValueError('cannot create an empty region')

    def __hash__(self):
        return hash(self.id)

    def __contains__(self, item):
        return as_region(item) in self._regions

    def __iter__(self):
        return iter(self._regions)

    def __len__(self):
        return len(self._regions)

    def __getattr__(self, item):
        try:
            return self._cache[item]
        except KeyError:
            pass

        try:
            result = self._cache[item] = self._get_field(item)
            return result
        except ValueError:
            raise AttributeError(item)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            place, attr = key
            return self[place][key]

        region = None
        if isinstance(key, str):
            region = as_region(key)
        if isinstance(key, Region):
            region = key
        if region in self:
            return region
        raise KeyError(key)

    def __repr__(self):
        regions = sorted([repr(r.id) for r in self])
        if len(regions) > 5:
            regions = [*regions[:4], "..."]
        args = ['{' + ', '.join(regions) + '}']
        if self.name is not None:
            args.append(f'name={self.name}')
        elif self.year is not None:
            args.append(f'year={self.year}')
        return f"RegionSet({','.join(args)})"

    def __str__(self):
        regions = sorted([r.id for r in self])
        if len(regions) > 5:
            n = len(regions) - 4
            regions = [*regions[:4], f"({n} more)"]
        regions = ", ".join(regions)
        return (
            f"Region Set\n" f"  name     : {self.name}\n" f"  regions  : {regions}"
        )

    def _get_field(self, key):
        column = REGION_UNIVERSE.column(key)
        data = column.select([r.id for r in self._regions])
        self.__dict__[key] = data
        return data

    #
    # Conversions
    #
    def aggregate(self, field, by: Union[Callable[[List[T]], T], str] = "sum"):
        """
        Aggregate values with function.

        It accepts any function that receives a list and reduce it to the final
        aggregate or some string alias:

        - "sum": sum all values of field.
        - "pop-sum": sum each value multiplied by the respective region population.
        - "pop-mean": return the mean weighted by population.
        - "single": return a value if all values are identical.
        """
        values = (region[field] for region in self._regions)
        if callable(by):
            return by([*values])
        if by == "sum":
            return inplace_sum(filter(lambda x: x is not None, values))
        elif by == "pop-sum":
            populations = (region.population for region in self._regions)
            values = (x * pop for x, pop in zip(values, populations))
            return inplace_sum(values)
        elif by == "mean":
            return self.aggregate(values) / len(self._regions)
        elif by == "pop-mean":
            return self.aggregate(values, "pop-sum") / self.population
        elif by == "single":
            value, *error = set(values)
            if error:
                raise ValueError("regions do not share the same value")
            return value
        else:
            raise ValueError(f"invalid aggregation method: {by}")

    def max(self, field) -> Region:
        """
        Compute the region with the maximum value of the field.
        """
        return self._minmax(field, max)

    def min(self, field) -> Region:
        """
        Compute the region with the minimum value of the field.
        """
        return self._minmax(field, min)

    def _minmax(self, field, fn):
        values = ((r, v) for r in self._regions if (v := r[field]) is not None)
        region, _ = fn(values, key=lambda x: x[1])
        return region

    def to_dict(self, fields: Sequence[str] = ('name',)) -> list:
        """
        Convert Regions to a list of dictionary including all requested
        attributes.
        """
        return [r.to_dict(fields) for r in self]

    def to_dataframe(self, fields: Sequence[str] = ('name',)) -> pd.DataFrame:
        """
        Convert Region to a pandas Series element including the requested
        attributes.
        """
        return pd.DataFrame(self.to_dict(fields)).set_index("id")

    #
    # Hierarchies
    #
    @property
    def parent(self) -> Optional["Region"]:
        """
        Parent region to all included sub-regions.
        """
        pk = self.parent_id
        return Region(pk) if pk else None
