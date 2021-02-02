from typing import Optional
from weakref import WeakValueDictionary
from collections.abc import Set
import pandas as pd

from .region import as_region


class RegionSet(Set):
    """
    Represent a set of regions in mundi db.
    """

    name: str
    __slots__ = "name", "year", "_regions", "_cache"

    def __init__(self, regions, name=None, year=None):
        self.name = name
        self.year = year
        self._cache = {}
        self._regions = frozenset(map(as_region, regions))

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
        raise AttributeError(item)

    def _get_field(self, key):
        fget = REGION_PLUGINS[key]
        self.__dict__[key] = value = fget(self)
        return value

    #
    # Conversions
    #
    def to_dict(self, *fields: str) -> list:
        """
        Convert Regions to a list of dictionary including all requested
        attributes.
        """
        return [r.to_dict(*fields) for r in self]

    def to_dataframe(self, *fields: str) -> pd.DataFrame:
        """
        Convert Region to a pandas Series element including the requested
        attributes.
        """
        return pd.DataFrame(self.to_dict(*fields))

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
