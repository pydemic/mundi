from functools import lru_cache
from typing import Optional, Iterator, TYPE_CHECKING
from weakref import WeakValueDictionary

import pandas as pd
from sqlalchemy import literal

from .. import db

REGION_DB = WeakValueDictionary()

if TYPE_CHECKING:
    from .region_set import RegionSet


class Region:
    """
    Represent entries/rows in the mundi db.
    """

    name: str
    _get_column = staticmethod(db.Universe.REGION.column)
    
    # Attributes from standard plugins
    population: Optional[int]
    age_distribution: Optional[pd.Series]
    age_pyramid: Optional[pd.DataFrame]

    def __new__(cls, ref, unsafe=False):
        if not isinstance(ref, str):
            raise TypeError(f"expect string, got {ref.__class__.__name__}")
        if unsafe:
            new = object.__new__(Region)
            new.__dict__["id"] = ref
            return new
        return make_region(ref)

    def __hash__(self):
        return hash(self.id)

    def __getnewargs__(self):
        return self.id,

    def __getitem__(self, key):
        try:
            column = self._get_column(key)
            value = column.get(self.id)
        except AttributeError as ex:
            raise RuntimeError(ex) from ex
        self.__dict__[key] = value
        return value

    def __setattr__(self, attr, value):
        raise AttributeError("Region objects are immutable")

    def __getattr__(self, item):
        if item.startswith("_") or hasattr(type(self), item):
            raise AttributeError(item)
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)

    def __repr__(self):
        try:
            args = f", name={self.name!r}"
        except AttributeError:
            args = ""
        return f"{self.__class__.__name__}({self.id!r}{args})"

    def __str__(self):
        return (
            f"Mundi Region\n"
            f"id       : {self.id}\n"
            f"name     : {self.name}\n"
            f"type     : {self.type}\n"
            f"subtype  : {self.subtype}"
        )

    def __lt__(self, other):
        if isinstance(other, Region):
            return self.id < other.id
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, Region):
            return self.id > other.id
        return NotImplemented

    #
    # Conversions
    #
    def to_dict(self, fields: str = ('name',)) -> dict:
        """
        Convert Region to a dictionary including all requested attributes.
        """
        out = {"id": self.id}
        for field in fields:
            out[field] = self[field]
        return out

    def to_series(self, fields: str = ('name',)) -> pd.Series:
        """
        Convert Region to a pandas Series element including the requested
        attributes.
        """
        return pd.Series(self.to_dict(fields))

    #
    # Hierarchies
    #
    @property
    def parent(self) -> Optional["Region"]:
        pk = self["parent_id"]
        return Region(pk) if pk else None

    def children(self, relation="default", *, deep=False, name=None) -> "RegionSet":
        """
        Return a RegionSet with all children.
        """
        from .region_set import RegionSet

        if name is None:
            name = f"{self.id} children"
        if relation in ("*", "all"):
            relation = None
        ids = iter(self._children_ids(relation, deep))
        return RegionSet(ids, name=name)

    def children_dataframe(
            self, relation="default", columns=("name",), *, deep=False
    ) -> pd.DataFrame:
        """
        Return a dataframe with all children.
        """
        raise NotImplementedError

    def _children_ids(self, relation, deep, max_depth=32) -> Iterator[str]:
        # FIXME: this query works, but is highly inefficient.
        if deep:
            if max_depth <= 0:
                raise ValueError("maximum depth reached")
            memo = set()
            for ref in self._children_ids(relation, False, max_depth - 1):
                yield ref
                for child_ref in Region(ref)._children_ids(relation, True, max_depth - 1):
                    if child_ref not in memo:
                        yield child_ref
                        memo.add(ref)

        table = db.RegionM2M
        query_args = [table.parent_id == literal(self.id)]
        if relation is not None:
            query_args.append(table.relation == relation)

        query = db.session().query(table).filter(*query_args).values(table.child_id)
        for (ref,) in query:
            yield ref

    def parents(self):
        """
        Return list of parents.
        """
        raise NotImplementedError


@lru_cache(1024)
def make_region(ref):
    """
    Create Region object from ref.
    """
    try:
        return REGION_DB[ref]
    except KeyError:
        if db.session().query(db.Region).filter(db.Region.id == ref).first() is None:
            raise ValueError("region does not exist")

        new = object.__new__(Region)
        new.__dict__["id"] = ref
        REGION_DB[ref] = new
        return new


def as_region(region) -> Region:
    """
    Convert string to Region.
    """
    if isinstance(region, Region):
        return region
    else:
        return Region(region)
