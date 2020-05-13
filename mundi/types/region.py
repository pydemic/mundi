from typing import Optional
from weakref import WeakValueDictionary

import pandas as pd

from ..db import db

REGION_PLUGINS = {}
REGION_DB = WeakValueDictionary()


class Region:
    """
    Represent entries/rows in the mundi db.
    """

    name: str

    def __new__(cls, id):
        if not isinstance(id, str):
            raise TypeError(f"expect string, got {id.__class__.__name__}")
        try:
            return REGION_DB[id]
        except KeyError:
            new = object.__new__(cls)
            new.__dict__["id"] = id
            REGION_DB[id] = new
            return new

    def __hash__(self):
        return hash(self.id)

    def __getstate__(self):
        return self.id

    def __setstate__(self, id):
        self.__dict__["id"] = id

    def __getitem__(self, key):
        return self._get_field(key)

    def __setattr__(self, attr, value):
        raise AttributeError("Region objects are immutable")

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError(item)
        try:
            return self._get_field(item)
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

    def _get_field(self, key):
        fget = REGION_PLUGINS[key]
        self.__dict__[key] = value = fget(self)
        return value

    #
    # Conversions
    #
    def to_dict(self, *fields: str) -> dict:
        """
        Convert Region to a dictionary including all requested attributes.
        """
        get = self._get_field
        return {"id": self.id, "name": self.name, **{f: get(f) for f in fields}}

    def to_series(self, *fields: str) -> pd.Series:
        """
        Convert Region to a pandas Series element including the requested
        attributes.
        """
        return pd.Series(self.to_dict(*fields))

    #
    # Hierarchies
    #
    @property
    def parent(self) -> Optional["Region"]:
        pk = self.parent_id
        return Region(pk) if pk else None

    def children(self, dataframe=False, deep=False, which="both"):
        """
        Return list of children.
        """
        if dataframe:
            raise NotImplementedError

        primary = which in ("primary", "both")
        secondary = which in ("secondary", "both")
        ids = self._children_ids(primary, secondary)
        if deep:
            non_processed = ids.copy()
            while non_processed:
                id = non_processed.pop()
                new = Region(id)._children_ids(primary, secondary)
                non_processed.update(new - ids)
                ids.update(new)

        return [Region(id) for id in ids]

    def _children_ids(self, primary, secondary) -> set:
        ids = set()

        if primary:
            res = db.query(parent_id=self.id, cols=("id",))
            ids.update(res["id"])

        if secondary:
            cmd = f"SELECT id FROM mundi WHERE alt_parents LIKE '%;{self.id}%';"
            ids.update(map(lambda x: x[0], db.raw_sql(cmd)))

        return ids

    def parents(self, dataframe=False):
        """
        Return list of parents.
        """
        raise NotImplementedError
