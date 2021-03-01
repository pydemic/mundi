import pandas as pd
from sqlalchemy import (
    Column,
    String,
    ForeignKey,
    Integer,
    Boolean,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, backref

from .core import Universe, ComputedColumn, TableInfo, Table, session

#
# HDF5 tables
#
AgeDistributionsInfo = TableInfo.registered(
    "age_distributions",
    universe=Universe.REGION,
    columns=("all", "female", "male"),
    export_columns=[
        lambda table: ComputedColumn(
            "age_distribution",
            table,
            arguments=("age_distributions.all",),
            to_scalar=lambda x: x,
            to_table=lambda x: x,
        ),
        lambda table: ComputedColumn(
            "age_pyramid",
            table,
            arguments=("age_distributions.female", "age_distributions.male"),
            to_scalar=lambda f, m: pd.DataFrame({"female": f, "male": m}),
            to_table=lambda f, m: pd.concat({"female": f, "male": m}, axis=1),
        ),
    ],
    internal_columns=True,
)
HistoricAgeDistributions = TableInfo.registered(
    "historic_age_distributions",
    universe=Universe.HISTORIC,
    columns=("all", "female", "male"),
    internal_columns=True,
)


#
# Special columns
#
def mundi_ref(key=None, primary_key=False, **kwargs) -> Column:
    """
    Create a foreign key reference to a mundi id. This method fixes the
    correct data type for char-based foreign key relations.

    Args:
        key:
            Name of the table.column to point to as a ForeignKey relationship.
        primary_key:
            If True, declares field as a primary key column for database.

    Keyword Args:
        Keywords are forwarded to the Column() constructor.
    """
    if not key and not primary_key:
        raise TypeError("either key or primary key must be given!")
    if not primary_key:
        kwargs.setdefault("index", True)
    else:
        kwargs["primary_key"] = True

    if key is not None:
        return Column(String(16), ForeignKey(key), **kwargs)
    return Column(String(16), **kwargs)


def population_column(**kwargs):
    return Column(
        Integer(),
        doc="Total population. Computed from age_distribution, when available.",
        index=True,
        nullable=False,
        **kwargs,
    )


def id_column(**kwargs):
    return mundi_ref(
        key=Region.id,
        primary_key=True,
        doc="Unique identifier for each region. Primary key.",
        **kwargs,
    )


class MundiRegistry(Table):
    """
    Register mundi elements
    """

    __tablename__ = "mundi_registry"

    is_joinable = False
    plugin_key = Column(String(64), primary_key=True)
    table_name = Column(String(64), primary_key=True)
    is_populated = Column(Boolean, default=False)

    @classmethod
    def has_populated_table(cls, plugin, table=None):
        """
        Return true if registry has populated tables.
        """
        if not table:
            tables = plugin.__tables__
            return all(cls.has_populated_table(plugin, table) for table in tables)

        key = getattr(plugin, "plugin_key", plugin)
        query = session().query(MundiRegistry).filter_by(plugin_key=key, table_name=table)
        return getattr(query.first(), "is_populated", False)


#
# Main region tables
#
class Region(Table):
    """
    Basic representation of Mundi regions.
    """

    __tablename__ = "region"
    is_joinable = True
    keep_id = True
    universe = Universe.REGION

    id = mundi_ref(
        primary_key=True,
        doc="Unique identifier for each region. This derived from alpha2 codes "
            "and works as a primary key.",
    )
    name = Column(
        String,
        doc="Human-readable name of region (English version).",
        nullable=False,
        index=True,
    )
    type = Column(
        String(32),
        doc="Region main type. Usually must be queried with subtype to "
            "specify different kinds of regions.",
        nullable=False,
        index=True,
    )
    subtype = Column(
        String(32),
        doc="Optional Region sub-type. This helps to differentiate "
            "sub-categories of regions",
        index=True,
    )
    short_code = Column(
        String(16),
        doc="Small identification code (uses ISO alpha-2 for countries)",
        index=True,
    )
    long_code = Column(
        String(32),
        doc="A longer identification code (uses ISO alpha-3 for countries)",
        index=True,
    )
    numeric_code = Column(
        String(32),
        doc="An optional numeric identification for region. "
            "It is stored as string since the number of digits may convey meaning.",
        index=True,
    )
    country_id = mundi_ref(
        "region.id",
        doc="Country id for regions within a country. "
            "Only applies to regions within countries.",
        index=True,
    )
    parent_id = mundi_ref(
        "region.id",
        doc="Reference to parent element. Access object using the 'parent' relationship",
        index=True,
    )
    region = Column(
        String(16),
        doc="United nations classification for world regions. "
            "Sub-regions inherit from parents.",
        index=True,
    )
    level = Column(
        Integer,
        doc="Level in the mundi hierarchy. Level starts at 0 = XX/World and "
            "increases by one at each additional nesting.",
        index=True,
    )
    children = relationship(
        "Region", backref=backref("parent", remote_side=[id]), foreign_keys=[parent_id]
    )
    subdivisions = relationship(
        "Region", backref=backref("country", remote_side=[id]), foreign_keys=[country_id]
    )

    def __repr__(self):
        return f"<Region id={self.id!r}, name={self.name!r}>"


class RegionM2M(Table):
    """
    Alternative hierarchy of regions.
    """

    __tablename__ = "region_m2m"
    __table_args__ = (
        PrimaryKeyConstraint("parent_id", "child_id", "relation", name="pk"),
    )
    is_joinable = False
    parent_id = mundi_ref("region.id")
    child_id = mundi_ref("region.id")
    relation = Column(String(32))
    parent = relationship(
        "Region",
        backref=backref("parents_alt", remote_side="Region.id"),
        foreign_keys=[parent_id],
    )
    child = relationship(
        "Region",
        backref=backref("children_alt", remote_side="Region.id"),
        foreign_keys=[child_id],
    )


class RegionDataMixin:
    is_joinable = True
    universe = Universe.REGION

    @property
    def id(self):
        raise NotImplementedError

    @property
    def region(self):
        raise NotImplementedError


class Demography(RegionDataMixin, Table):
    __tablename__ = "demography"

    id = id_column()
    population = population_column()
    region = relationship("Region")


class HistoricDemography(Table):
    __tablename__ = "historic_demography"
    __table_args__ = (UniqueConstraint("region_id", "year"),)
    universe = Universe.HISTORIC

    pk = Column(Integer, primary_key=True, autoincrement=True)
    region_id = mundi_ref(
        key=Region.id,
        doc="Unique identifier for each region. Primary key.",
    )
    year = Column(
        Integer(),
        doc="Year in which population information was collected.",
        index=True,
        nullable=False,
    )
    population = population_column()
    region = relationship("Region")


class Healthcare(RegionDataMixin, Table):
    __tablename__ = "healthcare"

    id = id_column()
    hospital_capacity = Column(
        Integer,
        default=0,
        nullable=False,
        doc="Total capacity of clinical beds in hospitals and clinics.",
    )
    hospital_capacity_public = Column(
        Integer, default=0, nullable=False, doc="Total capacity in the public sector."
    )
    icu_capacity = Column(
        Integer,
        default=0,
        nullable=False,
        doc="Total capacity of ICU beds in hospitals.",
    )
    icu_capacity_public = Column(
        Integer,
        default=0,
        nullable=False,
        doc="Total capacity of ICUs in the public sector.",
    )
    region = relationship("Region")
