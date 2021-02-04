from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,
    LargeBinary,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship, backref

from .database import Base, MundiRef


class MundiRegistry(Base):
    """
    Register mundi elements
    """

    __tablename__ = "mundi_registry"

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
        query = (
            new_session().query(MundiRegistry).filter_by(plugin_key=key, table_name=table)
        )
        return getattr(query.first(), "is_populated", False)


#
# Main region tables
#
class Region(Base):
    """
    Basic representation of Mundi regions.
    """

    __tablename__ = "region"

    id = MundiRef(
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
    country_id = MundiRef(
        "region.id",
        doc="Country id for regions within a country. "
        "Only applies to regions within countries.",
        index=True,
    )
    parent_id = MundiRef(
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


class RegionM2M(Base):
    """
    Alternative hierarchy of regions.
    """

    __tablename__ = "region_m2m"
    __table_args__ = (
        PrimaryKeyConstraint("parent_id", "child_id", "relation", name="pk"),
    )

    parent_id = MundiRef("region.id")
    child_id = MundiRef("region.id")
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
    @declared_attr
    def id(cls):
        return MundiRef(
            key=Region.id,
            primary_key=True,
            doc="Unique identifier for each region. Primary key.",
        )

    @declared_attr
    def region(cls):
        return relationship("Region")


class DemographyMixin:
    """
    Basic container for demographic data.

    Tabular data is packaged into numpy arrays, which are then serialized with
    ndarray.to_string() method.
    """

    population = Column(
        Integer(),
        doc="Total population. Computed from age_distribution, when available.",
        index=True,
        nullable=False,
    )
    age_distribution = Column(
        LargeBinary(),
        doc="Binary blob representing age distribution in 5 years bins. "
        "Store a numpy array. Computed from age_pyramid, when available.",
    )
    age_pyramid = Column(
        LargeBinary(),
        doc="Binary blob representing gender-stratified age distribution in 5 "
        "years bins. Store a numpy array.",
    )


class Demography(RegionDataMixin, DemographyMixin, Base):
    __tablename__ = "demography"


class HistoricDemography(DemographyMixin, Base):
    __tablename__ = "historic_demography"
    __table_args__ = (UniqueConstraint("region_id", "year"),)

    pk = Column(Integer, primary_key=True, autoincrement=True)
    region_id = MundiRef(
        key=Region.id,
        doc="Unique identifier for each region. Primary key.",
    )
    year = Column(
        Integer(),
        doc="Year in which population information was collected.",
        index=True,
        nullable=False,
    )
    region = relationship("Region")


class Healthcare(RegionDataMixin, Base):
    __tablename__ = "healthcare"

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
