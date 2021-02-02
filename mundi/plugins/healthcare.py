from types import ModuleType
from typing import Union

from ..db import Plugin, Column
from mundi.enums import Fill

ModRef = Union[str, ModuleType]


class Healthcare(Plugin):
    """
    Demography plugin gather information from various data sources concerning
    basic demographic parameters of a population.

    * population: Total population
    * age_distribution: Age distribution for groups of 5 years
    * age_pyramid: Age distribution disaggregated by sex
    """

    hospital_capacity: int = Column(
        aggregation=Fill.SUM_CHILDREN,
        description="Total capacity of clinical beds in hospitals and clinics.",
    )
    hospital_capacity_public: int = Column(
        aggregation=Fill.SUM_CHILDREN, description="Total capacity in the public sector."
    )
    icu_capacity: int = Column(
        aggregation=Fill.SUM_CHILDREN,
        description="Total capacity of ICU beds in hospitals.",
    )
    icu_capacity_public: int = Column(
        aggregation=Fill.SUM_CHILDREN,
        description="Total capacity of ICUs in the public sector.",
    )
