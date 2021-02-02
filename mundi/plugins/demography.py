import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Union, Tuple

import numpy as np
import pandas as pd
import sidekick.api as sk
from sqlalchemy import Column, Integer, LargeBinary
from sqlalchemy.orm import relationship

from mundi.db import MundiRef
from ..db import Plugin, Base, Fill
from ..source import Data

ModRef = Union[str, ModuleType]


class Demography(Base):
    """
    Basic container for demographic data.

    Tabular data is packaged into numpy arrays, which are then serialized with
    ndarray.to_string() method.
    """

    __tablename__ = "demography"

    id = MundiRef(primary_key=True, doc="Unique identifier for each region. Primary key.")
    reference_year = Column(
        Integer(), doc="Year in which population information was collected."
    )
    population = Column(Integer(), doc="Total population")
    age_distribution = Column(
        LargeBinary(), doc="Binary blob representing age distribution in 5 years bins."
    )
    age_pyramid = Column(
        LargeBinary(),
        doc="Binary blob representing gender-stratified age distribution in 5 "
        "years bins.",
    )
    region = relationship  # One-to-one relations...


class DemographyData(Data):
    """
    Base data extractor class for demography plugin.
    """


class Demography(Plugin):
    """
    Demography plugin gather information from various data sources concerning
    basic demographic parameters of a population.

    * population: Total population
    * age_distribution: Age distribution for groups of 5 years
    * age_pyramid: Age distribution disaggregated by sex
    """

    population: int = Column(
        aggregation=Fill.SUM_CHILDREN,
        description="Total population (estimated for 2020).",
    )
    age_distribution: int = Column(
        index=pd.Index(sk.nums(0, 5, ..., 100)),
        aggregation=Fill.SUM_CHILDREN,
        description="Age distribution in groups of 5 years.",
    )
    age_pyramid: int = Column(
        index=pd.Index(sk.nums(0, 5, ..., 100)),
        columns=["male", "female"],
        aggregation=Fill.SUM_CHILDREN,
        description="Age distribution disaggregated by sex (male/female).",
    )

    def age_pyramid__value(self, db, ref, infer=False) -> pd.DataFrame:
        """
        Return the age pyramid for the given region.
        """
        try:
            return self.value(db, "age_pyramid", ref)
        except LookupError:
            if not infer:
                raise

        ages = self.value(db, "age_distribution", ref)
        if infer is True:
            male = ages // 2
        else:
            male = (ages * infer).astype(int)
        female = ages - male
        return pd.DataFrame({"male": male, "female": female})


def age_distribution(df):
    """
    Return age_distribution for given region or collection of regions.
    """
    data, is_row = loader("mundi_demography", "age-distribution", df)
    data.name = "age_distribution"
    return data


def population(df):
    """
    Return population for given region or collection of regions.
    """
    data, is_row = loader("mundi_demography", "population", df)
    if not is_row:
        return data["population"]
    else:
        return np.sum(data)


def loader(package: ModRef, db_name, idx) -> Tuple[pd.DataFrame, bool]:
    """
    Load distribution from package.

    Return a tuple of (Data, is_row). The boolean "is_row" tells
    the returned data concerns a collection of items or a single row in the
    database.
    """

    db = database(package, db_name + ".pkl.gz")

    if isinstance(idx, (pd.DataFrame, pd.Series)):
        idx = idx.index
    elif isinstance(idx, str):
        idx = mundi.code(idx)
        df, _ = loader(package, db_name, [idx])
        return df.iloc[0], True

    # Try to get from index
    reindex = db.reindex(idx)
    return reindex, False


@lru_cache(32)
def database(package, name):
    """Lazily load db from name"""

    if isinstance(package, str):
        package = sys.modules[package]
    path = Path(package.__file__).parent.absolute()
    db_path = path / "databases" / name
    return pd.read_pickle(db_path)
