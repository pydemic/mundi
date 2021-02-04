from types import ModuleType
from typing import Union

import numpy as np
import pandas as pd
import sidekick.api as sk

from .. import db
from ..pipeline import DataIO, Collector

ModRef = Union[str, ModuleType]


class DemographyData(DataIO):
    """
    Base data extractor class for demography plugin.
    """

    DEMOGRAPHY_DATA_TYPES = {
        "population": "uint32",
        "age_distribution": {"uint32", object},
        "age_pyramid": {"uint32", object},
    }
    HISTORIC_DEMOGRAPHY_DATA_TYPES = {
        "region_id": "string",
        "year": "uint16",
        "population": "uint32",
        "age_distribution": {"uint32", object},
        "age_pyramid": {"uint32", object},
    }

    def extend_dataframe(self, df, attr, extra=("", ""), name=None):
        src: pd.DataFrame = getattr(self, attr)
        name = name or attr
        if src is pd.NA:
            df[(name, *extra)] = pd.NA
            return df
        src.columns = multi_index(name, src.columns)
        if src.columns.nlevels == 2:
            src.columns = pd.MultiIndex.from_tuples((*t, "") for t in src.columns)
        return pd.concat([df, src], axis=1)

    @sk.lazy
    def population(self):
        if self.age_distribution is pd.NA:
            raise ValueError("must provide population or age distribution")
        return self.age_distribution.sum(axis=1).astype("uint32")

    @sk.lazy
    def age_distribution(self):
        if self.age_pyramid is pd.NA:
            return pd.NA
        return (self.age_pyramid["female"] + self.age_pyramid["male"]).astype("uint32")

    @sk.lazy
    def age_pyramid(self):
        return pd.NA

    @sk.lazy
    def demography(self):
        df = pd.DataFrame({("population", "", ""): self.population})
        df = self.extend_dataframe(df, "age_distribution")
        df = self.extend_dataframe(df, "age_pyramid")
        df.columns = pd.MultiIndex.from_tuples(df.columns)
        return df.fillna(pd.NA)

    @sk.lazy
    def historic_population(self):
        df = self.historic_age_distribution
        if df is pd.NA:
            raise ValueError("must provide population or age distribution")
        return df.sum(axis=1).astype("uint32")

    @sk.lazy
    def historic_age_distribution(self):
        df = self.historic_age_pyramid
        if df is pd.NA:
            return pd.NA
        return (df["female"] + df["male"]).astype("uint32")

    @sk.lazy
    def historic_age_pyramid(self):
        return pd.NA

    @sk.lazy
    def historic_demography(self):
        df = pd.DataFrame({("population", "", ""): self.historic_population})
        df = self.extend_dataframe(
            df, "historic_age_distribution", name="age_distribution"
        )
        df = self.extend_dataframe(df, "historic_age_pyramid", name="age_pyramid")
        df.columns = pd.MultiIndex.from_tuples(df.columns)
        df = (
            df.reset_index()
            .rename(columns={"id": "region_id"})
            .astype({("region_id", "", ""): "string", ("year", "", ""): "uint16"})
        )
        return df

    def collect(self):
        return {
            "demography": self.demography,
            "historic_demography": self.historic_demography,
        }


@Collector.register("historic_demography")
class HistoricDemographyCollector(Collector):
    duplicate_indexes = ["region_id", "year"]
    auto_index = True


class DemographyPlugin(db.Plugin):
    """
    Demography plugin gather information from various data sources concerning
    basic demographic parameters of a population.

    * population: Total population
    * age_distribution: Age distribution for groups of 5 years
    * age_pyramid: Age distribution disaggregated by sex
    """

    name = "demography"
    tables = {"demography": db.Demography, "historic_demography": db.HistoricDemography}
    data_tables = {"demography"}
    transformers = {
        "age_distribution": "to_distribution",
        "age_pyramid": "to_pyramid",
    }

    def to_distribution(self, raw):
        """
        Convert raw Series of bytes content into a dataframe with
        age_distribution.
        """
        if raw is None:
            return None
        data = np.fromstring(raw, dtype="uint32")
        n = 5 * len(data)
        index = pd.RangeIndex(0, n, 5, name="age")
        return pd.Series(data, index=index, name="age_distribution")

    def to_pyramid(self, raw):
        """
        Convert raw Series of bytes content into a dataframe with
        age_pyramid.
        """
        if raw is None:
            return None
        data = np.fromstring(raw, dtype="uint32")
        female = data[: len(data) // 2]
        male = data[len(female) :]
        n = 5 * len(female)
        index = pd.RangeIndex(0, n, 5, name="age")
        return pd.DataFrame({"female": female, "male": male}, index=index)


def multi_index(prefix, idxs):
    """
    Add same prefix level in all entries of index.
    """
    if idxs.nlevels == 1:
        tuples = ((prefix, idx) for idx in idxs)
    else:
        tuples = ((prefix, *idx) for idx in idxs)
    return pd.MultiIndex.from_tuples(tuples)


if __name__ == "__main__":
    DemographyPlugin.cli()
