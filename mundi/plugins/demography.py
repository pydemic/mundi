from types import ModuleType
from typing import Union

import pandas as pd
import sidekick.api as sk

from .. import db
from ..pipeline import DataIO, Collector, HDF5Importer

ModRef = Union[str, ModuleType]


class DemographyData(DataIO):
    """
    Base data extractor class for demography plugin.
    """

    dtype = "uint32"
    na_value = 2 ** 32 - 1
    _age_distribution_range = range(0, 101, 5)
    _age_distribution_types = {i: "uint32" for i in _age_distribution_range}
    table_dtypes = {
        "demography": {"population": "uint32"},
        "historic_demography": {"population": "uint32"},
        "age_distributions": {
            "all": _age_distribution_types,
            "male": _age_distribution_types,
            "female": _age_distribution_types,
        },
        # 'historic_age_distributions': {
        #     "all": "uint32", "male": "uint32", "female": "uint32"
        # },
    }

    @sk.lazy
    def demography(self):
        return pd.DataFrame({"population": self.population})

    @sk.lazy
    def population(self) -> pd.DataFrame:
        df = self.age_distribution.sum(axis=1).astype(self.dtype)
        df.index.name = "id"
        return df

    @sk.lazy
    def age_distributions(self):
        return {
            "all": self.age_distribution,
            "female": self.age_pyramid["female"],
            "male": self.age_pyramid["male"],
        }

    @sk.lazy
    def age_distribution(self) -> pd.DataFrame:
        df = (self.age_pyramid["female"] + self.age_pyramid["male"]).astype(self.dtype)
        df.index.name = "id"
        return df

    @sk.lazy
    def age_pyramid(self) -> pd.DataFrame:
        df = self.empty_age_pyramid()
        df.index = df.index = pd.Index([], name="id")
        return df

    @sk.lazy
    def historic_demography(self):
        return pd.DataFrame({"population": self.historic_population})

    @sk.lazy
    def historic_population(self):
        df = self.historic_age_distribution.sum(axis=1).astype(self.dtype)
        df.index.names = ["region_id", "year"]
        return df

    @sk.lazy
    def historic_age_distributions(self):
        return {
            "all": self.historic_age_distribution,
            "female": self.historic_age_pyramid["female"],
            "male": self.historic_age_pyramid["male"],
        }

    @sk.lazy
    def historic_age_distribution(self):
        df = self.historic_age_pyramid
        df = (df["female"] + df["male"]).astype(self.dtype)
        df.index.names = ["id", "year"]
        return df

    @sk.lazy
    def historic_age_pyramid(self):
        df = self.empty_age_pyramid()
        df.index = pd.MultiIndex.from_tuples([], names=["id", "year"])
        return df

    def age_distribution_columns(self) -> pd.MultiIndex:
        return pd.MultiIndex.from_tuples(range(0, 101, 5), names=("age",))

    def age_pyramid_columns(self) -> pd.MultiIndex:
        cols = [
            *(("female", i) for i in range(0, 101, 5)),
            *(("male", i) for i in range(0, 101, 5)),
        ]
        return pd.MultiIndex.from_tuples(cols, names=("gender", "age"))

    def empty_age_distribution(self, **kwargs) -> pd.DataFrame:
        return self._empty_dataframe(self.age_distribution_columns())

    def empty_age_pyramid(self, **kwargs) -> pd.DataFrame:
        return self._empty_dataframe(self.age_pyramid_columns())

    def _empty_dataframe(self, columns: pd.Index, index=None) -> pd.DataFrame:
        n = len(columns)
        m = 0 if index is None else len(index)
        return pd.DataFrame(
            [[pd.NA] * n] * m, dtype=self.dtype, columns=columns, index=index
        )


@Collector.register("age_distributions", universe=db.Universe.REGION)
@Collector.register("historic_demography", universe=db.Universe.HISTORIC)
@Collector.register("demography", universe=db.Universe.REGION)
class DemographyCollector(Collector):
    DEFAULT_FILL_POLICY = Collector.FILL_SUM_CHILDREN


class DemographyPlugin(db.Plugin):
    """
    Demography plugin gather information from various data sources concerning
    basic demographic parameters of a population.

    * population: Total population
    * age_distribution: Age distribution for groups of 5 years
    * age_pyramid: Age distribution disaggregated by sex
    """

    name = "demography"
    tables = [
        db.Demography.info,
        db.AgeDistributionsInfo,
        # db.HistoricDemography.info,
        # db.HistoricAgeDistributionsInfo,
    ]
    importers = {
        "age_distributions": HDF5Importer,
        "historic_age_distributions": HDF5Importer,
    }
    data_tables = {"demography"}


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
