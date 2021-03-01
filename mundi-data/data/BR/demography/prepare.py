import pandas as pd
import sidekick.api as sk

from mundi.plugins.demography import DemographyData


class BrazilianDemography(DemographyData):
    """
    Demographic data from Brazilian sub-divisions.

    Data extracted from:

    FREIRE, F.H.M.A; GONZAGA, M.R; QUEIROZ, B.L. Projeção populacional municipal
    com estimadores bayesianos, Brasil 2010 - 2030. In: Sawyer, D.O (coord.).
    Seguridade Social Municipais. Projeto Brasil 3 Tempos. Secretaria  Especial
    de Assuntos Estratégicos da Presidência da República (SAE/SG/PR) , United
    Nations Development Programme, Brazil (UNDP) and International Policy Centre
    for Inclusive Growth. Brasília (IPC-IG), 2019
    """

    @sk.lazy
    def raw_historic_age_pyramid(self):
        """
        Return a dataframe with age distribution separating location, year and
        gender into different columns.
        """
        df = self.read_csv("age-distribution.csv.gz", index_col=None)
        df = df.drop(columns=["name", "state", "total"])
        df["id"] = df.pop("code").apply(lambda x: f"BR-{x}")
        df["95"] = df["100"] = 0
        df["gender"] = df["gender"].replace({"f": "female", "m": "male"}).astype("string")
        columns = ["id", "gender", "year", *map(str, range(0, 101, 5))]
        return df[columns]

    @sk.lazy
    def age_pyramid(self):
        df: pd.DataFrame = self.raw_historic_age_pyramid.demography.now()
        df = df.demography.unpivot_pyramid("gender").astype(self.dtype)
        df.columns = self.age_pyramid_columns()
        return df

    @sk.lazy
    def historic_age_pyramid(self):
        df = self.raw_historic_age_pyramid.demography.unpivot_pyramid("gender")
        df.columns = self.age_pyramid_columns()
        return df.astype(self.dtype)


def fix_columns(df, name):
    """
    Create multi-index for male/female columns of age distributions
    """
    df.columns = pd.MultiIndex.from_tuples(
        ((name, int(x)) for x in df.columns), names=["gender", "age"]
    )
    return df


if __name__ == "__main__":
    BrazilianDemography.cli()
