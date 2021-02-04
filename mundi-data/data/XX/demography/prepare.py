import sidekick as sk

import mundi
from mundi.plugins.demography import DemographyData


class Countries(DemographyData):
    """
    Collect basic country information.

    Data from United Nations, Population Division, Department of Economic and
    Social Affairs

    World Population Prospects 2019

    File POP/15-1: Annual total population (both sexes combined) by five-year age group,
    region, subregion and country, 1950-2100 (thousands)
    Estimates, 1950 - 2020
    POP/DB/WPP/Rev.2019/POP/F15-1

    © August 2019 by United Nations, made available under a Creative Commons license CC
    BY 3.0 IGO: http://creativecommons.org/licenses/by/3.0/igo/
    Suggested citation: United Nations, Department of Economic and Social Affairs,
    Population Division (2019). World Population Prospects 2019, Online Edition. Rev. 1.
    """

    @sk.lazy
    def data_source(self):
        code_map = (
            mundi.countries_dataframe(["short_code", "numeric_code"])
            .set_index("numeric_code")["short_code"]
            .to_dict()
        )

        # Read raw data and transform a few columns
        data = self.read_csv("age-distribution.csv.gz", index_col=None).astype(
            {"year": "int32"}
        )
        data["code"] = data["code"].apply(lambda x: f"{x:03}")
        data["id"] = data["code"].apply(code_map.get)

        # Channel Islands is not present in the Mundi database.
        # TODO: investigate it. Is Pycountry using an old ISO standard? Is it not
        # registered in ISO? Is is just a weird geographical denomination?
        removed = set(data[data["id"].isna()]["name"])
        if removed:
            print(f"WARNING: removed items {removed}")

        # Reorganize data
        data = (
            data[data["id"].notna()]
            .drop(columns=["code", "name"])
            .set_index(["id", "year"])
        )
        data = data.applymap(
            lambda x: 1000 * (x if isinstance(x, int) else int(x.replace(" ", "")))
        ).astype(int)
        data.columns = map(int, data)
        return data

    @sk.lazy
    def age_distribution(self):
        return (
            self.data_source.reset_index()
            .groupby("id")
            .max("year")
            .drop(columns="year")
            .fillna(0)
            .astype("uint32")
        )

    @sk.lazy
    def historic_age_distribution(self):
        return self.data_source.fillna(0).astype("uint32")


if __name__ == "__main__":
    Countries.cli()
