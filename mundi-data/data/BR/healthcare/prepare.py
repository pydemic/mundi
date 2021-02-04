import pandas as pd
import sidekick.api as sk

import mundi
from mundi.pipeline.data import DataIO


class HealthcareData(DataIO):
    """
    Data about healthcare facilities extracted from CNES (02/2020)
    """

    @sk.lazy
    def healthcare(self):
        cities = mundi.regions_dataframe(country_id="BR", type="city")
        cities = (
            cities.mundi["short_code"]
            .reset_index()
            .rename(columns={"short_code": "city_id"})
        )

        data = self.read_csv("cnes.csv", dtype={"city_id": "string"}, index_col=None)
        data = (
            data[["city_id", "clinical", "clinical_sus", "icu", "icu_sus"]]
            .set_index("city_id")
            .groupby("city_id")
            .sum()
            .reset_index()
        )

        return (
            pd.merge(cities, data, on="city_id")
            .drop(columns="city_id")
            .set_index("id")
            .rename(
                {
                    "clinical": "hospital_capacity",
                    "clinical_sus": "hospital_capacity_public",
                    "icu": "icu_capacity",
                    "icu_sus": "icu_capacity_public",
                },
                axis=1,
            )
            .astype("uint32")
        )

    def collect(self):
        return {"healthcare": self.healthcare}


if __name__ == "__main__":
    HealthcareData.cli()
