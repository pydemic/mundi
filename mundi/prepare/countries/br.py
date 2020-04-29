from collections import namedtuple

import pandas as pd
import sidekick as sk

from .. import pycountry
from ..region import Region


class BrRegions(Region):
    @sk.lazy
    def _states(self):
        subs = pycountry.sub_divisions()
        return subs[subs["country_code"] == "BR"]

    state_names = sk.lazy(lambda br: br._states["name"].to_dict())
    state_codes = sk.lazy(lambda br: {v: k for k, v in br.state_names.items()})

    @sk.lazy
    def dtb(self):
        states = {v: k.split("-")[1] for k, v in self._states["name"].to_dict().items()}

        # Read raw data from IBGE
        dtb = pd.read_csv(self.DATA / "br" / "divisao-do-territorio-distritos.csv")
        dtb.columns = [
            "state",
            "state_name",
            "meso_code",
            "meso_name",
            "micro_code",
            "micro_name",
            "city_short_code",
            "city_code",
            "city_name",
            "district_short_code",
            "district_code",
            "district_name",
        ]
        dtb["state_code"] = dtb["state"]
        dtb["state"] = dtb["state_name"].apply(states.__getitem__)
        return dtb

    @sk.lazy
    def regions(self):
        df = pd.DataFrame(
            [
                ["Norte", "1"],
                ["Nordeste", "2"],
                ["Sudeste", "3"],
                ["Sul", "5"],
                ["Centro-Oeste", "5"],
            ],
            index=[f"BR-{i}" for i in range(1, 6)],
            columns=["name", "code"],
        )
        df["type"] = "region"
        df["numeric_code"] = df["code"]
        df["country_code"] = "BR"
        df["parent_id"] = "BR"
        df["long_code"] = pd.NA
        return df.astype("string")[self.cols]

    @sk.lazy
    def subdivisions(self):
        # Collect dataframes
        cols = ["code", "name", "type", "parent_id", "long_code"]
        Row = namedtuple("Row", cols)

        # Regions
        meso = set()
        micro = set()
        city = set()
        district = set()

        for _, row in self.dtb.iterrows():
            meso_id = f"{row.state_code}{row.meso_code:02}"
            micro_id = f"{meso_id}{row.micro_code:02}"
            city_long_id = str(row.city_code)
            city_id = str(row.city_code)[:-1]  # strip unused last digit
            district_id = str(row.district_code)

            meso.add(Row(meso_id, row.meso_name, "meso-region", str(row.state), pd.NA))
            micro.add(Row(micro_id, row.micro_name, "micro-region", meso_id, pd.NA))
            city.add(Row(city_id, row.city_name, "municipality", micro_id, city_long_id))
            district.add(Row(district_id, row.district_name, "district", city_id, pd.NA))

        def mk_table(data):
            df = pd.DataFrame(list(data), columns=cols)
            df["numeric_code"] = df["long_code"].fillna(df["code"])
            df.index = "BR-" + df["numeric_code"]
            df["country_code"] = "BR"
            df["parent_id"] = "BR-" + df["parent_id"].astype("string")
            return df[self.cols]

        br_meso = mk_table(meso)
        br_micro = mk_table(micro)
        br_city = mk_table(city)
        br_district = mk_table(district)

        df = pd.concat([br_meso, br_micro, br_city, br_district])
        df["type"] = df["type"]

        return df.astype("string")

    @sk.lazy
    def states(self):
        # Save numeric codes for states
        codes = self.dtb[["state", "state_code"]].drop_duplicates()
        codes.index = "BR-" + codes.pop("state").astype("string")
        states = self._states.copy()
        states.loc[:, "numeric_code"] = codes["state_code"].apply(str).astype("string")

        # Assign region numbers
        states.loc[:, "parent_id"] = "BR-" + states["numeric_code"].str[:1]
        states.loc["BR-DF", "type"] = "state"
        return states

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def load(self):
        data = [self.states, self.regions, self.subdivisions]
        df = pd.concat(data)
        return df[self.cols]


def regions():
    return BrRegions().load_cached()


if __name__ == "__main__":
    print(regions())
