from collections import defaultdict
from collections import namedtuple
from typing import Dict

import pandas as pd
import sidekick.api as sk

from mundi.helpers.br import ibge_city_code
from mundi.plugins.region import RegionData

inv = lambda d: {v: k for k, v in d.items()}


class Brazil(RegionData):
    """
    This script uses information from IBGE (Brazilian Institute of Geography and
    Statistics) territorial definitions to feed the mundi database.
    """

    STATE_CODES = {
        "Acre": "AC",
        "Alagoas": "AL",
        "Amazonas": "AM",
        "Amapá": "AP",
        "Bahia": "BA",
        "Ceará": "CE",
        "Distrito Federal": "DF",
        "Espírito Santo": "ES",
        "Goiás": "GO",
        "Maranhão": "MA",
        "Minas Gerais": "MG",
        "Mato Grosso do Sul": "MS",
        "Mato Grosso": "MT",
        "Pará": "PA",
        "Paraíba": "PB",
        "Pernambuco": "PE",
        "Piauí": "PI",
        "Paraná": "PR",
        "Rio de Janeiro": "RJ",
        "Rio Grande do Norte": "RN",
        "Rondônia": "RO",
        "Roraima": "RR",
        "Rio Grande do Sul": "RS",
        "Santa Catarina": "SC",
        "Sergipe": "SE",
        "São Paulo": "SP",
        "Tocantins": "TO",
    }

    @sk.lazy
    def territory_table(self):
        """
        Full data from IBGE about the division of territories up to the level of
        districts.

        TODO: reference the data source in IBGE's website.
        """

        # Read raw data from IBGE
        out = self.read_csv("divisao-do-territorio-distritos.csv", index_col=None)
        out.columns = [
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
        out["state_code"] = out["state"]
        out["state"] = out["state_name"].apply(self.STATE_CODES.get)
        return out

    #
    # Gradually build dataframe
    #
    @sk.lazy
    def region_macros(self):
        """
        Macro regions, i.e., Norte, Nordeste, Sudeste, Sul e Centro-Oeste.
        """

        out = pd.DataFrame(
            [
                ["Norte", "1"],
                ["Nordeste", "2"],
                ["Sudeste", "3"],
                ["Sul", "5"],
                ["Centro-Oeste", "5"],
            ],
            index=[f"BR-{i}" for i in range(1, 6)],
            columns=["name", "short_code"],
        )
        out["numeric_code"] = out["short_code"]
        return self.fill_region(
            out.astype("string"),
            type="region",
            subtype="macro-region",
            country_id="BR",
            parent_id="BR",
            level=3,
        )

    @sk.lazy
    def region_states(self) -> pd.DataFrame:
        """
        Dataframe with all Brazilian states.
        """
        # Load states from prepared pycountry database
        out = (
            pd.Series(self.STATE_CODES, name="short_code")
            .reset_index()
            .astype("string")
            .rename({"index": "name"}, axis=1)
        )
        out.index = "BR-" + out["short_code"]

        # Save numeric codes for states
        codes = self.territory_table[["state", "state_code"]].drop_duplicates()
        codes.index = "BR-" + codes.pop("state").astype("string")

        # Assign region numbers
        out["numeric_code"] = codes["state_code"].apply(str).astype("string")
        out["parent_id"] = "BR-" + out["numeric_code"].str[:1]
        out = self.fill_region(out, type="state", level=4, country_id="BR")
        out.loc["BR-DF", "subtype"] = "federal district"
        return out

    @sk.lazy
    def region_subdivisions(self):
        """
        IBGE subdivisions from the official territory sub-division table.
        """
        # Collect dataframes
        cols = ["short_code", "name", "type", "subtype", "parent_id", "long_code"]
        Row = namedtuple("Row", cols)

        # Regions
        data = set()
        for _, row in self.territory_table.iterrows():
            meso = f"{row.state_code}{row.meso_code:02}"
            micro = f"{meso}{row.micro_code:02}"
            city7 = str(row.city_code)
            city6 = str(row.city_code)[:-1]  # strip unused last digit
            district = str(row.district_code)

            data.update(
                [
                    Row(
                        meso,
                        row.meso_name,
                        "region",
                        "meso-region",
                        str(row.state),
                        pd.NA,
                    ),
                    Row(micro, row.micro_name, "region", "micro-region", meso, pd.NA),
                    Row(city6, row.city_name, "city", "municipality", micro, city7),
                    Row(district, row.district_name, "district", pd.NA, city7, pd.NA),
                ]
            )

        df = pd.DataFrame(list(data))
        df["numeric_code"] = df["long_code"].fillna(df["short_code"])
        df["parent_id"] = "BR-" + df["parent_id"]
        df.index = "BR-" + df["numeric_code"]
        df.index.name = "id"
        df = df.astype("string")

        type_levels = {"city": 7, "district": 8}
        subtype_levels = {"meso-region": 5, "micro-region": 6}
        df["level"] = (
            df["type"]
            .apply(type_levels.get)
            .fillna(df["subtype"].apply(subtype_levels.get))
            .astype("uint8")
        )

        return self.fill_region(df.sort_values("level"), country_id="BR")

    @sk.lazy
    def healthcare_zone_per_city(self) -> pd.DataFrame:
        """
        Raw data source for SUS macro regions.

        It returns a table with one row per city with columns:
         - uf: 2 letter state code
         - uf_id: mundi BR-<uf> code.
         - city_id: mundi city id.
         - region_id: SUS micro region id.
         - region_name: SUS micro region name.
         - macro_name: SUS macro region name.
         - state_id: 2 digit state code.
        """
        df = self.read_csv("regionais-de-saude-macro.csv", index_col=0)

        # This column does not seems to index anything useful
        df = df.drop(columns=["macro_id"])

        # Apply mundi code convention BR-SUS:<???> to healthcare regions
        df["region_id"] = df["region_id"].apply(lambda x: f"BR-SUS:{x}").astype("string")
        df["city_id"] = (
            df["city_id"].apply(lambda x: f"BR-{ibge_city_code(str(x))}").astype("string")
        )
        df["uf_id"] = df["region_id"].str[7:9]
        df["state_id"] = "BR-" + df["uf"]

        return df

    @sk.lazy
    def region_SUS_macros(self):
        """
        SUS macro healthcare regions.

        It seems that there is no official codes for SUS healthcare macro
        regions. We assign an arbitrary 4 digit code with the 2 first digits
        coming from the state and the other two created sequentially from the
        list of macro regions.

        The mundi code is assigned as BR-SUS:<numeric_code>
        """
        data = self.healthcare_zone_per_city[["uf", "uf_id", "macro_name"]]

        macro_ids = defaultdict(dict)
        for _, (uf, uf_id, name) in data.sort_values("macro_name").iterrows():
            db = macro_ids[uf]
            if name not in db:
                db[name] = f"BR-SUS:{uf_id}{len(db) + 1:02};BR-{uf}"

        # A dataframe with name and id/parent_id
        partial = (
            pd.concat(pd.Series(rs) for rs in macro_ids.values())
            .reset_index()
            .astype("string")
            .rename(columns={"index": "name"})
        )
        out = pd.concat(
            [
                partial,
                partial.pop(0)
                .str.partition(";")
                .drop(columns=1)
                .rename(columns={0: "id", 2: "parent_id"}),
            ],
            axis=1,
        )
        out["short_code"] = out["id"].str[7:]
        out["numeric_code"] = out["id"].str[7:]
        out["long_code"] = out["id"].str[3:]
        out = out.set_index("id")
        return self.fill_region(
            out, country_id="BR", type="region", subtype="healthcare_region", level=5
        )

    @sk.lazy
    def region(self):
        df = self.safe_concat(
            [
                self.region_macros,
                self.region_states,
                self.region_subdivisions,
                self.region_SUS_macros,
            ]
        )
        df.index.name = "id"
        return df

    @sk.lazy
    def region_m2m_sus_cities(self):
        data = self.healthcare_zone_per_city[["state_id", "macro_name", "city_id"]]
        macros = self.region_SUS_macros[["parent_id", "name"]].reset_index()

        out = pd.merge(
            data.rename(columns={"macro_name": "name", "city_id": "id"}),
            macros.rename(columns={"id": "sus_id", "parent_id": "state_id"}),
            on=["state_id", "name"],
        )
        return (
            out.drop(columns=["state_id", "name"])
            .rename(columns={"id": "child_id", "sus_id": "parent_id"})
            .assign(relation="sus_region")
            .astype("string")
        )[["child_id", "parent_id", "relation"]]

    @sk.lazy
    def region_m2m(self):
        return self.safe_concat([self.region_m2m_sus_cities])


if __name__ == "__main__":
    Brazil.cli()
