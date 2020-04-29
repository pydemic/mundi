import pandas as pd

from .loader import Loader
from .pycountry import countries
from ..extra import REGIONS

INV_REGIONS = {v: k for k, v in REGIONS.items()}


class WbSummary(Loader):
    KEY = "wb"

    def load(self):
        db = countries()
        data = pd.read_csv(self.DATA / "generic" / "wb-summary.csv", index_col=0)
        data = data.drop(columns=["table_name", "notes"])
        data["code"] = pd.DataFrame(db.index, index=db["long_code"])
        data.index = data.pop("code")
        data = data.astype("string")
        data["income_group"] = (
            data["income_group"]
            .str.rpartition(" ")[0]
            .str.lower()
            .str.replace(" ", "-")
            .astype("category")
        )
        data["region"] = data["region"].apply(INV_REGIONS.__getitem__).astype("string")
        return data.sort_index()


def wb_summary():
    return WbSummary().load_cached()


if __name__ == "__main__":
    print(wb_summary())
