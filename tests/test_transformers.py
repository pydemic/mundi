import pandas as pd
from pandas.testing import assert_frame_equal

from mundi import transforms


class TestTransforms:
    # TODO: is this result right?
    def test_aggregate_by_sum(self):
        df = pd.DataFrame(
            [
                ["BR-5300108", 1, 2],
                ["BR-5208707", 3, 7],
                ["BR-5201405", 2, 1],
                ["BR-5", 5, 5],
            ],
            columns=["id", "x", "y"],
        ).set_index("id")

        agg = transforms.sum_children(df, relation="all")
        print(agg)
        assert_frame_equal(
            agg,
            pd.DataFrame(
                [
                    ["BR-5300108", 1, 2],
                    ["BR-5208707", 3, 7],
                    ["BR-5201405", 2, 1],
                    ["BR-5", 5, 5],
                    ["BR", 5, 5],
                    ["BR-520310", 5, 8],
                    ["BR-530101", 1, 2],
                    ["BR-5203", 5, 8],
                    ["BR-5301", 1, 2],
                    ["XSA", 5, 5],
                    ["BR-DF", 1, 2],
                    ["BR-GO", 5, 8],
                    ["XX", 5, 5],
                    # ["BR-SUS:5202", 3, 7],
                    # ["BR-SUS:5203", 2, 1],
                    # ["BR-SUS:5301", 1, 2],
                    ["BR-SUS:5205", 5, 8],
                ],
                columns=["id", "x", "y"],
            ).set_index("id"),
        )
