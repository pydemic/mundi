from enum import IntEnum


class Fill(IntEnum):
    """
    One of the possible types of aggregation
    """

    NONE = 0

    # Child aggregations
    SUM_CHILDREN = 1
    MAX_CHILDREN = 2
    MIN_CHILDREN = 3

    # Child statistics
    MEAN_CHILDREN = 10
    MEAN_POP_WEIGHT = 11
    MEDIAN_CHILDREN = 12

    # Inherit from parents
    INHERIT = 20
