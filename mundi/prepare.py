class Prepare:
    """
    Prepare data for inclusion in table.

    This creates the intermediary representation of data as a dataframe. This
    intermediary step exists to make mundi databases easier to share. This way,
    we can separate repositories that mine the original data sources and produce
    shareable dataframes to be inserted into the SQL database Mundi relies
    during runtime.

    By keeping the data sources separate, we can also update and fix errors in
    data separately from mundi code. Some users might even prefer to use the
    dataframes by themselves without ever installing mundi.
    """

    def __init__(self, table=None, path=None):
        self.table = table or type(self).__name__.lower()
        self.path = path or default_path(self.table, type(self))


def default_path(table, cls=None):
    """
    Return the default path to the data directory for the given table.

    If the optional cls is given, it attempts to use extra information about
    the location of the class source code to infer where the data repository
    is.
    """
