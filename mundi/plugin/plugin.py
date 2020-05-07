from abc import ABC

PLUGIN_DB = {}


class Plugin(ABC):
    """
    Plugins make data available for mundi.
    """

    def __init__(self, name):
        self.name = name

    def register(self, force=False):
        """
        Register plugin in the plugin database.
        """


class ColumnPlugin(Plugin, ABC):
    """
    pass
    """
