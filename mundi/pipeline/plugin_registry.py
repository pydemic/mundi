import sidekick.api as sk


@sk.once
class PluginRegistry:
    """
    Global plugin registry.

    This is a singleton constructor and instances of the registry are recycled
    after first initialization.
    """

    def __init__(self):
        self.plugins = {}

    def registry(self, plugin, namespace=None):
        """
        Register plugin under the given namespace.
        """
        raise NotImplementedError
