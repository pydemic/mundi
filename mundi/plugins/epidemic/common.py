from sidekick.cache import set_global_memory_provider, path_memory_provider
from sidekick.functions import once


@once
def init_cache():
    set_global_memory_provider(path_memory_provider("mundi", config_folder=True))
