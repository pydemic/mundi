import contextlib
import time
from abc import ABC
from collections import defaultdict
from pathlib import Path
from typing import Dict, TypeVar, Type, List

import pkg_resources
import sidekick.api as sk

from . import Table, SqlColumn
from .. import config
from .. import db
from ..logging import log
from ..pipeline import Collector, Importer, find_prepare_scripts, execute_prepare_script

PLUGIN_INSTANCES = {}
PLUGIN_SUBCLASSES = []
T = TypeVar("T")


class Plugin(ABC):
    """
    Base plugin class.
    """

    name: str
    tables: List[db.TableInfo] = None
    data_tables: List[Type[Table]] = None
    collectors: Dict[str, Type[Collector]] = defaultdict(lambda: Collector)
    importers: Dict[str, Type[Importer]] = defaultdict(lambda: Importer)
    prefix: str = ""

    @classmethod
    def cli(cls, click=sk.import_later("click")):
        """
        Execute data processing actions related to plugin.
        """

        from .. import db

        def select_tables(plugin, table):
            if table:
                select = set(table.split(","))
                yield from sk.filter(lambda x: x.name in select, plugin .tables)
            else:
                yield from plugin.tables

        @click.group()
        def cli():
            pass

        @cli.command()
        @click.option("--path", "-p", help="Location of mundi-path")
        @click.option(
            "--verbose", "-v", is_flag=True, default=False, help="Verbose output"
        )
        def prepare(path, verbose):
            click.echo(path)
            if path is None:
                path = config.mundi_data_path() / "data"
            else:
                path = Path(path)
            plugin = cls.instance()
            plugin.prepare(path, verbose=verbose)

        @cli.command()
        @click.option("--path", "-p", help="Location of mundi-path")
        @click.option(
            "--verbose", "-v", is_flag=True, default=False, help="Verbose output"
        )
        @click.option("--table", "-t", help="Select specific table")
        @click.option(
            "--max-size", "-m", type=int, help="set maximum size of collected chunks"
        )
        def collect(path, verbose, table, max_size):
            opts = {"verbose": verbose, "max_size": max_size}
            if path is None:
                path = config.mundi_data_path() / "build"
            else:
                path = Path(path)
            plugin = cls.instance()

            for table in select_tables(plugin, table):
                try:
                    plugin.collect(path, table, **opts)
                except Exception as ex:
                    msg = f"error collecting table {plugin.name}.{table}: {ex}"
                    raise RuntimeError(msg) from ex

        @cli.command("import")
        @click.option("--path", "-p", help="Location of mundi-path")
        @click.option(
            "--verbose", "-v", is_flag=True, default=False, help="Verbose output"
        )
        @click.option("--table", "-t", help="Select specific table")
        def import_(path, verbose, table):
            if path is None:
                path = config.mundi_data_path() / "build" / "databases"
            else:
                path = Path(path)

            plugin = cls.instance()
            db.create_tables()
            for table in select_tables(plugin, table):
                plugin.import_data(path, table, verbose=verbose)

        return cli()

    @classmethod
    def instance(cls):
        """
        Return singleton instance to class.
        """
        try:
            return PLUGIN_INSTANCES[cls]
        except KeyError:
            PLUGIN_INSTANCES[cls] = plugin = cls()
            return plugin

    @classmethod
    def init_plugins(cls):
        """
        Initialize the plugin system.
        """
        load_external_plugins()
        n = len(PLUGIN_SUBCLASSES)
        log.info(f"initializing {n} plugins")

        plugins = []
        for plugin_cls in PLUGIN_SUBCLASSES:
            plugin = plugin_cls.instance()
            log.info(f"initializing plugin: {plugin}")
            plugins.append(plugin)

        for plugin in plugins:
            plugin.register()

    def __init_subclass__(cls, **kwargs):
        PLUGIN_SUBCLASSES.append(cls)

    def __init__(self):
        if type(self) in PLUGIN_INSTANCES:
            raise RuntimeError("trying to initialize plugin twice.")
        if not hasattr(self, "name"):
            cls = type(self).__name__
            raise RuntimeError(f"Class {cls} does not implement a 'name' attribute.")
        if not hasattr(self, "tables"):
            cls = type(self).__name__
            raise RuntimeError(f"Class {cls} does not implement a 'tables' attribute.")
        self.name = self.name
        PLUGIN_INSTANCES[type(self)] = self

    #
    # Pipeline
    #
    def prepare(self, path, verbose=False):
        """
        Execute all prepare.py scripts in the given path.
        """
        for part in find_prepare_scripts(path, verbose=verbose):
            plugin, region, path = part
            if plugin != self.name:
                continue
            execute_prepare_script(path, verbose=verbose, plugin=plugin, region=region)

    def collect(self, path: Path, info: db.TableInfo, verbose: bool = False, **kwargs):
        """
        Collect chunks for the given table.

        Extra keyword arguments are forwarded to the collector class constructor.
        """
        with _timing(f"collecting {info.name!r}...", verbose):
            collector_cls = self.collectors.get(info.name, Collector)
            collector = collector_cls(path, info, **kwargs)
            return collector.process()

    def import_data(self, path: Path, table: db.TableInfo, verbose: bool = False):
        """
        Reload data in path and save in the SQL database under table.
        """
        with _timing(f"saving to db {table.name!r}...", verbose):
            importer_cls = self.importers.get(table.name, Importer)
            importer = importer_cls(path, table)
            importer.save()

    #
    # Plugin registry
    #
    def register(self):
        """
        Register itself in the plugins registry that maps fields to SQL queries
        to the database.
        """


@contextlib.contextmanager
def _timing(msg, verbose):
    t0 = None
    if verbose:
        print(msg, end=" ", flush=True)
        t0 = time.time()
    yield
    if verbose:
        dt = time.time() - t0
        print(f"[{dt:.2}sec]")


def load_external_plugins():
    """
    Load external plugins, including entry points.
    """
    from ..plugins import demography, region, healthcare

    _, _, _ = demography, region, healthcare

    for entry_point in pkg_resources.iter_entry_points("mundi_plugin"):
        entry_point.load()
