import contextlib
import time
from abc import ABC
from collections import defaultdict
from pathlib import Path
from typing import Dict, TypeVar, Type, List

import pandas as pd
import pkg_resources
import sidekick.api as sk
from sqlalchemy import Column
from sqlalchemy.orm.properties import ColumnProperty

from .. import config
from .database import Base
from ..logging import log
from ..pipeline import Collector, Importer, find_prepare_scripts, execute_prepare_script
from ..utils import read_file

PLUGIN_INSTANCES = {}
PLUGIN_SUBCLASSES = []
T = TypeVar("T")


class Plugin(ABC):
    """
    Base plugin class.
    """

    name: str
    tables: Dict[str, Base]
    data_tables: List[Type[Base]] = None
    collectors: Dict[str, Type[Collector]] = defaultdict(lambda: Collector)
    importers: Dict[str, Type[Importer]] = defaultdict(lambda: Importer)
    columns: Dict[str, Column] = None
    prefix: str = ""

    @classmethod
    def cli(cls, click=sk.import_later("click")):
        """
        Execute data processing actions related to plugin.
        """

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
        def collect(path, verbose, table):
            if path is None:
                path = config.mundi_data_path() / "build"
            else:
                path = Path(path)
            plugin = cls.instance()

            if table is None:
                for table in plugin.tables:
                    plugin.collect(path, table, verbose=verbose)
            else:
                plugin.collect(path, table, verbose=verbose)

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
            if table:
                tables = table.split(",")
            else:
                tables = list(plugin.tables)

            db.create_tables()
            for table in tables:
                plugin.reload(path, table, verbose=verbose)

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

    @classmethod
    def get_column(cls, column: str):
        """
        Return the column associated with the given column name.
        """
        for plugin in PLUGIN_INSTANCES.values():
            try:
                return plugin.columns[column]
            except KeyError:
                pass

        raise ValueError(f"no column named {column}")

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
        for part in find_prepare_scripts(path):
            plugin, region, path = part
            if plugin != self.name:
                continue
            execute_prepare_script(path, verbose=verbose)

    def collect(self, path, table, verbose=False):
        """
        Collect chunks for the given table.
        """
        with _timing(f"collecting {table!r}...", verbose):
            collector = self.collectors[table](path, table)
            collector.process()

    def reload(self, path, table, verbose=False):
        """
        Reload data in path and save in the SQL database under table.
        """
        with _timing(f"saving to db {table!r}...", verbose):
            importer = self.importers[table](path, table)
            importer.clear()
            for chunk in importer.load_chunks():
                importer.save(chunk)

    def save_data(self, data, table, verbose=False):
        """
        Save collected data into database.
        """
        with _timing(f"saving to db {table!r}...", verbose):
            importer = self.importers[table](".", table)
            importer.clear()
            importer.save(data)

    def load_data(self, table, verbose=False) -> pd.DataFrame:
        """
        Load data from disk or from URL.
        """
        with _timing(f"loading table {table!r}...", verbose):
            uri, is_remote = config.mundi_data_uri(table)
            return read_file(uri)

    #
    # Plugin registry
    #
    def register(self):
        """
        Register itself in the plugins registry that maps fields to SQL queries
        to the database.
        """

        if self.data_tables is None:
            self.data_tables = list(self.tables)

        # Fill the columns attribute, if empty
        if self.columns is None:
            self.columns = columns = {}
            prefix = self.prefix
            for table_name in self.data_tables:
                table = self.tables[table_name]
                for attr, prop in table.__mapper__.attrs.items():
                    key = prefix + attr
                    if key in columns:
                        continue
                    if isinstance(prop, ColumnProperty):
                        columns[key] = prop.expression

    #
    # Query terms
    #


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
