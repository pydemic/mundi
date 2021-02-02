import contextlib
import re
import time
from abc import ABC
from collections import defaultdict
from pathlib import Path
from typing import Dict, TypeVar, Type

import pandas as pd
import sidekick.api as sk

from .. import config
from ..db import Base
from ..pipeline import Collector, Importer, find_prepare_scripts, execute_prepare_script
from ..utils import read_file

sqlite3 = sk.import_later("sqlite3")
IS_CODE_PATH = re.compile(r"[A-Z]{2}(-\w+)?")
PICKLE_EXTENSIONS = (".pkl", ".pickle", ".pkl.gz", ".pickle.gz")
SQL_EXTENSIONS = (".sql", ".sqlite")
CSV_EXTENSIONS = (".csv", ".csv.gz", ".csv.bz2")
HDF5_EXTENSIONS = (".hdf", ".hfd5")

PLUGIN_INSTANCES = {}

T = TypeVar("T")


class Plugin(ABC):
    """
    Base plugin class.
    """

    name: str
    tables: Dict[str, Base]
    collectors: Dict[str, Type[Collector]] = defaultdict(lambda: Collector)
    importers: Dict[str, Type[Importer]] = defaultdict(lambda: Importer)

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
        def collect(path, verbose):
            if path is None:
                path = config.mundi_data_path() / "build"
            else:
                path = Path(path)
            plugin = cls.instance()

            for table in plugin.tables:
                plugin.collect(path, table, verbose=verbose)

        @cli.command()
        @click.option("--path", "-p", help="Location of mundi-path")
        @click.option(
            "--verbose", "-v", is_flag=True, default=False, help="Verbose output"
        )
        @click.option("--table", "-t", default="", help="Select specific table")
        def reload(path, verbose, table):
            if path is None:
                path = config.mundi_data_path() / "build" / "databases"
            else:
                path = Path(path)
            plugin = cls.instance()

            for table in table.split(",") or plugin.tables:
                plugin.reload(path, table, verbose=verbose)

        return cli()

    def __init__(self):
        if type(self) in PLUGIN_INSTANCES:
            raise RuntimeError("trying to initialize plugin twice.")
        if not hasattr(self, "name"):
            raise RuntimeError("Class does not implement a 'name' attribute.")
        if not hasattr(self, "tables"):
            raise RuntimeError("Class does not implement a 'tables' attribute.")
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

    def register(self):
        """
        Register itself in the plugins registry that maps fields to SQL queries
        to the database.
        """
        raise NotImplementedError

    def unregister(self):
        """
        Un-register plugin from the global plugin registry.
        """
        raise NotImplementedError


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
