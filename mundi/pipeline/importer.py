from abc import ABC
from pathlib import Path
from typing import Iterable

import pandas as pd

from .. import db
from ..logging import log
from ..utils import read_file


class Importer(ABC):
    """
    Base class responsible for importing data from dataframes to the SQL
    database.

    Importers do not validate data.
    """

    path: Path
    table: str

    @classmethod
    def cli(cls):
        """
        Main CLI interface for data importers.
        """

        import click

        @click.command()
        @click.argument("path")
        @click.option("--table", "-t", default="region")
        @click.option("--verbose", "-v", is_flag=True, default="display debug messages")
        @click.option("--clear", "-c", is_flag=True, help="clear table before importing")
        def run(path, table, clear, verbose):
            db.create_tables()
            if verbose:
                log.setLevel("DEBUG")
            importer = cls(path, table=table)
            if clear:
                importer.clear()
            importer.process()

        run()

    def __init__(self, path, table, chunk_size=1024):
        self.path = Path(path)
        self.table = table
        self.chunk_size = chunk_size

    def load_chunks(self) -> Iterable[pd.DataFrame]:
        """
        Loads data from main source.

        Return an iterable with data chunks that comfortably fits in memory.
        """
        data: pd.DataFrame = read_file(self.path / f"{self.table}.pkl.gz")
        idx = 0
        size = len(data)
        while idx < size:
            chunk = data[idx : idx + self.chunk_size]
            yield chunk
            idx += self.chunk_size

    def save(self, data: pd.DataFrame):
        """
        Saves chunk of data to the database.
        """
        cls = db.get_table(self.table)
        session = db.session()
        rows = []

        if hasattr(cls, "id"):
            data = data.reset_index()

        for row_data in data.to_dict("records"):
            kwargs = clean_row(row_data)
            row = cls(**kwargs)
            rows.append(row)

        log.debug(f"[{self.table}] saving {len(rows)} rows to db")
        session.add_all(rows)
        session.commit()

    def clear(self):
        """
        Clear data in table.
        """

        cls = db.get_table(self.table)
        session = db.session()
        session.query(cls).delete()
        session.commit()
        log.info(f"[{self.table}] deleted all entries")

    def process(self):
        """
        Load data in save all data chunks in iterable.
        """

        # Save chunks
        for chunk in self.load_chunks():
            self.save(chunk)


def clean_row(data):
    """
    Clean a record dictionary replacing pd.NA to None.
    """
    return {k: (None if v is pd.NA else v) for k, v in data.items()}


if __name__ == "__main__":
    Importer.cli()
