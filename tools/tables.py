from typing import List

from pyiceberg.table.metadata import TableMetadata
from pyiceberg.typedef import Identifier

from catalog import load_catalog


def list_tables(namespace: str | Identifier) -> List[Identifier]:
    catalog = load_catalog()

    return catalog.list_tables(namespace)


def get_table_metadata(table_identifier: str | Identifier) -> TableMetadata:
    catalog = load_catalog()
    table = catalog.load_table(table_identifier)

    return table.metadata


def get_table_contents(table_identifier: str | Identifier, start: int = 0, end: int = -1) -> List[dict]:
    catalog = load_catalog()
    table = catalog.load_table(table_identifier)
    df = table.scan().to_polars()
    row_limit = df.shape[0]
    if end != -1:
        row_limit = min((end - start + 1), row_limit)

    df = df.slice(start, row_limit)

    return df.to_dicts()
