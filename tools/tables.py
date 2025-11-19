from typing import Annotated, List, Optional, Union

from pydantic import Field
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.typedef import Identifier

from catalog import load_catalog


async def list_tables(
    namespace: Annotated[Union[str, Identifier], Field(description="The namespace to list tables from.")],
) -> List[Identifier]:
    """List all tables in a namespace.

    Args:
        namespace (str | Identifier): The namespace to list tables from.

    Returns:
        List[Identifier]: A list of table identifiers in the namespace.
    """
    catalog = load_catalog()

    return catalog.list_tables(namespace)


async def read_table_metadata(
    table_identifier: Annotated[Union[str, Identifier], Field(description="The identifier of the table.")],
) -> TableMetadata:
    """Retrieve metadata for a table.

    Args:
        table_identifier (str | Identifier): The identifier of the table.

    Returns:
        TableMetadata: The table metadata.
    """
    catalog = load_catalog()
    table = catalog.load_table(table_identifier)

    return table.metadata


async def read_table_contents(
    table_identifier: Annotated[Union[str, Identifier], Field(description="Table identifier.")],
    start: Annotated[
        int,
        Field(description="Row index to start pagination, inclusive."),
    ] = 0,
    end: Annotated[
        Optional[int],
        Field(description="Row index to end pagination, exclusive."),
    ] = None,
) -> List[dict]:
    """Retrieve table contents with optional pagination.

    Supports negative indices for both start and end parameters to enable
    reading from the end of the table. When end is None (default), all rows
    from start index to the end of the table are returned.

    Args:
        table_identifier (str | Identifier): The identifier of the table.
        start (int): Row index to start pagination (inclusive). Defaults to 0.
            Negative indices count from the end of the table.
        end (Optional[int]): Row index to end pagination (exclusive). Defaults
            to None (end of table). Negative indices count from the end of the
            table.

    Returns:
        List[dict]: A list of dictionaries representing the table rows.
    """
    catalog = load_catalog()
    table = catalog.load_table(table_identifier)
    df = table.scan().to_polars()

    row_limit = df.shape[0]
    if start < 0:
        start += row_limit
    if end is None:
        end = row_limit
    elif end < 0:
        end += row_limit
    else:
        end = min(end, row_limit)

    df = df.slice(start, end - start)

    return df.to_dicts()
