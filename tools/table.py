"""Table-related helpers wrapping a PyIceberg Catalog.

This module exposes TableTools, a convenience wrapper around a Catalog
to list tables, read table metadata, and page table contents into Python
dictionaries.
"""

from typing import Annotated, List, Optional, Union

from pydantic import Field
from pyiceberg.catalog import Catalog
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.typedef import Identifier


class TableTools:
    """Convenience helpers for reading Iceberg table information.

    Attributes:
        catalog: Catalog instance used to load
            and inspect tables.
    """

    catalog: Catalog

    def __init__(self, catalog: Catalog) -> None:
        """Initialize TableTools.

        Args:
            catalog: The catalog used to access
                tables.
        """
        self.catalog = catalog

    async def list_tables(
        self,
        namespace: Annotated[Union[str, Identifier], Field(description="The namespace to list tables from.")],
    ) -> List[Identifier]:
        """List all tables in a namespace.

        Args:
            namespace: The namespace to list tables from.

        Returns:
            A list of table identifiers in the namespace.
        """

        return self.catalog.list_tables(namespace)

    async def read_table_metadata(
        self,
        table_identifier: Annotated[Union[str, Identifier], Field(description="The identifier of the table.")],
    ) -> TableMetadata:
        """Retrieve metadata for a table.

        Args:
            table_identifier: The identifier of the table.

        Returns:
            The table metadata object.
        """
        table = self.catalog.load_table(table_identifier)

        return table.metadata

    async def read_table_contents(
        self,
        table_identifier: Annotated[Union[str, Identifier], Field(description="Table identifier")],
        start: Annotated[
            int,
            Field(description="Row index to start pagination, inclusive."),
        ] = 0,
        end: Annotated[
            Optional[int],
            Field(description="Row index to end pagination, exclusive."),
        ] = None,
    ) -> str:
        """Retrieve table contents with optional pagination.

        Supports negative indices for both `start` and `end` parameters to enable
        reading from the end of the table. When ``end`` is ``None`` (default),
        all rows from ``start`` to the end of the table are returned.

        Args:
            table_identifier: The identifier of the table.
            start: Row index to start pagination (inclusive). Defaults to
                ``0``. Negative indices count from the end of the table.
            end: Row index to end pagination (exclusive).
                Defaults to ``None`` (end of table). Negative indices count
                from the end of the table.

        Returns:
            JSON representation of the table rows.
        """
        table = self.catalog.load_table(table_identifier)

        snapshot = table.current_snapshot()
        if snapshot is None:
            raise ValueError(f"Table: {table_identifier} has no current snapshot.")
        summary = snapshot.summary
        if summary is None:
            raise ValueError(f"Snapshot for Table: {table_identifier} has no summary.")
        row_limit = int(summary.get("total-records", "0"))

        if start < 0:
            start += row_limit
        if end is None:
            end = row_limit
        elif end < 0:
            end = max(0, end + row_limit)
        else:
            end = min(end, row_limit)

        try:
            df = table.to_polars()
            df = await df.slice(start, end - start).collect_async()

        except OSError:
            df = table.scan().to_polars()
            df = df.slice(start, end - start)

        return df.write_json()
