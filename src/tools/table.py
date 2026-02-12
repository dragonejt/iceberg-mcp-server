"""Table-related helpers wrapping a PyIceberg Catalog.

This module exposes TableTools, a convenience wrapper around a Catalog
to list tables, read table metadata, and page table contents into Python
dictionaries.
"""

from pathlib import Path
from typing import Annotated, Literal

import polars as pl
from pyarrow import Table
from pyarrow.csv import CSVWriter
from pyarrow.ipc import RecordBatchFileWriter
from pyarrow.parquet import ParquetWriter
from pydantic import Field
from pyiceberg.catalog import Catalog
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Snapshot
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
        namespace: Annotated[str | Identifier, Field(description="The namespace to list tables from.")],
    ) -> Annotated[list[Identifier], Field(description="List of table identifiers in namespace.")]:
        """List all tables in a namespace.

        Args:
            namespace: The namespace to list tables from.

        Returns:
            A list of table identifiers in the namespace.
        """

        return self.catalog.list_tables(namespace)

    async def read_table_metadata(
        self,
        identifier: Annotated[str | Identifier, Field(description="The identifier of the table.")],
    ) -> Annotated[TableMetadata, Field(description="Table metadata object.")]:
        """Retrieve metadata for a table.

        Args:
            identifier: The identifier of the table.

        Returns:
            The table metadata object.
        """
        table = self.catalog.load_table(identifier)

        return table.metadata

    async def read_table_snapshots(
        self,
        identifier: Annotated[str | Identifier, Field(description="The identifier of the table.")],
        snapshot_id: Annotated[int | None, Field(description="Optional snapshot ID for time travel.")] = None,
        limit: Annotated[int | None, Field(description="Maximum number of snapshots to return.")] = None,
    ) -> Annotated[list[Snapshot], Field(description="List of snapshot objects.")]:
        """
        Retrieve snapshot information for a table.

        Args:
            identifier: The identifier of the table.
            snapshot_id: Optional snapshot ID for time travel queries.
            limit: Optional limit on number of snapshots to return.

        Returns:
            List of snapshot objects.
        """
        table = self.catalog.load_table(identifier)
        snapshots = table.inspect.snapshots()

        if snapshot_id is not None:
            snapshots = [s for s in snapshots if s.snapshot_id == snapshot_id]

        if limit is not None:
            snapshots = snapshots[:limit]

        return snapshots

    async def read_table_contents(
        self,
        identifier: Annotated[str | Identifier, Field(description="The identifier of the table.")],
        limit: Annotated[
            int | None,
            Field(description="Maximum number of rows to return."),
        ] = None,
    ) -> Annotated[str, Field(description="JSON representation of the table rows.")]:
        """Retrieve table contents.

        Args:
            identifier: The identifier of the table.
            limit: Maximum number of rows to return. If ``None`` (default),
                all rows are returned.

        Returns:
            JSON representation of the table rows.
        """
        table = self.catalog.load_table(identifier)
        df = table.scan(limit=limit).to_polars()

        return df.write_json()

    async def download_table_contents(
        self,
        identifier: Annotated[str | Identifier, Field(description="The identifier of the table.")],
        file: Annotated[Path, Field(description="Path of downloaded table file.")],
    ) -> None:
        """Download table contents to a file.

        Args:
            identifier: The identifier of the table.
            file: Path of downloaded table file.

        Raises:
            FileNotFoundError: If the parent directory does not exist.
            ValueError: If the file extension is unsupported.
        """
        if file.parent.is_dir() is False:
            raise FileNotFoundError(f"Parent directory {file.parent.resolve()} must exist!")

        table = self.catalog.load_table(identifier)
        batch_reader = table.scan().to_arrow_batch_reader()

        match file.suffix:
            case ".csv":
                writer = CSVWriter(file, batch_reader.schema)
            case ".parquet" | ".pqt":
                writer = ParquetWriter(file, batch_reader.schema)
            case ".feather" | ".ftr":
                writer = RecordBatchFileWriter(file, batch_reader.schema)
            case _:
                raise ValueError(f"Unsupported file extension: {file.suffix}")

        list(map(writer.write_batch, batch_reader))
        writer.close()

    async def create_table(
        self,
        identifier: Annotated[str | Identifier, Field(description="The identifier of the table.")],
        contents: Annotated[dict[str, list] | None, Field(description="Columnar dictionary of table contents.")] = None,
        file: Annotated[Path | None, Field(description="Path to table file.")] = None,
    ) -> Annotated[str, Field(description="JSON representation of 5 table rows.")]:
        """Create a new Iceberg table and populate it with contents.

        Creates a new table in the catalog with the specified identifier using
        the schema inferred from the provided contents/file, then overwrites the table
        with the actual data. Either contents or file must be provided.

        Args:
            identifier: The identifier of the table to create.
            contents: A columnar dictionary where keys are column names and values
                are lists of column data.
            file: Path to table file. All file types supported by Polars can be used.

        Raises:
            ValueError: If none or both contents and file are provided.
        """

        if contents is not None and file is not None:
            raise ValueError("Only one of contents or file can be provided!")
        elif contents is not None:
            table_contents = Table.from_pydict(contents)
        elif file is not None:
            table_contents = self._read_table_from_file(file)
        else:
            raise ValueError("One of contents or file must be provided!")

        table = self.catalog.create_table(identifier, table_contents.schema)

        table.overwrite(table_contents)

        return await self.read_table_contents(identifier, limit=5)

    async def write_table(
        self,
        identifier: Annotated[str | Identifier, Field(description="The identifier of the table.")],
        mode: Annotated[
            Literal["append", "overwrite"], Field(description="Append the contents or overwrite the table.")
        ],
        contents: Annotated[dict[str, list] | None, Field(description="Columnar dictionary of table contents.")] = None,
        file: Annotated[Path | None, Field(description="Path to table file.")] = None,
    ) -> Annotated[str, Field(description="JSON representation of 5 table rows.")]:
        """Write data to an existing Iceberg table.

        Args:
            identifier: The identifier of the table.
            mode: Append the contents or overwrite the table.
            contents: A columnar dictionary where keys are column names and values
                are lists of column data.
            file: Path to table file. All file types supported by Polars can be used.

        Returns:
            JSON representation of 5 table rows.

        Raises:
            ValueError: If both contents and file are provided, or if neither is provided,
                or if an invalid write mode is provided.
        """
        if contents is not None and file is not None:
            raise ValueError("Only one of contents or file can be provided!")
        elif contents is not None:
            table_contents = Table.from_pydict(contents)
        elif file is not None:
            table_contents = self._read_table_from_file(file)
        else:
            raise ValueError("One of contents or file must be provided!")

        table = self.catalog.load_table(identifier)

        match mode:
            case "append":
                table.append(table_contents)
            case "overwrite":
                table.overwrite(table_contents)
            case _:
                raise ValueError(f"Invalid write table mode provided: {mode}")

        return await self.read_table_contents(identifier, limit=5)

    async def delete_table(
        self, identifier: Annotated[str | Identifier, Field(description="The identifier of the table.")]
    ) -> Annotated[list[Identifier], Field(description="List of remaining tables in namespace.")]:
        """Delete a table from the catalog.

        Args:
            identifier: The identifier of the table.

        Returns:
            List of remaining tables in namespace.
        """
        self.catalog.drop_table(identifier)

        namespace = Catalog.namespace_from(identifier)

        return await self.list_tables(namespace)

    def _read_table_from_file(self, file: Annotated[Path, Field(description="Path of table file.")]) -> Table:
        """Read table contents from a file.

        Args:
            file: Path of table file.

        Returns:
            PyArrow Table containing the file contents.

        Raises:
            ValueError: If the file extension is unsupported.
        """
        match file.suffix:
            case ".csv":
                return pl.read_csv(file).to_arrow()
            case ".xslx" | ".xls":
                return pl.read_excel(file).to_arrow()
            case ".json":
                return pl.read_json(file).to_arrow()
            case ".parquet" | ".pqt":
                return pl.read_parquet(file).to_arrow()
            case ".avro":
                return pl.read_avro(file).to_arrow()
            case ".feather" | ".ftr":
                return pl.read_ipc(file).to_arrow()
            case _:
                raise ValueError(f"Unsupported file extension: {file.suffix}")
