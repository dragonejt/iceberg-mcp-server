"""Query helper utilities for executing SQL against Apache Iceberg.

This module provides a small helper class to execute SQL queries and return
results as a list of dictionaries.
"""

from pathlib import Path
from typing import Annotated, Optional

from duckdb import DuckDBPyConnection
from duckdb import connect as ddb_connect
from pyarrow.csv import CSVWriter
from pyarrow.ipc import RecordBatchFileWriter
from pyarrow.parquet import ParquetWriter
from pydantic import Field
from pyiceberg.catalog import Catalog, CatalogType, infer_catalog_type


class QueryTools:
    """Utilities for executing SQL queries against Iceberg via a DuckDB connection.

    Attributes:
        duckdb: Active DuckDB connection.
    """

    duckdb: DuckDBPyConnection

    def __init__(self, duckdb: DuckDBPyConnection) -> None:
        """Initialize QueryTools with a DuckDB connection.

        Args:
            connection: The DuckDB connection to use
                for executing queries.
        """
        self.duckdb = duckdb

    async def sql_query(
        self,
        query: Annotated[str, Field(description="DuckDB SQL query.")],
        file: Annotated[Optional[Path], Field(description="File path to write SQL query results.")] = None,
    ) -> Annotated[str, Field(description="Query results by row as JSON.")]:
        """Execute a SQL query and return results as a list of dicts or write to a file.

        The query is executed using the embedded DuckDB connection and the
        result is materialized into a Polars DataFrame. If no file is provided,
        the result is converted to a list of dictionaries and returned as JSON.
        If a file is provided, the results are written to the specified file in the
        appropriate format (CSV, Parquet, or Feather).

        Note:
            When querying Iceberg tables, the SQL table name should be of the format catalog.table_identifier.

        Args:
            query: The SQL query string to execute.
            file: Optional file path to write SQL query results. If provided,
                the results will be written to the specified file instead of being returned as JSON.

        Returns:
            Query results by row as JSON if file is not provided, otherwise a message
            indicating the file path and size of the written query results.

        Raises:
            FileNotFoundError: If the parent directory of the specified file does not exist.
            ValueError: If the file extension is unsupported.
        """
        result = self.duckdb.sql(query).execute()

        if file is None:
            return result.pl().write_json()
        elif file.parent.is_dir() is False:
            raise FileNotFoundError(f"Parent directory {file.parent.resolve()} must exist!")
        else:
            batch_reader = result.arrow()
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

            return f"Query result file: {file.resolve()} has file size of {file.stat().st_size} bytes."


def load_duckdb(catalog: Catalog) -> Optional[DuckDBPyConnection]:
    """Create and configure a DuckDB connection with the Iceberg extension.

    The function connects to an in-memory DuckDB instance, loads the
    Iceberg extension, and attaches an Iceberg catalog using the same
    environment variables as :func:`load_catalog`.

    Returns:
        The configured DuckDB connection.

    Raises:
        ValueError: If the catalog type specified by the environment is not
            supported.
    """
    con = ddb_connect()
    con.load_extension("iceberg")

    catalog_type = infer_catalog_type(catalog.name, catalog.properties)

    match catalog_type:
        case CatalogType.REST:
            if "oauth2-server-uri" in catalog.properties:
                con.sql(f"""
                        CREATE SECRET iceberg_secret (
                            TYPE iceberg,
                            CLIENT_ID '{catalog.properties["client-id"]}',
                            CLIENT_SECRET '{catalog.properties["client-secret"]}',
                            OAUTH2_SERVER_URI '{catalog.properties["oauth2-server-uri"]}'
                        );
                        """)
            else:
                con.sql(f"""
                        CREATE SECRET iceberg_secret (
                            TYPE iceberg,
                            TOKEN '{catalog.properties["token"]}'
                        );
                        """)
            con.sql(f"""
                    ATTACH '{catalog.properties.get("warehouse", "")}' AS catalog (
                        TYPE iceberg,
                        SECRET iceberg_secret,
                        ENDPOINT '{catalog.properties["uri"]}'
                    );
                    """)

        case _:
            return None

    return con
