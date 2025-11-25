"""Query helper utilities for executing SQL against Apache Iceberg.

This module provides a small helper class to execute SQL queries and return
results as a list of dictionaries.
"""

from typing import Optional

from duckdb import DuckDBPyConnection
from duckdb import connect as ddb_connect
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

    async def sql_query(self, query: str) -> str:
        """Execute a SQL query and return results as a list of dicts.

        The query is executed using the embedded DuckDB connection and the
        result is materialized into a Polars DataFrame before being converted
        to a list of dictionaries.

        Note:
            When querying Iceberg tables, the SQL table name should be of the format catalog.table_identifier.

        Args:
            query: The SQL query string to execute.

        Returns:
            Query results by row as JSON.
        """
        return self.duckdb.sql(query).execute().pl().write_json()

    @staticmethod
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
