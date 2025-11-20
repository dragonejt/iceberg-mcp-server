"""Query helper utilities for executing SQL against Apache Iceberg.

This module provides a small helper class to execute SQL queries and return
results as a list of dictionaries.
"""

from typing import List

from duckdb import DuckDBPyConnection


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

    async def sql_query(self, query: str) -> List[dict]:
        """Execute a SQL query and return results as a list of dicts.

        The query is executed using the embedded DuckDB connection and the
        result is materialized into a Polars DataFrame before being converted
        to a list of dictionaries.

        Note:
            When querying Iceberg tables, the SQL table name should be of the format catalog.table_identifier.

        Args:
            query: The SQL query string to execute.

        Returns:
            Query results where each row is represented as a
            dictionary mapping column names to values.
        """
        return self.duckdb.sql(query).execute().pl().to_dicts()
