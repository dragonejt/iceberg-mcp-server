"""Helpers for loading catalogs and DuckDB with Iceberg support.

This module provides convenience functions used to instantiate a Catalog
based on environment configuration and to initialize a DuckDB connection
with the Iceberg extension and attached catalog.
"""

from os import getenv
from typing import Optional

import duckdb
from pyiceberg.catalog import Catalog, CatalogType, load_rest


def load_catalog() -> Catalog:
    """Load a PyIceberg Catalog according to environment variables.

    Returns:
        The configured Catalog instance.

    Raises:
        ValueError: If the catalog type specified by the environment is not
            supported.
    """
    catalog_type = CatalogType(getenv("ICEBERG_CATALOG_TYPE"))

    match catalog_type:
        case CatalogType.REST:
            return load_rest(
                "default",
                {
                    "uri": getenv("ICEBERG_CATALOG_URI"),
                    "token": getenv("ICEBERG_CATALOG_TOKEN"),
                    "warehouse": getenv("ICEBERG_CATALOG_WAREHOUSE"),
                },
            )
        case _:
            raise ValueError(f"Unsupported catalog type: {catalog_type.value}")


def load_duckdb() -> Optional[duckdb.DuckDBPyConnection]:
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
    con = duckdb.connect()
    con.load_extension("iceberg")

    catalog_type = CatalogType(getenv("ICEBERG_CATALOG_TYPE"))
    match catalog_type:
        case CatalogType.REST:
            con.sql(f"""
                    CREATE SECRET iceberg_secret (
                        TYPE iceberg,
                        TOKEN '{getenv("ICEBERG_CATALOG_TOKEN")}'
                    );
                    """)
            con.sql(f"""
                    ATTACH '{getenv("ICEBERG_CATALOG_WAREHOUSE")}' AS catalog (
                    TYPE iceberg,
                    SECRET iceberg_secret,
                    ENDPOINT '{getenv("ICEBERG_CATALOG_URI")}'
                    );
                    """)

        case _:
            return None

    return con
