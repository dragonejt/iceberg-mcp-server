from os import getenv

from pyiceberg.catalog import Catalog, CatalogType, load_rest


def load_catalog() -> Catalog:
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
