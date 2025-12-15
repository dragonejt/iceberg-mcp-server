"""Namespace management helpers that wrap a PyIceberg Catalog.

This module exposes NamespaceTools, a thin helper around a Catalog
instance to list and create namespaces.
"""

from typing import Annotated, List, Union

from pydantic import Field
from pyiceberg.catalog import Catalog
from pyiceberg.typedef import Identifier


class NamespaceTools:
    """Helper methods for working with Iceberg namespaces.

    Attributes:
        catalog: Catalog instance used to perform
            namespace operations.
    """

    catalog: Catalog

    def __init__(self, catalog: Catalog) -> None:
        """Initialize the NamespaceTools wrapper.

        Args:
            catalog: Catalog used for namespace
                operations.
        """
        self.catalog = catalog

    async def list_namespaces(
        self,
        namespace: Annotated[Union[str, Identifier], Field(description="Parent namespace identifier to search.")] = (),
    ) -> Annotated[List[Identifier], Field(description="List of namespace identifiers.")]:
        """List all namespaces under a parent namespace.

        Args:
            namespace: Parent namespace identifier to
                search. Defaults to the root namespace when omitted or empty.

        Returns:
            A list of namespace identifiers.
        """

        return self.catalog.list_namespaces(namespace)

    async def create_namespace(
        self,
        namespace: Annotated[Union[str, Identifier], Field(description="Namespace to create.")],
    ) -> Annotated[List[Identifier], Field(description="List of all namespaces under root namespace.")]:
        """Create a new namespace if it does not already exist.

        Args:
            namespace: Namespace to create.

        Returns:
            A list of all namespaces under the root namespace.
        """
        self.catalog.create_namespace_if_not_exists(namespace)

        return await self.list_namespaces()

    async def delete_namespace(
        self,
        namespace: Annotated[Union[str, Identifier], Field(description="Namespace to delete.")],
    ) -> Annotated[List[Identifier], Field(description="List of remaining namespaces under root namespace.")]:
        """Delete a namespace from the catalog.

        Args:
            namespace: Namespace to delete.

        Returns:
            List of remaining namespaces under root namespace.
        """
        self.catalog.drop_namespace(namespace)

        return await self.list_namespaces()
