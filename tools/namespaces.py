from typing import Annotated, List, Union

from pydantic import Field
from pyiceberg.typedef import Identifier

from catalog import load_catalog


async def list_namespaces(
    namespace: Annotated[Union[str, Identifier], Field(description="Parent namespace identifier to search.")] = (),
) -> List[Identifier]:
    """List all namespaces under a parent namespace.

    Args:
        namespace (str | Identifier): Parent namespace identifier to
            search. Defaults to root namespace.

    Returns:
        List[Identifier]: A list of namespace identifiers.
    """
    catalog = load_catalog()

    return catalog.list_namespaces(namespace)


async def create_namespace(
    namespace: Annotated[Union[str, Identifier], Field(description="Namespace to create.")],
) -> List[Identifier]:
    """Create a new namespace if it does not already exist.

    Args:
        namespace (str | Identifier): Namespace to create.

    Returns:
        List[Identifier]: A list of all namespaces under the parent namespace.
    """
    catalog = load_catalog()
    catalog.create_namespace_if_not_exists(namespace)

    return catalog.list_namespaces(namespace)
