from typing import List, Union

from pyiceberg.typedef import Identifier

from catalog import load_catalog


def list_namespaces(namespace: Union[str, Identifier, None]) -> List[Identifier]:
    catalog = load_catalog()

    return catalog.list_namespaces(namespace)
