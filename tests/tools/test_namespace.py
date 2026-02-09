from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock

from pyiceberg.catalog import Catalog

from src.tools.namespace import NamespaceTools


class TestNamespace(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_catalog = Mock(spec=Catalog)
        self.tools = NamespaceTools(self.mock_catalog)

    async def test_list_namespaces_returns_all_namespaces(self) -> None:
        namespace_list = [("namespace1",), ("namespace2",)]
        self.mock_catalog.list_namespaces.return_value = namespace_list

        result = await self.tools.list_namespaces()

        self.assertEqual(result, namespace_list)
        self.mock_catalog.list_namespaces.assert_called_once()

    async def test_list_namespaces_with_parent_returns_child_namespaces(self) -> None:
        child_namespace_list = [("child_namespace",)]
        self.mock_catalog.list_namespaces.return_value = child_namespace_list

        result = await self.tools.list_namespaces("parent_namespace")

        self.assertEqual(result, child_namespace_list)
        self.mock_catalog.list_namespaces.assert_called_once_with("parent_namespace")

    async def test_create_namespace_creates_and_returns_updated_list(self) -> None:
        new_namespace_list = [("new_namespace",), ("existing_namespace",)]
        self.mock_catalog.create_namespace_if_not_exists.return_value = None
        self.mock_catalog.list_namespaces.return_value = new_namespace_list

        result = await self.tools.create_namespace("new_namespace")

        self.mock_catalog.create_namespace_if_not_exists.assert_called_once_with("new_namespace")
        self.assertEqual(result, new_namespace_list)

    async def test_delete_namespace_drops_and_returns_updated_list(self) -> None:
        remaining_namespace_list = [("remaining_namespace",)]
        self.mock_catalog.drop_namespace.return_value = None
        self.mock_catalog.list_namespaces.return_value = remaining_namespace_list

        result = await self.tools.delete_namespace("test_namespace")

        self.mock_catalog.drop_namespace.assert_called_once_with("test_namespace")
        self.assertEqual(result, remaining_namespace_list)
