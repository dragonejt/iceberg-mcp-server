from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock, patch

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from polars import DataFrame
from pyiceberg.catalog import Catalog, CatalogType

from iceberg_mcp_server.tools.query import QueryTools, load_duckdb


class TestQuerySQL(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.df = DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})
        self.mock_duckdb = Mock(spec=DuckDBPyConnection)
        self.tools = QueryTools(self.mock_duckdb)

        mock_result = Mock(spec=DuckDBPyRelation)
        mock_result.pl.return_value = self.df
        mock_result.arrow.return_value = self.df.to_arrow().to_reader()
        self.mock_duckdb.sql.return_value.execute.return_value = mock_result

    async def test_sql_query_with_csv_file(self) -> None:
        with TemporaryDirectory() as parent_dir:
            file_path = Path(parent_dir) / "test.csv"

            await self.tools.sql_query("SELECT * FROM CATALOG", file_path)

            self.assertTrue(file_path.is_file())

    async def test_sql_query_with_unsupported_file_extension(self) -> None:
        with self.assertRaises(ValueError) as context:
            await self.tools.sql_query("SELECT * FROM CATALOG", Path("test.unsupported"))

        self.assertIn("Unsupported file extension", str(context.exception))

    async def test_sql_query_with_nonexistent_parent_directory(self) -> None:
        with self.assertRaises(FileNotFoundError) as context:
            await self.tools.sql_query("SELECT * FROM CATALOG", Path("/nonexistent/test.csv"))

        self.assertIn("Parent directory", str(context.exception))

    async def test_sql_query(self) -> None:
        result = await self.tools.sql_query("SELECT * FROM CATALOG")

        self.assertEqual(result, self.df.write_json())
        self.mock_duckdb.sql.assert_called_with("SELECT * FROM CATALOG")


class TestQueryLoadDuckDB(IsolatedAsyncioTestCase):
    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_glue_glue_profile(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {
            "type": "glue",
            "glue.id": "123456789012",
            "glue.region": "us-east-1",
            "glue.profile-name": "default",
        }

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType(mock_catalog.properties["type"]), mock_catalog.properties)

        mock_conn.install_extension.assert_any_call("iceberg")
        mock_conn.install_extension.assert_any_call("aws")
        mock_conn.load_extension.assert_any_call("iceberg")
        mock_conn.load_extension.assert_any_call("aws")
        self.assertEqual(result, mock_conn)

    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_glue_catalog_client_profile(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {
            "type": "glue",
            "glue.id": "123456789012",
            "client.region": "us-west-2",
            "client.profile-name": "my-profile",
        }

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType(mock_catalog.properties["type"]), mock_catalog.properties)

        mock_conn.install_extension.assert_any_call("iceberg")
        mock_conn.install_extension.assert_any_call("aws")
        mock_conn.load_extension.assert_any_call("iceberg")
        mock_conn.load_extension.assert_any_call("aws")
        self.assertEqual(result, mock_conn)

    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_glue_catalog_client_credentials(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {
            "type": "glue",
            "glue.id": "123456789012",
            "client.region": "eu-west-1",
            "client.access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "client.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        }

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType.GLUE, mock_catalog.properties)

        mock_conn.install_extension.assert_any_call("iceberg")
        mock_conn.install_extension.assert_any_call("aws")
        mock_conn.load_extension.assert_any_call("iceberg")
        mock_conn.load_extension.assert_any_call("aws")
        self.assertEqual(result, mock_conn)

    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_glue_credentials(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {
            "type": "glue",
            "glue.id": "123456789012",
            "glue.region": "us-east-1",
            "glue.access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "glue.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        }

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType(mock_catalog.properties["type"]), mock_catalog.properties)

        mock_conn.install_extension.assert_any_call("iceberg")
        mock_conn.install_extension.assert_any_call("aws")
        mock_conn.load_extension.assert_any_call("iceberg")
        mock_conn.load_extension.assert_any_call("aws")
        self.assertEqual(result, mock_conn)

    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_glue_explicit_credentials(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {
            "type": "glue",
            "glue.id": "123456789012",
            "glue.region": "us-east-1",
            "glue.access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "glue.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        }

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType.GLUE, mock_catalog.properties)

        mock_conn.install_extension.assert_any_call("iceberg")
        mock_conn.install_extension.assert_any_call("aws")
        mock_conn.load_extension.assert_any_call("iceberg")
        mock_conn.load_extension.assert_any_call("aws")
        self.assertEqual(result, mock_conn)

    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_rest_catalog_oauth2(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {
            "warehouse": "test_warehouse",
            "uri": "http://test-uri",
            "oauth2-server-uri": "http://oauth-uri",
            "client-id": "test-client-id",
            "client-secret": "test-client-secret",
        }

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType.REST, mock_catalog.properties)

        mock_conn.install_extension.assert_any_call("iceberg")
        mock_conn.load_extension.assert_called_with("iceberg")
        self.assertEqual(result, mock_conn)

    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_rest_catalog_token(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {"warehouse": "test_warehouse", "uri": "http://test-uri", "token": "test-token"}

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType.REST, mock_catalog.properties)

        mock_conn.install_extension.assert_called_with("iceberg")
        self.assertEqual(result, mock_conn)

    @patch("iceberg_mcp_server.tools.query.ddb_connect")
    def test_load_duckdb_with_unsupported_catalog_type(
        self,
        mock_connect: Mock,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {}

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn

        result = load_duckdb(CatalogType.BIGQUERY, mock_catalog.properties)

        self.assertIsNone(result)
