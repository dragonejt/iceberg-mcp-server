import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock, patch

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from polars import DataFrame
from pyiceberg.catalog import Catalog, CatalogType

from src.tools.query import QueryTools, load_duckdb


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
        with tempfile.TemporaryDirectory() as parent_dir:
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
    @patch("src.tools.query.infer_catalog_type")
    @patch("src.tools.query.ddb_connect")
    def test_load_duckdb_with_rest_catalog_oauth2(
        self,
        mock_connect,
        mock_infer,
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
        mock_infer.return_value = CatalogType.REST

        result = load_duckdb(mock_catalog)

        mock_conn.load_extension.assert_called_with("iceberg")
        self.assertEqual(result, mock_conn)

    @patch("src.tools.query.infer_catalog_type")
    @patch("src.tools.query.ddb_connect")
    def test_load_duckdb_with_rest_catalog_token(
        self,
        mock_connect,
        mock_infer,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {"warehouse": "test_warehouse", "uri": "http://test-uri", "token": "test-token"}

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn
        mock_infer.return_value = CatalogType.REST

        result = load_duckdb(mock_catalog)

        mock_conn.load_extension.assert_called_with("iceberg")
        self.assertEqual(result, mock_conn)

    @patch("src.tools.query.infer_catalog_type")
    @patch("src.tools.query.ddb_connect")
    def test_load_duckdb_with_unsupported_catalog_type(
        self,
        mock_connect,
        mock_infer,
    ) -> None:
        mock_catalog = Mock(spec=Catalog)
        mock_catalog.name = "test_catalog"
        mock_catalog.properties = {}

        mock_conn = Mock(spec=DuckDBPyConnection)
        mock_connect.return_value = mock_conn
        mock_infer.return_value = CatalogType.BIGQUERY  # Unsupported type

        result = load_duckdb(mock_catalog)

        self.assertIsNone(result)
