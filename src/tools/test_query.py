from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock

from duckdb import DuckDBPyConnection
from polars import DataFrame

from src.tools.query import QueryTools


class TestQuery(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.df = DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})
        self.mock_duckdb = Mock(spec=DuckDBPyConnection)
        self.tools = QueryTools(self.mock_duckdb)

    async def test_sql_query(self) -> None:
        self.mock_duckdb.sql.return_value.execute.return_value.pl.return_value = self.df
        result = await self.tools.sql_query("SELECT * FROM CATALOG")

        self.assertEqual(result, self.df.write_json())
        self.mock_duckdb.sql.assert_called_with("SELECT * FROM CATALOG")
