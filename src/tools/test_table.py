import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock

from polars import DataFrame
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Snapshot

from src.tools.table import TableTools


class TestTable(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_catalog = Mock(spec=Catalog)
        self.tools = TableTools(self.mock_catalog)
        self.mock_table = Mock(spec=Table)
        self.mock_catalog.load_table.return_value = self.mock_table

        self.mock_metadata = Mock(spec=TableMetadata)
        self.mock_table.metadata = self.mock_metadata

        self.mock_snapshot1 = Mock(spec=Snapshot)
        self.mock_snapshot1.snapshot_id = 1

        self.mock_snapshot2 = Mock(spec=Snapshot)
        self.mock_snapshot2.snapshot_id = 2
        self.mock_snapshot2.parent_snapshot_id = 1

        self.mock_table.current_snapshot.return_value = self.mock_snapshot2
        self.mock_table.inspect.snapshots.return_value = [self.mock_snapshot1, self.mock_snapshot2]

        self.df = DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})
        self.mock_snapshot2.summary = {"total-records": len(self.df)}
        self.mock_table.to_polars.side_effect = OSError
        self.mock_table.scan.return_value.to_polars.return_value = self.df

    async def test_list_tables_returns_table_list(self) -> None:
        self.mock_catalog.list_tables.return_value = [("table1",), ("table2",)]

        result = await self.tools.list_tables("namespace")

        self.assertEqual(result, [("table1",), ("table2",)])
        self.assertEqual(self.mock_catalog.list_tables.call_args, (("namespace",),))
        self.assertEqual(self.mock_catalog.list_tables.call_count, 1)

    async def test_read_table_metadata_returns_table_metadata(self) -> None:
        result = await self.tools.read_table_metadata("test_table")

        self.assertEqual(result, self.mock_metadata)

        self.assertEqual(self.mock_catalog.load_table.call_args, (("test_table",),))
        self.assertEqual(self.mock_catalog.load_table.call_count, 1)

    async def test_read_table_contents_returns_full_table(self) -> None:
        result = await self.tools.read_table_contents("test_table")

        self.assertEqual(result, self.df.write_json())

    async def test_read_table_contents_with_pagination_returns_page(self) -> None:
        result = await self.tools.read_table_contents("test_table", end=2)

        self.assertEqual(result, DataFrame({"a": [1, 2], "b": [3, 4]}).write_json())

    async def test_create_table_returns_tail(self) -> None:
        self.mock_catalog.create_table.return_value = self.mock_table

        await self.tools.create_table("test_table", self.df.to_dict(as_series=False))

        self.mock_catalog.create_table.assert_called_with("test_table", self.df.to_arrow().schema)
        self.mock_table.overwrite.assert_called_with(self.df.to_arrow())

    async def test_overwrite_table_returns_tail(self) -> None:
        self.mock_catalog.create_table.return_value = self.mock_table

        await self.tools.write_table("test_table", "overwrite", self.df.to_dict(as_series=False))

        self.mock_table.overwrite.assert_called_with(self.df.to_arrow())

    async def test_append_table_returns_tail(self) -> None:
        self.mock_catalog.create_table.return_value = self.mock_table

        await self.tools.write_table("test_table", "append", self.df.to_dict(as_series=False))

        self.mock_table.append.assert_called_with(self.df.to_arrow())

    async def test_read_table_snapshots_returns_all_snapshots(self) -> None:
        result = await self.tools.read_table_snapshots("test_table")

        self.assertEqual(len(result), 2)

        self.assertEqual(result[0].snapshot_id, 1)
        self.assertEqual(result[1].snapshot_id, 2)

    async def test_read_table_snapshots_filters_by_snapshot_id(self) -> None:
        result = await self.tools.read_table_snapshots("test_table", snapshot_id=1)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].snapshot_id, 1)

    async def test_read_table_snapshots_respects_limit(self) -> None:
        result = await self.tools.read_table_snapshots("test_table", limit=1)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].snapshot_id, 1)

    async def test_delete_table_calls_drop_and_returns_remaining_tables(self) -> None:
        self.mock_catalog.drop_table.return_value = None
        self.mock_catalog.list_tables.return_value = [("remaining_table",)]

        result = await self.tools.delete_table("test_table")

        self.assertEqual(self.mock_catalog.drop_table.call_args, (("test_table",),))
        self.assertEqual(self.mock_catalog.drop_table.call_count, 1)
        self.assertEqual(result, [("remaining_table",)])

    async def test_download_table_contents_with_csv_file(self) -> None:
        with tempfile.TemporaryDirectory() as parent_dir:
            file_path = Path(parent_dir) / "test.csv"

            self.mock_table.scan.return_value.to_arrow_batch_reader.return_value = self.df.to_arrow().to_reader()

            await self.tools.download_table_contents("test_table", file_path)

            self.assertTrue(file_path.is_file())

    async def test_download_table_contents_with_unsupported_file_extension(self) -> None:
        with self.assertRaises(ValueError):
            await self.tools.download_table_contents("test_table", Path("test.unsupported"))

    async def test_download_table_contents_with_nonexistent_parent_directory(self) -> None:
        with self.assertRaises(FileNotFoundError):
            await self.tools.download_table_contents("test_table", Path("/nonexistent/test.csv"))

    async def test_create_table_with_contents(self) -> None:
        self.mock_catalog.create_table.return_value = self.mock_table

        result = await self.tools.create_table("test_table", self.df.to_dict(as_series=False))

        self.mock_catalog.create_table.assert_called_once()
        self.mock_table.overwrite.assert_called_once()
        self.assertIsInstance(result, str)

    async def test_create_table_with_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.csv"
            self.df.write_csv(file_path)

            self.mock_catalog.create_table.return_value = self.mock_table

            await self.tools.create_table("test_table", file=file_path)

            self.mock_catalog.create_table.assert_called_once()
            self.mock_table.overwrite.assert_called_once()

    async def test_create_table_with_both_contents_and_file(self) -> None:
        with self.assertRaises(ValueError):
            await self.tools.create_table("test_table", self.df.to_dict(as_series=False), file=Path("test.csv"))

    async def test_create_table_with_neither_contents_nor_file(self) -> None:
        with self.assertRaises(ValueError):
            await self.tools.create_table("test_table")

    async def test_write_table_append_mode(self) -> None:
        await self.tools.write_table("test_table", "append", self.df.to_dict(as_series=False))

        self.mock_table.append.assert_called_once()

    async def test_write_table_overwrite_mode(self) -> None:
        await self.tools.write_table("test_table", "overwrite", self.df.to_dict(as_series=False))

        self.mock_table.overwrite.assert_called_once()

    async def test_write_table_with_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.csv"
            self.df.write_csv(file_path)

            await self.tools.write_table("test_table", "append", file=file_path)

            self.mock_table.append.assert_called_once()

    async def test_write_table_with_both_contents_and_file(self) -> None:
        with self.assertRaises(ValueError) as context:
            await self.tools.write_table(
                "test_table", "append", self.df.to_dict(as_series=False), file=Path("test.csv")
            )

        self.assertIn("Only one of contents or file can be provided", str(context.exception))

    async def test_write_table_with_neither_contents_nor_file(self) -> None:
        with self.assertRaises(ValueError) as context:
            await self.tools.write_table("test_table", "append")

        self.assertIn("One of contents or file must be provided", str(context.exception))
