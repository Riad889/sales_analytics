import unittest
from unittest.mock import Mock

from jobs.sales import Sales
from jobs.iceberg_table_manager import IcebergTableManager


class IcebergTableManagerTests(unittest.TestCase):
    def test_create_table_uses_schema_when_table_is_missing(self):
        spark = Mock()
        spark.catalog.tableExists.return_value = False
        manager = IcebergTableManager(spark)

        manager.create_table("sales_analytics.sales")

        spark.sql.assert_called_once()
        spark.catalog.tableExists.assert_called_once_with("sales_analytics.sales")

    def test_sales_job_creates_table_before_processing(self):
        sales = Sales.__new__(Sales)
        sales.spark = Mock()
        sales.sales_processor = Mock()
        sales.sales_processor.iceberg_table_manager = Mock()
        sales.sales_processor.process_sales.return_value = Mock()
        sales.spark.read.json.return_value = Mock()

        sales.run()

        sales.sales_processor.iceberg_table_manager.create_table.assert_called_once_with(
            table_name="sales_analytics.sales"
        )


if __name__ == "__main__":
    unittest.main()
