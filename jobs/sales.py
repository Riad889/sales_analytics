from pathlib import Path
from processors.sales_processor import SalesProcessor

from base import Core

BASE_DIR = Path(__file__).resolve().parent.parent


class Sales(Core):
    def __init__(self, spark_session):
        super().__init__()

        self.spark = spark_session
        self.sales_processor = SalesProcessor(spark=self.spark)

    def run(self):
        sales_data_path = f"{BASE_DIR}/data/sales_data.jsonl"

        sales_df = self.spark.read.json(sales_data_path)

        final_df = self.sales_processor.process_sales(sales_data=sales_df)
        self.sales_processor.insert_data(df=final_df)

        return final_df
