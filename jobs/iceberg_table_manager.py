from logger import logging

from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shared.utils import Utils

BASE_DIR = Path(__file__).resolve().parent.parent


class IcebergTableManager:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.utils = Utils()
        self.logger = logging.getLogger(__name__)

    def create_table(self, table_name: str):
        filepath = f"{BASE_DIR}/jobs/schema/{table_name}.sql"

        sql = self.utils.load_schema_file(filepath=filepath)
        try:
            self.spark.sql(sql)
            self.logger.info(f"{table_name} is successfully created")

        except Exception as error:
            self.logger.error(error)

    def align_df_with_table(self, df: DataFrame, table_name: str) -> DataFrame:
        table_schema = self.spark.table(table_name).schema

        table_columns = {field.name: field.dataType for field in table_schema}
        aligned_columns = []
        for column_name, data_type in table_columns.items():
            if column_name in df.columns:
                column = F.col(column_name).cast(data_type)
            else:
                column = F.lit(None).cast(data_type)

            aligned_columns.append(column.alias(column_name))

        return df.select(*aligned_columns)

    def insert_data_into_db(self, df: DataFrame, table_name: str):
        if df is None or df.isEmpty():
            return
        df.writeTo(table_name).append()
