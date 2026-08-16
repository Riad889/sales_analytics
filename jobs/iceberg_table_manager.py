from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shared.utils import Utils


class IcebergTableManager:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.utils = Utils()

    def create_table(self):
        pass

    def align_df_with_table(self, df: DataFrame, table_name: str):
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
