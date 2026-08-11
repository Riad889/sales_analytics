from pathlib import Path

from base import Core

BASE_DIR = Path(__file__).resolve().parent.parent
class Booking(Core):
    def __init__(self, spark_session):
        super().__init__()
        self.spark = spark_session

    def run(self):
        booking_data_path = f"{BASE_DIR}/data/booking_data.jsonl"
        booking_df = self.spark.read.json(booking_data_path)

        booking_df.show(5)

        
