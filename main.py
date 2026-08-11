from pathlib import Path
from pyspark.sql import SparkSession

from importlib import import_module
from argparse import ArgumentParser

BASE_DIR = Path(__file__).resolve().parent


def initalize_spark_session(app_name="Travel Analytics") -> SparkSession:
    WARE_HOUSE_PATH = BASE_DIR / "iceberg/warehouse"

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(WARE_HOUSE_PATH))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    return spark


def main():
    parser = ArgumentParser(description="Generate booking data")
    parser.add_argument(
        "--data_size",
        type=int,
        required=False,
        default=100_00_000,
        help="Number of booking records to generate (default: 100,000,000)",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        required=False,
        default=100_000,
        help="Number of booking records to generate per chunk (default: 100,000)",
    )

    parser.add_argument(
        "--data_generator", choices=["booking"],required=False, help="Type of data generator to use"
    )

    parser.add_argument(
            "--cron_job",
            type=str,
            required=False,
            help="Name of the cron job to run"
        )
    args = parser.parse_args()

    if args.data_generator:
        module_name = f"generators.{args.data_generator}_generator"

        generator_module = import_module(module_name)
        class_name = f"{args.data_generator.capitalize()}Generator"
        generator_class = getattr(generator_module, class_name)
        generator = generator_class(
            data_size=args.data_size, chunk_size=args.chunk_size
        )
        filepath = BASE_DIR / f"data/{args.data_generator}_data.jsonl"
        filepath.parent.mkdir(parents=True, exist_ok=True)

        for chunk in generator.generate_data():
            generator.shared_utils.save_to_jsonl(chunk, filepath)

    elif args.cron_job:
        spark = initalize_spark_session()
        module_name = f"jobs.{args.cron_job}"
        job_module = import_module(module_name)
        class_name = f"{args.cron_job.capitalize()}"
        job_class = getattr(job_module, class_name)
        job_instance = job_class(spark)
        job_instance.run()


if __name__ == "__main__":
    main()
