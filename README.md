# Sales Analytics

A small PySpark + Iceberg pipeline that generates sales data, transforms it, and writes it to an Iceberg table.

## Features

- Generates realistic sales records in JSONL chunks
- Processes nested sales payloads into a flat analytics table
- Creates the Iceberg table automatically if it does not already exist
- Runs the job through a CLI entrypoint

## Project structure

- `main.py` — CLI entrypoint
- `generators/` — data generation logic
- `jobs/` — processing jobs and schema definitions
- `processors/` — transformation pipeline
- `data/` — generated JSONL files
- `iceberg/warehouse/` — local Iceberg warehouse

## Prerequisites

- Python 3.11+
- A virtual environment
- Java installed for Spark

## Setup

From the project root:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

If you are using the project environment already created in this repo, you can use:

```bash
.venv\Scripts\python.exe
```

## 1) Generate sales data

This creates a JSONL file in `data/sales_data.jsonl`:

```bash
python main.py --data_generator sales --data_size 100000 --chunk_size 50000
```

Example options:

- `--data_size`: total number of rows to generate
- `--chunk_size`: rows per chunk written to file
- `--data_generator`: currently supports `sales`

The generated file looks like this:

```json
{"order_id":"ORD-...","order_date":"2026-08-01T...","customer":{"customer_id":"CUST-..."},"product":{"product_id":"P-..."},"pricing":{"total_amount":...}}
```

## 2) Run the processing job

This reads the generated file, transforms the nested JSON into the final analytics shape, and writes to the Iceberg table:

```bash
python main.py --cron_job sales
```

The code checks whether the table exists before loading data. If it is missing, it creates it from the SQL schema in `jobs/schema/sales_analytics.sql`.

## 3) Table creation behavior

The table creation logic is implemented at the code level:

- `jobs/iceberg_table_manager.py`
- `jobs/schema/sales_analytics.sql`

Before the sales job processes any records, it calls:

```python
self.sales_processor.iceberg_table_manager.create_table(
    table_name="sales_analytics.sales"
)
```

If the table is missing, the manager loads the SQL schema and executes it. If it already exists, it skips creation.

## Example end-to-end flow

```bash
.venv\Scripts\python.exe main.py --data_generator sales --data_size 5000 --chunk_size 1000
.venv\Scripts\python.exe main.py --cron_job sales
```

## Notes

- The local Iceberg warehouse is created under `iceberg/warehouse/`.
- The default Spark app name is `Travel Analytics`.
- Spark is configured with a local Iceberg catalog and runtime package.

## Troubleshooting

### Import errors

Make sure you are running inside the project virtual environment and from the project root.

### Table not found

Verify that the schema file exists:

```bash
jobs/schema/sales_analytics.sql
```

### No data written

Check that `data/sales_data.jsonl` exists and contains valid JSON lines before running the job.
