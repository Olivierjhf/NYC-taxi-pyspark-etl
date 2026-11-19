
from pathlib import Path

import pyarrow.parquet as pq
import duckdb

from src.logger import setup_logger

logger = setup_logger(__name__)

RAW_FILE = Path("data/raw/yellow_tripdata_2024-01.parquet")

def inspect_raw_data():

    try:
        logger.info("Starting raw data validation")

        # Step 1: Check whether the file exists
        if not RAW_FILE.exists():
            raise FileNotFoundError(
                f"Raw dataset not found: {RAW_FILE}"
            )

        # Step 2: Check file size
        file_size = RAW_FILE.stat().st_size

        if file_size == 0:
            raise ValueError("Raw dataset is empty")

        logger.info(
            "File size: %.2f MB",
            file_size / (1024 * 1024)
        )

        # Step 3: Read Parquet metadata
        parquet_file = pq.ParquetFile(RAW_FILE)

        metadata = parquet_file.metadata
        schema = parquet_file.schema_arrow

        logger.info("Parquet validation successful")
        logger.info("Total rows: %s", metadata.num_rows)
        logger.info("Total columns: %s", len(schema))
        logger.info(
            "Total row groups: %s",
            metadata.num_row_groups
        )

        # Step 4: Display column names and data types
        logger.info("Dataset schema:")

        for field in schema:
            logger.info(
                "Column: %s | Type: %s | Nullable: %s",
                field.name,
                field.type,
                field.nullable
            )

        # Step 5: Query the Parquet file using DuckDB
        with duckdb.connect() as connection:

            result = connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(RAW_FILE)]
            ).fetchone()

            duckdb_row_count = result[0]

            # Step 6: Compare row counts
            if duckdb_row_count != metadata.num_rows:
                raise ValueError(
                    "Row count mismatch between PyArrow and DuckDB"
                )

            logger.info(
                "DuckDB row count verified: %s",
                duckdb_row_count
            )

            # Step 7: Preview five records
            cursor = connection.execute(
                "SELECT * FROM read_parquet(?) LIMIT 5",
                [str(RAW_FILE)]
            )

            column_names = [
                column[0] for column in cursor.description
            ]

            sample_rows = cursor.fetchall()

            print("\nNYC TAXI DATA PREVIEW")
            print("-" * 80)
            print(" | ".join(column_names))
            print("-" * 80)

            for row in sample_rows:
                print(" | ".join(str(value) for value in row))

        logger.info(
            "Raw dataset validation completed successfully"
        )

    except Exception:
        logger.exception("Raw dataset validation failed")
        raise

if __name__ == "__main__":
    inspect_raw_data()