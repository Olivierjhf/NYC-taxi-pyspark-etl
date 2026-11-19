
from src.extract import create_spark_session, extract_raw_data
from src.transform import add_bronze_metadata
from src.cleaning import clean_silver_data
from src.aggregation import build_gold_dataset

from src.load import (
    write_bronze_data,
    write_silver_data,
    write_gold_data
)

from src.validation import (
    validate_not_empty,
    validate_required_columns,
    validate_no_nulls,
    validate_silver_trips,
    validate_gold_metrics,
    validate_gold_duplicates,
    validate_layer_counts
)

from src.logger import setup_logger

logger = setup_logger(__name__)



def run_etl_pipeline():

    spark = None

    try:
        logger.info("=" * 60)
        logger.info("NYC TAXI ETL PIPELINE STARTED")
        logger.info("=" * 60)

        # Step 1: Initialize Spark
        spark = create_spark_session()

        # Step 2: Extract raw taxi data
        raw_df = extract_raw_data(spark)

        logger.info("Raw data extraction completed")


        # Step 3: Transform raw data into Bronze
        bronze_df = add_bronze_metadata(raw_df)

        # Step 4: Save data into Bronze layer
        write_bronze_data(bronze_df)

        logger.info("Bronze layer completed successfully")


        # Step 5: Clean Bronze data into Silver
        silver_df = clean_silver_data(bronze_df)

        # Step 6: Save data into Silver layer
        write_silver_data(silver_df)

        logger.info("Silver layer completed successfully")


        # Step 7: Aggregate Silver data into Gold
        gold_df = build_gold_dataset(silver_df)

        # Step 8: Save data into Gold layer
        write_gold_data(gold_df)

        logger.info("Gold layer completed successfully")


        # Step 9: Validate that all layers contain records
        logger.info("Starting data quality validation")

        validate_not_empty(bronze_df, "Bronze")
        validate_not_empty(silver_df, "Silver")
        validate_not_empty(gold_df, "Gold")

        logger.info("Non-empty validation completed successfully")


        # Step 10: Validate required columns

        validate_required_columns(
            bronze_df,
            [
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "ingestion_timestamp",
                "source_system"
            ],
            "Bronze"
        )

        validate_required_columns(
            silver_df,
            [
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "trip_distance",
                "fare_amount",
                "passenger_count"
            ],
            "Silver"
        )

        validate_required_columns(
            gold_df,
            [
                "pickup_date",
                "pickup_location_id",
                "total_trips",
                "total_passengers",
                "avg_trip_distance",
                "avg_fare_amount",
                "total_revenue"
            ],
            "Gold"
        )


        # Step 11: Validate Silver columns for NULL values
        validate_no_nulls(
            silver_df,
            [
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "trip_distance",
                "fare_amount",
                "passenger_count"
            ],
            "Silver"
        )


        # Step 12: Validate Silver taxi trips
        validate_silver_trips(silver_df)

        # Step 13: Validate Gold metrics
        validate_gold_metrics(gold_df)

        logger.info("Silver and Gold metric validations completed")


        # Step 14: Validate Gold aggregation uniqueness
        validate_gold_duplicates(gold_df)

        logger.info("Gold duplicate validation completed successfully")


        # Step 15: Compare record counts across layers
        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        gold_count = gold_df.count()

        validate_layer_counts(
            bronze_count,
            silver_count,
            gold_count
        )

        logger.info(
            "Record counts - Bronze: %s | Silver: %s | Gold: %s",
            bronze_count,
            silver_count,
            gold_count
        )

        logger.info("All data quality validations passed")


    except Exception:
        logger.exception("ETL pipeline failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    run_etl_pipeline()

    