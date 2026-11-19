
from src.extract import create_spark_session

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

BRONZE_PATH = "data/lakehouse/bronze"
SILVER_PATH = "data/lakehouse/silver"
GOLD_PATH = "data/lakehouse/gold"



def run_validation_pipeline():

    spark = None

    try:
        # Step 1: Create Spark session
        spark = create_spark_session()

        # Step 2: Read Bronze data
        logger.info("Reading Bronze data")

        bronze_df = (
            spark.read
            .format("parquet")
            .load(BRONZE_PATH)
        )

        # Step 3: Read Silver data
        logger.info("Reading Silver data")

        silver_df = (
            spark.read
            .format("parquet")
            .load(SILVER_PATH)
        )

        # Step 4: Read Gold data
        logger.info("Reading Gold data")

        gold_df = (
            spark.read
            .format("parquet")
            .load(GOLD_PATH)
        )

        logger.info("All three layers loaded successfully")

        # Step 5: Validate that all three layers are not empty
        logger.info("Starting non-empty validation")

        validate_not_empty(bronze_df, "Bronze")
        validate_not_empty(silver_df, "Silver")
        validate_not_empty(gold_df, "Gold")

        # Step 6: Validate required Bronze columns
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

        # Step 7: Validate required Silver columns
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

        # Step 8: Validate required Gold columns
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


        # Step 9: Validate Silver columns for NULL values
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


        # Step 10: Validate Silver taxi trips
        validate_silver_trips(silver_df)

        # Step 11: Validate Gold metrics
        validate_gold_metrics(gold_df)

        # Step 12: Check for duplicate Gold records
        validate_gold_duplicates(gold_df)


        # Step 13: Get record counts from all three layers
        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        gold_count = gold_df.count()

        # Step 14: Compare record counts
        validate_layer_counts(
            bronze_count,
            silver_count,
            gold_count
        )

        logger.info(
            "All data quality validations completed successfully!"
        )





    except Exception:
        logger.exception("Data validation pipeline failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    run_validation_pipeline()

    