
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.logger import setup_logger

logger = setup_logger(__name__)


def validate_not_empty(df: DataFrame, layer_name: str) -> None:

    logger.info("Validating that %s layer is not empty", layer_name)

    record_count = df.count()

    if record_count == 0:
        raise ValueError(
            f"{layer_name} layer is empty"
        )

    logger.info(
        "%s validation passed. Total records: %s",
        layer_name,
        record_count
    )



def validate_required_columns(
    df: DataFrame,
    required_columns: list,
    layer_name: str
) -> None:

    logger.info(
        "Checking required columns in %s layer",
        layer_name
    )

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{layer_name} layer is missing columns: "
            f"{missing_columns}"
        )

    logger.info(
        "Required column validation passed for %s",
        layer_name
    )



def validate_no_nulls(
    df: DataFrame,
    required_columns: list,
    layer_name: str
) -> None:

    logger.info(
        "Checking for NULL values in %s layer",
        layer_name
    )

    for column_name in required_columns:

        null_count = (
            df.filter(col(column_name).isNull())
            .count()
        )

        if null_count > 0:
            raise ValueError(
                f"{layer_name} layer contains "
                f"{null_count} NULL values in "
                f"column: {column_name}"
            )

    logger.info(
        "NULL validation passed for %s layer",
        layer_name
    )



def validate_silver_trips(df: DataFrame) -> None:

    logger.info("Validating Silver taxi trip records")

    invalid_trips = df.filter(
        (col("trip_distance") <= 0)
        | (col("fare_amount") <= 0)
        | (col("passenger_count") <= 0)
        | (col("dropoff_datetime") <= col("pickup_datetime"))
    )

    invalid_count = invalid_trips.count()

    if invalid_count > 0:
        raise ValueError(
            f"Silver layer contains {invalid_count} invalid trips"
        )

    logger.info(
        "Silver trip validation passed. No invalid trips found"
    )



def validate_gold_metrics(df: DataFrame) -> None:

    logger.info("Validating Gold layer metrics")

    invalid_records = df.filter(
        col("pickup_date").isNull()
        | col("pickup_location_id").isNull()
        | (col("total_trips") <= 0)
        | (col("total_passengers") <= 0)
        | (col("avg_trip_distance") <= 0)
        | (col("avg_fare_amount") <= 0)
    )

    invalid_count = invalid_records.count()

    if invalid_count > 0:
        raise ValueError(
            f"Gold layer contains {invalid_count} invalid records"
        )

    logger.info(
        "Gold metrics validation passed. No invalid records found"
    )



def validate_gold_duplicates(df: DataFrame) -> None:

    logger.info("Checking for duplicate Gold records")

    duplicates = (
        df.groupBy(
            "pickup_date",
            "pickup_location_id"
        )
        .count()
        .filter(col("count") > 1)
    )

    duplicate_count = duplicates.count()

    if duplicate_count > 0:
        raise ValueError(
            f"Gold layer contains {duplicate_count} duplicate groups"
        )

    logger.info(
        "Gold duplicate validation passed. No duplicates found"
    )


def validate_layer_counts(
    bronze_count: int,
    silver_count: int,
    gold_count: int
) -> None:

    logger.info("Comparing record counts across layers")

    if silver_count > bronze_count:
        raise ValueError(
            "Silver record count cannot exceed Bronze record count"
        )

    if gold_count > silver_count:
        raise ValueError(
            "Gold record count cannot exceed Silver record count"
        )

    logger.info(
        "Layer count validation passed. "
        "Bronze: %s | Silver: %s | Gold: %s",
        bronze_count,
        silver_count,
        gold_count
    )
