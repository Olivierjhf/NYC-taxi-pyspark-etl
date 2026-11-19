
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    avg,
    round
)

from src.logger import setup_logger

logger = setup_logger(__name__)



def aggregate_daily_taxi_data(df: DataFrame) -> DataFrame:

    logger.info("Starting daily taxi data aggregation")

    # Extract the pickup date from the timestamp
    df = df.withColumn(
        "pickup_date",
        to_date(col("pickup_datetime"))
    )

    # Group data by pickup date and pickup location
    gold_df = (
        df.groupBy(
            "pickup_date",
            "pickup_location_id"
        )
        .agg(
            count("*").alias("total_trips"),

            sum("passenger_count").alias("total_passengers"),

            round(
                avg("trip_distance"), 2
            ).alias("avg_trip_distance"),

            round(
                avg("fare_amount"), 2
            ).alias("avg_fare_amount"),

            round(
                sum("total_amount"), 2
            ).alias("total_revenue")
        )
    )

    logger.info("Daily taxi data aggregation completed")

    return gold_df



def sort_gold_data(df: DataFrame) -> DataFrame:

    logger.info("Sorting Gold dataset")

    sorted_df = df.orderBy(
        col("pickup_date").asc(),
        col("pickup_location_id").asc()
    )

    logger.info("Gold dataset sorted successfully")

    return sorted_df


def build_gold_dataset(df: DataFrame) -> DataFrame:

    logger.info("Starting Gold layer transformations")

    # Step 1: Aggregate daily taxi data
    gold_df = aggregate_daily_taxi_data(df)

    # Step 2: Sort the aggregated results
    gold_df = sort_gold_data(gold_df)

    logger.info("Gold layer transformations completed successfully")

    return gold_df
