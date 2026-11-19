
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.logger import setup_logger

logger = setup_logger(__name__)


# Let's remove missing critical values
def remove_missing_critical_values(df: DataFrame) -> DataFrame:

    logger.info("Removing records with missing critical values")

    cleaned_df = df.filter(
        col("tpep_pickup_datetime").isNotNull()
        & col("tpep_dropoff_datetime").isNotNull()
        & col("PULocationID").isNotNull()
        & col("DOLocationID").isNotNull()
    )

    logger.info(
        "Missing critical values removed successfully"
    )

    return cleaned_df


# Let's remove invalid trips
def remove_invalid_trips(df: DataFrame) -> DataFrame:

    logger.info("Removing invalid taxi trips")

    valid_df = df.filter(
        (col("trip_distance") > 0)
        & (col("fare_amount") > 0)
        & (col("passenger_count") > 0)
        & (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    )

    logger.info("Invalid taxi trips removed successfully")

    return valid_df


# Lets remove duplicates 

def remove_duplicate_trips(df: DataFrame) -> DataFrame:

    logger.info("Removing duplicate taxi trips")

    deduplicated_df = df.dropDuplicates([
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID"
    ])

    logger.info("Duplicate taxi trips removed successfully")

    return deduplicated_df


# Let's standardize the column names.

def standardize_column_names(df: DataFrame) -> DataFrame:

    logger.info("Standardizing column names")

    standardized_df = (
        df.withColumnRenamed(
            "VendorID", "vendor_id"
        )
        .withColumnRenamed(
            "RatecodeID", "rate_code_id"
        )
        .withColumnRenamed(
            "PULocationID", "pickup_location_id"
        )
        .withColumnRenamed(
            "DOLocationID", "dropoff_location_id"
        )
        .withColumnRenamed(
            "Airport_fee", "airport_fee"
        )
        .withColumnRenamed(
            "tpep_pickup_datetime", "pickup_datetime"
        )
        .withColumnRenamed(
            "tpep_dropoff_datetime", "dropoff_datetime"
        )
    )

    logger.info("Column names standardized successfully")

    return standardized_df



def clean_silver_data(df: DataFrame) -> DataFrame:

    logger.info("Starting Silver layer transformations")

    # Step 1: Remove records with missing critical values
    df = remove_missing_critical_values(df)

    # Step 2: Remove invalid taxi trips
    df = remove_invalid_trips(df)

    # Step 3: Remove duplicate taxi trips
    df = remove_duplicate_trips(df)

    # Step 4: Standardize column names
    df = standardize_column_names(df)

    logger.info("Silver transformations completed successfully")

    return df



