from pathlib import Path
from pyspark.sql import SparkSession
from src.logger import setup_logger

logger = setup_logger(__name__) # Creates a logger specifically for this extraction module.

# RAW_DATA_PATH defines where our original NYC Taxi Parquet file is stored.
RAW_DATA_PATH = Path(
    "data/raw/yellow_tripdata_2024-01.parquet" 
)

def create_spark_session():

    logger.info("Initializing Spark Session")

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_ETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    logger.info(
        "Spark session created successfully. Version: %s",
        spark.version 
    )

    return spark


def extract_raw_data(spark):

    logger.info("Starting raw data extraction")

    # Check whether the raw dataset exists
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Raw dataset not found: {RAW_DATA_PATH}"
        )

    # Read the Parquet dataset into a Spark DataFrame

    df = (
        spark.read
        .format("parquet")
        .load(str(RAW_DATA_PATH))
    )

    logger.info("Raw data loaded successfully")

    return df 



if __name__ == "__main__":

    spark = None

    try:
        # Create Spark session
        spark = create_spark_session()

        # Extract raw taxi data
        df = extract_raw_data(spark)

        # Display the dataset schema
        logger.info("Displaying raw dataset schema")
        df.printSchema()

        # Count the records
        total_records = df.count()

        logger.info(
            "Total records extracted: %s",
            total_records
        )

        print("\nFirst 5 records:")
        df.show(5, truncate=False)

    except Exception:
        logger.exception("Extraction test failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")

