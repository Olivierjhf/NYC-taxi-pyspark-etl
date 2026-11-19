
from src.extract import create_spark_session
from src.cleaning import clean_silver_data
from src.load import write_silver_data

from src.logger import setup_logger

logger = setup_logger(__name__)

BRONZE_PATH = "data/lakehouse/bronze"

def run_silver_pipeline():

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

        bronze_count = bronze_df.count()

        logger.info(
            "Total Bronze records: %s",
            bronze_count
        )

        # Step 3: Clean Bronze data
        silver_df = clean_silver_data(bronze_df)

        # Step 4: Count cleaned records
        silver_count = silver_df.count()

        logger.info(
            "Total Silver records: %s",
            silver_count
        )

        # Step 5: Display Silver schema
        logger.info("Displaying Silver schema")
        silver_df.printSchema()

        # Step 6: Save Silver data
        write_silver_data(silver_df)

        logger.info("Silver pipeline completed successfully")

    except Exception:
        logger.exception("Silver pipeline failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    run_silver_pipeline()