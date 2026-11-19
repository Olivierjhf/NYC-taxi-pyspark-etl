
from src.extract import create_spark_session, extract_raw_data
from src.transform import add_bronze_metadata
from src.load import write_bronze_data

from src.logger import setup_logger

logger = setup_logger(__name__)

def run_bronze_pipeline():

    spark = None

    try:
        # Step 1: Create Spark session
        spark = create_spark_session()

        # Step 2: Extract raw taxi data
        raw_df = extract_raw_data(spark)

        # Step 3: Add Bronze metadata
        bronze_df = add_bronze_metadata(raw_df)

        # Step 4: Display Bronze schema
        logger.info("Displaying Bronze schema")
        bronze_df.printSchema()

        # Step 5: Save data into Bronze layer
        write_bronze_data(bronze_df)

        logger.info("Bronze pipeline completed successfully")

    except Exception:
        logger.exception("Bronze pipeline failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    run_bronze_pipeline()