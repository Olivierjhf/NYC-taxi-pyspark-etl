
from src.extract import create_spark_session
from src.aggregation import build_gold_dataset
from src.load import write_gold_data

from src.logger import setup_logger

logger = setup_logger(__name__)

SILVER_PATH = "data/lakehouse/silver"

def run_gold_pipeline():

    spark = None

    try:
        # Step 1: Create Spark session
        spark = create_spark_session()

        # Step 2: Read Silver data
        logger.info("Reading Silver data")

        silver_df = (
            spark.read
            .format("parquet")
            .load(SILVER_PATH)
        )

        silver_count = silver_df.count()

        logger.info(
            "Total Silver records: %s",
            silver_count
        )

        # Step 3: Build Gold dataset
        gold_df = build_gold_dataset(silver_df)

        # Step 4: Count Gold records
        gold_count = gold_df.count()

        logger.info(
            "Total Gold records: %s",
            gold_count
        )

        # Step 5: Display Gold schema
        logger.info("Displaying Gold schema")
        gold_df.printSchema()

        # Step 6: Display sample Gold records
        logger.info("Displaying sample Gold records")
        gold_df.show(10, truncate=False)

        # Step 7: Save Gold data
        write_gold_data(gold_df)

        logger.info("Gold pipeline completed successfully")

    except Exception:
        logger.exception("Gold pipeline failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    run_gold_pipeline()