
from pathlib import Path

from pyspark.sql import DataFrame

from src.logger import setup_logger

logger = setup_logger(__name__)

BRONZE_PATH = Path("data/lakehouse/bronze")

def write_bronze_data(df: DataFrame) -> None:

    try:
        logger.info("Starting Bronze layer data load")

        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .save(str(BRONZE_PATH))
        )

        logger.info(
            "Bronze data successfully saved to %s",
            BRONZE_PATH
        )

    except Exception:
        logger.exception("Bronze layer data load failed")
        raise



SILVER_PATH = Path("data/lakehouse/silver")

def write_silver_data(df: DataFrame) -> None:

    try:
        logger.info("Starting Silver layer data load")

        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .save(str(SILVER_PATH))
        )

        logger.info(
            "Silver data successfully saved to %s",
            SILVER_PATH
        )

    except Exception:
        logger.exception("Silver layer data load failed")
        raise



GOLD_PATH = Path("data/lakehouse/gold")

def write_gold_data(df: DataFrame) -> None:

    try:
        logger.info("Starting Gold layer data load")

        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .save(str(GOLD_PATH))
        )

        logger.info(
            "Gold data successfully saved to %s",
            GOLD_PATH
        )

    except Exception:
        logger.exception("Gold layer data load failed")
        raise