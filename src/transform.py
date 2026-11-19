from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit 

from src.logger import setup_logger

logger = setup_logger(__name__)


def add_bronze_metadata(df: DataFrame) -> DataFrame:

    logger.info("Adding metadata to Bronze layer")

    bronze_df = (
        df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_system",
            lit("nyc_tlc")
        )
    )

    logger.info("Bronze metadata added successfully")

    return bronze_df

    

