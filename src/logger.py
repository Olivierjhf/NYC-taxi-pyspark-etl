
import logging
import os

from logging.handlers import RotatingFileHandler

def setup_logger(name="nyc_taxi_etl"):

    # Create the logs directory if it does not exist
    os.makedirs("logs", exist_ok=True)

    # Create the logger
    logger = logging.getLogger(name)

    # Set the logging level
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    # Define the log message format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Display logs in the terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Save logs in a file
    file_handler = RotatingFileHandler(
        "logs/etl_pipeline.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5
    )

    file_handler.setFormatter(formatter)

    # Attach handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger