from src.logger import setup_logger

# Create our logger
logger = setup_logger("logger_test")

# Test different logging levels
logger.debug("This is a debug message")
logger.info("ETL pipeline started successfully")
logger.warning("This is a warning message")
logger.error("This is an error message")

print("Logger test completed!")