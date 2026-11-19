
import urllib.request
import shutil

from pathlib import Path

from src.logger import setup_logger

logger = setup_logger(__name__)

URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2024-01.parquet"
)

OUTPUT_PATH = Path("data/raw/yellow_tripdata_2024-01.parquet")

def download_taxi_data():

    try:
        logger.info("Starting NYC Taxi dataset download")

        # Create the raw data directory if necessary
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Download to a temporary file first
        temp_path = OUTPUT_PATH.with_suffix(".tmp")

        request = urllib.request.Request(
            URL,
            headers={"User-Agent": "NYC-Taxi-ETL/1.0"}
        )

        with urllib.request.urlopen(request, timeout=120) as response:

            if response.status != 200:
                raise RuntimeError(
                    f"Unexpected HTTP status: {response.status}"
                )

            with open(temp_path, "wb") as file:
                shutil.copyfileobj(response, file)

        # Check that the downloaded file is not empty
        if not temp_path.exists() or temp_path.stat().st_size == 0:
            raise RuntimeError("Downloaded file is empty or missing")

        # Move the completed download to its final location
        temp_path.replace(OUTPUT_PATH)

        file_size_mb = OUTPUT_PATH.stat().st_size / (1024 * 1024)

        logger.info(
            "Download completed successfully. File size: %.2f MB",
            file_size_mb
        )

    except Exception:
        logger.exception("NYC Taxi dataset download failed")
        raise

if __name__ == "__main__":
    download_taxi_data()