# NYC Taxi Data Engineering Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline using **PySpark** to process NYC Yellow Taxi trip data for January 2024.

The pipeline follows the **Medallion Architecture**, transforming raw data through three layers:

- **Bronze:** Ingests and stores the original raw taxi trip data.
- **Silver:** Cleans, validates, and transforms the raw records.
- **Gold:** Produces aggregated business-ready metrics for analytical reporting.

The project also incorporates **Apache Airflow** to orchestrate and schedule the ETL workflow.

The goal is to demonstrate practical data engineering skills, including large-scale data processing, data quality validation, pipeline orchestration, and analytical data preparation.


## ETL Architecture

The pipeline uses the Medallion Architecture to organize data processing into three layers.

```text
                NYC Yellow Taxi Dataset
                    January 2024
                          |
                          v
                  +---------------+
                  |  Raw Data      |
                  |  Parquet       |
                  +---------------+
                          |
                          v
                  +---------------+
                  | Bronze Layer  |
                  | Raw Ingestion |
                  +---------------+
                          |
                          v
                  +---------------+
                  | Silver Layer  |
                  | Data Cleaning |
                  | Transformation|
                  +---------------+
                          |
                          v
                  +---------------+
                  | Gold Layer    |
                  | Aggregations  |
                  | Analytics     |
                  +---------------+
                          |
                          v
                  +---------------+
                  | Data Quality  |
                  | Validation    |
                  +---------------+
```

### Layer Responsibilities

**1. Bronze Layer**
- Reads the original NYC Yellow Taxi Parquet dataset.
- Preserves the ingested records.
- Establishes the initial stage of the data pipeline.

**2. Silver Layer**
- Cleans and transforms raw taxi trip records.
- Applies data validation and quality checks.
- Removes invalid records.

**3. Gold Layer**
- Aggregates cleaned taxi trip data into analytical metrics.
- Produces reporting-ready datasets.
- Saves the final output in Parquet format.

**Pipeline Orchestration**

Apache Airflow orchestrates the ETL pipeline, allowing it to run automatically according to a defined schedule or through manual execution.


## Technologies Used

| Technology | Purpose |
|---|---|
| Python 3.14 | Pipeline development and scripting |
| PySpark 4.3 | Distributed data processing |
| Apache Spark | Large-scale data transformation |
| Apache Airflow 3.4 | ETL workflow orchestration |
| Apache Parquet | Columnar data storage |
| PyArrow | Parquet data support |
| DuckDB | Analytical data querying |
| Java 21 | Spark execution environment |
| Git and GitHub | Version control and project hosting |
| Ubuntu Linux | Development environment |

## Key Engineering Concepts

- ETL (Extract, Transform, Load)
- Medallion Architecture (Bronze, Silver, Gold)
- Data cleaning and transformation
- Data quality validation
- Distributed data processing
- Parquet file storage
- Workflow orchestration with Airflow
- Modular Python project structure
- Logging and error handling


## Pipeline Execution Results

The pipeline successfully processed the January 2024 NYC Yellow Taxi dataset.

### Record Processing Summary

| Layer | Records |
|---|---:|
| Raw Dataset | 2,964,624 |
| Bronze | 2,964,624 |
| Silver | 2,723,743 |
| Gold | 6,679 |

### Data Quality Results

- Bronze ingestion successfully preserved all source records.
- Silver processing retained approximately **91.88%** of the original records.
- **240,881 records (8.12%)** were removed during cleaning and validation.
- Gold aggregation generated **6,679 analytical records**.
- No invalid Silver records were detected during final validation.
- No invalid Gold metrics were detected.
- Duplicate Gold date/location groups were checked.
- Layer record-count validation passed.

### Final Execution Status

All data quality validations passed successfully.

The pipeline completed execution, and the Spark session stopped successfully.


## Installation and Setup

### Prerequisites

Before running the project, make sure you have installed:

- Python 3.14
- Java 21
- Apache Spark / PySpark
- Apache Airflow 3.4
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/Olivierjhf/NYC-taxi-pyspark-etl.git
```

### 2. Navigate to the Project Directory

```bash
cd NYC-taxi-pyspark-etl
```

### 3. Create a Python Virtual Environment

```bash
python3 -m venv .venv
```

### 4. Activate the Environment

```bash
source .venv/bin/activate
```

### 5. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 6. Configure Java

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### 7. Run the ETL Pipeline

```bash
python main.py
```

The pipeline executes the Bronze, Silver, and Gold processing stages, followed by data quality validation.


## Project Structure

```text
NYC-taxi-pyspark-etl/
│
├── dags/
│   └── nyc_taxi_etl_dag.py
│
├── data/
│   ├── raw/
│   │   └── yellow_tripdata_2024-01.parquet
│   └── lakehouse/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
├── logs/
│
├── src/
│   ├── __init__.py
│   ├── aggregation.py
│   ├── cleaning.py
│   ├── download_data.py
│   ├── extract.py
│   ├── inspect_raw_data.py
│   ├── load.py
│   ├── logger.py
│   ├── transform.py
│   ├── validation.py
│   ├── test_bronze.py
│   ├── test_gold.py
│   ├── test_logger.py
│   ├── test_silver.py
│   └── test_validation.py
│
├── main.py
├── requirements.txt
├── README.md
└── .gitignore
```

### Project Highlights

- Modular and reusable Python code.
- Separate processing logic for each medallion layer.
- Automated workflow orchestration with Apache Airflow.
- Logging and data quality validation.
- Parquet-based data storage.
- Automated testing.
- Organized project structure.
```

### Project Highlights

- Modular and reusable Python code.
- Separate processing logic for each medallion layer.
- Automated workflow orchestration with Apache Airflow.
- Logging and data quality validation.
- Parquet-based data storage.
- Automated testing.

## Project Highlights

- Modular and reusable Python code.
- Separate processing logic for each medallion layer.
- Automated workflow orchestration with Apache Airflow.
- Logging and data quality validation.
- Parquet-based data storage.



## Dataset

This project uses the **NYC Yellow Taxi Trip Records – January 2024** dataset.

The dataset is publicly available from the New York City Taxi and Limousine Commission (TLC).

Official dataset source:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The original dataset is stored locally and excluded from Git tracking to keep the repository lightweight.

## Future Improvements

Potential extensions to this project include:

- Integrating AWS S3 for cloud-based data storage.
- Implementing Databricks for scalable cloud processing.
- Adding incremental data ingestion.
- Implementing automated testing in a CI/CD workflow.
- Adding analytical dashboards using the Gold layer.

## Author

**Data Engineering Portfolio Project**

GitHub: https://github.com/Olivierjhf

## License

This project is intended for educational and portfolio purposes.