# Retail-Sales-Data-ETL-Pipeline
**Apache Spark | Apache Airflow | AWS S3 | MySQL | Python**

---

## Overview

This project implements a **production-style batch ETL pipeline** for processing retail sales data using **Apache Spark**, orchestrated with **Apache Airflow**.  
The pipeline ingests raw sales files from **Amazon S3**, validates and transforms the data, persists curated datasets, and ensures **idempotent processing** using a MySQL-backed staging layer.

The implementation closely mirrors **real-world data engineering systems**, focusing on reliability, observability, and data quality rather than notebook-style experimentation.

---

## Technology Stack

| Layer | Technology |
|------|-----------|
| Orchestration | Apache Airflow |
| Processing | Apache Spark (Local Mode) |
| Storage | AWS S3 |
| Metadata & Tracking | MySQL |
| Language | Python |
| Environment | Windows + WSL (Local setup) |

---

## High-Level Architecture

![Architecture Diagram](docs/Architecture.png)

1. **Ingestion**
   - Raw CSV/JSON files are read from an S3 source directory using `s3a://`.
   - File discovery is handled dynamically.

2. **Validation & Data Quality**
   - Schema enforcement
   - Mandatory field validation
   - Business rule checks (e.g., positive prices)
   - Duplicate handling

3. **Transformation**
   - Data normalization
   - Aggregations and joins
   - Partitioned Parquet output for analytics

4. **Persistence**
   - Cleaned datasets written to S3 (data mart layer)
   - Invalid records written to a separate S3 error path

5. **Idempotency**
   - MySQL staging table tracks file processing status
   - Prevents reprocessing of previously ingested files

6. **File Lifecycle Management**
   - Successfully processed files → `processed/`
   - Failed files → `error/`

7. **Orchestration**
   - Airflow DAG controls execution, retries, and failure handling
   - Spark jobs executed via `spark-submit`

---

## Database ER Diagram
![ER Diagram](docs/database_schema.png)

---

## Folder Structure
```
Sales-Data-ETL/
├── airflow/
│   └── sales_etl_dag.py
│
├── docs/
│   ├── Architecture.png
│   └── database_schema.png
│
├── resources/
│   ├── dev/
│   │   ├── config.yaml
│   │   └── requirements.txt
│   │
│   └── sql_scripts/
│       └── create_table_scripts.sql
│
├── scripts/
│   ├── generate_data/
│   │   ├── generate_csv_data.py
│   │   ├── generate_customer_table_data.py
│   │   ├── generate_product_table_data.py
│   │   ├── upload_file_to_s3.py
│   │   └── write_generated_data_to_database.py
│   │
│   └── main/
│       ├── move/
│       │   └── move_files.py
│       │
│       ├── read/
│       │   ├── aws_read.py
│       │   └── database_read.py
│       │
│       ├── transformations/
│       │   └── jobs/
│       │       ├── customer_reporting_transformations.py
│       │       ├── data_validation.py
│       │       ├── dimension_tables_join.py
│       │       ├── main.py
│       │       └── salesperson_reporting_transformations.py
│       │
│       ├── utility/
│       │   ├── config_loader.py
│       │   ├── logging_config.py
│       │   ├── my_sql_session.py
│       │   └── spark_session.py
│       │
│       └── write/
│           ├── database_write.py
│           └── dataframe_format_writer.py
│
└── README.md
```
---

## How to Run

### 1. Clone the repository
```
git clone https://github.com/Aditya-Shinde-21/Retail-Sales-Data-ETL-Pipeline.git
cd Sales-Data-ETL
```
### 2. Create virtual environment
```
python -m venv .venv
source .venv/bin/activate      # Linux / macOS
.venv\Scripts\activate         # Windows
```
### 3. Install dependencies
```
pip install -r requirements.txt
```
Required Windows Subsystem for Linux (WSL) for Airflow if using Windows/macOS setup )

### 4. Configure AWS and MySQL
#### Configure AWS credentials for S3 access and update MySQL connection details in ![config](resources/dev/config.py)

### 5. Generate data and upload it to s3
#### Generate data from ![write to local](scripts/generate_data/generate_csv_data.py) and ![write to database](scripts/generate_data/write_generated_data_to_database.py)
#### Upload data to s3 from ![upload to s3](scripts/generate_data/upload_file_to_s3.py)

### 6. Airflow setup
#### Open WSL terminal and activate airflow virtual environment. Copy DAG to airflow/dags/
```
source ~/airflow_venv/bin/activate
mkdir -p ~/airflow/dags
cp /mnt/d/Retail-Sales-Data-ETL-Pipeline/airflow/sales_etl_dag.py \
~/airflow/dags/
```

### 7. ETL orchestration
#### Run ETL orchestration from wsl
```
airflow standalone
```
## Airflow Configuration

- AWS and MySQL credentials are managed using **Airflow Connections**
- No credentials are hardcoded in Spark or application code
- Environment variables are injected automatically by Airflow at runtime

---

## Data Validation Examples

- Non-null primary key validation  
- Positive price validation  
- Duplicate removal based on business keys  
- Schema conformity checks  

Invalid records are written to a **separate S3 location** for audit, troubleshooting, and reprocessing.

---

## Performance Observations

- Disk spill analysis using **Spark UI**
- Partition tuning (`repartition` vs `coalesce`)
- Comparison between optimized and unoptimized pipeline runs
- Demonstrates realistic performance behavior across different data scales and resource constraints

---

## Limitations

- Spark runs in **local mode** (no distributed cluster)
- IAM roles are not used (local AWS credentials via Airflow Connections)
- SCD Type 2 logic is discussed but not fully implemented

---

## Future Enhancements

- Implement **SCD Type 2** for product pricing
- Add automated **data quality metrics**
- Introduce backfill and reprocessing strategies
- Integrate **Delta Lake** or **Apache Iceberg**
- Deploy Spark on cloud platforms (EMR / Databricks)

---

## Why This Project Matters

This is **not a toy ETL pipeline**.

It demonstrates:

- Real-world failure scenarios
- Debugging and recovery workflows
- Production-style logging and observability
- Practical integration of **Apache Spark** and **Apache Airflow**

---

## Author

**Aditya Shinde**  
Data Engineer  
Passionate about building scalable data pipelines
