from pyspark.sql import SparkSession
from scripts.main.utility.logging_config import logger

"""
Creates and returns a SparkSession.
Spark execution mode (local / cluster) is controlled externally
by spark-submit, in Airflow dag.
"""

def get_spark_session():
    try:
        spark = (
            SparkSession.builder
            .appName("Retail-Sales-Data-ETL")
            .config("spark.sql.shuffle.partitions", "6")
            .config("spark.default.parallelism", "6")

            # Spark UI persistence
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "file:///mnt/d/DE_Project_Files/spark-event-logs")
            .config("spark.history.fs.logDirectory", "file:///mnt/d/DE_Project_Files/spark-event-logs")

            # MySQL connector jar
            .config("spark.driver.extraClassPath",
                    "/mnt/c/Users/Aditya/PycharmProjects/de_project1/jars/mysql-connector-j-9.5.0.jar")
            .getOrCreate()
        )

        return spark

    except Exception:
        logger.exception("Failed to create Spark session")
        raise