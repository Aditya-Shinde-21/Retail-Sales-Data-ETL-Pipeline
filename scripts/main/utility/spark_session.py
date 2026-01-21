from pyspark.sql import SparkSession

"""
Creates and returns a SparkSession.
Spark execution mode (local / cluster) is controlled externally
by spark-submit, in Airflow dag.
"""

def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("Sales-Data-ETL-Optimized")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.default.parallelism", "6")

        # Spark UI persistence
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///mnt/filepath/to/spark-event-logs")
        .config("spark.history.fs.logDirectory", "file:///mnt/filepath/to/spark-event-logs")

        # MySQL connector jar
        .config("spark.driver.extraClassPath",
        "/mnt/filepath/to/mysql-connector-j-9.5.0.jar")
        .getOrCreate()
    )

    return spark
