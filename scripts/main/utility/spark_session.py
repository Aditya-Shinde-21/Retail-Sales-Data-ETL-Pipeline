import findspark
findspark.init()
from pyspark.sql import *
from resources.dev import config
from scripts.main.utility.encrypt_decrypt import decrypt

def spark_session():
    spark = SparkSession.builder.master("local[4]")\
        .appName("test_run")\
        .config("spark.driver.extraClassPath", "C:\\mysql_connector_java\\mysql-connector-j-9.5.0.jar") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8")\
        .getOrCreate()

    # Update hadoop configuration to read from s3
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", decrypt(config.aws_access_key))
    hadoop_conf.set("fs.s3a.secret.key", decrypt(config.aws_secret_key))
    hadoop_conf.set("fs.s3a.endpoint", f"s3.{config.region}.amazonaws.com")

    return spark

if __name__ == "__main__":
    spark = spark_session()
    print("Created spark session:", spark)
    spark.stop()