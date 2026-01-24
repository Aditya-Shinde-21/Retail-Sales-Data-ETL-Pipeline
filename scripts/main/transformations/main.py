# Standard libraries
import datetime
from urllib.parse import urlparse

# Third-party libraries
from pyspark.sql.functions import concat_ws, lit
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import boto3

# Project modules
from scripts.main.transformations.jobs.data_validation import DataQualityValidator
from scripts.main.utility.logging_config import logger
from scripts.main.utility.config_loader import load_config
from scripts.main.move.move_files import move_file_s3_to_s3
from scripts.main.read.database_read import DatabaseReader
from scripts.main.transformations.jobs.dimension_tables_join import enrich_with_dimensions
from scripts.main.transformations.jobs.customer_reporting_transformations import customer_reporting_table_write
from scripts.main.transformations.jobs.salesperson_reporting_transformations import salesperson_reporting_table_write
from scripts.main.utility.my_sql_session import get_mysql_connection
from scripts.main.read.aws_read import S3Reader
from scripts.main.utility.spark_session import get_spark_session
from scripts.main.write.dataframe_format_writer import FormatWriter

# *********************************************************************************************************
# Get Configuration
config = load_config()

# *********************************************************************************************************
## Get all filepaths from s3 source directory
# Get S3 Client
s3_client = boto3.client("s3")

# Get file path of all files in source directory
all_files = S3Reader(s3_client).list_files(bucket_name=config["aws"]["bucket_name"],
                                           folder_path=config["s3_directories"]["s3_source_directory"])

if not all_files:
    logger.error("Pipeline cannot proceed. No files found at %s in bucket %s",
                  config["s3_directories"]["s3_source_directory"],
                  config["aws"]["bucket_name"])
    raise RuntimeError("No input files available for processing")

logger.info("Absolute path on s3 bucket for all files %s", all_files)

# *********************************************************************************************************
# Identify the csv files in all files
try:
    csv_files = []
    error_files = []

    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(file)
        else:
            error_files.append(file)

    if not csv_files:
        logger.error("Pipeline cannot proceed. No '.csv' files found in S3 source directory: %s",
                        config["s3_directories"]["s3_source_directory"])
        raise RuntimeError("Mandatory CSV files missing in S3 source directory")

    logger.info("List of csv files that needs to be processed %s", csv_files)

except Exception:
    logger.exception("ETL failed during CSV identification")
    raise

# **************************************************************************************************
# Create spark session
spark = get_spark_session()
logger.info("Spark session created successfully")

# **************************************************************************************************
# Check for missing columns
logger.info("Checking schema of csv files")
try:
    correct_files = []

    for file in csv_files:
        try:
            file_schema = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(file)
                .columns
            )
        except Exception:
            logger.exception("S3 read failed")
            raise

        logger.info("Schema for %s is %s", file, file_schema)
        logger.info("Mandatory columns are: %s", config["mandatory_columns"])

        missing_columns = set(config["mandatory_columns"]) - set(file_schema)

        if missing_columns:
            logger.info("Missing columns in %s: %s", file, missing_columns)
            error_files.append(file)
        else:
            logger.info("No missing columns in %s", file)
            correct_files.append(file)

    logger.info("List of correct files: %s", correct_files)
    logger.info("List of error files: %s", error_files)

except Exception:
    logger.exception("ETL failed during schema validation. Required columns missing")
    raise

# **************************************************************************************************
# Move error files to error directory
try:
    if error_files:
        logger.info("Moving error files to error file directory")

        for file_path in error_files:
            message = move_file_s3_to_s3(
                s3_client=s3_client,
                s3a_path=file_path,
                destination_directory=config["s3_directories"]["s3_error_directory"]
            )
            logger.info("Moved error file from %s to %s", file_path, message)

except Exception:
    logger.exception("ETL failed during error file handling")
    raise

# Check if at least one correct file exists
if not correct_files:
        logger.error("Pipeline cannot proceed. All CSV files failed schema validation. "
                        "Source directory: %s",
                        config["s3_directories"]["s3_source_directory"])
        raise RuntimeError("No valid CSV files after schema validation")

# **************************************************************************************************
# Check for files from last run
'''
Check if the same file is present in the staging table with status = "A"
If so then don't delete and try to re-run
'''
if correct_files:
    placeholders = ",".join(["%s"] * len(correct_files))

    statement = f"""
    SELECT DISTINCT file_location
    FROM {config['database']['name']}.{config['tables']['staging_table']}
    WHERE file_location IN ({placeholders}) AND status = 'A'
    """

    logger.info(f"dynamically created statement: {statement}")

    try:
        with get_mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, tuple(correct_files))
                data = cursor.fetchall()

    except Exception:
        logger.exception("Failed while checking previous run status of file in MySQL staging table")
        raise

    if data:
        active_files = [row[0] for row in data]
        logger.info("Your last run FAILED!!!")
        logger.info("Active files: %s", active_files)
    else:
        logger.info("last run was successful")

else:
    logger.error("Pipeline cannot proceed. No correct files available for processing.")
    raise RuntimeError("Zero valid input files after schema validation")

# **************************************************************************************************
## Staging table needs to be updated with the files to process with status as Active(A)
# Make list of SQL statements where each statement is a record of a correct file

logger.info("Updating product_staging_table that we have started the process")
insert_statements = []
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        parsed = urlparse(file)
        filename = parsed.path.rstrip("/").split("/")[-1]

        statement = f"""
        INSERT INTO {config['database']['name']}.{config['tables']['staging_table']}
        (file_name, file_location, created_date, status)
        VALUES ('{filename}', '{file}', '{formatted_date}', 'A')
        """
        insert_statements.append(statement)

    logger.info(f"Insert statements created for staging table: ")
    for statement in insert_statements:
        logger.info(statement)

    # Execute SQL statements in MySQL
    logger.info("Connecting to MySQL server")

    try:
        with get_mysql_connection() as connection:
            with connection.cursor() as cursor:
                logger.info("Connected to My SQL server successfully")
                for statement in insert_statements:
                    cursor.execute(statement)

            connection.commit()

        logger.info("Staging table updated successfully")

    except Exception:
        logger.exception("Failed while inserting file run status in MySQL staging table")
        raise

else:
    raise RuntimeError("No valid files to process")

# **************************************************************************************************
## Create dataframe and load data from correct files
# Creating empty dataframe with required schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("sales_date", DateType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True),
])

sales_df = spark.createDataFrame(data=[], schema=schema)

# Storing concatenated data from additional columns in 'additional_column'
for file in correct_files:
    try:
        logger.info(f"Loading file: {file.split('/')[-1]}")

        file_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "DROPMALFORMED") \
            .option("badRecordsPath",
                    f"s3a://{config['aws']['bucket_name']}/{config['s3_directories']['s3_bad_records_directory']}") \
            .load(file)

    except Exception:
        logger.exception("csv read from S3 failed")
        raise

    file_schema = file_df.columns
    extra_columns = list(set(file_schema) - set(config["mandatory_columns"]))
    logger.info(f"Extra columns: {extra_columns}")

    if extra_columns:
        logger.info("Fixing extra column coming from source")

        file_df = file_df.withColumn("additional_column", concat_ws(", ", *extra_columns))\
            .select(*config["mandatory_columns"], "additional_column")

        logger.info(f"Concatenated data from {extra_columns} and Added to 'additional_column'")

    else:
        file_df = file_df.withColumn("additional_column", lit(None))\
            .select(*config["mandatory_columns"], "additional_column")

    sales_df = sales_df.union(file_df)

if sales_df.rdd.isEmpty():
    logger.error("'sales_df' is empty. Pipeline cannot proceed")
    raise RuntimeError("No records to process")

# **************************************************************************************************
## Ensuring only clean records are processed
# Flag invalid records
s3_invalid_path = f"s3a://{config['aws']['bucket_name']}/{config['s3_directories']['s3_invalid_records_directory']}"
validator = DataQualityValidator(s3_invalid_path)
sales_df_clean = validator.validate_and_split(sales_df)

logger.info("Final Dataframe which will be processed:")
sales_df_clean.show()

# **************************************************************************************************
## Enrich the data from all dimension tables
# Connecting to MySQL with DatabaseReader
database_client = DatabaseReader()

# Creating dataframe for all tables
# customer table
logger.info("Loading customer table into customer_table_df")
customer_table_df = database_client.create_dataframe(spark, config["tables"]["dimension"]["customer_table"])

# product table
logger.info("Loading product table into product_table_df")
product_table_df = database_client.create_dataframe(spark, config["tables"]["dimension"]["product_table"])
# sales team table
logger.info("Loading sales team table into sales_team_table_df")
sales_team_table_df = database_client.create_dataframe(spark, config["tables"]["dimension"]["sales_team_table"])

# store table
logger.info("Loading store table into store_table_df")
store_table_df = database_client.create_dataframe(spark, config["tables"]["dimension"]["store_table"])

# **************************************************************************************************
fact_dimension_join_df = enrich_with_dimensions(sales_df_clean,
                                                customer_table_df,
                                                store_table_df,
                                                sales_team_table_df,
                                                product_table_df)

fact_dimension_join_df.persist(StorageLevel.MEMORY_AND_DISK)
# Final enriched data
logger.info("Final Enriched Data")
fact_dimension_join_df.show()

# **************************************************************************************************
# Write the customer data into customer data mart in parquet format
logger.info("Writing data into customer data mart")
final_customer_data_mart_df = fact_dimension_join_df.select(
    "customer_id",
    "first_name",
    "last_name",
    "address",
    "pincode",
    "phone_number",
    "sales_date",
    "sales_date_year",
    "sales_date_month",
    "product_id",
    "product_name",
    "quantity",
    "price",
    "total_cost")

logger.info("Final data for customer data mart")
final_customer_data_mart_df.show()

parquet_writer = FormatWriter(mode = "append", data_format = "parquet")
s3_output_path = f"s3a://{config['aws']['bucket_name']}/{config['s3_directories']['s3_customer_datamart_directory']}"
parquet_writer.write_to_format(final_customer_data_mart_df, s3_output_path)
logger.info("Customer Datamart written to: '%s'", s3_output_path)

# **************************************************************************************************
# Write sales team dataframe into Sales team data mart
logger.info("Writing data into Sales team data mart")
final_sales_team_data_mart_df = fact_dimension_join_df.select(
    "store_id",
    "sales_person_id",
    "sales_person_first_name",
    "sales_person_last_name",
    "store_manager_name",
    "manager_id",
    "sales_person_address",
    "sales_person_pincode",
    "total_cost",
    "sales_date",
    "sales_date_year",
    "sales_date_month")

logger.info(" Final data for sales team data mart")
final_sales_team_data_mart_df.show()

s3_output_path = f"s3a://{config['aws']['bucket_name']}/{config['s3_directories']['s3_sales_team_datamart_directory']}"
parquet_writer.write_to_format(final_sales_team_data_mart_df, s3_output_path)
logger.info("sales team Datamart written to: '%s'", s3_output_path)

# **************************************************************************************************
# Customer reporting table
# Find out the total purchase amount every month of a customer
# Write data into MySQL table
logger.info("Calculating customer's total purchase every month")
customers_reporting_table = config["tables"]["datamart"]["customers_monthly_sales_table"]
customer_reporting_table_write(final_customer_data_mart_df, customers_reporting_table)
logger.info("Calculation done and data written into table")

# **************************************************************************************************
# Transformations on sales team data
# Find out the total sales done by each sales person every month
# For each store give the top performer a 1% incentive of total sales of the month
# Write the data into MySQL table
logger.info("Calculating sales person monthly incentive")
salesperson_reporting_table = config["tables"]["datamart"]["sales_person_incentive_table"]
salesperson_reporting_table_write(final_sales_team_data_mart_df, salesperson_reporting_table)
logger.info("Calculation done and data written into table")

# **************************************************************************************************
# Unpersist any persisted dataframes
fact_dimension_join_df.unpersist(blocking=True)
# **************************************************************************************************

# Move the files on s3 source directory to processed folder
for file_path in correct_files:
    message = move_file_s3_to_s3(
        s3_client=s3_client,
        s3a_path=file_path,
        destination_directory=config["s3_directories"]["s3_processed_directory"]
    )
    logger.info(f"Moved %s to %s", file_path, message)

# **************************************************************************************************

# Update processed file status as Inactive(I) in staging table
update_statements = []
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for file in correct_files:
    parsed = urlparse(file)
    filename = parsed.path.rstrip("/").split("/")[-1]

    statement = f'''
    UPDATE {config["database"]["name"]}.{config["tables"]["staging_table"]}
    SET status = 'I', updated_date = '{formatted_date}'
    WHERE file_name = '{filename}'
    '''

    update_statements.append(statement)

    logger.info("Update statements created for staging table:")
    for statement in update_statements:
        logger.info(statement)

    logger.info("Connecting to MySQL server")

    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            logger.info(" Connected to My SQL server successfully")
            for statement in update_statements:
                cursor.execute(statement)

        connection.commit()

logger.info("Staging table updated successfully")

# **************************************************************************************************
spark.stop()
