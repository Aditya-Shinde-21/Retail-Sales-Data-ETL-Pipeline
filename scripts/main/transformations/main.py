# Standard libraries
import datetime

# Spark libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.storagelevel import StorageLevel

# Project modules
from resources.dev import config
from scripts.main.move.move_files import *
from scripts.main.read.database_read import DatabaseReader
from scripts.main.transformations.jobs.customer_mart_sql_transform_write import *
from scripts.main.transformations.jobs.sales_mart_sql_transform_write import *
from scripts.main.upload.upload_to_s3 import *
from scripts.main.utility.s3_client_object import *
from scripts.main.utility.logging_config import logger
from scripts.main.utility.my_sql_session import *
from scripts.main.read.aws_read import *
from scripts.main.utility.spark_session import *
from scripts.main.transformations.jobs.dimension_tables_join import *
from scripts.main.write.format_writer import *

# *********************************************************************************************************
# Get S3 Client: access_key_id and secret_key_id will be accessed from airflow connection
s3_client = boto3.client("s3")

# Get file path of all files in source directory
try:
    all_files = S3Reader().list_files(s3_client=s3_client,
                                      bucket_name=config.bucket_name,
                                      folder_path=config.s3_source_directory)

    if not all_files:
        logger.info(f"No files available at {config.s3_source_directory} in bucket:{config.bucket_name}")
        raise Exception("No files available to process")

except Exception as e:
    logger.error("Exited with error:- %s", e)
    raise e
logger.info("Absolute path on s3 bucket for all files %s", all_files)
print("all files:", *all_files)

# *********************************************************************************************************
# Identify the csv files in all files
csv_files = []
error_files = []
for file in all_files:
    if file.endswith(".csv"):
        csv_files.append(file)
    else:
        error_files.append(file)

if not csv_files:
    logger.error("No '.csv' files in s3 source directory")
    raise Exception("No '.csv' files in s3 source directory")

logger.info("**********************************************************************")
logger.info("List of csv files that needs to be processed %s", csv_files)

# **************************************************************************************************
### Check csv file schema to identify correct files
## Create spark session
spark = spark_session()
print("Created spark session:", spark)

# **************************************************************************************************
## check schema
logger.info("************** Checking Schema of csv files ****************")
correct_files = []
for file in csv_files:
    file_schema = spark.read.format("csv")\
        .option("header", "true") \
        .load(file).columns
    # Scans only header row and returns column names in a list

    logger.info(f"Schema for {file} is {file_schema}")
    logger.info(f"Mandatory columns are: {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(file_schema)
    # set(a) - set(b) returns elements in 'a' that are not in 'b'

    logger.info(f"Missing columns: {missing_columns}")

    if missing_columns:
        error_files.append(file)
    else:
        logger.info(f"No missing columns in {file}")
        correct_files.append(file)

logger.info(f"****************** List of correct files ********************\n{correct_files}")
logger.info(f"****************** List of error files ********************\n{error_files}")

# **************************************************************************************************
# Move error files to error file directory
if error_files:
    logger.info("****************** Moving error files to error file directory ********************")
    for file_path in error_files:
        message = move_file_s3_to_s3(s3_client=s3_client,
                                     s3a_path=file_path,
                                     destination_directory=config.s3_error_directory)
        logger.info(f"moved error files to {message}")

else:
    logger.info("**************** No error files in source directory ********************")

# **************************************************************************************************
# Check for files from last run
'''
Check if the same file is present in the staging table with status = "A"
If so then don't delete and try to re-run
'''
if correct_files:
    statement = f"""
    select distinct file_location
    from {config.database_name}.{config.staging_table}
    where file_location in ({",".join(f"'{x}'" for x in all_files)}) and status = 'A'
    """

    logger.info(f"dynamically created statement: {statement}")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run FAILED!!!")
        print(data)
    else:
        logger.info("last run successful")

else:
    logger.info("**************** No files to process!!! *******************")

# **************************************************************************************************
## Staging table needs to be updated with the files to process with status as Active(A)
# Make list of SQL statements where each statement is a record of a correct file
logger.info("********** Updating product_staging_table that we have started the process **********")
insert_statements = []
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        parsed = urlparse(file)
        filename = parsed.path.rstrip("/").split("/")[-1]

        statement = f"INSERT INTO {config.database_name}.{config.staging_table}"\
                        f"(file_name, file_location, created_date, status)"\
                        f"VALUES ('{filename}', '{file}', '{formatted_date}', 'A');"
        insert_statements.append(statement)

    logger.info(f"Insert statements created for staging table --- {insert_statements}")

    # Execute SQL statements in My SQL
    logger.info("***************** Connecting with My SQL server ****************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("**************** Connected to My SQL server successfully ***************")

    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
    logger.info("**************** Staging table updated successfully ***************")

else:
    logger.error("*********** No files to process ************")
    raise Exception("************** No Data to process *******************")

# **************************************************************************************************
## Create dataframe and load data from correct files
# Creating empty dataframe with required schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True),
])

final_df_to_process = spark.createDataFrame(data=[], schema=schema)

# Storing concatenated data from additional columns in 'additional_column'
for file in correct_files:
    file_df = spark.read.format("csv")\
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file)

    file_schema = file_df.columns
    extra_columns = list(set(file_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns: {extra_columns}")

    if extra_columns:
        logger.info("**************** Fixing extra column coming from source ***************")
        file_df = file_df.withColumn("additional_column", concat_ws(", ", *extra_columns))\
            .select(*config.mandatory_columns, "additional_column")

        logger.info(f"Concatenated data from {extra_columns} and Added to 'additional_column'")

    else:
        file_df = file_df.withColumn("additional_column", lit(None))\
            .select(*config.mandatory_columns, "additional_column")

    final_df_to_process = final_df_to_process.union(file_df)

logger.info("@@@@@@@@@@@@@@@@@@@@@ Final Dataframe which will be processed:")
final_df_to_process.persist(StorageLevel.MEMORY_AND_DISK)
final_df_to_process.show()

# **************************************************************************************************
## Read the data from all dimension tables
# Connecting to MySQL with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

# Creating dataframe for all tables
# customer table
logger.info("************* Loading customer table into customer_table_df *************")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

# product table
logger.info("************* Loading product table into product_table_df *************")
product_table_df = database_client.create_dataframe(spark, config.product_table)
# sales team table
logger.info("************* Loading sales team table into sales_team_table_df *************")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# store table
logger.info("************* Loading store table into store_table_df *************")
store_table_df = database_client.create_dataframe(spark, config.store_table)

# **************************************************************************************************
fact_dimension_join_df = dimension_table_joins(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df,
                                                        product_table_df)

final_df_to_process.unpersist(blocking=True )
fact_dimension_join_df.persist(StorageLevel.MEMORY_AND_DISK)
# Final enriched data
logger.info("******************* Final Enriched Data *********************")
fact_dimension_join_df.show()

# **************************************************************************************************
# Write the customer data into customer data mart in parquet format
# write reporting data into mysql table also
logger.info("*************** Write data into customer data mart *************")
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

logger.info("*************** Final data for customer data mart ***************")
final_customer_data_mart_df.show()

parquet_writer = FormatWriter(mode = "overwrite", data_format = "parquet")
s3_output_path = f"s3a://{config.bucket_name}/{config.s3_customer_datamart_directory}/customer_datamart.parquet"
parquet_writer.write_to_format(final_customer_data_mart_df, s3_output_path)
print("Dataframe written to:", s3_output_path)

# **************************************************************************************************
# Write sales team dataframe into Sales team data mart
logger.info("*************** Write data into sales team data mart *************")
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

logger.info("*************** Final data for sales team data mart ***************")
final_sales_team_data_mart_df.show()


s3_output_path = f"s3a://{config.bucket_name}/{config.s3_sales_datamart_directory}/sales_datamart.parquet"
parquet_writer.write_to_format(final_sales_team_data_mart_df, s3_output_path)
print("Dataframe written to:", s3_output_path)

# **************************************************************************************************
# Also writing the data into partitions for sales data mart
logger.info("******************** writing sales data mart partitioned **************************")

s3_output_path = f"s3a://{config.bucket_name}/sales_data_mart_partitioned/sales_datamart_partitioned.parquet"
final_sales_team_data_mart_df.repartition(4).write.format("parquet")\
    .option("header", "true")\
    .mode("overwrite")\
    .partitionBy("sales_date_year", "sales_date_month", "store_id")\
    .option("path", s3_output_path)\
    .save()
    
print("Dataframe written to:", s3_output_path)

# **************************************************************************************************
# Transformations on customer data
# Find out the total purchase amount every month of a customer
# Write data into MySQL table
logger.info("********** Calculating customer's total purchase every month **********")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("************ Calculation done and data written into table ***********")

# **************************************************************************************************
# Transformations on sales team data
# Find out the total sales done by each sales person every month
# For each store give the top performer a 1% incentive of total sales of the month
# Write the data into MySQL table
logger.info("********** Calculating sales team data mart **********")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("************ Calculation done and data written into table ***********")

# **************************************************************************************************
# unpersist any persisted dataframes
fact_dimension_join_df.unpersist(blocking=True)

# **************************************************************************************************
# Move the files on s3 source directory to processed folder
for file_path in correct_files:
    message = move_file_s3_to_s3(s3_client=s3_client,
                                 s3a_path=file_path,
                                 destination_directory=config.s3_processed_directory)
    logger.info(f"moved proccessed files to {message}")

# **************************************************************************************************
# Update processed file status as Inactive(I) in staging table
if correct_files:
    update_statements = []
    formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  
    for file in correct_files:
        parsed = urlparse(file)
        filename = parsed.path.rstrip("/").split("/")[-1]

        statement = f"UPDATE {config.database_name}.{config.staging_table}"\
                    f"SET status = 'I', updated_date = '{formatted_date}'"\
                    f"WHERE file_name = '{filename}';"
        update_statements.append(statement)

        logger.info(f"Update statements created for staging table --- {update_statements}")
        logger.info("**************** Connecting with MySQL server ******************")
        connection = get_mysql_connection()
        cursor = connection.cursor()
        logger.info("**************** Connected to MySQL server successfully ***************")

        for statement in update_statements:
            cursor.execute(statement)
            connection.commit()
        cursor.close()
        connection.close()
      
else:
    logger.error("************** There is some error in process please check ********************")
    sys.exit()

# **************************************************************************************************
spark.stop()

