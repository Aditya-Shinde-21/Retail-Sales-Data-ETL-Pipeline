import os

key = "%sales_data_etl%"
iv = "!&$cryptodome$&!"
salt = "@AesEncryption@"

#AWS Access And Secret key
aws_access_key = "NrkN4xUucSuJdh2+Aq9iONVAoYKbk9mBrynvqncb14g=" #encrypted
aws_secret_key = "IRlAahjf8eB99ZiIuCPdPhHxy2Ii5EZdPXFqwLJsBgCPCTolqh8Qek1EuBdyaREl" #encrypted
bucket_name = "customer-sales-data-project"
region = "us-east-1"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "sales_data"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "Aditya@2025",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
staging_table = "staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_id","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "D:\\DE_Project_Files\\file_from_s3\\"
customer_data_mart_local_file = "D:\\DE_Project_Files\\customer_data_mart\\"
sales_team_data_mart_local_file = "D:\\DE_Project_Files\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "D:\\DE_Project_Files\\sales_partition_data\\"
error_folder_path_local = "D:\\DE_Project_Files\\error_files\\"
