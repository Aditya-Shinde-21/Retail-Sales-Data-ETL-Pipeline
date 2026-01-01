# key, iv, salt for AES Encryption
key = "your-key"
iv = "your-iv"
salt = "your-salt"

#AWS Access And Secret key
aws_access_key = "encrypted-access-key" #encrypt using scripts/main/encrypt_decrypt.py
aws_secret_key = "encrypted-secret-key" #encrypt using scripts/main/encrypt_decrypt.py
bucket_name = "s3-bucket-name"
region = "s3-bucket-region"
s3_customer_datamart_directory = "s3-directory" #|
s3_sales_datamart_directory = "s3-directory"    #|
s3_source_directory = "s3-directory"            #|-> correcponding folder/directory names, example: s3_source_directory = "sales_data/"
s3_error_directory = "s3-directory"             #|
s3_processed_directory = "s3-directory"         #|


#Database credential
# MySQL database connection properties
database_name = "database_name"
url = f"jdbc:mysql://host.docker.internal:3306/{database_name}"
properties = {
    "user": "root",
    "password": "user-password",
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



