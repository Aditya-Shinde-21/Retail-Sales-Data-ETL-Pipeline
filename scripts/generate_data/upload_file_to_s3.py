import os
import boto3

def upload_to_s3(s3_client, s3_directory, s3_bucket, local_file_path):
    s3_prefix = f"{s3_directory}"
    try:
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"

                print(f"Uploading: {file}")
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                print(f"Uploaded {file} to {s3_bucket}/{s3_key}")

    except Exception as e:
        raise e

aws_access_key = input("Enter AWS Access Key: ")
aws_secret_key = input("Enter AWS Secret Key: ")
s3_client = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key).client('s3')

local_file_path = "D:\\DE_Project_Files\\sales_data_to_s3\\"
s3_directory = "sales_data/"
s3_bucket = "customer-sales-data-project"

upload_to_s3(s3_client, s3_directory, s3_bucket, local_file_path)