import os
from resources.dev import config
from scripts.main.utility.s3_client_object import *
from scripts.main.utility.encrypt_decrypt import *


def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    try:
        # get s3 client
        s3_client_provider = S3ClientProvider(decrypt(config.aws_access_key), decrypt(config.aws_secret_key))
        s3_client = s3_client_provider.get_client()
        
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                print(file)
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_directory}{file}"
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                print(f"Uploaded file to {s3_bucket}/{s3_key}")
                
    except Exception as e:
        raise e

local_file_path = "filepath to generated csv files"
upload_to_s3(config.s3_source_directory, config.bucket_name, local_file_path)
