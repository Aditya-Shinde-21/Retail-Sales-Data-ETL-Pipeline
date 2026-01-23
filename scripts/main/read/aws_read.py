from botocore.exceptions import ClientError
from scripts.main.utility.logging_config import *

class S3Reader:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def list_files(self, bucket_name,folder_path):
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")

            files = []

            for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_path):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith("/"):
                        files.append(f"s3a://{bucket_name}/{obj['Key']}")

            logger.info("Total files found in bucket '%s' under '%s': %d",bucket_name,folder_path,len(files))

            return files

        except ClientError:
            logger.exception("Failed to list files from S3 bucket '%s' with prefix '%s'",bucket_name,folder_path)
            raise