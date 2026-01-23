from urllib.parse import urlparse
from scripts.main.utility.logging_config import *
from botocore.exceptions import ClientError


def move_file_s3_to_s3(s3_client, s3a_path, destination_directory):
    parsed = urlparse(s3a_path)
    bucket = parsed.netloc
    source_key = parsed.path.lstrip("/")

    filename = source_key.split("/")[-1]
    destination_key = f"{destination_directory.rstrip('/')}/{filename}"

    # Check if source exists
    try:
        s3_client.head_object(Bucket=bucket, Key=source_key)

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.warning(f"Source file already moved or missing: {source_key}, Skipping.")
            return None
        else:
            raise

    # Copy
    s3_client.copy_object(Bucket=bucket,
                          CopySource={"Bucket": bucket, "Key": source_key},
                          Key=destination_key)

    # Delete
    s3_client.delete_object(Bucket=bucket, Key=source_key)

    return f"s3a://{bucket}/{destination_key}"