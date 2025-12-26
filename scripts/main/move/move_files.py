import traceback
from urllib.parse import urlparse
from scripts.main.utility.logging_config import *

def move_file_s3_to_s3(s3_client, s3a_path, source_directory, destination_directory):
    try:
        parsed = urlparse(s3a_path)
        bucket = parsed.netloc
        source_key = parsed.path.lstrip("/")

        # Safety check
        if not source_key.startswith(source_directory):
            raise ValueError(f"File not under source directory: {source_key}")

        # Preserve filename
        filename = source_key.split("/")[-1]
        destination_key = f"{destination_directory}{filename}"

        # COPY
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": source_key},
            Key=destination_key
        )

        # DELETE (only after successful copy)
        s3_client.delete_object(
            Bucket=bucket,
            Key=source_key
        )

        return f"s3a://{bucket}/{destination_key}"

    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e