from botocore.exceptions import ClientError
from logger_config import logger
import os

def sync_files(s3, bucket_name, data_folder_path):
    try:
        folder = os.listdir(data_folder_path)
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name)
        existing_objects = set()

        for page in pages:
            if 'Contents' in page:
                existing_objects.update(obj['Key'] for obj in page['Contents'])

        for file in folder:
            file_path = os.path.join(data_folder_path, file)
            try:
                if file not in existing_objects:
                    s3.upload_file(file_path, bucket_name, file)
                    logger.info(f"{file} uploaded.")
                else:
                    logger.info(f"Skipped {file} already existed in bucket")
            except ClientError as e:
                logger.error(f"Failed uploading {file}: {e}")

    except ClientError as e:
        logger.error(f"Error during file sync: {e}")


from s3_utils import get_s3_client, ensure_bucket

if __name__ == "__main__":
    bucket_name = "clickstream-datalake"
    data_folder_path = "./data"

    s3 = get_s3_client()  # this knows LocalStack endpoint
    ensure_bucket(s3, bucket_name)

    sync_files(s3, bucket_name, data_folder_path)