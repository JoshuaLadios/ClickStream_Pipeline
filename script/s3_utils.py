import boto3
from botocore.exceptions import ClientError
from logger_config import logger

# Setup your s3(LocalStack) Connection
def get_s3_client(endpoint_url="http://localstack:4566"):
    return boto3.client(
        "s3",
        endpoint_url = endpoint_url,
        aws_access_key_id = "test",
        aws_secret_access_key = "test",
        region_name = "us-east-1"
    )

def ensure_bucket(s3_client, bucket_name):
# Check whether the buckets exist if not create one
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        if bucket_name not in buckets:
            response = s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully!")
        # else:
        #     print(f"Bucket '{bucket_name}' already exist!")

    except ClientError as e:
        logger.error(f"Error listing bucket: {e}")
        return None