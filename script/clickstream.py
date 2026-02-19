import boto3
from botocore.exceptions import ClientError

# Setup your s3(LocalStack) Connection
s3 = boto3.client(
    "s3",
    endpoint_url = "http://localhost:4566",
    aws_access_key_id = "test",
    aws_secret_access_key = "test",
    region_name = "us-east-1"
)

# Check whether the buckets are existing if not create one
bucket_name = "clickstream-datalake"

try:
    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    if bucket_name not in buckets:
        response = s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully!")
    else:
        print(f"Bucket '{bucket_name}' already exist!")

    # The Idea is to loop through the data_generator folder file to check if the files are already uploaded in the bucket
    # If not then upload it. If something is missing upload that missing object/file

except ClientError as e:
    print(f"Error listing bucket: {e}")

# Check whether the object/data are in the container if not, upload P.S(The volumes are mounted)