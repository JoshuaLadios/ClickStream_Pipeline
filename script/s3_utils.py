import boto3
from botocore.exceptions import ClientError
import os

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
            print(f"Bucket '{bucket_name}' created successfully!")
        # else:
        #     print(f"Bucket '{bucket_name}' already exist!")

    except ClientError as e:
        print(f"Error listing bucket: {e}")

# def sync_files(s3, bucket_name):
#     try:
#         data_folder_path = "/clickstream/data"
#         folder = [file for file in os.listdir(data_folder_path)]
#         objects = s3.list_objects_v2(Bucket=bucket_name)
#         existing_objects = set()

#         if 'Contents' in objects:
#             existing_objects = {obj['Key'] for obj in objects['Contents']}

#         for file in folder:
#             file_path = os.path.join(data_folder_path, file)
#             try:
#                 if file not in existing_objects:
#                     s3.upload_file(file_path, bucket_name, file)
#                     print(f"{file} uploaded.")
#                 else:
#                     print(f"{file} already existed in bucket")
#             except ClientError as e:
#                 print(f"Failed uploading {file}: {e}")

#     except ClientError as e:
#         print(f"Error during file sync: {e}")

# def main():
#     ensure_bucket(s3, bucket_name)
#     sync_files(s3, bucket_name)

# if __name__ == "__main__":
#     try:
#         main()
#     except ClientError as e:
#         print(f"AWS error: {e}")