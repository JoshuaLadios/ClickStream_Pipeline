from botocore.exceptions import ClientError
import os

def sync_files(s3, bucket_name, data_folder_path):
    try:
        folder = [file for file in os.listdir(data_folder_path)]
        objects = s3.list_objects_v2(Bucket=bucket_name)
        existing_objects = set()

        if 'Contents' in objects:
            existing_objects = {obj['Key'] for obj in objects['Contents']}

        for file in folder:
            file_path = os.path.join(data_folder_path, file)
            try:
                if file not in existing_objects:
                    s3.upload_file(file_path, bucket_name, file)
                    print(f"{file} uploaded.")
                else:
                    print(f"{file} already existed in bucket")
            except ClientError as e:
                print(f"Failed uploading {file}: {e}")

    except ClientError as e:
        print(f"Error during file sync: {e}")