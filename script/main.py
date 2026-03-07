from s3_utils import get_s3_client, ensure_bucket
from file_syncing import sync_files
from clickstream import clean_and_validate

BUCKET_NAME = "clickstream-datalake"
DATA_PATH = "/clickstream/data"

def main():
    try:
        s3 = get_s3_client()
        ensure_bucket(s3, BUCKET_NAME)
        sync_files(s3, BUCKET_NAME, DATA_PATH)
    except Exception as e:
        print(f"Failed in setting up s3: {e}")
        return
    
    print("Starting Clickstream clean and validate")
    clean_and_validate(BUCKET_NAME)
    print("Pipeline Finished successfully")

if __name__ == "__main__":
    main()