from s3_utils import get_s3_client, ensure_bucket
from file_syncing import sync_files

BUCKET_NAME = "clickstream-datalake"
DATA_PATH = "/clickstream/data"

def main():
    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET_NAME)
    sync_files(s3, BUCKET_NAME, DATA_PATH)

if __name__ == "__main__":
    main()