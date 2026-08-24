from fastapi import FastAPI
from script.s3_utils import get_s3_client, ensure_bucket
from script.file_syncing import sync_files

app = FastAPI()

BUCKET_NAME = "clickstream-datalake"
DATA_PATH = "/clickstream/data"

s3 = get_s3_client()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/bucket")
def create_bucket():
    ensure_bucket(s3, BUCKET_NAME)

    return {
        "status": "Success",
        "bucket": BUCKET_NAME
    }
    
@app.post("/sync")
def sync():
    result = sync_files(s3, BUCKET_NAME, DATA_PATH)

    return {
        "status": "Success",
        "bucket": BUCKET_NAME,
        "result": result
    }