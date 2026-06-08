import json
import os
import boto3
from botocore.client import Config

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio123")
S3_BUCKET = os.getenv("S3_BUCKET", "big-data-lake")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

def ensure_bucket(client):
    buckets = client.list_buckets().get("Buckets", [])
    names = [b["Name"] for b in buckets]
    if S3_BUCKET not in names:
        client.create_bucket(Bucket=S3_BUCKET)

def upload_json(source, date_str, city_key, data):
    client = get_s3_client()
    ensure_bucket(client)

    key = f"raw/{source}/{date_str}/{city_key}.json"
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    print(f"[S3] Uploaded: s3://{S3_BUCKET}/{key}")
    return key