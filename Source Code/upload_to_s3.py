"""
Ultra Fast S3 Upload Script for Millions of Files
Optimized for AWS S3 / S3 Ninja
Uses parallel uploads and connection pooling
"""

import boto3
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

# CONFIG
USE_AWS_S3 = os.getenv('UPLOAD_TO_AWS', 'False').lower() == 'true'

BUCKET_NAME = os.getenv('AWS_S3_BUCKET', 'samsung-prism-logs')
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('AWS_REGION', 'us-east-1')

ENDPOINT_URL = None if USE_AWS_S3 else os.getenv('S3_ENDPOINT_URL', 'http://localhost:9000')

MOCK_DATA_DIR = "mock_s3_bucket"

# PERFORMANCE SETTINGS
MAX_WORKERS = 64          # Number of parallel uploads
CONNECTION_POOL = 100     # HTTP connections
PROGRESS_INTERVAL = 1000

# S3 CLIENT CONFIG
config = Config(
    region_name=REGION,
    max_pool_connections=CONNECTION_POOL,
    retries={"max_attempts": 5}
)

s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    config=config
)

uploaded = 0
failed = 0
start_time = time.time()


def get_s3_key(file_path):
    """Convert local path to S3 key"""
    rel = os.path.relpath(file_path, MOCK_DATA_DIR)
    return rel.replace("\\", "/")


def upload_file(file_path):
    """Upload single file"""
    global uploaded, failed

    try:
        key = get_s3_key(file_path)

        s3.upload_file(
            file_path,
            BUCKET_NAME,
            key
        )

        uploaded += 1

        if uploaded % PROGRESS_INTERVAL == 0:
            elapsed = time.time() - start_time
            rate = uploaded / elapsed

            print(
                f"Uploaded {uploaded:,} files | "
                f"{rate:.0f} files/sec"
            )

    except Exception as e:
        failed += 1
        print("Failed:", file_path, e)


def get_all_files():
    """Scan dataset"""
    files = []

    for root, dirs, filenames in os.walk(MOCK_DATA_DIR):
        for name in filenames:
            if name.endswith(".txt"):
                files.append(os.path.join(root, name))

    return files


def main():

    print("Scanning files...")

    files = get_all_files()

    total_files = len(files)

    print("Total files:", f"{total_files:,}")
    print("Workers:", MAX_WORKERS)
    print()

    with ThreadPoolExecutor(MAX_WORKERS) as executor:

        futures = [
            executor.submit(upload_file, f)
            for f in files
        ]

        for _ in as_completed(futures):
            pass

    elapsed = time.time() - start_time

    print("\nUpload Complete")
    print("Uploaded:", uploaded)
    print("Failed:", failed)
    print("Time:", round(elapsed, 2), "seconds")
    print("Rate:", int(uploaded / elapsed), "files/sec")


if __name__ == "__main__":
    main()