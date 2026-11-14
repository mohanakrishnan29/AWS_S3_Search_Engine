import boto3

BUCKET_NAME = "samsung-prism-logs"

s3 = boto3.client("s3")

paginator = s3.get_paginator("list_objects_v2")

total = 0

for page in paginator.paginate(Bucket=BUCKET_NAME):
    if "Contents" in page:
        count = len(page["Contents"])
        total += count
        print("Objects counted:", total)

print("\nFinal total:", total)