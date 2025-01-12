import os
import re

import boto3
from aws_utils import glue, iam

iam.get_aws_credentials(os.environ)


def get_file_paths(bucket: str, prefix: str) -> list:
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    file_paths = [item["Key"] for item in response.get("Contents", [])]
    return file_paths


def extract_partitions(file_path: str) -> dict:
    pattern = r"(\w+)=(\d{4}|\d{1,2})"
    matches = re.findall(pattern, file_path)
    return {key: value for key, value in matches}


bucket = "greenmotion-bucket-905418370160"

glue_handler = glue.GlueHandler()


for prefix in [
    "do_you_spain/raw",
    "holiday_autos/raw",
    "rental_cars/raw",
    "do_you_spain/processed",
    "holiday_autos/processed",
    "rental_cars/processed",
]:
    database_name = prefix.split("/")[0]
    table_name = prefix.split("/")[1]
    file_paths = get_file_paths(bucket, prefix)
    print(f"File paths for {prefix}: {file_paths}")
    for path in file_paths:
        partitions = extract_partitions(path)
        try:
            glue_handler.add_partition_to_glue(
                database_name=database_name,
                table_name=table_name,
                bucket_name=bucket,
                partition_values=partitions,
            )
            print(f"Extracted partitions for {path}: {partitions}")
        except Exception as e:
            print(f"Error adding partitions for {path}: {e}")
