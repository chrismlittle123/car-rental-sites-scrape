import json
import logging
import os
from typing import Any, Dict

import process_do_you_spain  # type: ignore
import process_holiday_autos  # type: ignore
import process_rental_cars  # type: ignore
from aws_utils import iam, s3  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

iam.get_aws_credentials(os.environ)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Received event")
    logger.info(event)
    bucket_name = event["detail"]["bucket"]
    object_key = event["detail"]["object_key"]
    try:
        s3_handler = s3.S3Handler()
        partition_values, paths, file_name = s3.S3Utils.extract_partition_values(
            object_key
        )
        if paths[0] == "do_you_spain" and paths[1] == "raw":
            process_do_you_spain.main(
                s3_handler,
                bucket_name,
                partition_values,
                paths,
                file_name,
            )
        if paths[0] == "holiday_autos" and paths[1] == "raw":
            process_holiday_autos.main(
                s3_handler, bucket_name, partition_values, paths, file_name
            )
        if paths[0] == "rental_cars" and paths[1] == "raw":
            process_rental_cars.main(
                s3_handler, bucket_name, partition_values, paths, file_name
            )

        return {
            "statusCode": 200,
            "body": json.dumps("Processing completed successfully."),
        }
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise e
