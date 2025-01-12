import json
import logging
import os
from typing import Any

from aws_utils import iam, s3_router  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

iam.get_aws_credentials(os.environ)


def lambda_handler(event: dict, context: Any) -> dict:
    logger.info(f"Received event: {json.dumps(event)}")

    config = {
        "process_data": {
            "lambda_name": "greenmotion-process-data-lambda",
            "prefixes": ["do_you_spain/raw", "holiday_autos/raw", "rental_cars/raw"],
        },
        "add_partition": {
            "lambda_name": "aws-common-resources-add-partition-lambda",
            "prefixes": ["do_you_spain", "holiday_autos", "rental_cars"],
        },
    }

    s3_router.S3Router.handle_s3_event(event, config)

    return {"statusCode": 200, "body": json.dumps("Event processed successfully")}
