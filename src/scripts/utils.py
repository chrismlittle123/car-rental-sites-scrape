import datetime
import os
import subprocess
import sys
from io import BytesIO
from typing import Any, Dict, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pytz
from aws_utils import sns  # type: ignore

from aws_lambda.api.models.pydantic_models import PROJECT  # type: ignore


def get_bucket_name() -> str:
    return f"{PROJECT}-bucket-{os.environ.get('AWS_ACCOUNT_ID')}"


def get_location_config() -> Dict[str, Dict[str, Any]]:
    return {
        "do_you_spain": {
            "manchester": {
                "code": "MAN01",
                "rental_periods": [1, 3, 5, 7, 10, 14, 21, 28],
                "hour_of_day": 10,
            }
        },
        "holiday_autos": {
            "manchester": {
                "code": "Manchester",
                "rental_periods": [1, 3, 5, 7, 10, 14, 21, 28],
                "hour_of_day": 10,
            }
        },
        "rental_cars": {
            "manchester": {
                "code": "MAN",
                "name": "Manchester Airport",
                "location_type": "IATA",
                "rental_periods": [1, 3, 5, 7, 10, 14, 21, 28],
                "hour_of_day": 10,
            }
        },
    }


def get_location_from_command_line() -> str:
    if len(sys.argv) < 2:
        raise Exception("Please pass in a location argument")
    return sys.argv[1]


def get_date_range(
    rental_period: int, days_ahead: int, hour_of_day: int
) -> Tuple[datetime.datetime, datetime.datetime]:
    pickup_date = datetime.datetime.now() + datetime.timedelta(days=days_ahead)
    pickup_date_hour = pickup_date.replace(
        hour=hour_of_day, minute=0, second=0, microsecond=0
    )
    dropoff_date = pickup_date + datetime.timedelta(days=rental_period)
    dropoff_date_hour = dropoff_date.replace(
        hour=hour_of_day, minute=0, second=0, microsecond=0
    )
    return pickup_date_hour, dropoff_date_hour


def build_filename(
    rental_period: int, datetime_processed_string: str, site_name: str
) -> str:
    rental_period_string = str(rental_period).zfill(2)
    year = datetime_processed_string.split("T")[0].split("-")[0]
    month = datetime_processed_string.split("T")[0].split("-")[1]
    day = datetime_processed_string.split("T")[0].split("-")[2]
    hour = datetime_processed_string.split("T")[1].split(":")[0]
    return f"{site_name}/raw/year={year}/month={month}/day={day}/hour={hour}/rental_period={rental_period_string}/data.parquet"


def execute_curl_command(curl_command: str) -> Tuple[str, str]:
    result = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
    return result.stdout, result.stderr


def convert_data_to_parquet(data: Any) -> bytes:
    table = pa.Table.from_pylist(data)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def send_sns_notification(site_name: str, project: str) -> None:
    message = f"{site_name} scrape finished at {datetime.datetime.now(pytz.timezone('Europe/London')).strftime('%Y-%m-%dT%H:%M:%S')}"
    topic_name = f"{project}-lambda-notifications"
    sns_handler = sns.SNSHandler(topic_name)
    sns_handler.send_notification(message, "PROCESS_FINISHED")
