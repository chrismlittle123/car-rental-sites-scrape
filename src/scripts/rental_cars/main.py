import datetime
import json
import os
import random
import sys
import time
import urllib.parse
from typing import Any, Dict, Optional

import pytz
from aws_utils import iam, s3  # type: ignore

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import scripts.utils as utils  # type: ignore
from aws_lambda.api.models.pydantic_models import RentalCarsRawModel  # type: ignore

iam.get_aws_credentials(os.environ)


def url_encode(value: Any) -> Any:
    if isinstance(value, dict):
        return {url_encode(k): url_encode(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [url_encode(item) for item in value]
    else:
        return urllib.parse.quote(str(value))


def get_data(
    pickup_datetime: Optional[str],
    dropoff_datetime: Optional[str],
    days_ahead: Optional[int],
    rental_period: Optional[str],
    hour_of_day: Optional[int],
    search_criteria: Dict[str, Any],
    cookie_string: str,
) -> Dict[str, Any]:
    if days_ahead:
        pickup_datetime, dropoff_datetime = utils.get_date_range(
            rental_period, days_ahead, hour_of_day
        )
        pickup_datetime = pickup_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        dropoff_datetime = dropoff_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        pickup_datetime = pickup_datetime
        dropoff_datetime = dropoff_datetime

    search_criteria["pickUpDateTime"] = pickup_datetime
    search_criteria["dropOffDateTime"] = dropoff_datetime

    search_criteria = url_encode(search_criteria)
    print(f"Search Criteria: {search_criteria}")

    curl_command = f"""
    curl 'https://www.rentalcars.com/api/search-results?searchCriteria=%7B%22driversAge%22%3A{search_criteria["driversAge"]}%2C%22pickUpLocation%22%3A%22{search_criteria["pickUpLocation"]}%22%2C%22pickUpDateTime%22%3A%22{search_criteria["pickUpDateTime"]}%22%2C%22pickUpLocationType%22%3A%22{search_criteria["pickUpLocationType"]}%22%2C%22dropOffLocation%22%3A%22{search_criteria["dropOffLocation"]}%22%2C%22dropOffLocationType%22%3A%22{search_criteria["dropOffLocationType"]}%22%2C%22dropOffDateTime%22%3A%22{search_criteria["dropOffDateTime"]}%22%2C%22searchMetadata%22%3A%22%7B%5C%22pickUpLocationName%5C%22%3A%5C%22{search_criteria["searchMetadata"]["pickUpLocationName"]}%5C%22%2C%5C%22dropOffLocationName%5C%22%3A%5C%22{search_criteria["searchMetadata"]["dropOffLocationName"]}%5C%22%7D%22%7D&filterCriteria=%7B%7D&serviceFeatures=%5B%5D' \
      -H 'accept: application/json' \
      -H 'accept-language: en-GB,en-US;q=0.9,en;q=0.8' \
      -H 'cookie: {cookie_string}' \
      -H 'if-none-match: W/"ffa18-eF3BazN/2CKDVEOgPcu0Nn1ZxEo"' \
      -H 'priority: u=1, i' \
      -H 'referer: https://www.rentalcars.com/search-results?location=&dropLocation=&locationName=Manchester%20Airport&locationIata=MAN&dropLocationName=Manchester%20Airport&dropLocationIata=MAN&coordinates=53.362%2C-2.27344&dropCoordinates=53.362%2C-2.27344&driversAge=30&puDay=22&puMonth=9&puYear=2024&puMinute=0&puHour=10&doDay=25&doMonth=9&doYear=2024&doMinute=0&doHour=10&ftsType=A&dropFtsType=A' \
      -H 'sec-ch-ua: "Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"' \
      -H 'sec-ch-ua-mobile: ?0' \
      -H 'sec-ch-ua-platform: "macOS"' \
      -H 'sec-fetch-dest: empty' \
      -H 'sec-fetch-mode: cors' \
      -H 'sec-fetch-site: same-origin' \
      -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    """

    response, _ = utils.execute_curl_command(curl_command)
    return json.loads(response)


def build_json(
    vehicle_json: Dict[str, Any],
    search_criteria: Dict[str, Any],
    supplier_mappings: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    product_id = vehicle_json.get("productInfo", {}).get("productId", None)
    supplier_id = supplier_mappings.get(product_id, {}).get("supplierId", None)
    supplier_name = supplier_mappings.get(product_id, {}).get("supplierName", "")
    data = {
        "vehicle_id": vehicle_json.get("id", ""),
        "product_id": product_id,
        "supplier_id": supplier_id,
        "supplier_name": supplier_name,
        "make_and_model": vehicle_json.get("makeAndModel", ""),
        "transmission": vehicle_json.get("transmission", ""),
        "price": vehicle_json.get("price", {}).get("amount", None),
        "price_currency": vehicle_json.get("price", {}).get("currency", ""),
        "price_before_discounts": vehicle_json.get("price", {}).get("deposit", None),
        "car_categories": vehicle_json.get("carCategories", [""])[0],
        "car_class": vehicle_json.get("carClass", ""),
        "group": vehicle_json.get("group", [""])[0],
        "fuel_policy": vehicle_json.get("fuelPolicy", ""),
        "fleet_id": vehicle_json.get("fleetId", ""),
        "drive_away_price": vehicle_json.get("driveAwayPrice", {}).get("amount", None),
        "drive_away_price_currency": vehicle_json.get("driveAwayPrice", {}).get(
            "currency", ""
        ),
        "drivers_age": search_criteria.get("driversAge", ""),
        "pickup_location": search_criteria.get("pickUpLocation", ""),
        "pickup_datetime": search_criteria.get("pickUpDateTime", ""),
        "dropoff_location": search_criteria.get("dropOffLocation", ""),
        "dropoff_datetime": search_criteria.get("dropOffDateTime", ""),
        "pickup_location_name": search_criteria.get("searchMetadata", {}).get(
            "pickUpLocationName", ""
        ),
        "dropoff_location_name": search_criteria.get("searchMetadata", {}).get(
            "dropOffLocationName", ""
        ),
    }
    return data


def retrieve_cookies(s3_handler: Any, bucket_name: str) -> str:
    cookies = s3_handler.load_json_from_s3(bucket_name, "values/cookies.json")
    return "; ".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies)


def fetch_product_id_mappings(s3_handler: Any, bucket_name: str) -> Dict[str, str]:
    product_id_mappings = s3_handler.load_json_from_s3(
        bucket_name, "values/product_id_mappings.json"
    )
    return product_id_mappings


def get_supplier_mappings(
    product_id_mappings: Dict[str, str]
) -> Dict[str, Dict[str, str]]:
    suppliers = {
        "17": "Alamo",
        "27": "Arnold Clark",
        "47": "Avis",
        "62": "Budget",
        "82": "Dollar",
        "102": "Europcar",
        "137": "Hertz",
        "207": "Sixt",
        "482": "Enterprise",
        "516": "Green Motion",
        "2061": "Keddy By Europcar",
        "3486": "Drivalia",
        "4002": "THE OUT",
    }

    return {
        key: {"supplierId": value, "supplierName": suppliers[value]}
        for key, value in product_id_mappings.items()
    }


def get_product_id_mappings(data: Dict[str, Any]) -> Dict[str, str]:
    if data.get("depots"):
        depots = [depot for depot in data["depots"].values()]
        matches = data["matches"]
        product_id_mappings = {}
        for match in matches:
            vehicle = match["vehicle"]
            product_id = str(vehicle["productInfo"]["productId"])

            for depot in depots:
                if depot["companyId"] == product_id:
                    product_id_mappings[product_id] = depot["supplierId"]
                    break
        return product_id_mappings
    else:
        return {}


def process_vehicle_data(
    s3_handler: Any,
    bucket_name: str,
    product_id_mappings: Dict[str, str],
    cookie_string: str,
    supplier_mappings: Dict[str, Dict[str, str]],
    search_criteria: Dict[str, Any],
    hour_of_day: int,
    pickup_datetime: Optional[str] = None,
    dropoff_datetime: Optional[str] = None,
    days_ahead: Optional[int] = None,
    rental_period: Optional[str] = None,
) -> Dict[str, str]:
    return_data = get_data(
        pickup_datetime,
        dropoff_datetime,
        days_ahead,
        rental_period,
        hour_of_day,
        search_criteria,
        cookie_string,
    )

    product_id_mappings_temp = get_product_id_mappings(return_data)
    supplier_mappings_temp = get_supplier_mappings(product_id_mappings_temp)
    product_id_mappings.update(product_id_mappings_temp)
    supplier_mappings.update(supplier_mappings_temp)

    matches = return_data.get("matches")
    if matches:
        data = [
            build_json(match["vehicle"], search_criteria, supplier_mappings)
            for match in matches
        ]
        london_tz = pytz.timezone("Europe/London")
        datetime_processed_string = datetime.datetime.now(london_tz).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        for item in data:
            item["search_datetime"] = datetime_processed_string

        file_name = utils.build_filename(
            rental_period, datetime_processed_string, "rental_cars"
        )
        data_validated = [RentalCarsRawModel(**item).model_dump() for item in data]
        parquet_data = utils.convert_data_to_parquet(data_validated)
        s3_handler.upload_parquet_to_s3(bucket_name, file_name, parquet_data)

        if rental_period == "custom":
            print(f"Uploaded data for {pickup_datetime} to {dropoff_datetime}.")
        else:
            print(f"Uploaded data for {rental_period} days.")
        time.sleep(random.uniform(5, 15))
    else:
        if rental_period == "custom":
            print(
                f"Error: No matches found for {pickup_datetime} to {dropoff_datetime}."
            )
        else:
            print(f"Error: No matches found for {rental_period} days.")
    return product_id_mappings


def main() -> None:
    location = utils.get_location_from_command_line()
    print(f"Location: {location}")
    location_config = utils.get_location_config()["rental_cars"]

    print(f"Location config: {location_config}")

    location_code = location_config[location]["code"]
    location_name = location_config[location]["name"]
    location_type = location_config[location]["location_type"]
    rental_periods = location_config[location]["rental_periods"]
    hour_of_day = location_config[location]["hour_of_day"]

    search_criteria = {
        "driversAge": "30",
        "pickUpLocation": location_code,
        "pickUpLocationType": location_type,
        "dropOffLocation": location_code,
        "dropOffLocationType": location_type,
        "searchMetadata": {
            "pickUpLocationName": location_name,
            "dropOffLocationName": location_name,
        },
        "filterCriteria": {},
        "serviceFeatures": [],
    }

    bucket_name = utils.get_bucket_name()

    print(f"Search criteria: {search_criteria}")

    s3_handler = s3.S3Handler()

    cookie_string = retrieve_cookies(s3_handler, bucket_name)
    print(f"Cookie string: {cookie_string}")
    product_id_mappings = fetch_product_id_mappings(s3_handler, bucket_name)
    print(f"Product ID mappings: {product_id_mappings}")
    supplier_mappings = get_supplier_mappings(product_id_mappings)

    if os.environ.get("CUSTOM_CONFIG") == "true":
        pickup_datetime = os.environ.get("PICKUP_DATETIME")
        dropoff_datetime = os.environ.get("DROPOFF_DATETIME")
        print(f"Processing: {pickup_datetime} to {dropoff_datetime}")
        process_vehicle_data(
            s3_handler,
            bucket_name,
            product_id_mappings,
            cookie_string,
            supplier_mappings,
            search_criteria,
            hour_of_day,
            pickup_datetime,
            dropoff_datetime,
            days_ahead=None,
            rental_period="custom",
        )
    else:
        rental_periods = [1, 3, 5, 7, 10, 14, 21, 28]
        for rental_period in rental_periods:
            product_id_mappings = process_vehicle_data(
                s3_handler,
                bucket_name,
                product_id_mappings,
                cookie_string,
                supplier_mappings,
                search_criteria,
                hour_of_day,
                pickup_datetime=None,
                dropoff_datetime=None,
                days_ahead=30,
                rental_period=rental_period,
            )

    print(f"New Product ID mappings: {product_id_mappings}")
    s3_file_name = "values/product_id_mappings.json"
    s3_handler.upload_json_to_s3(bucket_name, s3_file_name, product_id_mappings)
    print("Uploaded product_id_mappings to S3.")
    utils.send_sns_notification("rental_cars", utils.PROJECT)


if __name__ == "__main__":
    main()
