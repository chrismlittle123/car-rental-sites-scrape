import datetime
import json
import os
import random
import sys
import time
from typing import Any, Dict, List, Optional

import pytz
from aws_utils import iam, s3  # type: ignore

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import scripts.utils as utils  # type: ignore
from aws_lambda.api.models.pydantic_models import \
    HolidayAutosRawModel  # type: ignore

iam.get_aws_credentials(os.environ)


def build_curl_command(
    location_code: str, pickup_datetime_string: str, dropoff_datetime_string: str
) -> str:
    return f"""
curl 'https://otageo.cartrawler.com/cartrawlerota/json?msg=%7B%22@Target%22:%22Production%22,%22@PrimaryLangID%22:%22en-gb%22,%22POS%22:%7B%22Source%22:%5B%7B%22@ERSP_UserID%22:%22MP%22,%22@ISOCurrency%22:%22GBP%22,%22@ISOCountry%22:%22GB%22,%22RequestorID%22:%7B%22@Type%22:%2216%22,%22@ID%22:%22463473%22,%22@ID_Context%22:%22CARTRAWLER%22%7D%7D,%7B%22RequestorID%22:%7B%22@Type%22:%2216%22,%22@ID%22:%222851726557498360%22,%22@ID_Context%22:%22CUSTOMERID%22%7D%7D,%7B%22RequestorID%22:%7B%22@Type%22:%2216%22,%22@ID%22:%221851726571809344%22,%22@ID_Context%22:%22ENGINELOADID%22%7D%7D,%7B%22RequestorID%22:%7B%22@Type%22:%2216%22,%22@ID%22:%22CTABE_V5:5.343.0%22,%22@Instance%22:%22tBIvNh%2FbAIVh30Jp6ls9y1qd8yE%3D%22,%22@ID_Context%22:%22VERSION%22%7D%7D,%7B%22RequestorID%22:%7B%22@Type%22:%2216%22,%22@ID%22:%223%22,%22@ID_Context%22:%22BROWSERTYPE%22%7D%7D%5D%7D,%22@xmlns%22:%22http:%2F%2Fwww.opentravel.org%2FOTA%2F2003%2F05%22,%22@xmlns:xsi%22:%22http:%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22,%22@Version%22:%221.005%22,%22VehAvailRQCore%22:%7B%22@Status%22:%22Available%22,%22VehRentalCore%22:%7B%22@PickUpDateTime%22:%22{pickup_datetime_string}%22,%22@ReturnDateTime%22:%22{dropoff_datetime_string}%22,%22PickUpLocation%22:%7B%22@CodeContext%22:%22CARTRAWLER%22,%22@LocationCode%22:218%7D,%22ReturnLocation%22:%7B%22@CodeContext%22:%22CARTRAWLER%22,%22@LocationCode%22:218%7D%7D,%22VehPrefs%22:%7B%22VehPref%22:%7B%22VehClass%22:%7B%22@Size%22:%220%22%7D%7D%7D,%22DriverType%22:%7B%22@Age%22:30%7D%7D,%22VehAvailRQInfo%22:%7B%22Customer%22:%7B%22Primary%22:%7B%22CitizenCountryName%22:%7B%22@Code%22:%22GB%22%7D%7D%7D,%22TPA_Extensions%22:%7B%22showBaseCost%22:true,%22ConsumerIP%22:%2294.4.182.136%22,%22GeoRadius%22:5,%22Window%22:%7B%22@name%22:%22Cheap%2520Car%2520Hire%2520-%2520Holiday%2520Autos%22,%22@engine%22:%22CTABE-V5.0%22,%22@svn%22:%225.343.0%22,%22@product%22:%22CarWeb%22,%22@region%22:%22en-gb%22,%22@device%22:%22DESKTOPWEB%22,%22UserAgent%22:%22Mozilla%2F5.0+(Macintosh;+Intel+Mac+OS+X+10_15_7)+AppleWebKit%2F537.36+(KHTML,+like+Gecko)+Chrome%2F128.0.0.0+Safari%2F537.36%22,%22BrowserName%22:%22chrome%22,%22BrowserVersion%22:%22128%22,%22URL%22:%22https:%2F%2Fwww.holidayautos.com%2Fbook%3FpickupDateTime%3D{pickup_datetime_string}%26returnDateTime%3D{dropoff_datetime_string}%26age%3D30%26clientID%3D463473%26ct%3DMP%26curr%3DGBP%26elID%3D1851726571809344%26ln%3Den-gb%26residenceID%3DGB%26pickupID%3D218%26pickupName%3D{location_code}%2520-%2520Airport%26returnID%3D218%26returnName%3D{location_code}%2520-%2520Airport%22%7D,%22Tracking%22:%7B%22SessionID%22:%22351726637674403%22,%22CustomerID%22:%222851726557498360%22,%22EngineLoadID%22:%221851726571809344%22%7D,%22RefID%22:%7B%7D%7D%7D%7D&type=OTA_VehAvailRateRQ' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'accept-language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'origin: https://www.holidayautos.com' \
  -H 'priority: u=1, i' \
  -H 'referer: https://www.holidayautos.com/' \
  -H 'sec-ch-ua: "Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: cors' \
  -H 'sec-fetch-site: cross-site' \
  -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
"""


def process_vehicle_data(
    s3_handler: Any,
    bucket_name: str,
    location_code: str,
    hour_of_day: int,
    pickup_datetime: Optional[str] = None,
    dropoff_datetime: Optional[str] = None,
    days_ahead: Optional[int] = None,
    rental_period: Optional[str] = None,
) -> None:
    if days_ahead is None:
        pickup_datetime_string = pickup_datetime
        dropoff_datetime_string = dropoff_datetime
    else:
        pickup_datetime, dropoff_datetime = utils.get_date_range(
            rental_period, days_ahead, hour_of_day
        )
        pickup_datetime_string = pickup_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        dropoff_datetime_string = dropoff_datetime.strftime("%Y-%m-%dT%H:%M:%S")

    formatted_curl_command = build_curl_command(
        location_code, pickup_datetime_string, dropoff_datetime_string
    )
    result, _ = utils.execute_curl_command(formatted_curl_command)
    response_json = json.loads(result)

    time.sleep(random.uniform(1, 3))

    data = extract_vehicle_data(
        response_json, pickup_datetime_string, dropoff_datetime_string
    )
    london_tz = pytz.timezone("Europe/London")
    datetime_processed_string = datetime.datetime.now(london_tz).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    for item in data:
        item["search_datetime"] = datetime.datetime.now(london_tz).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
    file_name = utils.build_filename(
        rental_period, datetime_processed_string, "holiday_autos"
    )
    data_validated = [HolidayAutosRawModel(**item).model_dump() for item in data]
    parquet_data = utils.convert_data_to_parquet(data_validated)
    s3_handler.upload_parquet_to_s3(bucket_name, file_name, parquet_data)


def extract_vehicle_data(
    response_json: Dict[str, Any],
    pickup_datetime_string: str,
    dropoff_datetime_string: str,
) -> List[Dict[str, Any]]:
    data = []
    pickup_location = (
        response_json.get("VehAvailRSCore", {})
        .get("VehRentalCore", {})
        .get("PickUpLocation", {})
        .get("@Name")
    )
    return_location = (
        response_json.get("VehAvailRSCore", {})
        .get("VehRentalCore", {})
        .get("ReturnLocation", {})
        .get("@Name")
    )
    for vendor in response_json.get("VehAvailRSCore", {}).get("VehVendorAvails", []):
        vendor_name = vendor.get("Vendor", {}).get("@CompanyShortName")
        division = vendor.get("Vendor", {}).get("@Division")

        for veh_avail in vendor.get("VehAvails", []):
            data.append(
                {
                    "vendor": vendor_name,
                    "pickup_location": pickup_location,
                    "return_location": return_location,
                    "division": division,
                    "vehicle": veh_avail.get("VehAvailCore", "")
                    .get("Vehicle", "")
                    .get("VehMakeModel", "")
                    .get("@Name"),
                    "transmission": veh_avail.get("VehAvailCore", "")
                    .get("Vehicle", "")
                    .get("@TransmissionType"),
                    "fuel_type": veh_avail.get("VehAvailCore", "")
                    .get("Vehicle", "")
                    .get("@FuelType"),
                    "passenger_quantity": veh_avail.get("VehAvailCore", "")
                    .get("Vehicle", "")
                    .get("@PassengerQuantity"),
                    "total_charge": veh_avail.get("VehAvailCore", "")
                    .get("TotalCharge", "")
                    .get("@RateTotalAmount"),
                    "pickup_datetime": pickup_datetime_string,
                    "dropoff_datetime": dropoff_datetime_string,
                }
            )

    return data


def main() -> None:
    location = utils.get_location_from_command_line()
    print(f"Location: {location}")

    location_config = utils.get_location_config()["holiday_autos"]

    location_code = location_config[location]["code"]
    rental_periods = location_config[location]["rental_periods"]
    hour_of_day = location_config[location]["hour_of_day"]

    s3_handler = s3.S3Handler()
    bucket_name = utils.get_bucket_name()

    if os.environ.get("CUSTOM_CONFIG") == "true":
        rental_period = "custom"
        pickup_datetime = os.environ.get("PICKUP_DATETIME")
        dropoff_datetime = os.environ.get("DROPOFF_DATETIME")
        print(f"Processing: {pickup_datetime} to {dropoff_datetime}")
        process_vehicle_data(
            s3_handler,
            bucket_name,
            location_code,
            hour_of_day,
            pickup_datetime,
            dropoff_datetime,
            days_ahead=None,
            rental_period=rental_period,
        )

    else:
        for rental_period in rental_periods:
            print(f"Processing rental period: {rental_period}")
            process_vehicle_data(
                s3_handler,
                bucket_name,
                location_code,
                hour_of_day,
                pickup_datetime=None,
                dropoff_datetime=None,
                days_ahead=30,
                rental_period=rental_period,
            )

    utils.send_sns_notification("holiday_autos", utils.PROJECT)


if __name__ == "__main__":
    main()
