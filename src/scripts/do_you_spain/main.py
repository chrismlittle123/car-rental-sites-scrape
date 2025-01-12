import datetime
import itertools
import os
import random
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import pytz
from aws_utils import iam, s3  # type: ignore
from bs4 import BeautifulSoup

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import scripts.utils as utils  # type: ignore
from aws_lambda.api.models.pydantic_models import DoYouSpainRawModel  # type: ignore

iam.get_aws_credentials(os.environ)


def get_text_if_exists(element: Any) -> str:
    return element.get_text(strip=True) if element else ""


def clean_price(price: str) -> float:
    return float(price.replace("£", "").replace(",", "").strip())


def get_supplier_code(logo_element: Any) -> str:
    logo_url = (
        logo_element["src"] if logo_element and "src" in logo_element.attrs else ""
    )
    if isinstance(logo_url, str):
        supplier_code = logo_url.split("/")[-1].replace("logo_", "").replace(".png", "")
    else:
        supplier_code = ""
    return supplier_code


def get_vehicle_data(article: BeautifulSoup) -> Dict[str, Any]:
    vehicle = get_text_if_exists(article.find("h2"))
    seats = get_text_if_exists(article.find("li", class_="sc-seats"))
    doors = get_text_if_exists(article.find("li", class_="sc-doors"))
    has_automatic_transmission = get_text_if_exists(
        article.find("li", class_="sc-transm-auto")
    )
    has_manual_transmission = get_text_if_exists(article.find("li", class_="sc-transm"))
    vehicle_type = get_text_if_exists(article.find("span", class_="cl--name-type"))
    full_price = clean_price(
        get_text_if_exists(article.find("span", class_="old-price-libras"))
    )
    discounted_price = clean_price(
        get_text_if_exists(article.find("span", class_="pr-libras"))
    )
    price_per_day = clean_price(
        get_text_if_exists(article.find("em", class_="price-day-libras"))
    )
    logo_element = article.find("span", class_="cl--car-rent-logo")
    supplier_code = get_supplier_code(
        logo_element.find("img") if logo_element else None
    )

    vehicle_data = {
        "vehicle": vehicle,
        "seats": seats,
        "doors": doors,
        "has_automatic_transmission": has_automatic_transmission,
        "has_manual_transmission": has_manual_transmission,
        "full_price": full_price,
        "discounted_price": discounted_price,
        "price_per_day": price_per_day,
        "supplier_code": supplier_code,
        "vehicle_type": vehicle_type,
    }
    return vehicle_data


def create_supplier_filter(supplier: str, suppliers: Dict[str, str]) -> str:
    not_selected_suppliers = [value for value in suppliers.keys() if value != supplier]
    return "%7C".join(not_selected_suppliers)


def create_datetime_filter(datetime_value: datetime.datetime) -> str:
    datetime_string = datetime_value.strftime("%d/%m/%Y")
    datetime_filter = datetime_string.replace("/", "%2F") + "+10%3A00"
    return datetime_filter


def get_curl_command(
    supplier: str,
    suppliers: Dict[str, str],
    vehicle_type: str,
    location_code: str,
    pickup_date: datetime.datetime,
    dropoff_date: datetime.datetime,
) -> str:
    not_selected_suppliers = create_supplier_filter(supplier, suppliers)
    pickup_datetime_filter = create_datetime_filter(pickup_date)
    dropoff_datetime_filter = create_datetime_filter(dropoff_date)

    session_id = "cf30e285-cdd5-4b5e-a22d-1419874f4183"

    return f"""curl 'https://www.doyouspain.com/do/list/en' \
  -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7' \
  -H 'accept-language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'cache-control: max-age=0' \
  -H 'content-type: application/x-www-form-urlencoded' \
  -H 'origin: https://www.doyouspain.com' \
  -H 'priority: u=0, i' \
  -H 'referer: https://www.doyouspain.com/do/list/en' \
  -H 'sec-ch-ua: "Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'sec-fetch-dest: document' \
  -H 'sec-fetch-mode: navigate' \
  -H 'sec-fetch-site: same-origin' \
  -H 'sec-fetch-user: ?1' \
  -H 'upgrade-insecure-requests: 1' \
  -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36' \
  --data-raw 'frmDestino={location_code}&frmDestinoFinal=&frmFechaRecogida={pickup_datetime_filter}&frmFechaDevolucion={dropoff_datetime_filter}&frmHasAge=False&frmEdad=35&frmPrvNo={not_selected_suppliers}&frmMoneda=GBP&frmJsonFilterInfo=&frmTipoVeh=CAR&idioma=EN&frmSession={session_id}&frmDetailCode=&frmPrv={supplier}&frmAer=none&frmFran=none&frmTrans=none&frmKm=none&frmDeb=none&frmDep=none&frmCancel=none&frmSeats=none&frmAgrp={vehicle_type}&valideHost=true'"""


def extract_vehicle_data(
    html_content: str,
    suppliers: Dict[str, str],
    pickup_date: datetime.datetime,
    dropoff_date: datetime.datetime,
) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_content, "html.parser")
    articles = soup.find_all("article", attrs={"data-order": True})
    data = []

    for article in articles:
        vehicle_data = get_vehicle_data(article)
        vehicle_data["pickup_datetime"] = pickup_date.strftime("%Y-%m-%dT%H:%M:%S")
        vehicle_data["dropoff_datetime"] = dropoff_date.strftime("%Y-%m-%dT%H:%M:%S")

        supplier_code = vehicle_data.get("supplier_code")
        if isinstance(supplier_code, str):
            vehicle_data["supplier_full_name"] = suppliers.get(supplier_code, "Unknown")
        else:
            vehicle_data["supplier_full_name"] = "Unknown"

        data.append(vehicle_data)

    return data


def get_supplier_vehicle_combination(
    suppliers: Dict[str, str], vehicle_types: List[str], rental_periods: List[str]
) -> List[Tuple[str, str, str]]:
    combinations = list(itertools.product(suppliers, vehicle_types, rental_periods))
    return combinations


def process_vehicle_data(
    s3_handler: s3.S3Handler,
    suppliers: Dict[str, str],
    bucket_name: str,
    location_code: str,
    hour_of_day: int,
    pickup_datetime: Optional[str] = None,
    dropoff_datetime: Optional[str] = None,
    days_ahead: Optional[int] = None,
    rental_period: Optional[str] = None,
) -> None:
    supplier = "ACE"
    vehicle_type = "CARG"

    if (
        days_ahead is None
        and isinstance(pickup_datetime, str)
        and isinstance(dropoff_datetime, str)
    ):
        pickup_date = datetime.datetime.fromisoformat(pickup_datetime)
        dropoff_date = datetime.datetime.fromisoformat(dropoff_datetime)
    else:
        pickup_date, dropoff_date = utils.get_date_range(
            rental_period, days_ahead, hour_of_day
        )

    curl_command = get_curl_command(
        supplier, suppliers, vehicle_type, location_code, pickup_date, dropoff_date
    )
    time.sleep(random.uniform(3, 5))

    html_content, _ = utils.execute_curl_command(curl_command)
    if len(html_content) < 16000:
        time.sleep(random.uniform(3, 5))
        html_content, _ = utils.execute_curl_command(curl_command)
        if len(html_content) < 16000:
            time.sleep(random.uniform(3, 5))
            html_content, _ = utils.execute_curl_command(curl_command)

    data = extract_vehicle_data(html_content, suppliers, pickup_date, dropoff_date)

    if len(data) > 0:
        london_tz = pytz.timezone("Europe/London")
        datetime_processed_string = datetime.datetime.now(london_tz).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        for item in data:
            item["search_datetime"] = datetime_processed_string
        file_name = utils.build_filename(
            rental_period, datetime_processed_string, "do_you_spain"
        )
        data_validated = [DoYouSpainRawModel(**item).model_dump() for item in data]
        parquet_data = utils.convert_data_to_parquet(data_validated)
        s3_handler.upload_parquet_to_s3(bucket_name, file_name, parquet_data)


def main() -> None:
    location = utils.get_location_from_command_line()
    print(f"Location: {location}")

    location_config = utils.get_location_config()["do_you_spain"]

    location_code = location_config[location]["code"]
    rental_periods = location_config[location]["rental_periods"]
    hour_of_day = location_config[location]["hour_of_day"]

    s3_handler = s3.S3Handler()
    suppliers = {
        "ALM": "Alamo",
        "AND": "Arnold Clark",
        "AVX": "Avis",
        "BGX": "Budget",
        "EDS": "Drivalia",
        "EDS1": "Rentacar",
        "EDX": "Drivalia",
        "ENT": "Enterprise",
        "ECR": "Europcar",
        "GMO": "Green Motion",
        "GMO1": "Green Motion Non-Refundable",
        "HER": "Hertz",
        "NAT": "National",
        "ROU": "Routes",
        "SXT": "Sixt",
        "SUR": "Surprice",
        "DTI": "Dollar",
        "KED": "Keddy",
        "USV": "U-Save",
        "THR": "Thrifty",
        "ACE": "ACE",
    }

    bucket_name = utils.get_bucket_name()

    if os.environ.get("CUSTOM_CONFIG") == "true":
        rental_period = "custom"
        pickup_datetime = os.environ.get("PICKUP_DATETIME")
        dropoff_datetime = os.environ.get("DROPOFF_DATETIME")
        print(f"Processing: {pickup_datetime} to {dropoff_datetime}")
        process_vehicle_data(
            s3_handler,
            suppliers,
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
                suppliers,
                bucket_name,
                location_code,
                hour_of_day,
                pickup_datetime=None,
                dropoff_datetime=None,
                days_ahead=30,
                rental_period=rental_period,
            )

    utils.send_sns_notification("do_you_spain", utils.PROJECT)


if __name__ == "__main__":
    main()
