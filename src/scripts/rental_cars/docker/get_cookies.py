import os
import time
from typing import Any, Dict, List

from aws_utils import iam, s3  # type: ignore
from undetected_chromedriver import Chrome, ChromeOptions  # type: ignore

iam.get_aws_credentials(os.environ)


def setup_driver() -> Chrome:
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = Chrome(options=options)
    return driver


def navigate_to_url(driver: Chrome, url: str) -> None:
    print(f"Navigating to {url}...")
    driver.get(url)
    time.sleep(5)
    print("Successfully navigated to RentalCars.com")


def upload_cookies_to_s3(
    s3_handler: s3.S3Handler, bucket_name: str, cookies: List[Dict[str, Any]]
) -> None:
    s3_handler.upload_json_to_s3(bucket_name, "values/cookies.json", cookies)
    print("Cookies uploaded successfully.")


def main() -> None:
    s3_handler = s3.S3Handler()
    bucket_name = f"greenmotion-bucket-{os.environ['AWS_ACCOUNT_ID']}"
    url = "https://www.rentalcars.com/"

    driver = setup_driver()
    navigate_to_url(driver, url)

    cookies = driver.get_cookies()
    print(f"Cookies: {cookies}")

    upload_cookies_to_s3(s3_handler, bucket_name, cookies)


if __name__ == "__main__":
    main()
