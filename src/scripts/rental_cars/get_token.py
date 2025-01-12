import os
import sys
import time
from typing import Any

import requests
from aws_utils import iam, s3  # type: ignore

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import scripts.utils as utils  # type: ignore

iam.get_aws_credentials(os.environ)  # type: ignore


def request_captcha(api_key: str, site_key: str, url: str) -> str:
    response = requests.post(
        "https://2captcha.com/in.php",
        {"key": api_key, "method": "hcaptcha", "sitekey": site_key, "pageurl": url},
    )
    return response.text.split("|")[1]


def wait_for_captcha(api_key: str, captcha_id: str) -> str:
    time.sleep(5)
    result = requests.get(
        f"https://2captcha.com/res.php?key={api_key}&action=get&id={captcha_id}"
    )
    while "CAPCHA_NOT_READY" in result.text:
        time.sleep(5)
        result = requests.get(
            f"https://2captcha.com/res.php?key={api_key}&action=get&id={captcha_id}"
        )
    return result.text.split("|")[1]


def upload_token_to_s3(s3_handler: Any, bucket_name: str, token: str) -> None:
    s3_handler.upload_json_to_s3(bucket_name, "values/token.json", {"value": token})


def get_token(api_key: str, site_key: str, url: str) -> str:
    captcha_id = request_captcha(api_key, site_key, url)
    print("Captcha ID received")
    token = wait_for_captcha(api_key, captcha_id)
    print(f"Token: {token}")
    return token


def main() -> None:
    s3_handler = s3.S3Handler()

    api_key = os.environ["CAPTCHA_API_KEY"]
    site_key = "4a23e8c5-05f9-459a-83ca-4a4041cf6bea"
    url = "https://www.rentalcars.com/"
    bucket_name = utils.get_bucket_name()

    try:
        token = get_token(api_key, site_key, url)
    except Exception as e:
        print(e)
        print("Request failed, retrying...")
        token = get_token(api_key, site_key, url)

    upload_token_to_s3(s3_handler, bucket_name, token)
    print("Token uploaded to S3")


if __name__ == "__main__":
    main()
