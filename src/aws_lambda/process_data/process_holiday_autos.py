import logging
from typing import Any, Dict, List

import pandas as pd
import utils  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def clean_transmission(df: pd.DataFrame) -> pd.DataFrame:
    return df


def get_make_and_model(df: pd.DataFrame) -> pd.DataFrame:
    df[["make", "model"]] = df["vehicle"].str.split(" ", n=1, expand=True)

    df = utils.clean_make(df)
    df = utils.clean_model(df)
    return df


def main(
    s3_handler: Any,
    bucket_name: str,
    partition_values: Dict[str, Any],
    paths: List[str],
    file_name: str,
) -> None:
    column_mappings = {
        "vendor": "supplier",
        "total_charge": "total_price",
    }

    utils.handler(
        s3_handler,
        bucket_name,
        partition_values,
        paths,
        file_name,
        column_mappings,
        get_make_and_model,
        clean_transmission,
    )
