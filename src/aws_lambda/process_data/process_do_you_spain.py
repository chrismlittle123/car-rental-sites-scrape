import logging
from typing import Any

import pandas as pd
import utils  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def clean_transmission(df: pd.DataFrame) -> pd.DataFrame:
    df["transmission"] = df.apply(
        lambda row: (
            "AUTOMATIC"
            if row.get("has_automatic_transmission") == "A"
            else ("MANUAL" if row.get("has_manual_transmission") == "M" else "UNKNOWN")
        ),
        axis=1,
    )
    return df


def clean_model_do_you_spain(df: pd.DataFrame) -> pd.DataFrame:
    df["model"] = (
        df["model"]
        .str.split(",")
        .str[0]
        .str.replace(r"\s*(AUTO|AUTOMATIC|4x4)\s*$", "", regex=True)
        .str.replace(r"\s*4 DOOR\s*|COUPE\s*|5 DOOR\s*|\+\s*GPS\s*", "", regex=True)
        .str.replace("SW", "ESTATE", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return df


def get_make_and_model(df: pd.DataFrame) -> pd.DataFrame:
    df[["make", "model"]] = df["vehicle"].str.split(" ", n=1, expand=True)

    df = utils.clean_make(df)
    df = clean_model_do_you_spain(df)
    return df


def main(
    s3_handler: Any,
    bucket_name: str,
    partition_values: dict,
    paths: list,
    file_name: str,
) -> None:
    column_mappings = {
        "supplier_full_name": "supplier",
        "full_price": "total_price",
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
