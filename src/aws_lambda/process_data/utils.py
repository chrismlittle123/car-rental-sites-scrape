import logging
from io import BytesIO
from typing import Any, Callable, Dict

import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def clean_make(df: pd.DataFrame) -> pd.DataFrame:
    df["make"] = df["make"].replace({"OPEL": "VAUXHALL", "VW": "VOLKSWAGEN"})
    df.loc[df["make"] == "MG", ["make", "model"]] = ["MG", "4"]
    return df


def clean_model(df: pd.DataFrame) -> pd.DataFrame:
    df["model"] = (
        df["model"]
        .str.replace(" OR SIMILAR", "", case=False)
        .str.replace("AUTOMATIC", "", case=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return df


def calculate_price_per_day(df: pd.DataFrame, rental_period: Any) -> pd.DataFrame:
    df["price_per_day"] = df["total_price"].apply(
        lambda total_price: calculate_individual_price_per_day(
            total_price, rental_period
        )
    )
    return df


def calculate_individual_price_per_day(total_price: Any, rental_period: Any) -> Any:
    try:
        rental_period = float(rental_period)
        total_price = float(total_price)
        if rental_period > 0:
            return round(total_price / rental_period, 2)
    except (ValueError, TypeError):
        return None


def assign_car_group_to_dataframe(
    df: pd.DataFrame, car_groups: pd.DataFrame
) -> pd.DataFrame:
    df = pd.merge(
        df.drop(columns=["group"], errors="ignore"),
        car_groups,
        on=["make", "model", "transmission"],
        how="left",
    )
    df.rename(columns={"group": "car_group"}, inplace=True)
    df["car_group"] = df["car_group"].fillna("OTHER")
    return df


def make_string_columns_uppercase(df: pd.DataFrame) -> pd.DataFrame:
    string_columns = [
        "make",
        "model",
        "vehicle",
        "make_and_model",
        "transmission",
        "car_group",
        "supplier",
    ]
    for column in string_columns:
        if column in df.columns:
            df[column] = df[column].str.upper()
    return df


def make_numeric_columns_float(df: pd.DataFrame) -> pd.DataFrame:
    numeric_columns = [
        "total_price",
        "price_per_day",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = df[column].astype(float)
    return df


def build_file_path(
    partition_values: Dict[str, Any], paths: list, file_name: str
) -> str:
    return (
        "/".join(paths)
        + "/"
        + "/".join([f"{key}={value}" for key, value in partition_values.items()])
        + "/"
        + file_name
    )


def upload_parquet_data(
    s3_handler: Any,
    bucket_name: str,
    df_processed: pd.DataFrame,
    partition_values: Dict[str, Any],
    paths: list,
    file_name: str,
) -> None:
    parquet_data = df_processed.to_parquet()
    paths.pop()
    paths.append("processed")
    file_path = build_file_path(partition_values, paths, file_name)
    s3_handler.upload_parquet_to_s3(bucket_name, file_path, parquet_data)


def process_data(
    df: pd.DataFrame,
    car_groups: pd.DataFrame,
    rental_period: Any,
    column_mappings: Dict[str, str],
    make_and_model_function: Callable[[pd.DataFrame], pd.DataFrame],
    clean_transmission_function: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    df.rename(columns=column_mappings, inplace=True)
    df = make_string_columns_uppercase(df)
    df = make_and_model_function(df)
    df = clean_transmission_function(df)
    df = assign_car_group_to_dataframe(df, car_groups)
    df = calculate_price_per_day(df, rental_period)
    df = make_numeric_columns_float(df)

    final_columns = [
        "make",
        "model",
        "transmission",
        "car_group",
        "supplier",
        "total_price",
        "price_per_day",
        "pickup_datetime",
        "dropoff_datetime",
    ]

    return df[final_columns]


def get_dataframe_from_csv(data_list: list) -> pd.DataFrame:
    return pd.DataFrame(columns=data_list[0], data=data_list[1:])


def get_dataframe_from_parquet(parquet_data: bytes) -> pd.DataFrame:
    return pd.read_parquet(BytesIO(parquet_data))


def handler(
    s3_handler: Any,
    bucket_name: str,
    partition_values: Dict[str, Any],
    paths: list,
    file_name: str,
    column_mappings: Dict[str, str],
    make_and_model_function: Callable[[pd.DataFrame], pd.DataFrame],
    clean_transmission_function: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    file_path = build_file_path(partition_values, paths, file_name)
    parquet_data = s3_handler.load_parquet_from_s3(bucket_name, file_path)
    df = get_dataframe_from_parquet(parquet_data)
    rental_period = partition_values.get("rental_period")

    car_groups = get_dataframe_from_csv(
        s3_handler.load_csv_from_s3(bucket_name, "car_groups/car_groups.csv")
    )
    df_processed = process_data(
        df,
        car_groups,
        rental_period,
        column_mappings,
        make_and_model_function,
        clean_transmission_function,
    )

    upload_parquet_data(
        s3_handler, bucket_name, df_processed, partition_values, paths, file_name
    )
