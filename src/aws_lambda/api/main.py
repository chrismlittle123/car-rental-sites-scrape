import json
import logging
import os
import sys
from typing import Optional, Type

from aws_utils import iam  # type: ignore
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from mangum import Mangum
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.pydantic_models import PROJECT, CarRentalDataItem  # type: ignore
from models.sqlalchemy_models import Base, create_model_class  # type: ignore

iam.get_aws_credentials(os.environ)


def get_table_schemas_path() -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_schemas_path = os.path.join(current_dir, "models", "table_schemas.json")
    return table_schemas_path


table_schemas_path = get_table_schemas_path()


def load_schemas(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


schemas = load_schemas(table_schemas_path)

app = FastAPI()


def get_database_url(database_name: str, workgroup: str, output_bucket: str) -> str:
    return f"awsathena+rest://{os.environ['AWS_ACCESS_KEY_ID']}:{os.environ['AWS_SECRET_ACCESS_KEY']}@athena.{os.environ['AWS_REGION']}.amazonaws.com:443/{database_name}?s3_staging_dir={output_bucket}&work_group={workgroup}"


def create_session(database_name: str, workgroup: str, output_bucket: str) -> Session:
    DATABASE_URL = get_database_url(database_name, workgroup, output_bucket)
    engine = create_engine(
        DATABASE_URL,
        connect_args={
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "aws_session_token": os.environ["AWS_SESSION_TOKEN"],
        },
    )
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


def query_results(
    db: Session,
    Processed: Type,
    pickup_datetime: Optional[str],
    dropoff_datetime: Optional[str],
    search_datetime: Optional[str],
    limit: int,
) -> list:
    if pickup_datetime and dropoff_datetime:
        return Processed.query_by_dates(db, pickup_datetime, dropoff_datetime, limit)
    else:
        if search_datetime:
            year, month, day_hour = search_datetime.split("-")
            day, hour = (
                day_hour.split("T")[0],
                search_datetime.split("T")[1].split(":")[0],
            )
            return Processed.query_by_search_datetime(db, year, month, day, hour, limit)
        else:
            raise ValueError("No datetime parameters provided")


def validate_results(results: list, table_name: str) -> list:
    return [
        CarRentalDataItem(
            **{
                **dict(row.__dict__),
                "source": table_name,
                "price_per_day": CarRentalDataItem.validate_price_per_day(
                    row.price_per_day
                ),
            }
        )
        for row in results
    ]


@app.get("/items/")
async def read_items(
    table_name: str,
    search_datetime: Optional[str] = None,
    pickup_datetime: Optional[str] = None,
    dropoff_datetime: Optional[str] = None,
    limit: int = 5,
) -> JSONResponse:
    workgroup = f"{PROJECT}-workgroup"
    output_bucket = f"s3://{PROJECT}-bucket-{os.environ['AWS_ACCOUNT_ID']}"
    db = create_session(table_name, workgroup, output_bucket)

    try:
        Processed = create_model_class(
            "processed",
            schemas["processed"]["columns"] + schemas["processed"]["partition_keys"],
        )

        results = query_results(
            db, Processed, pickup_datetime, dropoff_datetime, search_datetime, limit
        )

        logger.info(f"results: {results}")
        validated_results = validate_results(results, table_name)
        logger.info(f"validated_results: {validated_results}")
        Base.metadata.clear()
        return JSONResponse(content=[item.model_dump() for item in validated_results])
    finally:
        db.close()


lambda_handler = Mangum(app)
