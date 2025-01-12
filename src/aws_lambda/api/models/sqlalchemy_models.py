from typing import Any, Dict, List

import sqlalchemy
from sqlalchemy.ext import declarative

Base = declarative.declarative_base(metadata=sqlalchemy.MetaData())

primary_keys = [
    "year",
    "month",
    "day",
    "hour",
    "rental_period",
    "make",
    "model",
    "supplier",
    "transmission",
    "pickup_datetime",
    "dropoff_datetime",
]


def create_columns(schema: List[Dict[str, str]]) -> Dict[str, sqlalchemy.Column]:
    columns = {}
    for field in schema:
        field_name = field["name"]
        field_type = field["type"]
        if field_type == "string":
            columns[field_name] = sqlalchemy.Column(sqlalchemy.String)  # type: ignore
        elif field_type == "double":
            columns[field_name] = sqlalchemy.Column(sqlalchemy.Float)  # type: ignore
        elif field_type == "integer":
            columns[field_name] = sqlalchemy.Column(sqlalchemy.Integer)  # type: ignore
    return columns


def check_if_table_exists(table_name: str) -> bool:
    return table_name in Base.metadata.tables


def create_model_class(table_name: str, schema: List[Dict[str, str]]) -> type:
    model_class_name = table_name.capitalize()
    if hasattr(Base, model_class_name):
        return getattr(Base, model_class_name)

    columns = create_columns(schema)

    class_model = type(
        model_class_name,
        (Base,),
        {
            "__tablename__": table_name,
            **columns,
            "__table_args__": (sqlalchemy.PrimaryKeyConstraint(*primary_keys),),
        },
    )

    def query_by_dates(
        cls: Any,
        session: sqlalchemy.orm.Session,
        pickup_datetime: str,
        dropoff_datetime: str,
        limit: int,
    ) -> List[Any]:
        return (
            session.query(cls)
            .filter(
                cls.pickup_datetime == pickup_datetime,
                cls.dropoff_datetime == dropoff_datetime,
                cls.rental_period == "custom",
            )
            .limit(limit)
            .all()
        )

    def query_by_search_datetime(
        cls: Any,
        session: sqlalchemy.orm.Session,
        year: str,
        month: str,
        day: str,
        hour: str,
        limit: int,
    ) -> List[Any]:
        return (
            session.query(cls)
            .filter(
                cls.year == year,
                cls.month == month,
                cls.day == day,
                cls.hour == hour,
                cls.rental_period != "custom",
            )
            .limit(limit)
            .all()
        )

    setattr(class_model, "query_by_dates", classmethod(query_by_dates))
    setattr(
        class_model, "query_by_search_datetime", classmethod(query_by_search_datetime)
    )

    return class_model
