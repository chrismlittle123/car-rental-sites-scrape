import json
import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, create_model

PROJECT = "greenmotion"


def load_schemas() -> dict:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_schemas_path = os.path.join(current_dir, "table_schemas.json")

    with open(table_schemas_path) as f:
        return json.load(f)


def map_schema(schema: List[Dict[str, str]]) -> Dict[str, Any]:
    type_mapping = {
        "integer": int,
        "string": str,
        "double": float,
        "boolean": bool,
    }
    mapped_schema = {}
    for item in schema["columns"]:
        name = item["name"]
        mapped_schema[name] = (type_mapping[item["type"]], ...)
    return mapped_schema


schemas = load_schemas()

DoYouSpainRawModel = create_model(
    "DoYouSpainRawModel", **map_schema(schemas["do_you_spain_raw"])
)
HolidayAutosRawModel = create_model(
    "HolidayAutosRawModel", **map_schema(schemas["holiday_autos_raw"])
)
RentalCarsRawModel = create_model(
    "RentalCarsRawModel", **map_schema(schemas["rental_cars_raw"])
)

ProcessedModel = create_model("ProcessedModel", **map_schema(schemas["processed"]))


class CarRentalDataItem(BaseModel):
    make: str
    model: str
    transmission: str
    car_group: str
    supplier: str
    total_price: float
    price_per_day: Optional[float] = None
    pickup_datetime: str
    dropoff_datetime: str
    year: str
    month: str
    day: str
    hour: str
    rental_period: str
    source: str

    @classmethod
    def validate_price_per_day(cls: Any, value: str) -> Optional[str]:
        return None if value == "" else value
