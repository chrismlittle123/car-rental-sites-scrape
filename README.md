# greenmotion

TO DO:

- Create backend project template
- Create frontend project template

Start up:

python -m venv venv
source venv/bin/activate
pip install -r requirements_dev.txt

Checks:

pre-commit run --all-files

mypy {path_to_file_or_directory} --explicit-package-bases

Commands:

uvicorn src.lambda.api.main:app --host 0.0.0.0 --port 8000 --reload

curl -X GET "http://localhost:8000/items/?table_name=do_you_spain&pickup_datetime=2024-12-01T10:00:00&dropoff_datetime=2024-12-08T12:00:00&limit=10"

curl -X GET "http://localhost:8000/items/?table_name=do_you_spain&search_datetime=2024-10-29T17:00:00&limit=10"
