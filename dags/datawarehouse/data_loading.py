import json
import logging

logger = logging.getLogger(__name__)

def load_data(execution_date):
    file_date = execution_date.strftime("%Y-%m-%d")
    file_path = f"/opt/airflow/data/YT_data_{file_date}.json"

    logger.info(f"Processing file: YT_data_{file_date}.json")

    with open(file_path, "r", encoding="utf-8") as raw_data:
        return json.load(raw_data)
