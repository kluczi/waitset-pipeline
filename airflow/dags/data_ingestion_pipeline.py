from airflow.decorators import dag, task
from datetime import timedelta
import pendulum
from pathlib import Path

from src.services.convex_export_service import run_convex_export
from src.services.convex_extract_service import (
    extract_files,
    prepare_tables_folder,
    group_files_by_required_tables,
    get_export_dir,
    TABLES,
)
from src.services.convex_load_service import load_rows_into_db
# timedelta(minutes=30)


@dag(
    dag_id="data_ingestion",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Europe/Warsaw"),
    catchup=False,
)
def data_ingestion():

    @task
    def export_data():
        return run_convex_export()

    @task
    def prepare_extract_params(today_zip: str):
        today_zip_path = Path(today_zip)
        tables_folder = prepare_tables_folder(get_export_dir(today_zip_path))
        grouped_files = group_files_by_required_tables(today_zip_path, TABLES)

        return {
            "today_zip": str(today_zip_path),
            "tables_folder": str(tables_folder),
            "grouped_files": grouped_files,
        }

    @task
    def extract_data(parameters: dict):
        extract_files(
            Path(parameters["today_zip"]),
            Path(parameters["tables_folder"]),
            parameters["grouped_files"],
        )
        return parameters["today_zip"]

    @task
    def load_data(today_zip: str):
        load_rows_into_db(Path(today_zip))

    exported_zip = export_data()
    extract_params = prepare_extract_params(exported_zip)
    extracted = extract_data(extract_params)
    loaded = load_data(extracted)

    extracted >> loaded


data_ingestion()
