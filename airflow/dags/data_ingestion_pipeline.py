from airflow.decorators import dag, task
from datetime import timedelta
import pendulum

from src.services.convex_export_service import run_convex_export
from src.services.convex_extract_service import (
    extract_files,
    get_today_export_zip_path,
    prepare_tables_folder,
    group_files_by_required_tables,
    get_export_dir,
    TABLES,
)

from src.services.convex_load_service import load_rows_into_db


@dag(
    dag_id="data_ingestion",
    schedule=timedelta(minutes=30),
    start_date=pendulum.datetime(2026, 3, 1, tz="Europe/Warsaw"),
    catchup=False,
)
def data_ingestion():

    @task
    def export_data():
        run_convex_export()
        return get_today_export_zip_path()

    @task
    def prepare_extract_params(today_zip: str):
        tables_folder = prepare_tables_folder(get_export_dir(today_zip))
        grouped_files = group_files_by_required_tables(today_zip, TABLES)

        return {
            "today_zip": today_zip,
            "tables_folder": tables_folder,
            "grouped_files": grouped_files,
        }

    @task
    def extract_data(params: dict):
        extract_files(
            params["today_zip"],
            params["tables_folder"],
            params["grouped_files"],
        )

    @task
    def load_data():
        load_rows_into_db()

    exported_zip = export_data()
    extract_params = prepare_extract_params(exported_zip)
    extracted = extract_data(extract_params)
    loaded = load_data()

    extracted >> loaded


data_ingestion()
