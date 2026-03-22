from airflow.decorators import dag, task

# from datetime import timedelta
import pendulum
from pathlib import Path

from src.services.convex_export_service import run_convex_export
from src.services.convex_extract_service import (
    extract_files,
    extract_component_files,
    prepare_tables_folder,
    group_files_by_required_tables,
    group_component_files,
    get_export_dir,
    TABLES,
    COMPONENT_TABLES,
)
from src.services.convex_load_service import load_rows_into_db
# timedelta(minutes=30)


@dag(
    dag_id="convex_data_ingestion",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Europe/Warsaw"),
    catchup=False,
)
def convex_data_ingestion():

    @task
    def export_data():
        return run_convex_export()

    @task
    def prepare_extract_params(today_zip: str):
        today_zip_path = Path(today_zip)
        tables_folder = prepare_tables_folder(get_export_dir(today_zip_path))
        grouped_files = group_files_by_required_tables(today_zip_path, TABLES)
        component_files = group_component_files(today_zip_path, COMPONENT_TABLES)

        return {
            "today_zip": str(today_zip_path),
            "tables_folder": str(tables_folder),
            "grouped_files": grouped_files,
            "component_files": component_files,
        }

    @task
    def extract_data(parameters: dict):
        today_zip = Path(parameters["today_zip"])
        tables_folder = Path(parameters["tables_folder"])
        extract_files(today_zip, tables_folder, parameters["grouped_files"])
        extract_component_files(today_zip, tables_folder, parameters["component_files"])
        return parameters["today_zip"]

    @task
    def load_data(today_zip: str):
        load_rows_into_db(Path(today_zip))

    exported_zip = export_data()
    extract_params = prepare_extract_params(exported_zip)
    extracted = extract_data(extract_params)
    loaded = load_data(extracted)

    extracted >> loaded


convex_data_ingestion()
