from zipfile import ZipFile
from pathlib import Path
from convex_export_service import build_full_path

tables = ["projects", "signups", "user_subscription_tracking"]


def get_today_export_zip_path():
    return build_full_path()


def get_today_export_dir():
    return get_today_export_zip_path().parent


def open_export_zip(path):
    return ZipFile(path)


def get_files_list(zip_file):
    return zip_file.namelist()


def group_files_by_required_tables():
    grouped_files = {}

    with open_export_zip(get_today_export_zip_path()) as zip_file:
        file_list = get_files_list(zip_file)

        for file in file_list:
            path_parts = file.split("/")

            if len(path_parts) != 2:
                continue

            table_name = path_parts[0]
            file_name = path_parts[1]

            if table_name not in tables:
                continue

            if table_name not in grouped_files:
                grouped_files[table_name] = {}

            if file_name == "documents.jsonl":
                grouped_files[table_name]["documents"] = file
            elif file_name == "schema.jsonl":
                grouped_files[table_name]["schema"] = file

    return grouped_files


def create_table_dir():
    grouped_files = group_files_by_required_tables()
    today_export_dir = get_today_export_dir()
    main_folder = Path("tables")
    for table, _ in grouped_files.items():
        folder = today_export_dir / main_folder / table
        folder.mkdir(parents=True, exist_ok=True)


create_table_dir()
print(group_files_by_required_tables())
