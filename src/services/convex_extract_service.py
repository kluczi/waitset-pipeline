from zipfile import ZipFile
from pathlib import Path
from src.services.convex_export_service import (
    current_date,
    get_project_root,
    get_convex_root,
    generate_export_path,
    generate_export_file_name,
    build_full_path,
)

TABLES: list[str] = ["projects", "signups", "user_subscription_tracking"]


# def get_today_export_zip_path() -> Path:
#     project_root = get_project_root()
#     convex_root = get_convex_root(project_root)
#     export_path = generate_export_path(convex_root, date)
#     file_name = generate_export_file_name(date)
#     return build_full_path(export_path, file_name)


def get_export_dir(zip_path: Path) -> Path:
    return zip_path.parent


def open_export_zip(path: Path) -> ZipFile:
    return ZipFile(path)


def get_files_list(zip_file: ZipFile) -> list[str]:
    return zip_file.namelist()


def group_files_by_required_tables(
    zip_path: Path, tables: list[str]
) -> dict[str, dict[str, str]]:
    grouped_files: dict[str, dict[str, str]] = {}

    with open_export_zip(zip_path) as zip_file:
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
            # elif file_name == "generated_schema.jsonl":
            #     grouped_files[table_name]["schema"] = file

    return grouped_files


def prepare_tables_folder(export_dir: Path) -> Path:
    return export_dir / "tables"


def create_table_dirs(
    tables_folder: Path, grouped_files: dict[str, dict[str, str]]
) -> dict[str, dict[str, str | Path]]:
    prepared_tables: dict[str, dict[str, str | Path]] = {}

    for table, files in grouped_files.items():
        folder = tables_folder / table
        folder.mkdir(parents=True, exist_ok=True)
        prepared_tables[table] = {
            "dir": folder,
            "documents": files["documents"],
            # "schema": files["schema"],
        }

    return prepared_tables


def extract_files(
    zip_path: Path,
    tables_folder: Path,
    grouped_files: dict[str, dict[str, str]],
) -> None:
    with open_export_zip(zip_path) as zip_file:
        for table_files in grouped_files.values():
            zip_file.extract(table_files["documents"], path=tables_folder)
            # zip_file.extract(table_files["schema"], path=tables_folder)


# if __name__ == "__main__":
#     # today_zip = get_today_export_zip_path()
#     tables_folder = prepare_tables_folder(get_export_dir(today_zip))
#     grouped_files = group_files_by_required_tables(today_zip, TABLES)
#     tables = create_table_dirs(tables_folder, grouped_files)
#     extract_files(today_zip, tables_folder, grouped_files)
