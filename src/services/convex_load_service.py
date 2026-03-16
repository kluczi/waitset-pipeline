from pathlib import Path
import json
from convex_extract_service import get_export_dir, get_today_export_zip_path


def get_table_dirs():
    today_zip = get_today_export_zip_path()
    tables_path = get_export_dir(today_zip) / Path("tables")
    tables_dirs = []
    for table in tables_path.iterdir():
        if table.is_dir():
            tables_dirs.append(table)
    return tables_dirs


def get_jsonl_files(table_dir: Path):
    files = []
    for file in table_dir.glob("*.jsonl"):
        files.append(file.name)
    return files


def parse_jsonl_records(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            record = json.loads(line)
            yield record


# source_table, record_id, dict z danymi
# def build_raw_row(records: dict):

file_path = Path(
    "/Users/bartek/Desktop/waitset-pipeline/waitset-pipeline/convex/exports/2026-03-16/tables/projects/documents.jsonl"
)
for record in parse_jsonl_records(file_path):
    print(record)
