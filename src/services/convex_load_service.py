from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo  # to get Warsaw CET time
import json
from src.services.convex_extract_service import get_export_dir

import psycopg
from src.db.conn import get_connection
from src.db.insert import insert_to_projects, insert_to_signups, insert_to_ust
from src.db.create_tables import create_raw_schema
import hashlib


def compute_payload_hash(payload: dict) -> str:
    normalized = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# today table dirs
def get_table_dirs(today_zip: Path) -> list:
    # today_zip = get_today_export_zip_path()
    tables_path = get_export_dir(today_zip) / Path("tables")
    tables_dirs = []
    for table in tables_path.iterdir():
        if table.is_dir():
            tables_dirs.append(table)
    return tables_dirs


def get_jsonl_files(table_dir: Path) -> list:
    files = []
    for file in table_dir.glob("documents.jsonl"):
        files.append(file.name)
    return files


def parse_jsonl_records(file_path: Path) -> None:
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            record = json.loads(line)
            yield record


# tempfile
# parse->build_row->dict->row into batch->insert
# pamietac o incrementalu i diffie!!
# source_table, loaded_at, record_id, payload
def build_raw_row(source_path: Path, record: dict) -> dict:
    # table_name = source_path.parent.name
    return {
        "record_id": record.get("_id"),
        "loaded_at": datetime.now(ZoneInfo("Europe/Warsaw")).isoformat(),
        "payload_hash": compute_payload_hash(record),
        "payload": record,
    }


def map_dict_into_tuple(batch: dict):
    values = []
    for row in batch:
        values.append(
            (
                row["record_id"],
                row["loaded_at"],
                row["payload_hash"],
                json.dumps(row["payload"]),
            )
        )
    return values


def insert_raw_batch_into_db(table: str, batch: list, conn: psycopg):
    values = map_dict_into_tuple(batch)
    match table:
        case "projects":
            insert_to_projects(conn, values)
        case "signups":
            insert_to_signups(conn, values)
        case "user_subscription_tracking":
            insert_to_ust(conn, values)


def load_rows_into_batch(file_path: Path) -> list:
    batch = []
    BATCH_SIZE = 1000
    with get_connection() as conn:
        conn.autocommit = True
        create_raw_schema(conn)
        for idx, record in enumerate(parse_jsonl_records(file_path), start=1):
            raw_row = build_raw_row(file_path, record)
            batch.append(raw_row)
            if idx % BATCH_SIZE == 0:
                insert_raw_batch_into_db(file_path.parent.name, batch, conn)
                batch.clear()
        # insert if this anything<1000 left
        if batch:
            insert_raw_batch_into_db(file_path.parent.name, batch, conn)


def load_rows_into_db(zip_path: Path):
    tables_folder = get_table_dirs(zip_path)
    for table in tables_folder:
        path = table / "documents.jsonl"
        load_rows_into_batch(path)


if __name__ == "__main__":
    load_rows_into_db()

# file_path = Path(
#     "/Users/bartek/Desktop/waitset-pipeline/waitset-pipeline/convex/exports/2026-03-16/tables/projects/documents.jsonl"
# )


# for record in parse_jsonl_records(file_path):
#     print(build_raw_row(file_path, record))
