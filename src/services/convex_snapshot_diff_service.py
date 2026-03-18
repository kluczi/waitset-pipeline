from src.db.conn import get_connection
from src.db.select import (
    fetch_all_from_signups,
    fetch_all_from_projects,
    fetch_all_from_ust,
)
from src.db.insert import insert_to_projects, insert_to_signups, insert_to_ust
from src.services.convex_load_service import (
    map_dict_into_tuple,
    parse_jsonl_records,
    build_raw_row,
    get_table_dirs,
)
from src.db.delete import delete_from_projects, delete_from_ust, delete_from_signups
import psycopg
from pathlib import Path


def get_db_table_data(conn: psycopg, table: str) -> list:
    data = []
    match table:
        case "projects":
            data = fetch_all_from_projects(conn)
        case "signups":
            data = fetch_all_from_signups(conn)
        case "user_subscription_tracking":
            data = fetch_all_from_ust(conn)
    return data


def get_today_records(file_path: Path) -> list:
    rows = []
    for row in parse_jsonl_records(file_path):
        raw_row = build_raw_row(file_path, row)
        rows.append(raw_row)
    result = map_dict_into_tuple(rows)
    return result


def payload_hash_equal(today_hash: str, existing_hash: str):
    if today_hash != existing_hash:
        return False
    return True


def compute_diff(today_records: list, existing_records: list) -> list:
    today_ids = set(today_records.keys())
    db_ids = set(existing_records.keys())

    new_ids = today_ids - db_ids
    deleted_ids = db_ids - today_ids
    common_ids = today_ids & db_ids

    new_rows = [today_records[rid] for rid in new_ids]
    deleted_rows = [existing_records[rid] for rid in deleted_ids]

    updated_rows = []
    for rid in common_ids:
        if not payload_hash_equal(
            today_records[rid]["payload_hash"], existing_records[rid]["payload_hash"]
        ):
            updated_rows.append(today_records[rid])
    return new_rows, deleted_rows, updated_rows


def handle_new_rows(conn: psycopg, new_rows: list, table: str):
    new_raw_rows = map_dict_into_tuple(new_rows)
    if new_raw_rows:
        match table:
            case "projects":
                insert_to_projects(conn, new_raw_rows)
            case "signups":
                insert_to_signups(conn, new_raw_rows)
            case "user_subscription_tracking":
                insert_to_ust(conn, new_raw_rows)


def handle_updated_rows(
    conn: psycopg, existing_records: list, updated_rows: list, table: str
):
    pass


def handle_deleted_rows(conn: psycopg, deleted_rows: list, table: str):
    deleted_raw_rows = map_dict_into_tuple(deleted_rows)
    if deleted_raw_rows:
        match table:
            case "projects":
                delete_from_projects(conn, deleted_raw_rows)
            case "signups":
                delete_from_signups(conn, deleted_raw_rows)
            case "user_subscription_tracking":
                delete_from_ust(conn, deleted_raw_rows)


print(
    get_today_records(Path("convex/exports/2026-03-18/tables/projects/documents.jsonl"))
)

# with get_connection() as conn:
#     print(get_db_table_data(conn, "projects"))
