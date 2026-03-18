from src.db.conn import get_connection
from src.db.select import (
    fetch_all_from_signups,
    fetch_all_from_projects,
    fetch_all_from_ust,
)
from src.services.convex_load_service import (
    map_dict_into_tuple,
    parse_jsonl_records,
    build_raw_row,
    get_table_dirs,
)
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


print(
    get_today_records(Path("convex/exports/2026-03-18/tables/projects/documents.jsonl"))
)

# with get_connection() as conn:
#     print(get_db_table_data(conn, "projects"))
