from src.clients.posthog_client import PosthogClient
from src.services.convex_load_service import (
    compute_payload_hash,
    map_dict_into_tuple,
)
from datetime import datetime
from zoneinfo import ZoneInfo
from src.db.conn import get_connection

# from src.db.create_tables import create_raw_events_table, create_raw_persons_table
from src.db.insert import insert_to_events, insert_to_persons
import psycopg


def build_raw_row(record: dict):
    return {
        "record_id": record.get("id"),
        "loaded_at": datetime.now(ZoneInfo("Europe/Warsaw")).isoformat(),
        "payload_hash": compute_payload_hash(record),
        "payload": record,
    }


def insert_events_into_db(conn: psycopg, batch: list):
    values = map_dict_into_tuple(batch)
    insert_to_events(conn, values)


def insert_persons_into_db(conn: psycopg, batch: list):
    values = map_dict_into_tuple(batch)
    insert_to_persons(conn, values)


def load_events():
    client = PosthogClient()
    batch = []
    BATCH_SIZE = 1000
    with get_connection() as conn:
        conn.autocommit = True
        for idx, record in enumerate(client.get_events(), start=1):
            raw_row = build_raw_row(record)
            batch.append(raw_row)
            if idx % BATCH_SIZE == 0:
                insert_events_into_db(conn, batch)
                batch.clear()
        if batch:
            insert_events_into_db(conn, batch)


def load_persons():
    client = PosthogClient()
    batch = []
    BATCH_SIZE = 1000
    with get_connection() as conn:
        conn.autocommit = True
        for idx, record in enumerate(client.get_persons(), start=1):
            raw_row = build_raw_row(record)
            batch.append(raw_row)
            if idx % BATCH_SIZE == 0:
                insert_persons_into_db(conn, batch)
                batch.clear()
        if batch:
            insert_persons_into_db(conn, batch)


if __name__ == "__main__":
    client = PosthogClient()
    load_events(client)
