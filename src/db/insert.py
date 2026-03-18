import psycopg
from src.db.create_tables import (
    create_raw_projects_table,
    create_raw_signups_table,
    create_raw_ust_table,
)


def insert_to_projects(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_projects_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_projects(record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_signups(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_signups_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_signups (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_ust(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_ust_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_user_subscription_tracking (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )
