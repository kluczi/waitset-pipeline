import psycopg
from src.db.create_tables import (
    create_raw_projects_table,
    create_raw_signups_table,
    create_raw_ust_table,
    create_raw_users_table,
    create_raw_events_table,
    create_raw_persons_table,
    create_raw_pages_table,
    create_raw_project_context_table,
    create_raw_waitlists_table,
)


def insert_to_projects(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_projects_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_convex_projects(record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_signups(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_signups_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_convex_signups (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_ust(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_ust_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_convex_user_subscription_tracking (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_project_context(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_project_context_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_convex_project_context(record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_pages(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_pages_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_convex_pages(record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_users(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_users_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_convex_users (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_events(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_events_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_posthog_events (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_persons(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_persons_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_posthog_persons (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )


def insert_to_waitlists(conn: psycopg, values: list):
    with conn.cursor() as cur:
        create_raw_waitlists_table(conn)
        cur.executemany(
            """
        INSERT INTO raw.raw_convex_waitlists (record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
        """,
            values,
        )
