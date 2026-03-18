import psycopg


def delete_from_projects(conn: psycopg, record_id: list):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM raw.raw_projects WHERE record_id = ANY(%s)
            """,
            (record_id),
        )


def delete_from_signups(conn: psycopg, record_id: list):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM raw.signups WHERE record_id = ANY(%s)
            """,
            (record_id),
        )


def delete_from_ust(conn: psycopg, record_id: list):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM raw.user_subscription_tracking WHERE record_id = ANY(%s)
            """,
            (record_id),
        )
