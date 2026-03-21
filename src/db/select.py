import psycopg


def fetch_all_from_projects(conn: psycopg) -> list:
    results = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM raw.raw_convex_projects;
            """
        )
        results = cur.fetchall()
    return results


def fetch_all_from_ust(conn: psycopg):
    results = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM raw.raw_convex_user_subscription_tracking;
            """
        )
        results = cur.fetchall()
    return results


def fetch_all_from_signups(conn: psycopg):
    results = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM raw.raw_convex_signups;
            """
        )
        results = cur.fetchall()
    return results
