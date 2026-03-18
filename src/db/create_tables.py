def create_raw_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;            
        """)


def create_raw_projects_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_projects (
                record_id TEXT PRIMARY KEY,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB
            );
        """)


def create_raw_signups_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_signups (
                record_id TEXT PRIMARY KEY,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB
            );
        """)


def create_raw_ust_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_user_subscription_tracking (
                record_id TEXT PRIMARY KEY,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB
            );
        """)


"""
raw.raw_projects(record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
"""
