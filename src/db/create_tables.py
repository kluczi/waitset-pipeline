def create_raw_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;            
        """)


def create_raw_projects_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_convex_projects (
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


def create_raw_signups_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_convex_signups (
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


def create_raw_ust_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_convex_user_subscription_tracking (
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


def create_raw_project_context_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_convex_project_context (
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


def create_raw_pages_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_convex_pages (
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


def create_raw_users_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_convex_users (
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


def create_raw_events_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_posthog_events(
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


def create_raw_persons_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_posthog_persons(
                record_id TEXT,
                loaded_at TIMESTAMPTZ,
                payload_hash TEXT,
                payload JSONB,
                PRIMARY KEY(record_id, loaded_at)
            );
        """)


"""
raw.raw_projects(record_id, loaded_at, payload_hash, payload)
        VALUES (%s, %s, %s, %s)
"""
