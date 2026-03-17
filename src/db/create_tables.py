def create_raw_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;            
        """)


def create_raw_projects_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_projects (
                source TEXT,
                loaded_at TIMESTAMPTZ,
                updated_at TEXT,
                record_id TEXT,
                payload JSONB,
                PRIMARY KEY (source, record_id)
            );
        """)


def create_raw_signups_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_signups (
                source TEXT,
                loaded_at TIMESTAMPTZ,
                updated_at TEXT,
                record_id TEXT,
                payload JSONB,
                PRIMARY KEY (source, record_id)
            );
        """)


def create_raw_ust_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_user_subscription_tracking (
                source TEXT,
                loaded_at TIMESTAMPTZ,
                updated_at TEXT,
                record_id TEXT,
                payload JSONB,
                PRIMARY KEY (source, record_id)
            );
        """)
