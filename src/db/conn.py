import psycopg
import os
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> psycopg:
    return psycopg.connect(os.getenv("POSTGRES_DSN"))
