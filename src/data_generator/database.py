from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection

from config import DB_CONFIG

@contextmanager
def get_connection() -> Generator[connection, None, None]:
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()