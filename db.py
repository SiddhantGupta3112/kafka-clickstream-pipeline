import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
import logging

load_dotenv()

_db_pool = None

def get_pool():
    """Helper function to lazily initialize the pool only when needed."""
    global _db_pool
    if _db_pool is None:
        USER = os.getenv("POSTGRES_USER")
        PASSWORD = os.getenv("POSTGRES_PASSWORD")
        DB = os.getenv("POSTGRES_DB")
        db_host = os.getenv("POSTGRES_HOST")
        port = "5432"
        DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{db_host}:{port}/{DB}"
       
        _db_pool = SimpleConnectionPool(1, 3, dsn=DATABASE_URL)
    return _db_pool

@contextmanager
def get_db():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)