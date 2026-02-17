import psycopg2
from psycopg2 import pool
from src.config import DB_CONFIG
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    _instance = None

    def __init__(self):
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **DB_CONFIG)
            logger.info("PostgreSQL connection pool created successfully.")
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            cls._instance = DatabaseManager()
        return cls._instance

    def execute_query(self, query, params):
        conn = self.connection_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            conn.rollback()
        finally:
            self.connection_pool.putconn(conn)