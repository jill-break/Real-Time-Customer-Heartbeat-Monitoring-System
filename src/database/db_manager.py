import psycopg2
import os
from psycopg2 import pool
from contextlib import contextmanager
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    """
    Manages a thread-safe PostgreSQL connection pool.
    Follows the Singleton pattern to ensure only one pool exists.
    """
    _instance = None
    
    def __init__(self):
        self.connection_pool = None

    @classmethod
    def get_instance(cls):
        """Returns the singleton instance, initializing it if necessary."""
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._initialize_pool()
        return cls._instance

    def _initialize_pool(self):
        if self.connection_pool:
            return

        try:
            logger.info(f"Initializing connection pool on {settings.DB_HOST}:{settings.DB_PORT}...")
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,  # Adjustable based on your DB hardware
                host=settings.DB_HOST,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASS,
                port=settings.DB_PORT
            )
            logger.info("PostgreSQL connection pool established.")
            self._ensure_schema()
        except Exception as e:
            logger.critical(f"Failed to create database connection pool: {e}")
            raise

    def _ensure_schema(self):
        """Creates the necessary tables and indexes if they don't exist."""
        schema_path = os.path.join(os.path.dirname(__file__), 'db_schema.sql')
        
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(schema_sql)
                    conn.commit()
                    logger.info("Database schema verified/created.")
        except FileNotFoundError:
            logger.error(f"Schema file not found at {schema_path}")
            raise
        except Exception as e:
            logger.error(f"Error executing schema script: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Context manager to borrow a connection from the pool and return it.
        Ensures connections are returned even if an error occurs.
        """
        conn = self.connection_pool.getconn()
        try:
            yield conn
        finally:
            self.connection_pool.putconn(conn)

    def close_all(self):
        """Closes all connections in the pool (call on app shutdown)."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed.")

# Initialize the singleton instance for use across the app
# db_manager = DatabaseManager()  <-- Removed global auto-init