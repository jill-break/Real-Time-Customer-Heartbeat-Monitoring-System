import psycopg2
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

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance

    def _initialize_pool(self):
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
        create_table_query = """
        CREATE TABLE IF NOT EXISTS heartbeats (
            id SERIAL PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            heart_rate INTEGER NOT NULL,
            event_time TIMESTAMP NOT NULL,
            risk_level VARCHAR(20),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_customer_time ON heartbeats (customer_id, event_time DESC);
        """
        alter_table_query = """
        DO $$ 
        BEGIN 
            BEGIN
                ALTER TABLE heartbeats ADD COLUMN risk_level VARCHAR(20);
            EXCEPTION
                WHEN duplicate_column THEN RAISE NOTICE 'column risk_level already exists in heartbeats.';
            END;
        END $$;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                cur.execute(alter_table_query)
                conn.commit()
                logger.info("Database schema verified/created.")

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
db_manager = DatabaseManager()