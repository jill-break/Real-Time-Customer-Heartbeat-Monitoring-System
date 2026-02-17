import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Settings(BaseSettings):
    """
    Validates and loads environment variables.
    Settings are automatically mapped from .env file.
    """
    # Kafka Settings
    KAFKA_BROKER: str = "localhost:9092"
    KAFKA_TOPIC: str = "customer_heartbeats"
    
    # Postgres Settings
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "monitoring_db"
    DB_USER: str
    DB_PASS: str
    
    # Simulation Settings
    SIMULATION_INTERVAL: float = 1.0
    
    # Spark Settings
    # This package is required for Spark to talk to Kafka
    KAFKA_JAR_PACKAGE: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    # Postgres Jar for Spark to talk to Postgres
    POSTGRES_JAR_PACKAGE: str = "org.postgresql:postgresql:42.7.1"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

try:
    # Initialize settings
    settings = Settings()
    logger.info("Production configuration loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load configuration. Ensure .env is set correctly: {e}")
    raise SystemExit(1)