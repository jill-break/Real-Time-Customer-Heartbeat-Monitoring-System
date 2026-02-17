import os
from decouple import config, UndefinedValueError
from src.utils.logger import get_logger

logger = get_logger(__name__)

try:
    # Kafka Configurations
    KAFKA_BROKER = config('KAFKA_BROKER', default='localhost:9092')
    KAFKA_TOPIC = config('KAFKA_TOPIC', default='customer_heartbeats')
    
    KAFKA_CONFIG = {
        'broker': KAFKA_BROKER,
        'topic': KAFKA_TOPIC
    }

    # Database Configurations
    DB_CONFIG = {
        'host': config('DB_HOST', default='localhost'),
        'database': config('DB_NAME', default='monitoring_db'),
        'user': config('DB_USER'),
        'password': config('DB_PASS'),
        'port': config('DB_PORT', default=5432, cast=int)
    }

    # Simulation Settings
    SIMULATION_INTERVAL = config('SIMULATION_INTERVAL', default=1.0, cast=float)
    
    logger.info("Configuration loaded successfully from environment.")

except UndefinedValueError as e:
    logger.error(f"Missing required environment variable: {e}")
    # In production, we want the app to crash if config is missing
    raise SystemExit(1)
except Exception as e:
    logger.error(f"Unexpected error loading configuration: {e}")
    raise SystemExit(1)