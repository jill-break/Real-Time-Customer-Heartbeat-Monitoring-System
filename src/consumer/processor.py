from kafka import KafkaConsumer
import json
from src.config import KAFKA_CONFIG
from src.database.db_manager import DatabaseManager
from src.utils.logger import get_logger

logger = get_logger(__name__)

class HeartbeatConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['broker'],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.db = DatabaseManager.get_instance()

    def run(self):
        logger.info("Consumer service started. Listening for messages...")
        query = "INSERT INTO heartbeats (customer_id, heart_rate, event_time) VALUES (%s, %s, %s)"
        
        for msg in self.consumer:
            data = msg.value
            try:
                # Validation Logic
                if 30 <= data['heart_rate'] <= 220:
                    self.db.execute_query(query, (data['customer_id'], data['heart_rate'], data['timestamp']))
                    logger.info(f"Persisted data for {data['customer_id']}")
                else:
                    logger.warning(f"Filtered invalid heart rate: {data['heart_rate']}")
            except Exception as e:
                logger.error(f"Processing error: {e}")