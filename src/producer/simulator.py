import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from src.config import KAFKA_CONFIG
from src.utils.logger import get_logger

logger = get_logger(__name__)

class HeartbeatProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['broker'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )

    def start_simulation(self):
        logger.info("Starting heartbeat simulation...")
        customers = [f"USER_{i:03d}" for i in range(1, 21)]
        
        try:
            while True:
                payload = {
                    "customer_id": random.choice(customers),
                    "heart_rate": random.randint(55, 170),
                    "timestamp": datetime.now().isoformat()
                }
                self.producer.send(KAFKA_CONFIG['topic'], payload)
                logger.info(f"Published: {payload['customer_id']} - {payload['heart_rate']} BPM")
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user.")
        except Exception as e:
            logger.error(f"Producer encountered an error: {e}")