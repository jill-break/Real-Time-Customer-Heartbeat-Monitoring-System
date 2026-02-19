import json
import time
from kafka import KafkaProducer

from src.config import settings
from src.utils.logger import get_logger
from src.producer.generator import HeartbeatGenerator

# Initialize logger for this module
logger = get_logger(__name__)

class HeartbeatSimulator:
    """
    Simulates real-time customer heartbeat sensors and publishes events to Kafka.
    """
    def __init__(self):
        self.topic = settings.KAFKA_TOPIC
        self.generator = HeartbeatGenerator()
        
        try:
            # Production-grade producer config
            self.producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Acks=all ensures data is replicated to all followers before confirming
                acks='all',
                retries=5,
                # Batching for better throughput
                batch_size=16384,
                linger_ms=10,
                # Fix for kafka-python with newer brokers
                api_version=(2, 5, 0)
            )
            logger.info(f"Kafka Producer initialized for topic: {self.topic}")
        except Exception as e:
            logger.critical(f"Failed to initialize Kafka Producer: {e}")
            raise

    def on_send_success(self, record_metadata):
        """Callback for successful Kafka delivery."""
        pass

    def on_send_error(self, excp):
        """Callback for failed Kafka delivery."""
        logger.error(f"Message delivery failed: {excp}")

    def run(self):
        """Main loop for the simulator."""
        logger.info(f"Starting simulation with {len(self.generator.customers)} customers...")
        try:
            while True:
                for customer_id in self.generator.customers:
                    data = self.generator.generate_heartbeat(customer_id)
                    
                    self.producer.send(
                        self.topic, 
                        value=data
                    ).add_callback(self.on_send_success).add_errback(self.on_send_error)
                    
                    logger.info(f"Published heartbeat for {customer_id}: {data['heart_rate']} BPM")
                
                # Flush the producer to ensure messages are sent
                self.producer.flush()
                time.sleep(settings.SIMULATION_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("Simulator received shutdown signal.")
        except Exception as e:
            logger.error(f"Unexpected error in simulation loop: {e}")
        finally:
            self.producer.close()
            logger.info("Kafka Producer connection closed.")

if __name__ == "__main__":
    # For testing this module in isolation
    simulator = HeartbeatSimulator()
    simulator.run()