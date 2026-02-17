import sys
import argparse
from src.utils.logger import get_logger
from src.database.db_manager import db_manager
from src.producer.simulator import HeartbeatSimulator
from src.consumer.spark_processor import SparkHeartbeatProcessor


# Set up Hadoop environment for Windows
import os
hadoop_home = os.path.abspath(os.path.join(os.getcwd(), 'hadoop'))
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['hadoop.home.dir'] = hadoop_home
os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')

logger = get_logger(__name__)

def run_producer():
    """Starts the Kafka Heartbeat Simulator."""
    try:
        simulator = HeartbeatSimulator()
        simulator.run()
    except Exception as e:
        logger.error(f"Producer failed: {e}")

def run_consumer():
    """Starts the Spark Structured Streaming Processor."""
    try:
        processor = SparkHeartbeatProcessor()
        processor.run()
    except Exception as e:
        logger.error(f"Consumer failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="Real-Time Heartbeat Monitoring System")
    parser.add_argument(
        "mode", 
        choices=["producer", "consumer"], 
        help="Run the system in either 'producer' or 'consumer' mode."
    )
    
    args = parser.parse_args()

    try:
        if args.mode == "producer":
            run_producer()
        elif args.mode == "consumer":
            run_consumer()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        # Crucial for production: Cleanup resources
        db_manager.close_all()
        logger.info("System resources cleaned up. Goodbye!")

if __name__ == "__main__":
    main()