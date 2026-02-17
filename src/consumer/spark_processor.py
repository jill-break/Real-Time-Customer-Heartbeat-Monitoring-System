from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.config import settings
from src.database.db_manager import db_manager
from src.utils.logger import get_logger

logger = get_logger(__name__)

class SparkHeartbeatProcessor:
    def __init__(self):
        # 1. Initialize Spark Session with required Kafka & Postgres Jars
        self.spark = SparkSession.builder \
            .appName("HeartbeatRealTimeProcessor") \
            .config("spark.jars.packages", f"{settings.KAFKA_JAR_PACKAGE},{settings.POSTGRES_JAR_PACKAGE}") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # 2. Define Schema for incoming JSON
        self.schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("heart_rate", IntegerType(), True),
            StructField("timestamp", StringType(), True)
        ])

    def _upsert_to_postgres(self, df, batch_id):
        """
        Function called for every micro-batch. 
        Uses the DatabaseManager connection pool to insert data.
        """
        records = df.collect()
        if not records:
            return

        logger.info(f"Processing batch {batch_id} with {len(records)} records.")
        
        query = "INSERT INTO heartbeats (customer_id, heart_rate, event_time) VALUES (%s, %s, %s)"
        
        try:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    for row in records:
                        # Basic Processing: Filter anomalies before DB insertion
                        if 40 <= row['heart_rate'] <= 220:
                            cur.execute(query, (row['customer_id'], row['heart_rate'], row['timestamp']))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to Postgres: {e}")

    def run(self):
        logger.info("Starting Spark Structured Streaming Consumer...")

        try:
            # 3. Read from Kafka
            raw_df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", settings.KAFKA_BROKER) \
                .option("subscribe", settings.KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .load()

            # 4. Transform: Binary to JSON String to Structured Columns
            json_df = raw_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), self.schema).alias("data")) \
                .select("data.*")

            # 5. Sink: Write to Postgres using foreachBatch
            query = json_df.writeStream \
                .foreachBatch(self._upsert_to_postgres) \
                .option("checkpointLocation", "checkpoints/heartbeats") \
                .start()

            query.awaitTermination()

        except Exception as e:
            logger.critical(f"Spark Streaming Job failed: {e}")
        finally:
            self.spark.stop()
            logger.info("Spark Session stopped.")

if __name__ == "__main__":
    processor = SparkHeartbeatProcessor()
    processor.run()