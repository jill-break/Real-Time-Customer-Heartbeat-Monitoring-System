from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.config import settings
from src.database.db_manager import DatabaseManager
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
        
        import time
        self.last_batch_time = time.time()
        
        import time
        self.last_batch_time = time.time()
        
        import time
        self.last_batch_time = time.time()

    def _upsert_to_postgres(self, df, batch_id):
        """
        Function called for every micro-batch. 
        Uses the DatabaseManager connection pool to insert data.
        """
        records = df.collect()
        if not records:
            return

        import time
        self.last_batch_time = time.time()

        logger.info(f"Processing batch {batch_id} with {len(records)} records.")
        
        query = "INSERT INTO heartbeats (customer_id, heart_rate, event_time, risk_level) VALUES (%s, %s, %s, %s)"
        
        try:
            with DatabaseManager.get_instance().get_connection() as conn:
                with conn.cursor() as cur:
                    for row in records:
                        # Basic Processing: Filter anomalies before DB insertion (handled by Spark now, but double check doesn't hurt)
                        cur.execute(query, (row['customer_id'], row['heart_rate'], row['event_time'], row['risk_level']))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to Postgres: {e}")

    def _upsert_aggregates_to_postgres(self, df, batch_id):
        """
        Writes aggregated metrics to heartbeat_aggregates table.
        """
        records = df.collect()
        if not records:
            return

        logger.info(f"Writing {len(records)} aggregate records for batch {batch_id}.")
        
        query = """
            INSERT INTO heartbeat_aggregates (window_start, window_end, customer_id, avg_heart_rate, max_heart_rate)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (window_start, customer_id) 
            DO UPDATE SET 
                avg_heart_rate = EXCLUDED.avg_heart_rate,
                max_heart_rate = EXCLUDED.max_heart_rate;
        """
        
        try:
            with DatabaseManager.get_instance().get_connection() as conn:
                with conn.cursor() as cur:
                    for row in records:
                        cur.execute(query, (
                            row['window_start'], 
                            row['window_end'], 
                            row['customer_id'], 
                            row['avg_heart_rate'], 
                            row['max_heart_rate']
                        ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error writing aggregates batch {batch_id}: {e}")

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
            
            # Enhanced Processing:
            # 1. Type Casting: Convert string timestamp to TimestampType
            # 2. Filtering: Remove invalid heart rates (e.g., < 40)
            # 3. Enrichment: Add risk_level based on heart_rate
            processed_df = json_df \
                .withColumn("event_time", col("timestamp").cast(TimestampType())) \
                .filter((col("heart_rate") >= 30) & (col("heart_rate") <= 220)) \
                .withColumn("risk_level", 
                    when(col("heart_rate") > 140, "High")
                    .when((col("heart_rate") >= 100) & (col("heart_rate") <= 140), "Elevated")
                    .otherwise("Normal")
                )

            # 5. Sink: Write to Postgres using foreachBatch
            # Branch 1: Raw Data
            query_raw = processed_df.writeStream \
                .foreachBatch(self._upsert_to_postgres) \
                .option("checkpointLocation", "checkpoints/heartbeats_raw") \
                .trigger(processingTime="5 seconds") \
                .start()

            # Branch 2: Windowed Aggregates (1 minute window, sliding every 30 seconds)
            from pyspark.sql.functions import window, avg, max
            
            aggregates_df = processed_df \
                .withWatermark("event_time", "2 minutes") \
                .groupBy(
                    window("event_time", "1 minute", "30 seconds"),
                    "customer_id"
                ) \
                .agg(
                    avg("heart_rate").alias("avg_heart_rate"),
                    max("heart_rate").alias("max_heart_rate")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("customer_id"),
                    col("avg_heart_rate"),
                    col("max_heart_rate")
                )

            query_agg = aggregates_df.writeStream \
                .foreachBatch(self._upsert_aggregates_to_postgres) \
                .outputMode("update") \
                .option("checkpointLocation", "checkpoints/heartbeats_agg") \
                .trigger(processingTime="1 minute") \
                .start()

            self.spark.streams.awaitAnyTermination()

        except Exception as e:
            logger.critical(f"Spark Streaming Job failed: {e}")
        finally:
            self.spark.stop()
            logger.info("Spark Session stopped.")
            
    def _monitor_idle_state(self):
        """
        Background thread to check if data has stopped flowing.
        """
        import time
        from src.utils.alerter import Alerter
        
        logger.info(f"Idle monitor started (Threshold: {settings.DATA_IDLE_THRESHOLD_SECONDS}s)")
        
        while True:
            time.sleep(10)
            elapsed = time.time() - self.last_batch_time
            if elapsed > settings.DATA_IDLE_THRESHOLD_SECONDS:
                Alerter.send_alert(f"No data received for {int(elapsed)} seconds!")

if __name__ == "__main__":
    import threading
    monitor_thread = threading.Thread(target=processor._monitor_idle_state, daemon=True)
    monitor_thread.start()
    
    processor.run()