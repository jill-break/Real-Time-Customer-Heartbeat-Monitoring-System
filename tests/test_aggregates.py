import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def test_windowed_aggregation(spark_session):
    """Test the windowed aggregation logic."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # 3 data points within 1 minute for CUST-1
    data = [
        ("CUST-1", 60, "2026-01-01T10:00:00"),
        ("CUST-1", 80, "2026-01-01T10:00:30"),
        ("CUST-1", 100, "2026-01-01T10:00:50"),
        ("CUST-2", 120, "2026-01-01T10:00:00"), # Separate customer
    ]
    
    df = spark_session.createDataFrame(data, schema)
    
    # Apply logic from spark_processor.py
    processed_df = df.withColumn("event_time", to_timestamp(col("timestamp")))
    
    aggregates_df = processed_df \
        .groupBy(
            window("event_time", "1 minute"),
            "customer_id"
        ) \
        .agg(
            avg("heart_rate").alias("avg_heart_rate"),
            max("heart_rate").alias("max_heart_rate")
        )
    
    results = aggregates_df.collect()
    
    # Verify CUST-1
    cust1_res = next(r for r in results if r['customer_id'] == "CUST-1")
    assert cust1_res['avg_heart_rate'] == 80.0 # (60+80+100)/3
    assert cust1_res['max_heart_rate'] == 100
    
    # Verify CUST-2
    cust2_res = next(r for r in results if r['customer_id'] == "CUST-2")
    assert cust2_res['avg_heart_rate'] == 120.0
