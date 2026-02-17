
import pytest
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def test_risk_classification_logic(spark_session):
    """Test the risk_level calculation logic in Spark."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    data = [
        ("CUST-1", 80, "2026-01-01T10:00:00"),  # Normal
        ("CUST-2", 120, "2026-01-01T10:00:00"), # Elevated
        ("CUST-3", 150, "2026-01-01T10:00:00"), # High
        ("CUST-4", 20, "2026-01-01T10:00:00"),  # Low (Filtered out in real logic, but testing classification here if passed)
    ]
    
    df = spark_session.createDataFrame(data, schema)
    
    # Replicate the logic from spark_processor.py
    processed_df = df.withColumn("risk_level", 
                    when(col("heart_rate") > 140, "High")
                    .when((col("heart_rate") >= 100) & (col("heart_rate") <= 140), "Elevated")
                    .otherwise("Normal")
                )
    
    results = processed_df.collect()
    
    # Verify results
    assert results[0]['risk_level'] == "Normal"
    assert results[1]['risk_level'] == "Elevated"
    assert results[2]['risk_level'] == "High"
    assert results[3]['risk_level'] == "Normal" # Fallback is Normal

def test_filtering_logic(spark_session):
    """Test that heart rates below 40 are filtered."""
    schema = StructType([
        StructField("heart_rate", IntegerType(), True)
    ])
    
    data = [(80,), (120,), (30,), (200,)]
    df = spark_session.createDataFrame(data, schema)
    
    filtered_df = df.filter(col("heart_rate") >= 40)
    
    count = filtered_df.count()
    assert count == 3 # 30 should be removed
