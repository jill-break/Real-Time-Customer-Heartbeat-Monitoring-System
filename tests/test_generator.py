
import pytest
from src.producer.generator import HeartbeatGenerator

def test_initialization():
    """Test that generator initializes with default customers."""
    generator = HeartbeatGenerator(num_customers=5)
    assert len(generator.customers) == 5
    assert all(c.startswith("CUST-") for c in generator.customers)

def test_heart_rate_ranges():
    """Test that heart rates fall within realistic bounds."""
    generator = HeartbeatGenerator()
    customer_id = generator.customers[0]
    
    # Generate multiple samples to cover different ranges
    for _ in range(50):
        data = generator.generate_heartbeat(customer_id)
        assert 60 <= data['heart_rate'] <= 190

def test_data_structure():
    """Test output dictionary keys."""
    generator = HeartbeatGenerator()
    data = generator.generate_heartbeat("CUST-TEST")
    assert "customer_id" in data
    assert "heart_rate" in data
    assert "timestamp" in data
    assert isinstance(data['heart_rate'], int)
    assert isinstance(data['timestamp'], str)
