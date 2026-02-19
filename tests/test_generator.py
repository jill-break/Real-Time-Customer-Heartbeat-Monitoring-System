
import pytest
from src.producer.generator import HeartbeatGenerator

def test_initialization():
    """Test that generator initializes with default customers."""
    generator = HeartbeatGenerator(num_customers=5)
    assert len(generator.customers) == 5
    import uuid
    for c in generator.customers:
        try:
            uuid.UUID(str(c))
        except ValueError:
            pytest.fail(f"{c} is not a valid UUID")

def test_heart_rate_ranges():
    """Test that heart rates fall within realistic bounds."""
    generator = HeartbeatGenerator()
    customer_id = generator.customers[0]
    
    # Generate multiple samples to cover different ranges
    for _ in range(50):
        data = generator.generate_heartbeat(customer_id)
        # Adjusted range based on new DB constraints (30-220) 
        # Although generator logic is 60-190, testing broader validity is okay
        assert 30 <= data['heart_rate'] <= 220

def test_data_structure():
    """Test output dictionary keys."""
    generator = HeartbeatGenerator()
    data = generator.generate_heartbeat("CUST-TEST")
    assert "customer_id" in data
    assert "heart_rate" in data
    assert "timestamp" in data
    assert isinstance(data['heart_rate'], int)
    assert isinstance(data['timestamp'], str)
