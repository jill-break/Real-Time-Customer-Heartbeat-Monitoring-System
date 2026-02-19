from faker import Faker
import random
import uuid
from datetime import datetime

class HeartbeatGenerator:
    """
    Generates realistic heartbeat data for a set of customers.
    """
    def __init__(self, num_customers=15): # 15 customers per sec and 900 per min
        self.fake = Faker()
        self.customers = [str(uuid.uuid4()) for _ in range(num_customers)]

    def generate_heartbeat(self, customer_id: str) -> dict:
        """
        Creates a realistic heartbeat reading.
        Simulates: Normal (60-100), Elevated (101-140), and Critical (141+).
        """
        chance = random.random()
        if chance > 0.95:  # 5% chance of critical spike
            hr = random.randint(141, 190)
        elif chance > 0.80: # 15% chance of elevated
            hr = random.randint(101, 140)
        else: # 80% chance of normal
            hr = random.randint(60, 100)

        return {
            "customer_id": customer_id,
            "heart_rate": hr,
            "timestamp": datetime.now().isoformat()
        }
