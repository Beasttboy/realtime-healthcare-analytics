import json
import random
import uuid
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "bootstrap-server"
EVENT_HUB_NAME = "event-hub-name"
EVENT_HUB_CONNECTION_STRING = '<connection-string>'

TOPIC_NAME = EVENT_HUB_NAME

producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    "sasl.username": "$ConnectionString",
    "sasl.password": EVENT_HUB_CONNECTION_STRING,
})

# Departments in hospital
departments = ["Emergency", "Surgery", "ICU", "Pediatrics", "Maternity", "Oncology", "Cardiology"]

# Gender categories 
genders = ["Male", "Female", "Other"]

# Helper function to introduce dirty data (Enhanced)
def inject_dirty_data(record):
    # 5% chance invalid age (101-150)
    if random.random() < 0.05:
        record["age"] = random.randint(101, 150)
    
    # 5% chance future admission timestamp
    if random.random() < 0.05:
        record["admission_time"] = (datetime.utcnow() + timedelta(hours=random.randint(1, 72))).isoformat()
    
    # 3% chance discharge_time < admission_time (logical error)
    if random.random() < 0.03:
        record["discharge_time"] = (datetime.utcnow() - timedelta(hours=random.randint(1, 24))).isoformat()
    
    # 2% chance missing critical field
    if random.random() < 0.02:
        record.pop("department", None)
    
    return record

def generate_patient_event():
    admission_time = datetime.utcnow() - timedelta(hours=random.randint(0, 72))
    discharge_time = admission_time + timedelta(hours=random.randint(1, 72))

    event = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(genders),
        "age": random.randint(1, 100),
        "department": random.choice(departments),
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7),
        "event_timestamp": datetime.utcnow().isoformat(),  # Added for CDC
        "event_type": random.choice(["admission", "discharge", "transfer"])  # Added variety
    }

    return inject_dirty_data(event)

def delivery_report(err, msg):
    """Delivery callback for Kafka producer"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [partition {msg.partition()}]')

if __name__ == "__main__":
    print("Starting Hospital Patient Event Generator...")
    print(f"Producing to topic: {TOPIC_NAME}")
    
    try:
        while True:
            event = generate_patient_event()
            
            # Async produce with callback
            producer.produce(
                TOPIC_NAME, 
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report
            )
            
            # Trigger delivery callbacks
            producer.poll(0)
            
            print(f"Sent: patient_id={event['patient_id'][:8]}..., dept={event.get('department', 'MISSING')}")
            
            # Variable delay for realistic traffic (0.5-3s)
            time.sleep(random.uniform(0.5, 3.0))
            
    except KeyboardInterrupt:
        print("\n Stopping producer...")
        producer.flush(10)  # Wait max 10s for delivery
        print("All messages delivered!")
