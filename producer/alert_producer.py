import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "incident_alerts" 

systems = ["HPE-NONSTOP", "MAINFRAME", "LINUX-OPS"]
alerts = ["PROCESS_DUMP", "CPU_SPIKE", "DISK_FULL"]
components = ["$ZPMON", "$DBMON", "$APPMON"]

print("Starting alert producer...")

while True:
    alert = {
        "incident_id": f"INC{random.randint(1000,9999)}",
        "system": random.choice(systems),
        "component": random.choice(components),
        "alert_type": random.choice(alerts),
        "severity": random.choice(["LOW", "MEDIUM", "HIGH"]),
        "message": "Internal monitoring alert",
        "timestamp": datetime.utcnow().isoformat()
    }
     
    producer.send(topic, value=alert)
    print("Sent alert:", alert)
    
    time.sleep(3)  # wait 3 seconds before sending next alert
