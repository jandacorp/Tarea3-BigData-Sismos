#!/usr/bin/env python3
import json
import random
import time
from kafka import KafkaProducer

TOPIC = "sensor_data"

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print(f"Enviando datos simulados al topic {TOPIC}. Ctrl+C para finalizar.")

try:
    while True:
        sensor_data = generate_sensor_data()
        producer.send(TOPIC, value=sensor_data)
        producer.flush()
        print(f"Sent: {sensor_data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nProductor finalizado.")
finally:
    producer.close()
