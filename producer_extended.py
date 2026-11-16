import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--topic", default="campus-sensors")
parser.add_argument("--events", type=int, default=100)
parser.add_argument("--delay", type=float, default=0.2)
parser.add_argument("--keyed", action="store_true",
                    help="use sensor_id as the message key")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
)

per_partition = {}
try:
    for event_id in range(1, args.events + 1):
        event = {
            "event_id": event_id,
            "sensor_id": f"sensor-{random.randint(1, 4):02d}",
            "campus_building": random.choice(["A", "B", "C"]),
            "temperature_c": round(random.uniform(18, 42), 2),
            "humidity_pct": round(random.uniform(25, 85), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        key = event["sensor_id"] if args.keyed else None
        meta = producer.send(args.topic, key=key, value=event).get(timeout=10)
        per_partition[meta.partition] = per_partition.get(meta.partition, 0) + 1
        print(f"Sent -> partition={meta.partition} offset={meta.offset} | {event}")
        time.sleep(args.delay)
    producer.flush()
    print("\nEvents per partition:", dict(sorted(per_partition.items())))
finally:
    producer.close()