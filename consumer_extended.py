import argparse
import csv
import json
from collections import defaultdict
from kafka import KafkaConsumer

FIELDS = ["event_id", "sensor_id", "temperature_c", "humidity_pct", "timestamp"]

parser = argparse.ArgumentParser()
parser.add_argument("--topic", default="campus-sensors")
parser.add_argument("--group", default="tp05-extended")
parser.add_argument("--building", default=None, help="filter: A, B or C")
parser.add_argument("--csv", default=None, help="save events to this CSV file")
parser.add_argument("--threshold", type=float, default=35)
args = parser.parse_args()

consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=args.group,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

csv_file, writer = None, None
if args.csv:
    csv_file = open(args.csv, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()

total, count, alerts = 0.0, 0, 0
per_sensor = defaultdict(lambda: [0.0, 0])
per_partition = defaultdict(int)

print(f"Listening: topic={args.topic} group={args.group} building={args.building}")
try:
    for message in consumer:
        event = message.value
        if args.building and event.get("campus_building") != args.building:
            continue
        t = event["temperature_c"]
        total += t
        count += 1
        per_sensor[event["sensor_id"]][0] += t
        per_sensor[event["sensor_id"]][1] += 1
        per_partition[message.partition] += 1
        print(f'p={message.partition} off={message.offset} | {event["timestamp"]} | '
              f'{event["sensor_id"]} | bld={event.get("campus_building", "-")} | '
              f'T={t}°C | RH={event["humidity_pct"]}% | avg={total / count:.2f}°C')
        if t >= args.threshold:
            alerts += 1
            print("ALERT: High temperature detected!")
        if writer:
            writer.writerow(event)
            csv_file.flush()
except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
    if count:
        print(f"Events: {count} | Alerts: {alerts} | Global avg: {total / count:.2f}°C")
        for s, (sm, c) in sorted(per_sensor.items()):
            print(f"  {s}: avg={sm / c:.2f}°C over {c} events")
        print("Events per partition handled by this consumer:", dict(sorted(per_partition.items())))
finally:
    if csv_file:
        csv_file.close()
    consumer.close()