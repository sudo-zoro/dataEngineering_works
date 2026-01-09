from kafka import KafkaConsumer
import json
import snowflake.connector
from snowflake_config import SNOWFLAKE_CONFIG

consumer = KafkaConsumer(
    'events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cur = conn.cursor()

insert_sql = """
INSERT INTO realtime_db.kafka.events (event_id, user_id, event_type, amount, event_time)
VALUES (%s, %s, %s, %s, %s)
"""

for message in consumer:
    data = message.value

    # Basic validation
    if not all(k in data for k in ["event_id", "user_id", "event_type", "amount", "event_time"]):
        print("Invalid event skipped:", data)
        continue

    cur.execute(insert_sql, (
        data["event_id"],
        data["user_id"],
        data["event_type"],
        data["amount"],
        data["event_time"]
    ))

    print("Inserted into Snowflake:", data)
