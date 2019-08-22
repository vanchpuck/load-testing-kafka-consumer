from kafka import KafkaConsumer
from datetime import datetime, timezone

key_counts = {}

consumer = KafkaConsumer(
    'test1',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    key_deserializer=lambda m: m.decode('utf-8'),
)

for message in consumer:
    key = message.key

    expected_counter_value = 0 if key not in key_counts else key_counts[key]

    actual_counter_value = int.from_bytes(message.value[0:8], byteorder='big')

    produce_epoch = int.from_bytes(message.value[8:16], byteorder='big')
    consume_epoch = int(datetime.now(timezone.utc).timestamp() * 1000)
    delay = consume_epoch - produce_epoch
    print(delay)

    if actual_counter_value > expected_counter_value:
        print('Missing')
    elif actual_counter_value < expected_counter_value:
        print('Duplicate')

    key_counts[key] = actual_counter_value + 1