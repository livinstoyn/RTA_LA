from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transaction_times = defaultdict(list)

WINDOW_SECONDS = 60
MAX_TRANSACTIONS = 3

print("Monitor anomalii uruchomiony...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    tx_id = tx['tx_id']
    amount = tx['amount']
    store = tx['store']
    now = time.time()

    user_transaction_times[user_id].append(now)

    user_transaction_times[user_id] = [
        t for t in user_transaction_times[user_id]
        if now - t <= WINDOW_SECONDS
    ]

    count = len(user_transaction_times[user_id])

    if count > MAX_TRANSACTIONS:
        print(f" ALERT! user_id={user_id} | {count} transakcji w 60 sek | {tx_id} | {amount:.2f} PLN | {store}")
    else:
        print(f"  OK: {user_id} | {tx_id} | liczba transakcji w oknie={count}")