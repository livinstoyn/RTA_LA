from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {'count': 0, 'total': 0.0, 'min': float('inf'), 'max': 0.0})
msg_count = 0

print("Statystyki per kategoria...")

for message in consumer:
    tx = message.value
    cat = tx['category']
    amount = tx['amount']

    stats[cat]['count'] += 1
    stats[cat]['total'] += amount
    stats[cat]['min'] = min(stats[cat]['min'], amount)
    stats[cat]['max'] = max(stats[cat]['max'], amount)
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n--- Po {msg_count} wiadomościach ---")
        print(f"{'Kategoria':<12} | {'Liczba':>6} | {'Suma':>10} | {'Min':>8} | {'Max':>8}")
        print("-" * 55)
        for cat, s in stats.items():
            print(f"{cat:<12} | {s['count']:>6} | {s['total']:>10.2f} | {s['min']:>8.2f} | {s['max']:>8.2f}")