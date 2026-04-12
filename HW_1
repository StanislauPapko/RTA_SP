from kafka import KafkaConsumer
import json
from datetime import datetime
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)

for message in consumer:
    transaction = message.value
    user_id = transaction['user_id']
    event_time = datetime.fromisoformat(transaction['timestamp']).timestamp()
    
    user_history[user_id].append(event_time)
    user_history[user_id] = [t for t in user_history[user_id] if event_time - t <= 60]
    count = len(user_history[user_id])
    
    if count > 3:
        print(f"!!! ALERT: WYKRYTO ANOMALIĘ !!!")
        print(f"Użytkownik: {user_id} | Liczba transakcji: {count} w ciągu 60 s")
        print("-" * 50)
