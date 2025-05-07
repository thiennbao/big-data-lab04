import requests
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "btc-price"
FETCH_INTERVAL_SECONDS = 0.1

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer connected.")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit()

print(f"Starting data extraction for {KAFKA_TOPIC}...")
while True:
    try:
        response = requests.get(BINANCE_API_URL)
        response.raise_for_status()
        data = response.json()

        if "symbol" in data and "price" in data:
            event_time = datetime.now(timezone.utc).isoformat()
            data["timestamp"] = event_time

            try:
                data["price"] = float(data["price"])
            except ValueError:
                print(f"Warning: Could not convert price '{data['price']}' to float. Skipping record.")
                continue

            producer.send(KAFKA_TOPIC, value=data)
            print(f"Sent: {data}")

        else:
            print(f"Warning: Received unexpected format from Binance: {data}")

        time.sleep(FETCH_INTERVAL_SECONDS)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Binance: {e}")
        time.sleep(5)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        time.sleep(5)

# producer.flush()
# producer.close()
# print("Kafka Producer closed.")
