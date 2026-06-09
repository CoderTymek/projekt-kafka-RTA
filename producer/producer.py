from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "crypto_prices"

while True:
    try:
        response = requests.get(
            "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        )

        data = response.json()

        message = {
            "symbol": data["symbol"],
            "price": float(data["price"]),
            "timestamp": int(time.time())
        }

        producer.send(TOPIC, message)
        producer.flush()

        print("Wysłano:", message)

        time.sleep(5)

    except Exception as e:
        print("Błąd:", e)
        time.sleep(5)