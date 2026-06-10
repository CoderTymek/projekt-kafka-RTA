from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "crypto-stream"

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT"
]

while True:
    try:
        for symbol in SYMBOLS:

            response = requests.get(
                f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            )

            data = response.json()

            message = {
                "timestamp": int(time.time()),
                "symbol": data["symbol"],
                "price": float(data["price"])
            }

            producer.send(TOPIC, message)

            print("Wysłano:", message)

        producer.flush()

        time.sleep(2)

    except Exception as e:
        print("Błąd:", e)
        time.sleep(2)