from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "crypto-stream"

URL = 'https://api.binance.com/api/v3/ticker/price?symbols=["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT"]'

while True:
    try:
        response = requests.get(URL)
        data_list = response.json()

        for item in data_list:
            message = {
                "timestamp": int(time.time()),
                "symbol": item["symbol"],
                "price": float(item["price"])
            }

            producer.send(TOPIC, message)
            print("Wysłano:", message)

        producer.flush()
        time.sleep(2)

    except Exception as e:
        print("Błąd:", e)
        time.sleep(2)