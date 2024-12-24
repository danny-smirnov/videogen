# send_test_message.py
import json
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла (если используется)
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DOWNLOAD_TOPIC = "video_download_requests"

def send_test_message(video_id, channel_id):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    message = {"video_id": video_id, "channel_id": channel_id}
    producer.produce(DOWNLOAD_TOPIC, json.dumps(message).encode("utf-8"))
    producer.flush()
    print("Тестовое сообщение отправлено.")

if __name__ == "__main__":
    send_test_message("0XaQf8AdWEg", "test_channel")

