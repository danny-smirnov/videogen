# send_test_message.py
import json
import os
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DOWNLOAD_TOPIC = os.getenv("KAFKA_DOWNLOAD_TOPIC", "video_download_requests")
CUTTER_TOPIC = os.getenv("KAFKA_CUTTER_TOPIC", "video_cut_requests")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация Kafka Producer
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    """Функция обратного вызова для отчетов о доставке сообщений."""
    if err is not None:
        logger.error(f"Сообщение не доставлено: {err}")
    else:
        logger.info(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")

def send_test_message(video_id, channel_id):
    """Отправка тестового сообщения в топик загрузки видео."""
    message = {"video_id": video_id, "channel_id": channel_id}
    try:
        producer.produce(DOWNLOAD_TOPIC, json.dumps(message).encode("utf-8"), callback=delivery_report)
        producer.flush()
        logger.info("Тестовое сообщение отправлено в Downloader.")
    except Exception as e:
        logger.error(f"Ошибка при отправке тестового сообщения: {e}")

def send_cutter_message(data):
    """Отправка сообщения в топик Cutter."""
    try:
        producer.produce(CUTTER_TOPIC, json.dumps(data).encode("utf-8"), callback=delivery_report)
        producer.flush()
        logger.info("Тестовое сообщение отправлено в Cutter.")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения Cutter: {e}")

if __name__ == "__main__":
    # Пример отправки тестового сообщения в Downloader
    send_test_message("0XaQf8AdWEg", "test_channel")

    # Пример отправки сообщения Cutter
    data = {
        'video_id': 'UNUSUAL_memes',
        'channel_id': 'pohuy',
        's3_key': 'raw_videos/pohuy/UNUSUAL_memes.mp4'
    }

    send_cutter_message(data)
