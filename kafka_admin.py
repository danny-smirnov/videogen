# kafka_admin.py
import os
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def create_topic_if_not_exists(broker, topic_name, num_partitions=1, replication_factor=1):
    """
    Создаёт Kafka топик, если он не существует.

    :param broker: Строка подключения к Kafka (например, 'localhost:9092').
    :param topic_name: Имя топика для создания.
    :param num_partitions: Количество разделов для топика.
    :param replication_factor: Фактор репликации для топика.
    """
    admin_client = AdminClient({'bootstrap.servers': broker})

    try:
        existing_topics = admin_client.list_topics(timeout=10).topics
    except Exception as e:
        logger.error(f"Ошибка при получении списка топиков: {e}")
        return

    if topic_name in existing_topics:
        logger.info(f"Топик '{topic_name}' уже существует.")
        return

    # Создание топика
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    try:
        # AdminClient.create_topics() возвращает future для каждого топика
        futures = admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result()  # Ожидание завершения операции
                logger.info(f"Топик '{topic}' успешно создан.")
            except Exception as e:
                logger.error(f"Не удалось создать топик '{topic}': {e}")
    except Exception as e:
        logger.error(f"Ошибка при создании топика: {e}")

# Список необходимых топиков
required_topics = [
    {
        "name": os.getenv("KAFKA_DOWNLOAD_TOPIC", "video_download_requests"),
        "partitions": 3,
        "replication_factor": 1
    },
    {
        "name": os.getenv("KAFKA_CUTTER_TOPIC", "video_cut_requests"),
        "partitions": 3,
        "replication_factor": 1
    },
    {
        "name": os.getenv("KAFKA_UPLOADER_TOPIC", "video_upload_requests"),
        "partitions": 3,
        "replication_factor": 1
    }
]

# Пример использования
if __name__ == "__main__":
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    for topic in required_topics:
        create_topic_if_not_exists(
            broker=broker,
            topic_name=topic["name"],
            num_partitions=topic["partitions"],
            replication_factor=topic["replication_factor"]
        )
