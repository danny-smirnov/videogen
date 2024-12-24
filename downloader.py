import os
import json
import subprocess
import psycopg2
import boto3
from confluent_kafka import Consumer, Producer
from datetime import datetime
import logging
from psycopg2 import pool
import signal
import sys

# ========= Настройки =========
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DOWNLOAD_TOPIC = "video_download_requests"   # Из этого топика читаем
NEXT_TOPIC = "video_cut_requests"           # Сюда шлём Cutter'у

S3_ENDPOINT_URL = "http://localhost:9000"
S3_BUCKET_RAW = "raw-videos"

POSTGRES_HOST = "localhost"
POSTGRES_DB = "mydb"
POSTGRES_USER = "myuser"
POSTGRES_PASS = "mypass"

TEMP_DIR = "./tmp/downloader"

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

# ========= Логирование =========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========= Пул Подключений к PostgreSQL =========
postgres_pool = pool.SimpleConnectionPool(
    1, 20,
    host=POSTGRES_HOST,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASS
)

def get_postgres_conn():
    """Получаем соединение из пула."""
    return postgres_pool.getconn()

def release_postgres_conn(conn):
    """Возвращаем соединение в пул."""
    postgres_pool.putconn(conn)

# ========= Инициализация Базы Данных =========
def init_db():
    """Создаём таблицу videos (если не существует)."""
    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id VARCHAR(255) PRIMARY KEY,
                channel_id VARCHAR(255),
                s3_key TEXT,
                status VARCHAR(50),
                created_at TIMESTAMP
            );
            """)
            conn.commit()
    except Exception as e:
        logger.error(f"[INIT_DB] Ошибка инициализации базы данных: {e}")
    finally:
        release_postgres_conn(conn)

# ========= Загрузка Видео =========
def download_video(video_id):
    """Скачиваем ролик с YouTube (yt-dlp) в локальную папку и возвращаем путь к файлу."""
    logger.info(f"[DOWNLOAD_VIDEO] Начало скачивания видео: {video_id}")
    os.makedirs(TEMP_DIR, exist_ok=True)
    output_template = os.path.join(TEMP_DIR, f"{video_id}.%(ext)s")

    cmd = [
        "yt-dlp",
        "--output", output_template,
        "--retries", "10",
        f"https://www.youtube.com/watch?v={video_id}"
    ]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"[DOWNLOAD_VIDEO] Видео скачано: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"[DOWNLOAD_VIDEO] Ошибка скачивания видео {video_id}: {e.stderr}")
        raise

    # Parse stdout to find the downloaded file
    for line in result.stdout.splitlines():
        if "[download] Destination:" in line:
            path = line.split("Destination: ")[1].strip()
            if os.path.exists(path):
                logger.info(f"[DOWNLOAD_VIDEO] Файл найден: {path}")
                return path

    logger.error("[DOWNLOAD_VIDEO] yt-dlp: выходной файл не найден")
    raise FileNotFoundError("yt-dlp: выходной файл не найден")


# ========= Загрузка в S3 =========
def upload_to_s3(local_path, channel_id, video_id):
    """Заливаем локальный файл в S3 (raw). Возвращаем s3_key."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    basename = os.path.basename(local_path)
    s3_key = f"raw_videos/{channel_id}/{video_id}/{basename}"
    try:
        s3.upload_file(local_path, S3_BUCKET_RAW, s3_key)
        return s3_key
    except Exception as e:
        logger.error(f"[UPLOAD_TO_S3] Ошибка загрузки в S3: {e}")
        raise

# ========= Инициализация Kafka Producer =========
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"[KAFKA_PRODUCER] Сообщение не доставлено: {err}")
    else:
        logger.info(f"[KAFKA_PRODUCER] Сообщение доставлено в {msg.topic()} [{msg.partition()}]")

# ========= Обработка Запроса на Загрузку =========
def process_download_request(message_value):
    try:
        data = json.loads(message_value)
        video_id = data["video_id"]
        channel_id = data.get("channel_id", "default_channel")

        logger.info(f"[DOWNLOADER] Получено задание: video_id={video_id}, channel_id={channel_id}")

        # Шаг 1: Скачать локально
        local_path = download_video(video_id)

        # Шаг 2: Залить в S3
        s3_key = upload_to_s3(local_path, channel_id, video_id)
        logger.info(f"[DOWNLOADER] Файл {local_path} загружен в S3 -> {s3_key}")

        # Шаг 3: Сохранить инфо в Postgres
        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO videos (video_id, channel_id, s3_key, status, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (video_id) DO UPDATE 
                      SET channel_id=EXCLUDED.channel_id,
                          s3_key=EXCLUDED.s3_key,
                          status=EXCLUDED.status,
                          created_at=EXCLUDED.created_at
                    """,
                    (video_id, channel_id, s3_key, "downloaded", datetime.utcnow())
                )
                conn.commit()
        except Exception as e:
            logger.error(f"[POSTGRES] Ошибка записи в базу данных: {e}")
            raise
        finally:
            release_postgres_conn(conn)

        # Удаляем локальный файл
        try:
            os.remove(local_path)
            logger.info(f"[DOWNLOADER] Удален локальный файл: {local_path}")
        except Exception as e:
            logger.warning(f"[DOWNLOADER] Не удалось удалить файл {local_path}: {e}")

        # Шаг 4: Отправляем задание Cutter'у
        next_msg = {
            "video_id": video_id,
            "channel_id": channel_id,
            "s3_key": s3_key
        }
        producer.produce(
            NEXT_TOPIC,
            json.dumps(next_msg).encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)  # Обработка колбэков
        logger.info(f"[DOWNLOADER] Отправлено сообщение на Cutter → {NEXT_TOPIC}")

    except Exception as e:
        logger.error(f"[DOWNLOADER] Ошибка при обработке задания: {e}")
        # Дополнительно можно реализовать повторные попытки или отправку в отдельный топик ошибок

# ========= Обработка Сигналов для Graceful Shutdown =========
shutdown = False

def handle_signal(sig, frame):
    global shutdown
    shutdown = True
    logger.info("Получен сигнал завершения. Останавливаем потребителя...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ========= Основной Цикл =========
def start_downloader():
    """Основная цикл-функция, слушаем Kafka, обрабатываем сообщения."""
    init_db()  # Создаём (или проверяем) таблицу при старте

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "downloader_group",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([DOWNLOAD_TOPIC])
    logger.info("[DOWNLOADER] Ожидание сообщений из Kafka...")

    try:
        while not shutdown:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"[DOWNLOADER] Ошибка Consumer: {msg.error()}")
                continue

            process_download_request(msg.value().decode("utf-8"))
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        postgres_pool.closeall()
        producer.flush()
        logger.info("Сервис завершил работу.")

# ========= Запуск =========
if __name__ == "__main__":
    start_downloader()
