# downloader.py
import os
import json
import subprocess
import logging
import signal
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer
from kafka_admin import create_topic_if_not_exists
import psycopg2
from psycopg2 import pool
import boto3

# Загрузка переменных окружения из .env файла
load_dotenv()

# ========= Настройки =========
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DOWNLOAD_TOPIC = os.getenv("KAFKA_DOWNLOAD_TOPIC", "video_download_requests")
NEXT_TOPIC = os.getenv("KAFKA_CUTTER_TOPIC", "video_cut_requests")

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "raw-videos")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "myuser")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "mypass")

TEMP_DIR = os.path.expanduser(os.getenv("TEMP_DIR", "~/downloader"))

YTDLP_PROXY = os.getenv("YTDLP_PROXY", "socks5://ytdlp:ytdlp@147.45.134.54:1080/")

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
def initialize_postgres_pool(retries=5, delay=5):
    """Инициализирует пул подключений к PostgreSQL с повторными попытками."""
    attempt = 0
    while attempt < retries:
        try:
            postgres_pool = psycopg2.pool.SimpleConnectionPool(
                1, 20,
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASS
            )
            logger.info("Пул подключений к PostgreSQL инициализирован.")
            return postgres_pool
        except Exception as e:
            attempt += 1
            logger.error(f"Попытка {attempt} - Не удалось инициализировать пул подключений к PostgreSQL: {e}")
            time.sleep(delay)
    logger.critical("Не удалось установить соединение с PostgreSQL после нескольких попыток.")
    sys.exit(1)

postgres_pool = initialize_postgres_pool()

def get_postgres_conn():
    """Получаем соединение из пула."""
    try:
        return postgres_pool.getconn()
    except Exception as e:
        logger.error(f"Ошибка получения соединения из пула: {e}")
        raise

def release_postgres_conn(conn):
    """Возвращаем соединение в пул."""
    try:
        postgres_pool.putconn(conn)
    except Exception as e:
        logger.error(f"Ошибка возвращения соединения в пул: {e}")

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
            logger.info("Таблица 'videos' проверена/создана.")
    except Exception as e:
        logger.error(f"[INIT_DB] Ошибка инициализации базы данных: {e}")
        conn.rollback()
        raise
    finally:
        release_postgres_conn(conn)

# ========= Загрузка Видео =========
def download_video(video_id):
    """Скачиваем ролик с YouTube (yt-dlp) в локальную папку и возвращаем путь к файлу."""
    logger.info(f"[DOWNLOAD_VIDEO] Начало скачивания видео: {video_id}")
    absolute_temp_dir = os.path.abspath(TEMP_DIR)
    logger.info(f"[DOWNLOAD_VIDEO] Используем абсолютный путь: {absolute_temp_dir}")
    os.makedirs(absolute_temp_dir, exist_ok=True)
    output_template = os.path.join(absolute_temp_dir, f"{video_id}.%(ext)s")

    cmd = [
        "yt-dlp",
        "--output", output_template,
        '--proxy', YTDLP_PROXY,
        "--retries", "10",
        f"https://www.youtube.com/watch?v={video_id}"
    ]
    try:
        result = subprocess.run(cmd, check=True, 
                                capture_output=True, text=True)
        logger.info(f"[DOWNLOAD_VIDEO] Видео скачано: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"[DOWNLOAD_VIDEO] Ошибка скачивания видео {video_id}: {e.stderr}")
        raise

    # Предполагаем, что расширение видео - mp4
    local_path = os.path.join(absolute_temp_dir, f"{video_id}.mp4")
    if os.path.exists(local_path):
        logger.info(f"[DOWNLOAD_VIDEO] Файл найден: {local_path}")
        return local_path
    else:
        logger.error("[DOWNLOAD_VIDEO] yt-dlp: выходной файл не найден")
        raise FileNotFoundError("yt-dlp: выходной файл не найден")

# ========= Загрузка в S3 =========
def upload_to_s3(local_path, channel_id, video_id):
    """Заливаем локальный файл в S3 (raw). Возвращаем s3_key."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        use_ssl=False,         # Добавлено для MinIO без SSL
        verify=False           # Добавлено для MinIO без SSL
    )
    basename = os.path.basename(local_path)
    s3_key = f"raw_videos/{channel_id}/{video_id}/{basename}"
    try:
        s3.upload_file(local_path, S3_BUCKET_RAW, s3_key)
        logger.info(f"[UPLOAD_TO_S3] Файл {local_path} загружен в S3 -> {s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"[UPLOAD_TO_S3] Ошибка загрузки в S3: {e}")
        raise

# ========= Инициализация Kafka Producer =========
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    """Функция обратного вызова для отчетов о доставке сообщений."""
    if err is not None:
        logger.error(f"[KAFKA_PRODUCER] Сообщение не доставлено: {err}")
    else:
        logger.info(f"[KAFKA_PRODUCER] Сообщение доставлено в {msg.topic()} [{msg.partition()}]")

# ========= Обработка Запроса на Загрузку =========
def process_download_request(message_value, consumer):
    try:
        data = json.loads(message_value)
        video_id = data["video_id"]
        channel_id = data.get("channel_id", "default_channel")

        logger.info(f"[DOWNLOADER] Получено задание: video_id={video_id}, channel_id={channel_id}")

        # Шаг 1: Скачать локально
        try:
            local_path = download_video(video_id)
        except FileNotFoundError:
            # Если файл не найден, удалить его и попытаться скачать снова
            logger.warning(f"[DOWNLOAD_VIDEO] Файл {video_id}.mp4 не найден. Попытка удалить и скачать снова.")
            local_path = os.path.join(os.path.abspath(TEMP_DIR), f"{video_id}.mp4")
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                    logger.info(f"[DOWNLOAD_VIDEO] Удалён повреждённый файл: {local_path}")
                except Exception as e:
                    logger.error(f"[DOWNLOAD_VIDEO] Не удалось удалить файл {local_path}: {e}")
            # Попытка повторного скачивания
            local_path = download_video(video_id)

        # Шаг 2: Залить в S3
        s3_key = upload_to_s3(local_path, channel_id, video_id)

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
                logger.info(f"[POSTGRES] Информация о видео {video_id} сохранена.")
        except Exception as e:
            logger.error(f"[POSTGRES] Ошибка записи в базу данных: {e}")
            conn.rollback()
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

        # Только после успешной обработки фиксируем смещение
        consumer.commit(asynchronous=False)

    except json.JSONDecodeError as jde:
        logger.error(f"[DOWNLOADER] Некорректный JSON: {jde}. Сообщение: {message_value}")
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

    # Инициализация Kafka Consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": os.getenv("KAFKA_DOWNLOADER_GROUP_ID", "downloader_group_unique"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False             # Отключение автоматического коммита
    })
    
    # Убедимся, что топики существуют
    create_topic_if_not_exists(
        broker=KAFKA_BOOTSTRAP_SERVERS,
        topic_name=DOWNLOAD_TOPIC,
        num_partitions=3,
        replication_factor=1
    )
    
    create_topic_if_not_exists(
        broker=KAFKA_BOOTSTRAP_SERVERS,
        topic_name=NEXT_TOPIC,
        num_partitions=3,
        replication_factor=1
    )
    
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

            message_value = msg.value().decode("utf-8")
            process_download_request(message_value, consumer)
    except Exception as e:
        logger.error(f"[DOWNLOADER] Неожиданная ошибка: {e}")
    finally:
        consumer.close()
        postgres_pool.closeall()
        producer.flush()
        logger.info("Сервис завершил работу.")

# ========= Запуск =========
if __name__ == "__main__":
    start_downloader()
