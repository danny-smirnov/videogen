import os
import json
import subprocess
import psycopg2
import boto3
from confluent_kafka import Consumer, Producer
from datetime import datetime

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

TEMP_DIR = "/tmp/downloader"

# ========= Функции ==========

def get_postgres_conn():
    """Создаём подключение к Postgres (psycopg2)."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASS
    )

def init_db():
    """Создаём таблицу videos (если не существует)."""
    with get_postgres_conn() as conn:
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

def download_video(video_id):
    """Скачиваем ролик с YouTube (yt-dlp) в локальную папку."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    output_template = os.path.join(TEMP_DIR, f"{video_id}.%(ext)s")

    cmd = [
        "yt-dlp",
        "--output", output_template,
        "--retries", "10",
        f"https://www.youtube.com/watch?v={video_id}"
    ]
    subprocess.run(cmd, check=True)

    # yt-dlp может скачать в mp4, webm и т.д. Ищем реальный файл
    for fname in os.listdir(TEMP_DIR):
        if fname.startswith(video_id + "."):
            return os.path.join(TEMP_DIR, fname)

    raise FileNotFoundError("yt-dlp: выходной файл не найден")

def upload_to_s3(local_path, channel_id, video_id):
    """Заливаем локальный файл в S3 (raw). Возвращаем s3_key."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )
    basename = os.path.basename(local_path)
    s3_key = f"raw_videos/{channel_id}/{video_id}/{basename}"
    s3.upload_file(local_path, S3_BUCKET_RAW, s3_key)
    return s3_key

def process_download_request(message_value):
    """
    Обрабатываем входящее сообщение (JSON):
      {
        "video_id": "...",
        "channel_id": "..."
      }
    """
    data = json.loads(message_value)
    video_id = data["video_id"]
    channel_id = data.get("channel_id", "default_channel")

    print(f"[DOWNLOADER] Получено задание: video_id={video_id}, channel_id={channel_id}")

    # Шаг 1: Скачать локально
    local_path = download_video(video_id)

    # Шаг 2: Залить в S3
    s3_key = upload_to_s3(local_path, channel_id, video_id)
    print(f"[DOWNLOADER] Файл {local_path} загружен в S3 -> {s3_key}")

    # Шаг 3: Сохранить инфо в Postgres
    with get_postgres_conn() as conn:
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

    # Удаляем локальный файл
    os.remove(local_path)

    # Шаг 4: Отправляем задание Cutter'у
    #  Допустим, Cutter тоже принимает video_id, channel_id, s3_key
    next_msg = {
        "video_id": video_id,
        "channel_id": channel_id,
        "s3_key": s3_key
    }
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(NEXT_TOPIC, json.dumps(next_msg).encode("utf-8"))
    producer.flush()
    print(f"[DOWNLOADER] Отправлено сообщение на Cutter → {NEXT_TOPIC}")

def start_downloader():
    """Основная цикл-функция, слушаем Kafka, обрабатываем сообщения."""
    init_db()  # Создаём (или проверяем) таблицу при старте

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "downloader_group",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([DOWNLOAD_TOPIC])
    print("[DOWNLOADER] Ожидание сообщений из Kafka...")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("[DOWNLOADER] Ошибка Consumer:", msg.error())
            continue

        process_download_request(msg.value().decode("utf-8"))
        consumer.commit(asynchronous=False)

# Запускаем сразу
start_downloader()

