import os
import json
import requests
import psycopg2
import boto3
from kafka_admin import create_topic_if_not_exists
from confluent_kafka import Consumer
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
UPLOAD_TOPIC = "video_upload_requests"

S3_ENDPOINT_URL = "http://localhost:9000"
S3_BUCKET_COMBINED = "combined-videos"

POSTGRES_HOST = "localhost"
POSTGRES_DB = "mydb"
POSTGRES_USER = "myuser"
POSTGRES_PASS = "mypass"

# Данные для ВК
VK_ACCESS_TOKEN = "YOUR_VK_ACCESS_TOKEN"
VK_GROUP_ID = 123456789
VK_API_VERSION = "5.131"

TEMP_DIR = "/tmp/uploader"

def get_postgres_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASS
    )

def init_db():
    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS uploads (
                upload_id SERIAL PRIMARY KEY,
                final_id INT,
                video_id VARCHAR(255),
                vk_video_id VARCHAR(255),
                status VARCHAR(50),
                created_at TIMESTAMP
            );
            """)
            conn.commit()

def download_combined(s3_key, local_path):
    s3 = boto3.client("s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )
    bucket = S3_BUCKET_COMBINED
    s3.download_file(bucket, s3_key, local_path)

def vk_video_save(access_token, group_id, title="Video"):
    params = {
        "access_token": access_token,
        "group_id": group_id,
        "name": title,
        "v": VK_API_VERSION
    }
    resp = requests.get("https://api.vk.com/method/video.save", params=params).json()
    if "error" in resp:
        raise Exception(f"VK error: {resp['error']}")
    return resp["response"]["upload_url"], resp["response"]["video_id"]

def vk_upload_video(upload_url, filepath):
    with open(filepath, "rb") as f:
        files = {"video_file": f}
        resp = requests.post(upload_url, files=files).json()
    return resp

def process_upload_request(msg_value):
    """
    Сообщение:
    {
      "final_id": 123,
      "video_id": "...",
      "s3_key": "combined/.../xxx.mp4"
    }
    """
    data = json.loads(msg_value)
    final_id = data["final_id"]
    video_id = data["video_id"]
    s3_key = data["s3_key"]

    print(f"[UPLOADER] Получено: final_id={final_id}, video_id={video_id}")

    os.makedirs(TEMP_DIR, exist_ok=True)
    local_file = os.path.join(TEMP_DIR, os.path.basename(s3_key))
    download_combined(s3_key, local_file)

    # Загружаем во ВК
    title = f"Final video {video_id}"
    upload_url, vk_video_id = vk_video_save(VK_ACCESS_TOKEN, VK_GROUP_ID, title)
    resp = vk_upload_video(upload_url, local_file)
    print(f"[UPLOADER] Загружено во ВК -> vk_video_id={vk_video_id}, ответ={resp}")

    # Запись в Postgres
    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO uploads (final_id, video_id, vk_video_id, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (final_id, video_id, vk_video_id, "uploaded", datetime.utcnow()))
            conn.commit()

    os.remove(local_file)

def start_uploader():
    init_db()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "uploader_group",
        "auto.offset.reset": "earliest"
    })
    create_topic_if_not_exists(
        broker=KAFKA_BOOTSTRAP_SERVERS,
        topic_name=UPLOAD_TOPIC
    )
    consumer.subscribe([UPLOAD_TOPIC])

    print("[UPLOADER] Ожидание сообщений...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("[UPLOADER] Ошибка:", msg.error())
            continue

        process_upload_request(msg.value().decode("utf-8"))
        consumer.commit(asynchronous=False)

start_uploader()

