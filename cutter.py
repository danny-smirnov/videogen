import os
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/bin/ffmpeg"


from sklearn.cluster import DBSCAN
import cv2
from tqdm import tqdm
import numpy as np
from math import ceil
from confluent_kafka import Consumer, KafkaException
from kafka_admin import create_topic_if_not_exists

from moviepy.tools import subprocess_call
import json
import boto3
from dotenv import load_dotenv
load_dotenv()

def _get_outliers_of_interval(brightness_differencies: np.array, eps, min_samples):
    # Get second-degree derivative with zeroed positive values
    diffs = np.array(brightness_differencies, dtype=np.float32)
    normalized = (diffs - diffs.mean()) / (diffs.std())
    diff2 = np.diff(np.diff(normalized))
    neg_diff = np.minimum(diff2, 0)

    # Create the DBSCAN model
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)

    # Fit the model to the data
    dbscan.fit(neg_diff.reshape(-1, 1))

    # Get the labels of the data points
    labels = dbscan.labels_

    # Identify the outliers
    outliers = np.where(labels == -1)[0]

    return outliers + 2  # incrementing cause of 2-nd degree derivative


def get_timestamps_of_frame_change(
    path_to_video: str, eps=1, min_samples=5, n_seconds_in_part=30
) -> list[tuple[float, float]]:

    # TODO make overlaps in 30-sec splits scenes to assure adding edge frames if they are frame changes

    cap = cv2.VideoCapture(path_to_video)
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = round(cap.get(cv2.CAP_PROP_FPS))

    assert frame_count > 0, ValueError('Invalid video')
    overall_frame_changes = np.array([])

    # preprocessing 30-second splits
    # get first frame
    ret, prev_frame = cap.read()
    cv2.normalize(prev_frame, prev_frame, 0, 1, cv2.NORM_MINMAX)

    current_number_of_frames = 1

    number_of_parts = ceil((frame_count - 1) / (n_seconds_in_part * fps))
    for part in tqdm(range(number_of_parts)):
        diffs = []
        while (
            cap.isOpened()
            and current_number_of_frames < (part + 1) * n_seconds_in_part * fps
        ):
            ret, curr_frame = cap.read()
            if ret == False:
                break

            cv2.normalize(curr_frame, curr_frame, 0, 1, cv2.NORM_MINMAX)

            diff = ((curr_frame - prev_frame) ** 2).sum()
            prev_frame = curr_frame
            diffs.append(diff)
            current_number_of_frames += 1
        if len(diffs) == 0:
            break

        frame_changes_in_parts = (
            _get_outliers_of_interval(diffs, eps, min_samples)
            + part * n_seconds_in_part * fps
            + int(part == 0)
        )

        overall_frame_changes = np.concatenate(
            [frame_changes_in_parts, overall_frame_changes]
        )

    outliers_filled = np.insert(overall_frame_changes, 0, 0)

    video_intervals = []
    gross_fps = cap.get(cv2.CAP_PROP_FPS)
    for curr_frame_idx in range(len(outliers_filled) - 1):
        outliers_filled.sort()
        starttime = (outliers_filled[curr_frame_idx] + 1) / gross_fps
        endtime = (outliers_filled[curr_frame_idx + 1] - 3) / gross_fps
        video_intervals.append((starttime, endtime))

    return video_intervals




def split_video_by_scenes(
    video_path: str,
    save_path: str,
    time_intervals: list[tuple[float, float]],
    min_scene_length=4.5,
) -> None:

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    video_name = video_path.split("/")[-1].split(".")[:-1]
    scene_index = 0
    for starttime, endtime in time_intervals:

        if endtime - starttime >= min_scene_length:
            cmd = ["/opt/homebrew/bin/ffmpeg","-y",
                "-ss", "%0.2f"%starttime,
                "-i", video_path,
                "-t", "%0.2f"%(endtime-starttime),
                "-vcodec", "copy", "-acodec", "copy", os.path.join(save_path, f"{video_name}_{scene_index}.mp4")]

            subprocess_call(cmd)
            scene_index += 1
            
            
def create_scenes_from_video(
    video_path: str,
    save_path: str = None,
):
    
    if save_path is None:
        save_path = video_path.replace(".mp4", "_scenes")

    time_intervals = get_timestamps_of_frame_change(video_path)
    split_video_by_scenes(video_path, save_path, time_intervals)


    
def download_from_s3(bucket_name, s3_key, download_path):
    """Download a file from S3."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ['S3_ENDPOINT_URL'],
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY_ID'],
        verify=False
    )
    s3.download_file(bucket_name, s3_key, download_path)
    print(f"File downloaded from S3: {s3_key} -> {download_path}")
    
    
def upload_folder_to_s3(folder_path, bucket_name, s3_prefix):
    """Upload all files from a folder to S3 under a specific prefix."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ['S3_ENDPOINT_URL'],
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY_ID'],
        verify=False
    )
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, folder_path)
            s3_key = os.path.join(s3_prefix, relative_path)
            s3.upload_file(file_path, bucket_name, s3_key)
            print(f"Uploaded {file_path} to S3 as {s3_key}")


def main():
    kafka_config = {
        'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        'group.id': os.environ['KAFKA_GROUP_ID'],
        'auto.offset.reset': 'earliest',
    }
    topic = os.environ['KAFKA_CUTTER_TOPIC']
    bucket_name = os.environ['S3_BUCKET']    

    consumer = Consumer(kafka_config)
    
    create_topic_if_not_exists(
        broker=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        topic_name=topic
    )

    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll with a 1-second timeout

            if msg is None:
                continue

            if msg.error():
                # if msg.error().code() == KafkaException._PARTITION_EOF:
                #     continue
                # else:
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                # Parse the message
                data = json.loads(msg.value().decode('utf-8'))
                video_id = data['video_id']
                channel_id = data['channel_id']
                s3_key = data['s3_key']

                print(f"Received message: video_id={video_id}, channel_id={channel_id}, s3_key={s3_key}")

                # Download video from S3
                download_dir = f"/tmp/{video_id}/videos"
                os.makedirs(download_dir, exist_ok=True)
                downloaded_video = os.path.join(download_dir, f"{video_id}.mp4")
                download_from_s3(bucket_name, s3_key, downloaded_video)

                # Process the video
                scenes_dir = f"/tmp/{video_id}/scenes"
                os.makedirs(scenes_dir, exist_ok=True)

                save_path = os.path.join(scenes_dir, video_id)
                create_scenes_from_video(downloaded_video, save_path)

                # Upload scenes to S3
                upload_folder_to_s3(save_path, bucket_name, f"processed/")

            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()