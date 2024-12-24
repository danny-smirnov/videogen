import os
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/bin/ffmpeg"


from dotenv import load_dotenv
import random
import boto3
import cv2
import numpy as np
from moviepy.editor import VideoFileClip, concatenate_videoclips
from datetime import datetime
load_dotenv()


            
def download_random_videos(s3_bucket, s3_prefix, local_folder, num_videos=10):
    """Download random videos from an S3 directory."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ['S3_ENDPOINT_URL'],
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY_ID'],
        verify=False
    )
    
    objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix).get('Contents', [])

    # Filter video files
    video_files = [obj['Key'] for obj in objects if obj['Key'].endswith(('.mp4', '.avi', '.mov'))]

    if len(video_files) < num_videos:
        raise ValueError("Not enough videos in the specified S3 directory.")

    selected_videos = random.sample(video_files, num_videos)

    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    local_paths = []

    for video_key in selected_videos:
        local_path = os.path.join(local_folder, os.path.basename(video_key))
        s3.download_file(s3_bucket, video_key, local_path)
        local_paths.append(local_path)
        print(f"Downloaded {video_key} to {local_path}")

    return local_paths

def merge_videos(video_paths, output_path):
    """Merge multiple videos into one."""
    video_clips = [VideoFileClip(video) for video in video_paths]
    final_clip = concatenate_videoclips(video_clips, method='compose')
    final_clip.write_videofile(output_path, codec='libx264')
    print(f"Merged video saved to {output_path}")

def upload_video_to_s3(local_path, s3_bucket, s3_key):
    """Upload a video to S3."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ['S3_ENDPOINT_URL'],
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY_ID'],
        verify=False
    )
    s3.upload_file(local_path, s3_bucket, s3_key)
    print(f"Uploaded {local_path} to s3://{s3_bucket}/{s3_key}")
    
    
    
def main():
    s3_bucket = os.environ['S3_BUCKET']
    s3_prefix = "processed/"
    output_s3_prefix = "merged_videos/"

    local_folder = "./temp_videos"
    output_video = f"./merged_video_{datetime.now().strftime('%Y-%m-%d-%H:%M')}.mp4"

    try:
        # Step 1: Download random videos from S3
        random_videos = download_random_videos(s3_bucket, s3_prefix, local_folder, num_videos=10)

        # Step 2: Merge videos into one
        merge_videos(random_videos, output_video)

        # Step 3: Upload merged video back to S3
        output_s3_key = os.path.join(output_s3_prefix, os.path.basename(output_video))
        upload_video_to_s3(output_video, s3_bucket, output_s3_key)

    finally:
        pass
        # Cleanup temporary files
        if os.path.exists(local_folder):
            for file in os.listdir(local_folder):
                os.remove(os.path.join(local_folder, file))
            os.rmdir(local_folder)

        if os.path.exists(output_video):
            os.remove(output_video)

if __name__ == "__main__":
    main()
