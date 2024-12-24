import os
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/bin/ffmpeg"


from sklearn.cluster import DBSCAN
import cv2
from tqdm import tqdm
import numpy as np
from math import ceil

from moviepy.tools import subprocess_call


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
            
            

# @click.argument('save_path', default=None)
# @click.argument('v', default=None)
def create_scenes_from_video(
    video_path: str,
    save_path: str = None,
):
    assert os.path.exists(video_path), "Video doesn't exist"
    # assert video_path.endswith(".mp4"), "This is not video"
    # logging.info("SAVING VIDEO")
    if save_path is None:
        save_path = video_path.replace(".mp4", "_scenes")

    time_intervals = get_timestamps_of_frame_change(video_path)

    split_video_by_scenes(video_path, save_path, time_intervals)


if __name__ == "__main__":
    create_scenes_from_video(
        video_path='/Users/dosmirnov/BxUS1K7xu30.mp4',
        save_path='/Users/dosmirnov/scenes'
)