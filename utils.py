import os
import re
import yt_dlp
import logging
import requests
from PIL import Image
from moviepy import VideoFileClip
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def sanitize_filename(name: str) -> str:
    # Replace unsafe characters with underscore (keep alphanum, _, -, ., space)
    name = re.sub(r"[^\w\s\.\-]", "_", name)
    # Replace multiple underscores with a single one
    name = re.sub(r"_+", "_", name)
    # Remove spaces around underscores
    name = re.sub(r"\s*_\s*", "_", name)
    # Replace remaining spaces with underscores
    name = re.sub(r"\s+", "_", name)
    # Remove leading/trailing underscores
    name = name.strip("_")

    return name


def create_download_folder() -> str:
    download_folder = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "downloads"
    )
    os.makedirs(download_folder, exist_ok=True)
    return download_folder


def download_youtube_video(
    url: str,
    resolution: int = 1080,
) -> Tuple[
    Optional[str],  # video_path
    Optional[str],  # thumb_path
    Optional[str],  # title
    Optional[int],  # duration (secs)
    Optional[int],  # width
    Optional[int],  # height
]:
    """
    Downloads a YouTube video,
    generates a thumbnail frame:
     (video_path, thumb_path, title, duration, width, height)
    """

    download_folder = create_download_folder()

    try:
        valid_thumbnail = False

        # 1) Fetch metadata first
        with yt_dlp.YoutubeDL({"quiet": True}) as ydl:
            info = ydl.extract_info(url, download=False)

        if not info:
            return (None, None, None, None, None, None)

        title = info.get("title", "video")
        ext = info.get("ext", "mp4")
        safe = sanitize_filename(title)
        outtmpl = os.path.join(download_folder, f"{safe}.%(ext)s")

        # 2) Download
        ydl_opts = {
            # "format": f"best[height<={resolution}]",
            "format": f"bestvideo[height={resolution}]+bestaudio/best[height={resolution}]",
            "merge_output_format": "mp4",
            "outtmpl": outtmpl,
            "quiet": True,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        video_path = os.path.join(download_folder, f"{safe}.{ext}")

        if not os.path.exists(video_path):
            return (None, None, None, None, None, None)

        # 3) Fix metadata & thumbnail with moviepy
        clip = VideoFileClip(video_path)
        duration = int(clip.duration)
        width, height = clip.size

        if info is not None:
            thumb_url = info.get("thumbnail")
            thumb_path = os.path.join(download_folder, f"{safe}.jpg")

            if thumb_url:
                try:
                    # Download the image
                    r = requests.get(thumb_url)
                    r.raise_for_status()  # Raise error if download fails
                    with open(thumb_path, "wb") as f:
                        f.write(r.content)

                    # Check file size
                    if os.path.getsize(thumb_path) <= 200 * 1024:
                        with Image.open(thumb_path) as img:
                            # Convert to JPEG if needed
                            if img.format != "JPEG":
                                img = img.convert("RGB")
                                img.save(thumb_path, "JPEG")

                            # Resize the image to fit within the 320x320 dimension
                            if img.width > 320 or img.height > 320:
                                img.thumbnail((320, 320))  # Maintain aspect ratio

                            # Save the resized image if it's valid
                            img.save(thumb_path)

                            # Check if the thumbnail meets the size condition
                            if img.width <= 320 and img.height <= 320:
                                valid_thumbnail = True
                            else:
                                valid_thumbnail = False

                except Exception as e:
                    logger.error(f"Thumbnail processing failed: {e}")
                    thumb_path = None

        if valid_thumbnail:
            return video_path, thumb_path, title, duration, width, height
        else:
            return (None, None, None, None, None, None)

    except Exception as e:
        logger.error(f"Error in download_youtube_video: {e}")
        return (None, None, None, None, None, None)
