# 2026.07.03 12.00
import requests
import dlt
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
import logging

logger = logging.getLogger(__name__)

YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"
router = APIRouter()
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}


class ChannelRequest(BaseModel):
    channel_id: str                    # elfogad UC-s channel_id-t VAGY @handle-t (pl. "@big_ch")
    max_videos: int = 3                # Alapértelmezetten csak az utolsó 3 videó
    max_comments_per_video: int = 20   # Alapértelmezetten videónként csak 20 komment


def yt_get(endpoint: str, params: dict):
    """Egységes YouTube API hívás, mindig a helyes végponttal."""
    params["key"] = YOUTUBE_KEY
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
    if response.status_code != 200:
        raise Exception(f"YT API error {response.status_code} on /{endpoint}: {response.text}")
    return response.json()


def get_uploads_playlist_id(channel: str) -> str | None:
    """1. LÉPÉS: channels.list — a csatorna 'uploads' playlist ID-ja (1 quota unit)"""
    data = yt_get("channels", {"part": "contentDetails", "forHandle": channel})
    items = data.get("items", [])
    if not items:
        logger.warning("Channel not found: %s", channel)
        return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_playlist_video_ids(playlist_id: str, max_videos: int) -> list[str]:
    """2. LÉPÉS: playlistItems.list — legfrissebb videó ID-k (1 quota unit)"""
    data = yt_get("playlistItems", {"part": "contentDetails", "playlistId": playlist_id, "maxResults": max_videos})
    return [item["contentDetails"]["videoId"] for item in data.get("items", [])]


def get_videos_details(video_ids: list[str]) -> list[dict]:
    """3. LÉPÉS: videos.list — cím + statisztika egy batch hívásban (1 quota unit, max 50 ID/hívás)"""
    if not video_ids:
        return []
    data = yt_get("videos", {"part": "snippet,statistics", "id": ",".join(video_ids)})
    return data.get("items", [])


def get_video_comments(video_id: str, max_comments: int) -> list[dict]:
    """4. LÉPÉS: commentThreads.list — top-level kommentek (1 quota unit)"""
    try:
        data = yt_get("commentThreads", {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": min(max_comments, 100),
            "textFormat": "plainText",
            "order": "relevance",
        })
    except Exception as e:
        logger.warning("Comment fetch failed for %s: %s", video_id, e)
        return []

    comments = []
    for item in data.get("items", [])[:max_comments]:
        snippet = item["snippet"]["topLevelComment"]["snippet"]
        comments.append({
            "comment_id": item["id"],
            "author": snippet["authorDisplayName"],
            "comment_text": snippet["textDisplay"],
            "comment_published_at": snippet["publishedAt"],
        })
    return comments


def fetch_channel_analytics_pipeline(channel_id: str, max_videos: int, max_comments_per_video: int):
    """Generátor: videók + kommentek összegyűjtése a dlt számára"""
    playlist_id = get_uploads_playlist_id(channel_id)
    if not playlist_id:
        return

    video_ids = get_playlist_video_ids(playlist_id, max_videos)
    if not video_ids:
        logger.info("No videos found for channel %s", channel_id)
        return

    videos = get_videos_details(video_ids)
    logger.info("Fetched %d video(s) for channel %s", len(videos), channel_id)

    for video in videos:
        v_id = video["id"]
        v_title = video["snippet"]["title"]

        comments = get_video_comments(v_id, max_comments_per_video) if max_comments_per_video > 0 else []

        if not comments:
            # Videó szintű rekord akkor is, ha nincs komment — így a videó sosem veszik el
            yield {
                "comment_id": None,
                "video_id": v_id,
                "video_title": v_title,
                "channel_id": channel_id,
                "author": None,
                "comment_text": None,
                "comment_published_at": None,
                "upload_date": video["snippet"]["publishedAt"],
                "view_count": int(video["statistics"].get("viewCount", 0)),
                "like_count": int(video["statistics"].get("likeCount", 0)),
                "comment_count": int(video["statistics"].get("commentCount", 0)),
                "processed": False,
            }
            continue

        for c in comments:
            yield {
                "comment_id": c["comment_id"],
                "video_id": v_id,
                "video_title": v_title,
                "channel_id": channel_id,
                "author": c["author"],
                "comment_text": c["comment_text"],
                "comment_published_at": c["comment_published_at"],
                "upload_date": video["snippet"]["publishedAt"],
                "view_count": int(video["statistics"].get("viewCount", 0)),
                "like_count": int(video["statistics"].get("likeCount", 0)),
                "comment_count": int(video["statistics"].get("commentCount", 0)),
                "processed": False,
            }


def run_dlt_pipeline(channel_id: str, max_videos: int, max_comments_per_video: int):
    """dlt futtatása és Postgresbe mentés"""
    try:
        pipeline = dlt.pipeline(
            pipeline_name="youtube_channel_analytics",
            destination=dlt.destinations.postgres(credentials=DB_CONFIG),
            dataset_name="bronze"
        )

        info = pipeline.run(
            fetch_channel_analytics_pipeline(channel_id, max_videos, max_comments_per_video),
            table_name="youtube_comments_raw",
            write_disposition="merge",
            primary_key=["video_id", "comment_id"]
        )
        logger.info("dlt sikeresen végrehajtva: %s", info)

    except Exception as e:
        logger.exception("pipeline hiba: %s", e)


@router.post("/fetch-channel")
async def trigger_channel_fetch(request: ChannelRequest, background_tasks: BackgroundTasks):
    if not YOUTUBE_KEY:
        raise HTTPException(status_code=500, detail="Hiányzó YOUTUBE_API_KEY környezeti változó!")

    background_tasks.add_task(
        run_dlt_pipeline,
        request.channel_id,
        request.max_videos,
        request.max_comments_per_video
    )
    return {
        "status": "success",
        "message": f"The channel ({request.channel_id}) data gathering in the background. "
                   f"(Max {request.max_videos} videos, {request.max_comments_per_video} comment/video)"
    }
