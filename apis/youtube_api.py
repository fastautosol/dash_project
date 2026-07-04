# 2026.07.04 18.00
import requests
import dlt
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
import re
import html
import logging
import isodate
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"
router = APIRouter()
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

STORE_KEYWORDS = ("shopify", "store", "gumroad", "etsy", "tiktokshop", "merch", "shop")

_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001FAFF"  # symbols, pictographs, supplemental symbols
    "\U00002600-\U000027BF"  # misc symbols, dingbats
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002B00-\U00002BFF"  # misc arrows/symbols often used as emoji
    "\U0000FE0F"             # variation selector (emoji presentation)
    "]+",
    flags=re.UNICODE)


def clean_comment_text(text: str) -> str:
    if not text:
        return text
    text = html.unescape(text)                  # &amp; -> &, &#39; -> ' stb.
    text = re.sub(r"<[^>]+>", " ", text)         # esetleges maradék HTML tagek (pl. <br>)
    text = _EMOJI_PATTERN.sub("", text)          # emoji eltávolítása
    text = re.sub(r"\s+", " ", text).strip()     # többszörös szóköz összevonása
    return text


class ChannelRequest(BaseModel):
    channel_id: str                    # elfogad UC-s channel_id-t VAGY @handle-t (pl. "@big_ch")
    max_videos: int = 5                # Alapértelmezetten csak az utolsó 3 videó
    max_comments_per_video: int = 15   # Alapértelmezetten videónként csak 20 komment


def yt_get(endpoint: str, params: dict):
    params["key"] = YOUTUBE_KEY
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
    if response.status_code != 200:
        raise Exception(f"YT API error {response.status_code} on /{endpoint}: {response.text}")
    return response.json()


def get_uploads_playlist_id(channel: str) -> str | None:
    data = yt_get("channels", {"part": "contentDetails", "forHandle": channel})
    items = data.get("items", [])
    if not items:
        logger.warning("Channel not found: %s", channel)
        return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_playlist_video_ids(playlist_id: str, max_videos: int) -> list[str]:
    data = yt_get("playlistItems", {"part": "contentDetails", "playlistId": playlist_id, "maxResults": max_videos})
    return [item["contentDetails"]["videoId"] for item in data.get("items", [])]


def get_videos_details(video_ids: list[str]) -> list[dict]:
    if not video_ids:
        return []
    data = yt_get("videos", {"part": "snippet,statistics,contentDetails", "id": ",".join(video_ids)})
    return data.get("items", [])


def get_video_comments(video_id: str, max_comments: int) -> list[dict]:
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
            "comment_text": clean_comment_text(snippet.get("textOriginal", snippet.get("textDisplay", "")))[:500],
            "comment_published_at": snippet["publishedAt"],
            "comment_like_count": int(snippet.get("likeCount", 0)),
            "comment_reply_count": int(item["snippet"].get("totalReplyCount", 0)),
        })
    return comments


def fetch_channel_analytics_pipeline(channel_id: str, max_videos: int, max_comments_per_video: int):
    ingested_at = datetime.now(timezone.utc).isoformat()
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

        description = video["snippet"].get("description", "")
        links = re.findall(r'(https?://\S+)', description)
        has_store_link = any(kw in link.lower() for link in links for kw in STORE_KEYWORDS)
        duration_sec = int(isodate.parse_duration(video["contentDetails"]["duration"]).total_seconds())
        channel_title = video["snippet"].get("channelTitle")
        has_captions = video["contentDetails"].get("caption") == "true"
        video_definition = video["contentDetails"].get("definition")
        comments = get_video_comments(v_id, max_comments_per_video) if max_comments_per_video > 0 else []

        if not comments:
            # Videó szintű rekord akkor is, ha nincs komment — így a videó sosem veszik el
            # (comment_id sosem lehet NULL, mert az része a merge primary key-nek)
            yield {
                "comment_id": f"NO_COMMENT_{v_id}",
                "video_id": v_id,
                "video_title": v_title,
                "channel": channel,
                "author": None,
                "comment_text": None,
                "comment_published_at": None,
                "comment_like_count": 0,
                "comment_reply_count": 0,
                "upload_date": video["snippet"]["publishedAt"],
                "channel_title": channel_title,
                "has_captions": has_captions,
                "video_definition": video_definition,
                "view_count": int(video["statistics"].get("viewCount", 0)),
                "like_count": int(video["statistics"].get("likeCount", 0)),
                "comment_count": int(video["statistics"].get("commentCount", 0)),
                "description_snippet": description[:200],
                "has_store_link": has_store_link,
                "duration_sec": duration_sec,
                "processed": False,
                "_ingested_at": ingested_at,
            }
            continue

        for c in comments:
            yield {
                "comment_id": c["comment_id"],
                "video_id": v_id,
                "video_title": v_title,
                "channel": channel,
                "author": c["author"],
                "comment_text": c["comment_text"],
                "comment_published_at": c["comment_published_at"],
                "comment_like_count": c["comment_like_count"],
                "comment_reply_count": c["comment_reply_count"],
                "upload_date": video["snippet"]["publishedAt"],
                "channel_title": channel_title,
                "has_captions": has_captions,
                "video_definition": video_definition,
                "view_count": int(video["statistics"].get("viewCount", 0)),
                "like_count": int(video["statistics"].get("likeCount", 0)),
                "comment_count": int(video["statistics"].get("commentCount", 0)),
                "description_snippet": description[:200],
                "has_store_link": has_store_link,
                "duration_sec": duration_sec,
                "processed": False,
                "_ingested_at": ingested_at,
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
            fetch_channel_analytics_pipeline(channel, max_videos, max_comments_per_video),
            table_name="youtube_comments_raw",
            write_disposition="merge",
            primary_key=["video_id", "comment_id"]
        )
        logger.info("dlt sikeresen végrehajtva: %s", info)

    except Exception as e:
        logger.exception("pipeline hiba: %s", e)


@router.post("/")
async def trigger_channel_fetch(request: ChannelRequest, background_tasks: BackgroundTasks):
    if not YOUTUBE_KEY:
        raise HTTPException(status_code=500, detail="Hiányzó YOUTUBE_API_KEY környezeti változó!")

    background_tasks.add_task(
        run_dlt_pipeline,
        request.channel,
        request.max_videos,
        request.max_comments_per_video
    )

    return {"status": "success", "message": f"Channel: {request.channel} data gathering in background"}
