# 2026.10.04 - bronze: 1 sor / videó, kommentek jsonb-ben, transcript (youtube-transcript-api >= 1.0)
import requests
import dlt
import time
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
import re
import html
import logging
import isodate
from datetime import datetime, timezone
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
from youtube_transcript_api.proxies import GenericProxyConfig

logger = logging.getLogger(__name__)

YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"
router = APIRouter()
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
STORE_KEYWORDS = ("shopify", "store", "gumroad", "etsy", "tiktokshop", "merch", "shop")

# --- transcript beállítások ---
TRANSCRIPT_LANGS = ["hu", "en"]      # előnyben részesített nyelvek, ha nincs, a videó saját nyelve
NO_TRANSCRIPT = "no transcript"
STORE_SEGMENTS = True                # időbélyeges szegmensek jsonb-ben (chunkoláshoz / &t=123s linkhez)
TRANSCRIPT_DELAY_SEC = 1.0           # kis szünet videók között, csökkenti a blokkolás esélyét
TRANSCRIPT_PROXY_URL = os.getenv("TRANSCRIPT_PROXY_URL")  # opcionális, szerver IP-blokk esetére

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001FAFF"  # symbols, pictographs, supplemental symbols
    "\U00002600-\U000027BF"  # misc symbols, dingbats
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002B00-\U00002BFF"  # misc arrows/symbols often used as emoji
    "\U0000FE0F"             # variation selector (emoji presentation)
    "]+",
    flags=re.UNICODE)


def clean_text(text: str) -> str:
    if not text:
        return text
    text = html.unescape(text)                  # &amp; -> &, &#39; -> ' stb.
    text = re.sub(r"<[^>]+>", " ", text)        # esetleges maradék HTML tagek (pl. <br>)
    text = EMOJI_PATTERN.sub("", text)          # emoji eltávolítása
    text = re.sub(r"\s+", " ", text).strip()    # többszörös szóköz összevonása
    return text


class ChannelRequest(BaseModel):
    channel: str
    max_videos: int = 5
    max_comments_per_video: int = 15


def _build_transcript_api() -> YouTubeTranscriptApi:
    if TRANSCRIPT_PROXY_URL:
        return YouTubeTranscriptApi(
            proxy_config=GenericProxyConfig(http_url=TRANSCRIPT_PROXY_URL, https_url=TRANSCRIPT_PROXY_URL))
    return YouTubeTranscriptApi()


ytt_api = _build_transcript_api()


def yt_get(endpoint: str, params: dict):
    params["key"] = YOUTUBE_KEY
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
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
    """Lapozva (az API oldalanként max 50 elemet ad)."""
    ids, page_token = [], None
    while len(ids) < max_videos:
        params = {"part": "contentDetails", "playlistId": playlist_id,
                  "maxResults": min(50, max_videos - len(ids))}
        if page_token:
            params["pageToken"] = page_token
        data = yt_get("playlistItems", params)
        ids += [item["contentDetails"]["videoId"] for item in data.get("items", [])]
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return ids[:max_videos]


def get_videos_details(video_ids: list[str]) -> list[dict]:
    """A videos.list hívásonként max 50 ID-t fogad."""
    items = []
    for i in range(0, len(video_ids), 50):
        data = yt_get("videos", {"part": "snippet,statistics,contentDetails",
                                 "id": ",".join(video_ids[i:i + 50])})
        items.extend(data.get("items", []))
    return items


def get_video_comments(video_id: str, max_comments: int) -> list[dict]:
    """Bronze: csak a 3 lényeges mező marad, a feldolgozás a silver rétegben lesz."""
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
            "comment_text": clean_text(snippet.get("textOriginal", snippet.get("textDisplay", "")))[:500],
            "comment_published_at": snippet["publishedAt"],
            "comment_like_count": int(snippet.get("likeCount", 0)),
        })
    return comments


def get_transcript(video_id: str) -> dict:
    """
    status:
      ok            -> van felirat (kézi vagy automatikus), transcript_text kitöltve
      no_transcript -> nincs felirat / le van tiltva, transcript_text = "no transcript"
      error:<Név>   -> átmeneti hiba (pl. IP-blokk), transcript_text = None, később újra lehet próbálni
    """
    empty = {
        "transcript_text": NO_TRANSCRIPT,
        "transcript_status": "no_transcript",
        "transcript_language": None,
        "transcript_is_generated": None,
        "transcript_segments": [],
    }
    try:
        transcript_list = ytt_api.list(video_id)
        try:
            transcript = transcript_list.find_transcript(TRANSCRIPT_LANGS)
        except NoTranscriptFound:
            transcript = next(iter(transcript_list), None)  # a videó saját nyelve
        if transcript is None:
            return empty

        fetched = transcript.fetch()
        segments = [{"start": round(s.start, 1), "text": clean_text(s.text)} for s in fetched]
        segments = [s for s in segments if s["text"]]
        if not segments:
            return empty

        return {
            "transcript_text": " ".join(s["text"] for s in segments),
            "transcript_status": "ok",
            "transcript_language": fetched.language_code,
            "transcript_is_generated": fetched.is_generated,
            "transcript_segments": segments if STORE_SEGMENTS else [],
        }
    except (TranscriptsDisabled, NoTranscriptFound):
        return empty
    except Exception as e:
        logger.warning("Transcript fetch failed for %s: %s: %s", video_id, type(e).__name__, e)
        return {**empty, "transcript_text": None, "transcript_status": f"error:{type(e).__name__}"}


def fetch_channel_analytics_pipeline(channel: str, max_videos: int, max_comments_per_video: int):
    """Videónként EGY sor: metaadatok + comments (jsonb) + transcript."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    playlist_id = get_uploads_playlist_id(channel)
    if not playlist_id:
        return

    video_ids = get_playlist_video_ids(playlist_id, max_videos)
    if not video_ids:
        logger.info("No videos found for channel %s", channel)
        return

    videos = get_videos_details(video_ids)
    logger.info("Fetched %d video(s) for channel %s", len(videos), channel)

    for video in videos:
        v_id = video["id"]
        description = video["snippet"].get("description", "")
        links = re.findall(r'(https?://\S+)', description)
        has_store_link = any(kw in link.lower() for link in links for kw in STORE_KEYWORDS)
        duration_sec = int(isodate.parse_duration(video["contentDetails"]["duration"]).total_seconds())

        comments = get_video_comments(v_id, max_comments_per_video) if max_comments_per_video > 0 else []
        transcript = get_transcript(v_id)
        logger.info("Video %s transcript status: %s", v_id, transcript["transcript_status"])

        yield {
            "video_id": v_id,
            "video_title": video["snippet"]["title"],
            "channel": channel,
            "channel_title": video["snippet"].get("channelTitle"),
            "upload_date": video["snippet"]["publishedAt"],
            "has_captions": video["contentDetails"].get("caption") == "true",
            "video_definition": video["contentDetails"].get("definition"),
            "view_count": int(video["statistics"].get("viewCount", 0)),
            "like_count": int(video["statistics"].get("likeCount", 0)),
            "comment_count": int(video["statistics"].get("commentCount", 0)),
            "description_snippet": clean_text(description)[:200],
            "has_store_link": has_store_link,
            "duration_sec": duration_sec,
            "comments": comments,  # jsonb: [{comment_text, comment_published_at, comment_like_count}, ...]
            **transcript,          # transcript_text, transcript_status, transcript_language, transcript_is_generated, transcript_segments
            "processed": False,
            "_ingested_at": ingested_at,
        }
        time.sleep(TRANSCRIPT_DELAY_SEC)


def run_dlt_pipeline(channel: str, max_videos: int, max_comments_per_video: int):
    """dlt futtatása és Postgresbe mentés"""
    try:
        pipeline = dlt.pipeline(
            pipeline_name="youtube_channel",
            destination=dlt.destinations.postgres(credentials=DB_CONFIG),
            dataset_name="bronze")

        # a json hint miatt a dlt NEM bontja külön child táblákba, hanem jsonb oszlopba teszi
        resource = dlt.resource(
            fetch_channel_analytics_pipeline(channel, max_videos, max_comments_per_video),
            name="youtube_videos_raw",
            write_disposition="merge",
            primary_key="video_id",
            columns={
                "comments": {"data_type": "json"},
                "transcript_segments": {"data_type": "json"},
            })

        info = pipeline.run(resource)
        logger.info("dlt sikeresen végrehajtva: %s", info)

    except Exception as e:
        logger.exception("pipeline hiba: %s", e)


@router.post("/")
async def trigger_channel_fetch(request: ChannelRequest, background_tasks: BackgroundTasks):

    background_tasks.add_task(
        run_dlt_pipeline,
        request.channel,
        request.max_videos,
        request.max_comments_per_video)

    return {"status": "success", "message": f"Channel: {request.channel} data gathering in background"}
