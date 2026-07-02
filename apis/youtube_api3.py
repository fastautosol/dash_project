# 2026.07.02  18.00
import requests
import dlt
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"
router = APIRouter()
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

class ChannelRequest(BaseModel):
    channel_id: str
    max_videos: int = 3                # Alapértelmezetten csak az utolsó 3 videó
    max_comments_per_video: int = 20   # Alapértelmezetten videónként csak 20 komment

def get_channel_videos(channel_id: str, max_videos: int):
    """1. LÉPÉS: Lekéri a csatorna legfrissebb videóit"""
    url = "https://googleapis.com"
    params = {
        "key": YOUTUBE_API_KEY,
        "channelId": channel_id,
        "part": "snippet",
        "order": "date",       # A legfrissebbekkel kezdje
        "type": "video",
        "maxResults": max_videos
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"❌ YouTube API Hiba (Video keresés): {response.text}")
        return []
        
    videos = []
    for item in response.json().get("items", []):
        videos.append({
            "video_id": item["id"]["videoId"],
            "title": item["snippet"]["title"]
        })
    return videos

def fetch_channel_analytics_pipeline(channel_id: str, max_videos: int, max_comments_per_video: int):
    """2. LÉPÉS: Generátor, amely végigmegy a videókon és kigyűjti a kommenteket"""
    
    # Lekérjük a videók listáját
    videos = get_channel_videos(channel_id, max_videos)
    print(f"📹 Talált videók száma a csatornán: {len(videos)}")
    
    for video in videos:
        v_id = video["video_id"]
        v_title = video["title"]
        print(f"💬 Kommentek letöltése a '{v_title}' ({v_id}) videóhoz...")
        
        url = "https://googleapis.com"
        params = {
            "key": YOUTUBE_API_KEY,
            "textFormat": "plainText",
            "part": "snippet",
            "videoId": v_id,
            "maxResults": min(max_comments_per_video, 100) # Maximum 100-at enged a YouTube egyszerre
        }
        
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"⚠️ Nem sikerült letölteni a kommenteket ehhez a videóhoz: {v_id} (Lehet, hogy le vannak tiltva).")
            continue
            
        data = response.json()
        
        # Csak a kért mennyiségű kommentet dolgozzuk fel videónként
        comments = data.get("items", [])[:max_comments_per_video]
        
        for item in comments:
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            
            # Ezt a strukturált rekordot küldjük a dlt-nek
            yield {
                "comment_id": item["id"],
                "video_id": v_id,
                "video_title": v_title, # Extra metaadat, ami jól jön a RAG-nél!
                "channel_id": channel_id,
                "author": snippet["authorDisplayName"],
                "comment_text": snippet["textDisplay"],
                "processed": False
            }

def run_dlt_pipeline(channel_id: str, max_videos: int, max_comments_per_video: int):
    """3. LÉPÉS: A dlt futtatása és Postgresbe mentés"""
    try:
        pipeline = dlt.pipeline(
            pipeline_name="youtube_channel_analytics",
            destination="postgres",
            credentials=DB_CONFIG,
            dataset_name="bronze"
        )

        # dlt 'merge' (upsert) stratégia a duplikációk elkerülésére
        info = pipeline.run(
            fetch_channel_analytics_pipeline(channel_id, max_videos, max_comments_per_video),
            table_name="youtube_comments_raw",
            write_disposition="merge",
            primary_key="comment_id"
        )
        print(f"✅ dlt sikeresen végrehajtva: {info}")

    except Exception as e:
        print(f"❌ pipeline hiba: {str(e)}")

@router.post("/fetch-channel")
async def trigger_channel_fetch(request: ChannelRequest, background_tasks: BackgroundTasks):
    if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == "IDE_ÍRD_A_YOUTUBE_API_KULCSODAT":
        raise HTTPException(status_code=500, detail="Hiányzó YouTube API kulcs!")
        
    background_tasks.add_task(
        run_dlt_pipeline, 
        request.channel_id, 
        request.max_videos, 
        request.max_comments_per_video
    )
    return {
        "status": "success", 
        "message": f"A csatorna ({request.channel_id}) adatainak gyűjtése elindult a háttérben. "
                   f"(Max {request.max_videos} videó, videónként max {request.max_comments_per_video} komment)"
    }

