# 2026.10.06 - TMDB MCP szerver

import asyncio
import logging
from apis.tmdb_api import tmdb_get, clean_text

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

logger = logging.getLogger(__name__)

mcp = MCPServer("tmdb")

def mcp_asgi_app():
    """
    Az ASGI app, amit az app.py a /tmdb alá csatol.
    Végpont:
    https://fastautosol.com
    """
    # MIGRATION FIX: Configuration properties move from constructor to app builder method
    security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
    
    return mcp.streamable_http_app(
        stateless_http=True,
        transport_security=security
    )

async def _get(endpoint: str, params: dict | None = None):
    """
    A tmdb_get requests alapú, ezért külön szálon fut.
    """
    return await asyncio.to_thread(
        tmdb_get,
        endpoint,
        params,
    )

def _brief(movie: dict) -> dict:
    return {
        "id": movie.get("id"),
        "title": movie.get("title"),
        "release_date": movie.get("release_date"),
        "vote_average": movie.get("vote_average"),
        "overview": clean_text(movie.get("overview", ""))[:300],
    }

@mcp.tool()
async def search_movies(
    query: str,
    year: int = 0,
) -> list[dict]:
    """
    Film keresése cím alapján.
    year = opcionális megjelenési év.
    """
    params = {
        "query": query,
        "language": "en-US",
    }

    if year:
        params["primary_release_year"] = year

    data = await _get("search/movie", params) or {}

    return [
        _brief(movie)
        for movie in data.get("results", [])[:10]
    ]

@mcp.tool()
async def get_movie(
    movie_id: int,
) -> dict:
    """
    Részletes filmadatok TMDB ID alapján.
    """
    movie = await _get(
        f"movie/{movie_id}",
        {
            "append_to_response": "credits,keywords",
            "language": "en-US",
        },
    )

    if not movie:
        return {"error": f"Movie {movie_id} not found"}

    credits = movie.get("credits", {})
    keywords_payload = movie.get("keywords", {})
    
    keywords_list = (
        keywords_payload.get("keywords", []) 
        if isinstance(keywords_payload, dict) 
        else []
    )

    return {
        **_brief(movie),
        "tagline": movie.get("tagline"),
        "runtime": movie.get("runtime"),
        "status": movie.get("status"),
        "vote_count": movie.get("vote_count"),
        "imdb_id": movie.get("imdb_id"),
        "collection": (movie.get("belongs_to_collection") or {}).get("name"),
        "genres": [g["name"] for g in movie.get("genres", []) if "name" in g],
        "keywords": [k["name"] for k in keywords_list if "name" in k],
        "director": next(
            (
                c["name"]
                for c in credits.get("crew", [])
                if c.get("job") == "Director"
            ),
            None,
        ),
        "top_cast": [
            c["name"]
            for c in credits.get("cast", [])[:10]
        ],
    }

@mcp.tool()
async def find_collection(
    query: str,
) -> dict:
    """
    Franchise / collection keresés.
    Például: Superman, Batman, James Bond
    """
    found = await _get(
        "search/collection",
        {
            "query": query,
            "language": "en-US",
        },
    ) or {}

    results = found.get("results", [])

    if not results:
        return {"error": f"No collection found for '{query}'"}

    # BUGFIX: Safely fetch the ID from the index dict slice
    first_match_id = results[0].get('id') if results else None
    if not first_match_id:
         return {"error": "Invalid collection data format."}

    collection = await _get(
        f"collection/{first_match_id}",
        {
            "language": "en-US",
        },
    ) or {}

    parts = sorted(
        collection.get("parts", []),
        key=lambda p: p.get("release_date") or "9999",
    )

    return {
        "collection": collection.get("name"),
        "other_matches": [r.get("name") for r in results[1:5]],
        "movies": [_brief(movie) for movie in parts],
    }
