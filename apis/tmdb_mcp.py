# 2026.10.06 - TMDB MCP szerver az n8n ágensnek (élő TMDB-lekérdezések, NEM a dlt betöltő)
# Kompatibilis az mcp 1.x (FastMCP) és az mcp 2.x (MCPServer) verzióval is.
import asyncio
import logging

from apis.tmdb_api import tmdb_get, clean_text   # ugyanaz a token és hívó, mint a dlt betöltőnél

try:
    from mcp.server.mcpserver import MCPServer as _Server    # mcp >= 2
    MCP_V2 = True
except ImportError:
    from mcp.server.fastmcp import FastMCP as _Server        # mcp < 2
    MCP_V2 = False
from mcp.server.transport_security import TransportSecuritySettings

logger = logging.getLogger(__name__)

# Reverse proxy mögött a Host fejléc a publikus domain, ezt engedélyezni kell
# (különben az SDK DNS-rebinding védelme 421 "Invalid Host header" hibát ad).
ALLOWED_HOSTS = ["app.fastautosol.com", "localhost:*", "127.0.0.1:*"]
_SECURITY = TransportSecuritySettings(enable_dns_rebinding_protection=True, allowed_hosts=ALLOWED_HOSTS)
# stateless + json: nincs munkamenet-állapot, így több worker és proxy mellett is működik
_HTTP_KW = {"stateless_http": True, "json_response": True}

if MCP_V2:
    mcp = _Server("tmdb")
else:
    mcp = _Server("tmdb", transport_security=_SECURITY, **_HTTP_KW)


def mcp_asgi_app():
    """Az ASGI app, amit az app.py a /tmdb alá csatol (végpont: /tmdb/mcp)."""
    if MCP_V2:
        return mcp.streamable_http_app(transport_security=_SECURITY, **_HTTP_KW)
    return mcp.streamable_http_app()


async def _get(endpoint: str, params: dict | None = None):
    # a requests blokkoló, ezért külön szálon fut
    return await asyncio.to_thread(tmdb_get, endpoint, params)


def _brief(m: dict) -> dict:
    return {
        "id": m.get("id"),
        "title": m.get("title"),
        "release_date": m.get("release_date"),
        "vote_average": m.get("vote_average"),
        "overview": clean_text(m.get("overview", ""))[:300],
    }


@mcp.tool()
async def search_movies(query: str, year: int | None = None) -> list[dict]:
    """Filmek keresése cím alapján (opcionálisan megjelenési évvel). Max 10 találat."""
    params = {"query": query, "language": "en-US"}
    if year:
        params["primary_release_year"] = year
    data = await _get("search/movie", params) or {}
    return [_brief(m) for m in data.get("results", [])[:10]]


@mcp.tool()
async def get_movie(movie_id: int) -> dict:
    """Egy film részletes adatai a TMDB ID alapján: rendező, szereplők, műfaj, kulcsszavak, értékelés."""
    m = await _get(f"movie/{movie_id}", {"append_to_response": "credits,keywords", "language": "en-US"})
    if not m:
        return {"error": f"Movie {movie_id} not found"}
    credits = m.get("credits", {})
    return {
        **_brief(m),
        "tagline": m.get("tagline"),
        "runtime": m.get("runtime"),
        "status": m.get("status"),
        "vote_count": m.get("vote_count"),
        "imdb_id": m.get("imdb_id"),
        "collection": (m.get("belongs_to_collection") or {}).get("name"),
        "genres": [g["name"] for g in m.get("genres", [])],
        "keywords": [k["name"] for k in m.get("keywords", {}).get("keywords", [])],
        "director": next((c["name"] for c in credits.get("crew", []) if c.get("job") == "Director"), None),
        "top_cast": [c["name"] for c in credits.get("cast", [])[:10]],
    }


@mcp.tool()
async def find_collection(query: str) -> dict:
    """Filmsorozat/franchise (pl. 'Superman') összes része időrendben. Az első találó gyűjteményt adja."""
    found = await _get("search/collection", {"query": query, "language": "en-US"}) or {}
    results = found.get("results", [])
    if not results:
        return {"error": f"No collection found for '{query}'"}
    col = await _get(f"collection/{results[0]['id']}", {"language": "en-US"}) or {}
    parts = sorted(col.get("parts", []), key=lambda p: p.get("release_date") or "9999")
    return {
        "collection": col.get("name"),
        "other_matches": [r.get("name") for r in results[1:5]],
        "movies": [_brief(p) for p in parts],
    }
