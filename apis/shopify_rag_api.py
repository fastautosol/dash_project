# 2026.11.19  18.00
import asyncio
import os
import asyncpg
import httpx
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext

MISTRAL_API_KEY = os.environ["MISTRAL_API_KEY"]

# 1. Mit várunk vissza az AI-tól
class TicketResult(BaseModel):
    tag: str
    reason: str
    reply_hu: str

# 2. Agent létrehozása (a deps maga az SQL kapcsolat)
agent = Agent(
    "mistral:mistral-small-latest",
    deps_type=asyncpg.Connection,
    output_type=TicketResult,
    system_prompt="Osztályozd az ügyfélüzenetet! Először mindig hívd meg a `search_vectors` eszközt hasonló példákért.",
)

# 3. RAG kereső eszköz (pgvector)
@agent.tool
async def search_vectors(ctx: RunContext[asyncpg.Connection], query: str) -> str:
    """Hasonló múltbeli esetek keresése a shopify_vectors táblából."""
    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.mistral.ai/v1/embeddings",
            headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"},
            json={"model": "mistral-embed", "input": [query]},
        )
        vec = f"[{','.join(map(str, res.json()['data'][0]['embedding']))}]"

    rows = await ctx.deps.fetch(
        "SELECT text FROM shopify_vectors ORDER BY embedding <=> $1::vector LIMIT 3",
        vec,
    )
    return "\n---\n".join(r["text"] for r in rows)

# 4. Futás tesztelése
async def main():
    conn = await asyncpg.connect(
        host="postgresql", port=5432, database="n8n", user="sql_admin", password="sql_pass"
    )
    try:
        uzenet = "Nem kaptam meg a PDF letöltési linket a rendelés után, pedig kifizettem."
        result = await agent.run(uzenet, deps=conn)
        print(result.output.model_dump_json(indent=2))
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
