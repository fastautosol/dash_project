# 2026.11.19  18.00
import asyncio
import os
import asyncpg
import httpx
from pydantic import BaseModel
from pydantic_ai import Agent
from fastapi import APIRouter, HTTPException

router = APIRouter()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "mYhK6JLDugBGRQiVigUwzjVXX4fPropK")
if MISTRAL_API_KEY:
    os.environ["MISTRAL_API_KEY"] = MISTRAL_API_KEY

class QueryIn(BaseModel):
    uzenet: str

class TicketResult(BaseModel):
    tag: str
    reason: str
    reply_hu: str

# Már nincs szükség @agent.tool-ra, mert determinisztikusan keresünk előtte
agent = Agent("mistral:mistral-small-latest", output_type=TicketResult,
    system_prompt="Osztályozd az ügyfélüzenetet a megadott hasonló korábbi esetek alapján!")

async def get_similar_examples(conn: asyncpg.Connection, query: str) -> str:
    async with httpx.AsyncClient(timeout=20.0) as client:
        res = await client.post("https://api.mistral.ai/v1/embeddings", headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"},
            json={"model": "mistral-embed", "input": [query]})
        res.raise_for_status()
        vec = f"[{','.join(map(str, res.json()['data'][0]['embedding']))}]"

    rows = await conn.fetch("SELECT text FROM shopify_vectors ORDER BY embedding <=> $1::vector LIMIT 3", vec)
    return "\n---\n".join(r["text"] for r in rows)

@router.post("/test-rag", response_model=TicketResult)
async def test_rag(body: QueryIn):
    if not MISTRAL_API_KEY:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY nincs beállítva.")

    conn = None
    try:
        conn = await asyncpg.connect(host="postgresql", port=5432, database="n8n", user="sql_admin", password="sql_pass")
        # 1. Vektoros keresés
        pelda_esetek = await get_similar_examples(conn, body.uzenet)
        
        # 2. Szünet a Mistral ingyenes tier 1 kérés/mp (RPS) limitje miatt
        await asyncio.sleep(1.5)

        # 3. Egyetlen LLM hívás a kontextussal
        prompt = f"Hasonló korábbi esetek:\n{pelda_esetek}\n\nÚj ügyfélüzenet:\n{body.uzenet}"
        result = await agent.run(prompt)
        return result.output
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            await conn.close()
