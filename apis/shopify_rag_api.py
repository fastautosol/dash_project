# 2026.11.20  14.00
import asyncio
import os
import asyncpg
import httpx
from pydantic import BaseModel
from pydantic_ai import Agent
from typing import List, Optional
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

# 1. Batch Input és Output sémák
class SingleTicketIn(BaseModel):
    order_id: Optional[str] = None  # Opcionális azonosító, hogy visszakövethető legyen az n8n-ben
    uzenet: str

class BatchQueryIn(BaseModel):
    items: List[SingleTicketIn]

class BatchTicketOut(BaseModel):
    order_id: Optional[str] = None
    uzenet: str
    result: Optional[TicketResult] = None
    error: Optional[str] = None

# 2. Batch embedding lekérdezés (egyetlen HTTP kéréssel az összes üzenetre)
async def get_batch_embeddings(queries: List[str]) -> List[str]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        res = await client.post("https://api.mistral.ai/v1/embeddings", headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"}, json={"model": "mistral-embed", "input": queries})
        res.raise_for_status()
        sorted_data = sorted(res.json()["data"], key=lambda x: x["index"])

# Már nincs szükség @agent.tool-ra, mert determinisztikusan keresünk előtte
agent = Agent("mistral:ministral-8b-latest", output_type=TicketResult,
    system_prompt="Osztályozd az ügyfélüzenetet a megadott hasonló korábbi esetek alapján!")

async def get_similar_examples(conn: asyncpg.Connection, query: str) -> str:
    async with httpx.AsyncClient(timeout=20.0) as client:
        res = await client.post("https://api.mistral.ai/v1/embeddings", headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"}, json={"model": "mistral-embed", "input": [query]})
        res.raise_for_status()
        vec = f"[{','.join(map(str, res.json()['data'][0]['embedding']))}]"

    rows = await conn.fetch("SELECT text FROM shopify_vectors ORDER BY embedding <=> $1::vector LIMIT 3", vec)
    return "\n---\n".join(r["text"] for r in rows)

# ---------- Single-RAG végpont ---------
@router.post("/test-rag", response_model=TicketResult)
async def test_rag(body: QueryIn):
    if not MISTRAL_API_KEY:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY nincs beállítva.")
    conn = None
    try:
        conn = await asyncpg.connect(host="postgresql", port=5432, database="n8n", user="sql_admin", password="sql_pass")
        pelda_esetek = await get_similar_examples(conn, body.uzenet)
        await asyncio.sleep(1.5)
        prompt = f"Hasonló korábbi esetek:\n{pelda_esetek}\n\nÚj ügyfélüzenet:\n{body.uzenet}"
        result = await agent.run(prompt)
        return result.output
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            await conn.close()

# ---------- Multi-RAG végpont ---------
@router.post("/multi-rag", response_model=List[BatchTicketOut])
async def multi_rag(body: BatchQueryIn):
    if not body.items:
        return []
    if not MISTRAL_API_KEY:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY nincs beállítva.")

    conn = None
    semaphore = asyncio.Semaphore(1) # Mistral ingyenes csomagon max_concurrent=1; fizetős/Gemini/OpenAI esetén emelhető 5-10-re

    try:
        conn = await asyncpg.connect(host="postgresql", port=5432, database="n8n", user="sql_admin", password="sql_pass")
        uzenetek = [item.uzenet for item in body.items]
        vectors = await get_batch_embeddings(uzenetek)
        async def process_single(item: SingleTicketIn, vec: str) -> BatchTicketOut:
            async with semaphore:
                try:
                    rows = await conn.fetch("SELECT text FROM shopify_vectors ORDER BY embedding <=> $1::vector LIMIT 3", vec)
                    pelda_esetek = "\n---\n".join(r["text"] for r in rows)
                    await asyncio.sleep(1.2)  # Rate limit védelem az ingyenes Mistral csomaghoz
                    prompt = f"Hasonló korábbi esetek:\n{pelda_esetek}\n\nÚj ügyfélüzenet:\n{item.uzenet}"
                    classification = await run_agent_with_retry(prompt)
                    return BatchTicketOut(order_id=item.order_id, uzenet=item.uzenet, result=classification)
                except Exception as e:
                    # Ha 1 elem hibázik, a batch többi része nem áll le
                    return BatchTicketOut(order_id=item.order_id, uzenet=item.uzenet, error=f"{type(e).__name__}: {e}")

        # Figyelem: egyetlen asyncpg.Connection nem támogatja a párhuzamos lekérdezéseket,
        # ezért Semaphore(1) mellett szekvenciálisan fut; magasabb concurrency-hez asyncpg.create_pool kell.
        results = []
        for item, vec in zip(body.items, vectors):
            results.append(await process_single(item, vec))

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            await conn.close()
