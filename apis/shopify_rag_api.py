# 2026.11.20  14.00
import asyncio
import os
from typing import List, Optional
import asyncpg
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pydantic_ai import Agent

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

class SingleTicketIn(BaseModel):
    order_id: Optional[str] = None
    uzenet: str

class BatchQueryIn(BaseModel):
    items: List[SingleTicketIn]

class BatchTicketOut(BaseModel):
    order_id: Optional[str] = None
    uzenet: str
    result: Optional[TicketResult] = None
    error: Optional[str] = None

agent = Agent("mistral:ministral-8b-latest", output_type=TicketResult, system_prompt="Osztályozd az ügyfélüzenetet a megadott hasonló korábbi esetek alapján!")

async def get_batch_embeddings(queries: List[str]) -> List[str]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        res = await client.post("https://api.mistral.ai/v1/embeddings", headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"}, json={"model": "mistral-embed", "input": queries})
        res.raise_for_status()
        sorted_data = sorted(res.json()["data"], key=lambda x: x["index"])
        return [f"[{','.join(map(str, item['embedding']))}]" for item in sorted_data]

async def fetch_similar_examples(conn: asyncpg.Connection, vec: str, limit: int = 3) -> str:
    rows = await conn.fetch("SELECT text FROM shopify_vectors ORDER BY embedding <=> $1::vector LIMIT $2", vec, limit)
    return "\n---\n".join(r["text"] for r in rows)

async def run_agent_with_retry(prompt: str, retries: int = 3) -> TicketResult:
    for attempt in range(retries):
        try:
            result = await agent.run(prompt)
            return result.output
        except Exception as e:
            if "429" in str(e) and attempt < retries - 1:
                await asyncio.sleep(6 * (attempt + 1))
                continue
            raise

async def get_db_conn() -> asyncpg.Connection:
    return await asyncpg.connect(host="postgresql", port=5432, database="n8n", user="sql_admin", password="sql_pass")


# ---------- Single-RAG végpont ----------
@router.post("/test-rag", response_model=TicketResult)
async def test_rag(body: QueryIn):
    if not MISTRAL_API_KEY:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY nincs beállítva.")

    conn = None
    try:
        conn = await get_db_conn()
        vec = (await get_batch_embeddings([body.uzenet]))[0]
        pelda_esetek = await fetch_similar_examples(conn, vec)
        await asyncio.sleep(1.2)

        prompt = f"Hasonló korábbi esetek:\n{pelda_esetek}\n\nÚj ügyfélüzenet:\n{body.uzenet}"
        return await run_agent_with_retry(prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            await conn.close()


# ---------- Multi-RAG végpont ----------
@router.post("/multi-rag", response_model=List[BatchTicketOut])
async def multi_rag(body: BatchQueryIn):
    if not body.items:
        return []
    if not MISTRAL_API_KEY:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY nincs beállítva.")

    conn = None
    try:
        conn = await get_db_conn()
        uzenetek = [item.uzenet for item in body.items]
        vectors = await get_batch_embeddings(uzenetek)
        results: List[BatchTicketOut] = []
        for item, vec in zip(body.items, vectors):
            try:
                pelda_esetek = await fetch_similar_examples(conn, vec)
                await asyncio.sleep(1.2)
                prompt = f"Hasonló korábbi esetek:\n{pelda_esetek}\n\nÚj ügyfélüzenet:\n{item.uzenet}"
                classification = await run_agent_with_retry(prompt)
                results.append(BatchTicketOut(order_id=item.order_id, uzenet=item.uzenet, result=classification))
            except Exception as e:
                results.append(BatchTicketOut(order_id=item.order_id, uzenet=item.uzenet, error=f"{type(e).__name__}: {e}"))

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            await conn.close()
