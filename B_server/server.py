from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from contextlib import asynccontextmanager
import threading
import uvicorn

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from A_Core.store import CoreStore
from C_Persistence.persistence import Persistence
from D_Replication.replicator import replicate_async
from D_Replication.health import health_monitor

# -------------------- Models --------------------
class KeyValue(BaseModel):
    key: str
    value: str
    ttl : int | None = None

class ValueOnly(BaseModel):
    value: str
    ttl : int | None = None

class ReplicationRequest(BaseModel):
    op: str
    key: str
    value: str | None = None
    timestamp: float | None = None

# ----------------- FastAPI Setup -------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(
        target=health_monitor,
        args=(store,),
        daemon=True
    ).start()
    yield

app = FastAPI(title="PyKV Server", lifespan=lifespan)

core = CoreStore(capacity=100)
store = Persistence(core)

# ----------------- Routes -------------------
@app.post("/kv/", status_code=status.HTTP_201_CREATED)
async def add(item: KeyValue):          # client sends key & value
    if not store.put(item.key, item.value, ttl=item.ttl):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Key already exists")
    await replicate_async("SET", item.key, item.value)
    return {"message": "Key added"}


@app.get("/kv/{key}", status_code=status.HTTP_200_OK)       
async def get(key: str):          # lookup key
    value = store.get(key)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Key not found"
        )
    return {"key": key, "value": value}


@app.put("/kv/{key}", status_code=status.HTTP_200_OK)
async def update(key: str, item: ValueOnly):      # Update value
    if not store.update(key, item.value, ttl=item.ttl):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Key not found"
        )
    await replicate_async("UPDATE", key, item.value)
    return {"message": "Key updated"}


@app.delete("/kv/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(key: str):        # Delete key
    if not store.delete(key):       
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Key not found"
        )
    replicate_async("DELETE", key)
    return 


@app.get("/kv/", status_code=status.HTTP_200_OK)       
async def list_keys(prefix: str = None):      # List keys
    return {"keys": store.list_keys(prefix)}


@app.get("/stats", status_code=status.HTTP_200_OK)
def get_stats():
    cache = core.cache

    return {
        "capacity": cache.capacity,
        "size": len(cache.map),
        "evictions": getattr(cache, "evictions", 0),
        "hits": getattr(cache, "hits", 0),
        "misses": getattr(cache, "misses", 0)
    }


# ---------- Internal (replica only) ----------
@app.post("/internal/replicate")
async def internal_replicate(req: ReplicationRequest):
    """
    Receives replicated writes from another server
    """
    if req.op == "SET":
        store.put(req.key, req.value)
    elif req.op == "UPDATE":
        store.update(req.key, req.value)
    elif req.op == "DELETE":
        store.delete(req.key)

    return {"status": "replicated"}

@app.post("/internal/resync")
async def internal_resync(data: dict):
    """
    Full resync from primary
    """
    # Clear existing data
    for key in list(store.list_keys()):
        store.delete(key)

    # Rebuild store
    for key, value in data.items():
        store.put(key, value)

    return {"status": "resynced"}

# ---------- Run ----------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="127.0.0.1", port=port)