from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from A_Core.store import CoreStore  
from C_Persistence.persistence import Persistence

# -------------------- Models --------------------
class KeyValue(BaseModel):
    key: str
    value: str

class ValueOnly(BaseModel):
    value: str

# ----------------- FastAPI Setup -------------------
app = FastAPI(title="PyKV ")

core = CoreStore(capacity=100) 
store = Persistence(core)

@app.on_event("startup")
def startup_event():
    pass

# ----------------- Routes -------------------
@app.post("/kv/", status_code=status.HTTP_201_CREATED)
async def add(item: KeyValue):          # client sends key & value
    if not store.put(item.key, item.value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Key already exists")
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
    if not store.update(key, item.value):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Key not found"
        )
    return {"message": "Key updated"}


@app.delete("/kv/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(key: str):        # Delete key
    if not store.delete(key):       
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Key not found"
        )
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

# ----------------- Run -------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
