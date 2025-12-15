from fastapi import FastAPI, HTTPException
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

# ----------------- Routes -------------------
@app.post("/kv/")
async def add(item: KeyValue):          # client sends key & value
    if not store.put(item.key, item.value):
        raise HTTPException(400, "Key already exists")
    return {"message": "Key added"}

@app.get("/kv/{key}")       
async def get(key: str):          # lookup key
    value = store.get(key)
    if value is None:
        raise HTTPException(404, "Key not found")
    return {"key": key, "value": value}

@app.put("/kv/{key}")
async def update(key: str, item: ValueOnly):      # Update value
    if not store.update(key, item.value):
        raise HTTPException(404, "Key not found")
    return {"message": "Key updated"}

@app.delete("/kv/{key}")
async def delete(key: str):        # Delete key
    if not store.delete(key):       
        raise HTTPException(404, "Key not found")
    return {"message": "Key deleted"}

@app.get("/kv/")       
async def list_keys(prefix: str = None):      # List keys
    return {"keys": store.list_keys(prefix)}

# ----------------- Run -------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
