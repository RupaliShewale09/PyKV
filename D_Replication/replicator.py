import aiohttp, asyncio, time
from .health import get_healthy_replicas
from .config import ENABLE_TIMESTAMPS, MAX_RETRIES, IS_LEADER


async def replicate_async(op, key, value=None, ttl=None):
    if not IS_LEADER:
        return
    
    payload = {
        "op": op,
        "key": key,
        "value": value,
        "ttl" : ttl,
        "timestamp": time.time() if ENABLE_TIMESTAMPS else None
    }

    for url in get_healthy_replicas():
        asyncio.create_task(_send(url, payload))

async def _send(url, payload):
    for _ in range(MAX_RETRIES):
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{url}/internal/replicate", json=payload)
                return
        except:
            await asyncio.sleep(1)
