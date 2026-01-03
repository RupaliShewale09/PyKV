import aiohttp, asyncio, time
from D_Replication.health import get_healthy_replicas
from D_Replication.config import ENABLE_TIMESTAMPS, MAX_RETRIES

async def replicate_async(op, key, value=None):
    payload = {
        "op": op,
        "key": key,
        "value": value,
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
