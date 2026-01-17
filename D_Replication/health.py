import time
import requests
from .config import REPLICA_URLS, HEALTH_CHECK_INTERVAL
from .resync import resync

_healthy = set()
_last_healthy = set()

def health_monitor(user_stores):
    global _healthy, _last_healthy
    while True:
        new_healthy = set()
        for url in REPLICA_URLS:
            try:
                r = requests.get(f"{url}/stats", timeout=1)
                if r.status_code == 200:
                    new_healthy.add(url)
            except:
                pass
        
        recovered = new_healthy - _last_healthy
        
        for url in recovered:
            for username, store_instance in user_stores.items():
                try:
                    resync(user_stores, url)
                except:
                    continue

        _last_healthy = new_healthy
        _healthy = _last_healthy.copy()

        time.sleep(HEALTH_CHECK_INTERVAL)

def get_healthy_replicas():
    return list(_healthy)
