import time
import requests
from D_Replication.config import REPLICA_URLS, HEALTH_CHECK_INTERVAL
from D_Replication.resync import resync

_healthy = set()
_last_healthy = set()

def health_monitor(store):
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
            try:
                resync(store, url)
            except:
                pass

        _last_healthy = new_healthy
        _healthy = new_healthy

        time.sleep(HEALTH_CHECK_INTERVAL)

def get_healthy_replicas():
    return list(_healthy)
