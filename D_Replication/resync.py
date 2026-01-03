import requests

def resync(local_store, replica_url):
    data = local_store.dump_all()
    requests.post(f"{replica_url}/internal/resync", json=data, timeout=3)
