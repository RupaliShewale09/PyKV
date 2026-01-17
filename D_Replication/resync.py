import requests

def resync(username, local_store, replica_url):
    data = local_store.dump_all()
    requests.post(
        f"{replica_url}/internal/resync", 
        json=data, 
        params={"username": username},
        timeout=3
    )
