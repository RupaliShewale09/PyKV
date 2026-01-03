import json
import os


def recover(store, log_file):
    """
    Replay WAL to rebuild in-memory state
    """
    if not os.path.exists(log_file):
        return

    with open(log_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            entry = json.loads(line)
            op = entry.get("op")
            key = entry.get("key")
            value = entry.get("value")
            ttl = entry.get("ttl")

            if op == "SET":
                store.put(key, value, ttl)
            elif op == "UPDATE":
                store.update(key, value, ttl)
            elif op == "DELETE":
                store.delete(key)
