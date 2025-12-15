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
            entry = json.loads(line)
            op = entry["op"]
            key = entry["key"]
            value = entry.get("value")

            if op == "SET":
                store.put(key, value)
            elif op == "UPDATE":
                store.update(key, value)
            elif op == "DELETE":
                store.delete(key)
