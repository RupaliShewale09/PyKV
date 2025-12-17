import json
import os


def compact_log(log_file, lock):
    """
    Rewrite WAL as a self-contained snapshot
    """
    if not os.path.exists(log_file):
        return

    latest = {}

    with lock:
        with open(log_file, "r") as f:
            for line in f:
                entry = json.loads(line)
                key = entry["key"]
                op = entry["op"]

                if op == "DELETE":
                    latest[key] = None
                else:  # SET or UPDATE
                    latest[key] = entry["value"]

        with open(log_file, "w") as f:
            for key, value in latest.items():
                if value is None:
                    f.write(json.dumps({
                        "op": "DELETE",
                        "key": key
                    }) + "\n")
                else:
                    f.write(json.dumps({
                        "op": "SET",
                        "key": key,
                        "value": value
                    }) + "\n")

