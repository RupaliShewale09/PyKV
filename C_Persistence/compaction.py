import json
import os
import tempfile


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
                line = line.strip()
                if not line:
                    continue

                entry = json.loads(line)
                key = entry.get("key")
                op = entry.get("op")

                if op == "DELETE":
                    latest[key] = None
                else:  # SET or UPDATE
                    latest[key] = {
                        "value" : entry.get("value"),
                        "ttl" : entry.get("ttl")
                    }

        dir_name = os.path.dirname(log_file) or "."
        with tempfile.NamedTemporaryFile("w", delete=False, dir=dir_name) as tmp:
            for key, data in latest.items():
                if data is None:
                    tmp.write(json.dumps({
                        "op": "DELETE",
                        "key": key
                    }) + "\n")
                else:
                    tmp.write(json.dumps({
                        "op": "SET",
                        "key": key,
                        "value": data["value"],
                        "ttl" : data["ttl"]
                    }) + "\n")
            temp_name = tmp.name

        # Handle Windows permission errors
        try:
            os.replace(temp_name, log_file)
        except PermissionError:
            # On Windows, if replace fails, try remove and rename
            try:
                os.remove(log_file)
                os.rename(temp_name, log_file)
            except Exception:
                # If still fails, clean up temp file
                try:
                    os.remove(temp_name)
                except Exception:
                    pass
                raise
