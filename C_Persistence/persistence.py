import json
import os
from datetime import datetime
from threading import Lock

from .recover import recover
from .background import start_background_compaction

class Persistence:
    """
    Log every write operation
    """
    def __init__(self, store, log_file="D:\Programs\python\PyKV\data\wal.log"):
        self.store = store
        self.log_file = log_file
        self.lock = Lock()

        log_dir = os.path.dirname(self.log_file)
        if log_file:
            os.makedirs(log_dir, exist_ok=True)   # ensure directory eists
        recover(self.store, self.log_file)
        start_background_compaction(
            log_file=self.log_file,
            lock=self.lock,
            interval=10   
        )


    def _append_log(self, op, key, value=None):     # Log entry in JSON form
        entry = {
            "time": datetime.utcnow().isoformat(),
            "op": op,
            "key": key,
            "value": value
        }

        with self.lock:
            with open(self.log_file, "a", buffering=1) as f:
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())


    # Write operations-------------------------------------------
    def put(self, key, value):
        ok = self.store.put(key, value)
        if ok:
            self._append_log("SET", key, value)
        return ok

    def update(self, key, value):
        ok = self.store.update(key, value)
        if ok:
            self._append_log("UPDATE", key, value)
        return ok

    def delete(self, key):
        ok = self.store.delete(key)
        if ok:
            self._append_log("DELETE", key)
        return ok

    # read operations ----------------------------------------------
    def get(self, key):             # read only - do not modify data
        return self.store.get(key)

    def list_keys(self, prefix=None):
        return self.store.list_keys(prefix)

