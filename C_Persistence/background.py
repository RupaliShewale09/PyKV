import threading
import time
from .compaction import compact_log

def start_background_compaction(log_file, lock, interval=10):
    """
    Periodically compact WAL in background
    """
    def run():
        while True:
            time.sleep(interval)
            compact_log(log_file, lock)

    t = threading.Thread(target=run, name="WAL-Compaction-Thread", daemon=True)
    t.start()
