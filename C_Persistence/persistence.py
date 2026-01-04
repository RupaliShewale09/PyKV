import json
import os
from datetime import datetime
from threading import Lock, Thread, Event
import time
import logging

from .recover import recover
from .background import start_background_compaction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
)

class Persistence:
    """
    Log every write operation
    """
    def __init__(self, store, log_file=r"D:\Programs\python\PyKV\data\wal.log", flush_interval=1, max_batch=50):
        self.store = store
        self.log_file = log_file
        self.lock = Lock()
        self.queue = []                  # in-memory WAL buffer
        self.flush_interval = flush_interval
        self.max_batch = max_batch
        self.stop_event = Event()

        log_dir = os.path.dirname(self.log_file)
        if log_file:
            os.makedirs(log_dir, exist_ok=True)   # ensure directory eists
        recover(self.store, self.log_file)

        self.worker = Thread(target=self._wal_writer, daemon=True, name="Async-WAL-Writer")
        self.worker.start()

        start_background_compaction(
            log_file=self.log_file,
            lock=self.lock,
            interval=10   
        )
        logging.info("Persistence layer initialized")

    # ---------------- WAL enqueue ----------------
    def _enqueue_log(self, op, key, value=None, ttl=None):
        entry = {
            "time": datetime.utcnow().isoformat(),
            "op": op,
            "key": key,
            "value": value,
            "ttl": ttl
        }
        with self.lock:
            self.queue.append(entry)

    # ---------------- Background WAL writer ----------------
    def _wal_writer(self):
        logging.info("WAL writer thread started")
        while not self.stop_event.is_set():
            try:
                self._flush_queue()
            except Exception as e:
                # This should NEVER kill the thread
                logging.critical(
                    "Unexpected WAL writer error (thread kept alive): %s",
                    e,
                    exc_info=True,
                )
                time.sleep(1)

            time.sleep(self.flush_interval)

        # Final flush during shutdown
        logging.info("WAL writer flushing remaining entries before shutdown")
        self._flush_queue()

    def _flush_queue(self):
        with self.lock:
            if not self.queue:
                return
            batch = self.queue[:self.max_batch]
            self.queue = self.queue[self.max_batch:]

        # Write batch to WAL safely
        try:
            with open(self.log_file, "a", buffering=1) as f:
                for entry in batch:
                    f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
                    logging.error(
                        "WAL flush failed. Re-queueing batch. Error: %s", e,
                        exc_info=True
                    )

                    # Re-queue failed batch at the front (preserve order)
                    with self.lock:
                        self.queue = batch + self.queue

                    # Prevent busy spinning on persistent failure
                    time.sleep(1)

    # ---------------- Write operations ----------------
    def put(self, key, value, ttl=None):
        self._enqueue_log("SET", key, value, ttl)
        return self.store.put(key, value, ttl)

    def update(self, key, value, ttl=None):
        self._enqueue_log("UPDATE", key, value, ttl)
        return self.store.update(key, value, ttl)

    def delete(self, key):
        self._enqueue_log("DELETE", key)
        return self.store.delete(key)

    # ---------------- Read operations ----------------
    def get(self, key):
        return self.store.get(key)

    def list_keys(self, prefix=None):
        return self.store.list_keys(prefix)
    
    def dump_all(self):
        return self.store.dump_all()

    # ---------------- Graceful shutdown ----------------
    def shutdown(self):
        """Flush any remaining WAL entries and stop background thread"""
        logging.info("Shutting down persistence layer")
        self.stop_event.set()
        self.worker.join()
        logging.info("Persistence layer stopped cleanly")

