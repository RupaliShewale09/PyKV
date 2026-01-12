import requests
import time
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- Configuration ----------------

BASE_URL = "http://127.0.0.1:8000"
TOTAL_REQUESTS = 5000
THREADS = 20

# ---------------- Utilities ----------------

def random_key():
    return ''.join(random.choices(string.ascii_lowercase, k=10))

def random_value():
    return ''.join(random.choices(string.ascii_letters, k=20))

# ---------------- SET Benchmark ----------------

def set_key(key):
    try:
        r = requests.post(
            f"{BASE_URL}/kv/",
            json={"key": key, "value": random_value()},
            timeout=5
        )
        return r.status_code == 201
    except:
        return False

def benchmark_set():
    keys = [random_key() for _ in range(TOTAL_REQUESTS)]

    start = time.perf_counter()
    success = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(set_key, k) for k in keys]
        for f in as_completed(futures):
            if f.result():
                success += 1

    end = time.perf_counter()
    duration = end - start

    success_rate = (success / TOTAL_REQUESTS) * 100
    throughput = TOTAL_REQUESTS / duration
    avg_latency = (duration / TOTAL_REQUESTS) * 1000

    print("\n--- PyKV SET Benchmark ---")
    print(f"Total Requests : {TOTAL_REQUESTS}")
    print(f"Successful     : {success}")
    print(f"Success Rate   : {success_rate:.2f}%")
    print(f"Total Time     : {duration:.2f} sec")
    print(f"Throughput     : {throughput:.2f} ops/sec")
    print(f"Avg Latency    : {avg_latency:.2f} ms")

    return keys

# ---------------- GET Benchmark ----------------

def get_key(key):
    try:
        r = requests.get(f"{BASE_URL}/kv/{key}", timeout=5)
        return r.status_code == 200
    except:
        return False

def benchmark_get(keys):
    start = time.perf_counter()
    success = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(get_key, k) for k in keys]
        for f in as_completed(futures):
            if f.result():
                success += 1

    end = time.perf_counter()
    duration = end - start

    success_rate = (success / len(keys)) * 100
    throughput = len(keys) / duration
    avg_latency = (duration / len(keys)) * 1000

    print("\n--- PyKV GET Benchmark ---")
    print(f"Total Requests : {len(keys)}")
    print(f"Successful     : {success}")
    print(f"Success Rate   : {success_rate:.2f}%")
    print(f"Total Time     : {duration:.2f} sec")
    print(f"Throughput     : {throughput:.2f} ops/sec")
    print(f"Avg Latency    : {avg_latency:.2f} ms")

# ---------------- Python dict Baseline ----------------

def benchmark_dict():
    d = {}
    keys = [random_key() for _ in range(TOTAL_REQUESTS)]

    start = time.perf_counter()
    for k in keys:
        d[k] = random_value()
    for k in keys:
        _ = d[k]
    end = time.perf_counter()

    duration = end - start
    throughput = (TOTAL_REQUESTS * 2) / duration

    print("\n--- Python dict Baseline ---")
    print(f"Total Requests : {TOTAL_REQUESTS * 2}")
    print(f"Total Time     : {duration:.6f} sec")
    print(f"Throughput     : {throughput:.2f} ops/sec")

# ---------------- Run ----------------

if __name__ == "__main__":
    print("Starting PyKV Benchmark...")

    inserted_keys = benchmark_set()
    benchmark_get(inserted_keys)
    benchmark_dict()
