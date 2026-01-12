import requests
import os
import time

LEADER_PORT = int(os.getenv("LEADER_PORT", 8000))
REPLICA_COUNT = int(os.getenv("REPLICA_COUNT", 2))

BASES = [f"http://127.0.0.1:{LEADER_PORT}"]
for i in range(REPLICA_COUNT):
    BASES.append(f"http://127.0.0.1:{LEADER_PORT + i + 1}")


def get_active_base(write = False):
    for idx, base in enumerate(BASES):
        try:
            requests.get(f"{base}/stats", timeout=1)
            if write and idx != 0:
                continue
            return base, idx == 0
        except:
            continue
    raise Exception("No server available")

def menu():
    print("""
========= PyKV CLI =========
1. Add Key
2. Get Key
3. Update Key
4. Delete Key
5. List Key
6. Display all data
7. Exit
============================
""")

while True:
    menu()
    choice = input("Enter choice: ").strip()

    try:
        if choice in ["1", "3", "4"]:
            BASE, is_leader = get_active_base(write=True)
            if not is_leader:
                print("⚠️ Leader not available, cannot perform writes. Retrying in 2s...")
                time.sleep(2)
                continue
            print(f"Using Leader: {BASE}")

        else:
            BASE, is_leader = get_active_base(write=False)
            if is_leader:
                print(f"Using Leader: {BASE}")
            else:
                print(f"Using Replica: {BASE} (Read-only)")

    except Exception as e:
        print("No server available:", e)
        continue

    match choice:
        case "1":
            key = input("Key: ").strip()
            value = input("Value: ").strip()
            ttl_input = input("TTL in seconds (optional, press Enter to skip): ")
            ttl = int(ttl_input) if ttl_input else None

            res = requests.post(f"{BASE}/kv/", json={"key": key, "value": value, "ttl": ttl})
            print(res.json())

        case "2":
            key = input("Key: ").strip()
            res = requests.get(f"{BASE}/kv/{key}")
            print(res.json())

        case "3":
            key = input("Key: ").strip()
            value = input("New Value: ").strip()
            ttl_input = input("TTL in seconds (optional, press Enter to skip): ")
            ttl = int(ttl_input) if ttl_input else None

            res = requests.put(f"{BASE}/kv/{key}", json={"value": value, "ttl": ttl})
            print(res.json())

        case "4":
            key = input("Key: ").strip()
            res = requests.delete(f"{BASE}/kv/{key}")
            if res.status_code == 204:
                print({"message": "Key deleted"})
            else:
                print(res.json())

        case "5":
            prefix = input("Prefix (press Enter to skip): ")
            params = {"prefix": prefix} if prefix else {}
            res = requests.get(f"{BASE}/kv/", params=params)
            print(res.json())

        case "6":
            res = requests.get(f"{BASE}/kv-items")
            data = res.json().get("items", {})

            if not data:
                print("\n📭 Storage is empty.")
            else:
                print("\n" + "="*50)
                print(f"{'KEY':<15} | {'VALUE':<15} | {'TTL (s)':<10}")
                print("-" * 50)
                for key, info in data.items():
                    key = key
                    val = info['value']
                    ttl = info['ttl']
                    ttl_str = str(ttl) if ttl is not None else "Persistent"
                    print(f"{key:<15} | {val:<15} | {ttl_str:<10}")
                print("="*50)

        case "7":
            print("Exiting PyKV CLI...")
            break

        case _:
            print("Invalid choice. Try again.")
