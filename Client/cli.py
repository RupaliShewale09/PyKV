import requests

BASES = ["http://127.0.0.1:8000", "http://127.0.0.1:8001"]

def get_active_base():
    for base in BASES:
        try:
            requests.get(f"{base}/stats", timeout=1)
            return base
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
5. List Keys
6. Exit
============================
""")

while True:
    menu()
    choice = input("Enter choice: ")

    match choice:
        case "1":
            key = input("Key: ")
            value = input("Value: ")
            BASE = get_active_base()
            res = requests.post(f"{BASE}/kv/", json={"key": key, "value": value})
            print(res.json())

        case "2":
            key = input("Key: ")
            BASE = get_active_base()
            res = requests.get(f"{BASE}/kv/{key}")
            print(res.json())

        case "3":
            key = input("Key: ")
            value = input("New Value: ")
            BASE = get_active_base()
            res = requests.put(f"{BASE}/kv/{key}", json={"value": value})
            print(res.json())

        case "4":
            key = input("Key: ")
            BASE = get_active_base()
            res = requests.delete(f"{BASE}/kv/{key}")
            if res.status_code == 204:
                print({"message": "Key deleted"})


        case "5":
            prefix = input("Prefix (press Enter to skip): ")
            params = {"prefix": prefix} if prefix else {}
            BASE = get_active_base()
            res = requests.get(f"{BASE}/kv/", params=params)
            print(res.json())

        case "6":
            print("Exiting PyKV CLI...")
            break

        case _:
            print("Invalid choice. Try again.")
