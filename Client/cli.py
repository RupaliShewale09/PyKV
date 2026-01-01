import requests

BASE = "http://127.0.0.1:8000"

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
            res = requests.post(f"{BASE}/kv/", json={"key": key, "value": value})
            print(res.json())

        case "2":
            key = input("Key: ")
            res = requests.get(f"{BASE}/kv/{key}")
            print(res.json())

        case "3":
            key = input("Key: ")
            value = input("New Value: ")
            res = requests.put(f"{BASE}/kv/{key}", json={"value": value})
            print(res.json())

        case "4":
            key = input("Key: ")
            res = requests.delete(f"{BASE}/kv/{key}")
            if res.status_code == 204:
                print({"message": "Key deleted"})


        case "5":
            prefix = input("Prefix (press Enter to skip): ")
            params = {"prefix": prefix} if prefix else {}
            res = requests.get(f"{BASE}/kv/", params=params)
            print(res.json())

        case "6":
            print("Exiting PyKV CLI...")
            break

        case _:
            print("Invalid choice. Try again.")
