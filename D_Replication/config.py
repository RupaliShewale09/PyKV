import os

ROLE = os.getenv("ROLE", "LEADER")   # LEADER | REPLICA
IS_LEADER = ROLE == "LEADER"

# Base configuration for dynamic discovery
LEADER_PORT = int(os.getenv("LEADER_PORT", 8000))
REPLICA_COUNT = int(os.getenv("REPLICA_COUNT", 2))
LEADER_URL = os.getenv("LEADER_URL", f"http://127.0.0.1:{LEADER_PORT}")

# Generate the replica list dynamically to match the CLI and Launcher logic
REPLICA_URLS = [
    f"http://127.0.0.1:{LEADER_PORT + i + 1}" 
    for i in range(REPLICA_COUNT)
]

HEALTH_CHECK_INTERVAL = 5
REPLICATION_TIMEOUT = 2
MAX_RETRIES = 3

ENABLE_TIMESTAMPS = True
ENABLE_RESYNC = True