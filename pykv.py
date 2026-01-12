import subprocess
import os
import sys
import time
import signal
import argparse

# ---------------- Paths ----------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SERVER_FILE = os.path.join(PROJECT_ROOT, "B_server", "server.py")
STREAMLIT_FILE = os.path.join(PROJECT_ROOT, "Client", "app.py")
CLI_FILE = os.path.join(PROJECT_ROOT, "Client", "cli.py")

processes = []

# ---------------- Helpers ----------------
def start_process(cmd, env=None, shell=False):
    p = subprocess.Popen(
        cmd,
        env=env,
        stdout=sys.stdout,
        stderr=sys.stderr,
        shell=shell
    )
    processes.append(p)
    return p

def start_server(role, port, leader_url=None):
    env = os.environ.copy()
    env["ROLE"] = role
    env["PORT"] = str(port)
    if leader_url:
        env["LEADER_URL"] = leader_url

    print(f"🚀 Starting {role} on port {port}")
    start_process([sys.executable, SERVER_FILE], env)

def start_streamlit():
    print("🎨 Starting Streamlit UI")
    start_process(["streamlit", "run", STREAMLIT_FILE])


def start_cli():
    print("💻 Starting PyKV CLI in a new terminal...")
    env = os.environ.copy()

    # Windows: open new cmd window and run CLI (path without extra quotes)
    cmd = f'start cmd /k python "{CLI_FILE}"'
    start_process(cmd, env=env, shell=True)


def shutdown():
    print("\n🛑 Shutting down PyKV cluster...")
    for p in processes:
        p.terminate()
    sys.exit(0)

# ---------------- Main ----------------
if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: shutdown())

    # ---------------- Args ----------------
    parser = argparse.ArgumentParser("PyKV Automatic Cluster Launcher")
    parser.add_argument("--replicas", type=int, default=2, help="Number of replicas")
    parser.add_argument("--port", type=int, default=8000, help="Leader port")
    parser.add_argument("--streamlit", action="store_true", help="Auto start Streamlit UI")
    parser.add_argument("--cli", action="store_true", help="Start CLI after cluster is ready")
    args = parser.parse_args()

    leader_port = args.port
    replica_count = args.replicas

    # Set environment variables for CLI
    os.environ["LEADER_PORT"] = str(leader_port)
    os.environ["REPLICA_COUNT"] = str(replica_count)

    # ---------- Start Leader ----------
    start_server("LEADER", leader_port)
    time.sleep(1)

    # ---------- Start Replicas ----------
    for i in range(replica_count):
        replica_port = leader_port + i + 1
        start_server(
            role="REPLICA",
            port=replica_port,
            leader_url=f"http://127.0.0.1:{leader_port}"
        )

    # ---------- Start Streamlit ----------
    if args.streamlit:
        time.sleep(1)
        start_streamlit()

    # ---------- Start CLI in new terminal ----------
    if args.cli:
        time.sleep(1)
        start_cli()

    # ---------- Info ----------
    print("\n✅ PyKV Cluster Running")
    print(f"Leader   → http://127.0.0.1:{leader_port}")
    for i in range(replica_count):
        print(f"Replica {i+1} → http://127.0.0.1:{leader_port + i + 1}")

    if args.streamlit:
        print("Streamlit → http://localhost:8501")

    print("\nPress CTRL+C to stop everything\n")

    while True:
        time.sleep(1)
