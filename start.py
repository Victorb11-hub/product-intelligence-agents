"""
Product Intelligence System — Startup Script
==============================================
Starts both services in parallel:
  1. FastAPI agent server on port 8000
  2. React dashboard dev server on port 5173

Kills any stale processes on those ports first,
then starts fresh and runs a health check.

Usage:
  python start.py
"""
import os
import sys
import re
import time
import signal
import subprocess
import threading
from pathlib import Path

# ── Project paths ──
PROJECT_ROOT = Path(__file__).parent
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
AGENTS_DIR = PROJECT_ROOT / "agents"
ENV_FILE = AGENTS_DIR / ".env"

DASHBOARD_PORT = 5173
API_PORT = 8000

# ── Load environment variables from agents/.env ──
def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())
        print("[env] Loaded credentials from agents/.env")
    else:
        print("[env] WARNING: agents/.env not found — servers may fail to connect")

# ── Kill any existing process on a port ──
def kill_port(port):
    """Find and kill whatever is listening on this port (Windows)."""
    killed_pids = set()
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True,
        )
        for line in result.stdout.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                parts = line.strip().split()
                pid = parts[-1]
                if pid.isdigit() and int(pid) > 0 and pid not in killed_pids:
                    subprocess.run(["taskkill", "/F", "/PID", pid],
                                   capture_output=True, text=True)
                    killed_pids.add(pid)
                    print(f"[cleanup] Killed stale process PID {pid} on port {port}")
        return len(killed_pids) > 0
    except Exception:
        pass
    return False

# ── Process tracking ──
processes = []
dashboard_actual_port = DASHBOARD_PORT  # Updated once Vite reports its port

def cleanup(*_):
    print("\n[shutdown] Stopping all services...")
    for name, proc in processes:
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
                print(f"[shutdown] {name} stopped")
            except subprocess.TimeoutExpired:
                proc.kill()
                print(f"[shutdown] {name} killed")
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

# ── Stream process output with prefix, detect Vite port ──
def stream_output(proc, prefix):
    global dashboard_actual_port
    try:
        for line in iter(proc.stdout.readline, b""):
            text = line.decode("utf-8", errors="replace").rstrip()
            if text:
                print(f"[{prefix}] {text}")

                # Detect Vite's actual port from its output
                # Vite prints: "Local:   http://localhost:5173/"
                if prefix == "dashboard":
                    match = re.search(r"Local:\s+http://localhost:(\d+)", text)
                    if match:
                        dashboard_actual_port = int(match.group(1))
    except Exception:
        pass

# ── Start FastAPI server ──
def start_api_server():
    print(f"[api] Starting FastAPI agent server on port {API_PORT}...")

    proc = subprocess.Popen(
        [sys.executable, "-u", "-c",
         "import sys; sys.path.insert(0, r'" + str(PROJECT_ROOT) + "'); "
         "import uvicorn; uvicorn.run('agents.server:app', host='0.0.0.0', port="
         + str(API_PORT) + ", log_level='info')"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(PROJECT_ROOT),
        env={**os.environ},
    )
    processes.append(("API Server", proc))
    threading.Thread(target=stream_output, args=(proc, "api"), daemon=True).start()
    return proc

# ── Start Vite dashboard ──
def start_dashboard():
    print(f"[dashboard] Starting React dashboard on port {DASHBOARD_PORT}...")

    if not (DASHBOARD_DIR / "node_modules").exists():
        print("[dashboard] Installing npm dependencies...")
        subprocess.run(["npm", "install"], cwd=str(DASHBOARD_DIR), check=True,
                       capture_output=True, shell=True)

    proc = subprocess.Popen(
        ["npx", "vite", "--host", "--port", str(DASHBOARD_PORT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(DASHBOARD_DIR),
        shell=True,
        env={**os.environ},
    )
    processes.append(("Dashboard", proc))
    threading.Thread(target=stream_output, args=(proc, "dashboard"), daemon=True).start()
    return proc

# ── Health check ──
def health_check():
    import httpx

    print("\n[health] Running health checks...")
    checks = {"dashboard": False, "api": False, "supabase": False}

    for attempt in range(20):
        time.sleep(2)

        # Check dashboard on the actual port Vite chose
        if not checks["dashboard"]:
            try:
                r = httpx.get(f"http://localhost:{dashboard_actual_port}/", timeout=3)
                if r.status_code == 200:
                    checks["dashboard"] = True
            except Exception:
                pass

        # Check API server
        if not checks["api"]:
            try:
                r = httpx.get(f"http://localhost:{API_PORT}/health", timeout=3)
                if r.status_code == 200:
                    checks["api"] = True
            except Exception:
                pass

        if checks["dashboard"] and checks["api"]:
            break

    # Check Supabase
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_KEY", "")
    active_count = 0
    last_run = "Never"
    next_run = "Not scheduled"

    if supabase_url and supabase_key:
        try:
            headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"}
            r = httpx.get(f"{supabase_url}/rest/v1/products?select=id&active=eq.true",
                          headers=headers, timeout=5)
            if r.status_code == 200:
                checks["supabase"] = True
                active_count = len(r.json())

            r2 = httpx.get(
                f"{supabase_url}/rest/v1/agent_runs?select=completed_at&order=completed_at.desc&limit=1",
                headers=headers, timeout=5,
            )
            if r2.status_code == 200 and r2.json():
                last_run = r2.json()[0].get("completed_at", "Never")
                if last_run and len(last_run) > 19:
                    last_run = last_run[:19].replace("T", " ")

        except Exception:
            pass

    # Get next run from APScheduler (not Supabase schedules table)
    try:
        r_sched = httpx.get(f"http://localhost:{API_PORT}/scheduler/status", timeout=3)
        if r_sched.status_code == 200:
            sched_data = r_sched.json()
            next_run = sched_data.get("next_run", "Not scheduled")
    except Exception:
        pass

    # Print results
    dash_url = f"http://localhost:{dashboard_actual_port}"

    print()
    print("=" * 50)
    if all(checks.values()):
        print("  System ready:")
    else:
        print("  System partially ready:")

    print(f"    Dashboard:        {dash_url if checks['dashboard'] else 'FAILED TO START'}")
    print(f"    Agent server:     http://localhost:{API_PORT}" if checks["api"] else "    Agent server:     FAILED TO START")
    print(f"    Supabase:         {'connected' if checks['supabase'] else 'NOT CONNECTED'}")
    print(f"    Active products:  {active_count}")
    print(f"    Last run:         {last_run}")
    print(f"    Next scheduled:   {next_run}")
    print("=" * 50)

    if not checks["dashboard"]:
        print("\n  FIX: Dashboard failed to start.")
        print("  1. cd dashboard && npm install")
        print(f"  2. Check if port {dashboard_actual_port} is already in use")
    if not checks["api"]:
        print("\n  FIX: API server failed to start.")
        print("  1. pip install fastapi uvicorn")
        print(f"  2. Check if port {API_PORT} is already in use")
    if not checks["supabase"]:
        print("\n  FIX: Supabase not connected.")
        print("  1. Check agents/.env has valid SUPABASE_URL and SUPABASE_KEY")

    print()
    return all(checks.values())


# ── Main ──
if __name__ == "__main__":
    print("=" * 50)
    print("  Product Intelligence System")
    print("  Starting services...")
    print("=" * 50)
    print()

    load_env()

    # ── Backfill mode: --backfill [--product "Name"] ──
    if "--backfill" in sys.argv:
        product_filter = None
        if "--product" in sys.argv:
            try:
                product_filter = sys.argv[sys.argv.index("--product") + 1]
            except IndexError:
                print("[backfill] --product flag requires a value (product name)")
                sys.exit(1)
        print(f"[backfill] Running one-time 365-day historical pull"
              + (f" for {product_filter}" if product_filter else " for all active products"))
        sys.path.insert(0, str(AGENTS_DIR.parent))
        os.environ["BACKFILL_MODE"] = "1"
        if product_filter:
            os.environ["BACKFILL_PRODUCT"] = product_filter
        # Run pipeline directly without starting the scheduler
        from scheduler import run_full_pipeline
        try:
            run_full_pipeline()
            print("[backfill] Historical pull complete. Exiting.")
            sys.exit(0)
        except Exception as e:
            print(f"[backfill] FAILED: {e}")
            sys.exit(1)

    # Kill any stale processes on our ports first
    print("[cleanup] Checking for stale processes...")
    kill_port(DASHBOARD_PORT)
    kill_port(API_PORT)
    time.sleep(1)

    api_proc = start_api_server()
    dash_proc = start_dashboard()
    # Scheduler starts automatically inside the API server via @app.on_event("startup")

    health_thread = threading.Thread(target=health_check, daemon=True)
    health_thread.start()

    try:
        while True:
            for name, proc in processes:
                if proc.poll() is not None:
                    print(f"\n[error] {name} exited with code {proc.returncode}")
                    cleanup()
            time.sleep(5)
    except KeyboardInterrupt:
        cleanup()
