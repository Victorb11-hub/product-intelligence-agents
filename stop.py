"""
Product Intelligence System — Clean Shutdown
=============================================
Finds and stops both the FastAPI and Vite servers.

Usage:
  python stop.py
"""
import subprocess
import sys
import os


def kill_by_port(port, name):
    """Find and kill process on a specific port (Windows)."""
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True,
        )
        for line in result.stdout.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                parts = line.strip().split()
                pid = parts[-1]
                if pid.isdigit() and int(pid) > 0:
                    subprocess.run(["taskkill", "/F", "/PID", pid],
                                   capture_output=True, text=True)
                    print(f"[stop] {name} (PID {pid} on port {port}) stopped")
                    return True
        print(f"[stop] {name} not running on port {port}")
        return False
    except Exception as e:
        print(f"[stop] Error stopping {name}: {e}")
        return False


if __name__ == "__main__":
    print("Stopping Product Intelligence System...")
    print()
    kill_by_port(8000, "FastAPI Agent Server")
    kill_by_port(5173, "React Dashboard")
    print()
    print("All services stopped.")
