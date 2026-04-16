"""Quick script to inspect Supabase table columns via PostgREST."""
import os
from pathlib import Path
from dotenv import load_dotenv
from postgrest import SyncPostgrestClient

# Load env from agents/.env
load_dotenv(Path(__file__).parent / "agents" / ".env")

url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_KEY"]

rest_url = f"{url}/rest/v1"
headers = {
    "apikey": key,
    "Authorization": f"Bearer {key}",
    "Content-Type": "application/json",
}

client = SyncPostgrestClient(rest_url, headers=headers)

for table in ("signals_retail", "comments"):
    print(f"\n{'='*60}")
    print(f"TABLE: {table}")
    print(f"{'='*60}")
    try:
        resp = client.from_(table).select("*").limit(1).execute()
        if resp.data:
            cols = list(resp.data[0].keys())
            print(f"Columns ({len(cols)}):")
            for c in cols:
                print(f"  - {c}: {repr(resp.data[0][c])}")
        else:
            print("  (table exists but is empty)")
    except Exception as e:
        print(f"  ERROR: {e}")
