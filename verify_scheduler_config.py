"""
Verify the weekly scheduler config WITHOUT triggering a pipeline run.
Standalone diagnostic — does NOT call scheduler.start().
"""
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

# Patch Path.read_text to use utf-8 by default — needed because scheduler.py
# calls env_file.read_text() with the platform default (cp1252 on Windows),
# which chokes on UTF-8 bytes in agents/.env. We patch BEFORE importing scheduler.
_orig_read_text = Path.read_text
def _utf8_read_text(self, encoding=None, errors=None, newline=None):
    if encoding is None:
        encoding = "utf-8"
    if errors is None:
        errors = "replace"
    return _orig_read_text(self, encoding=encoding, errors=errors, newline=newline)
Path.read_text = _utf8_read_text

# ─── Step 1: Manually parse agents/.env ─────────────────────────────
env_file = PROJECT_ROOT / "agents" / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())
else:
    print(f"ERROR: env file not found at {env_file}")
    sys.exit(1)

print("=" * 70)
print("SCHEDULER CONFIG VERIFICATION (no pipeline run)")
print("=" * 70)

# ─── Step 8 (early): Confirm env loaded ─────────────────────────────
print("\n[ENV VARS]")
for v in ("PIPELINE_SCHEDULE_DAY_OF_WEEK", "PIPELINE_SCHEDULE_HOUR",
          "PIPELINE_SCHEDULE_MINUTE", "LOOKBACK_DAYS_BACKFILL",
          "LOOKBACK_DAYS_WEEKLY", "REDDIT_LOOKBACK_DAYS"):
    print(f"  {v} = {os.environ.get(v, '<unset>')}")

# ─── Step 3-6: Import & create scheduler, inspect job ───────────────
from scheduler import create_scheduler

scheduler = create_scheduler()
job = scheduler.get_job("weekly_pipeline")

print("\n[JOB: weekly_pipeline]")
if job is None:
    print("  ERROR: job 'weekly_pipeline' not found")
    sys.exit(1)

trigger = job.trigger
print(f"  Trigger type:        {type(trigger).__name__}")
print(f"  Trigger module:      {type(trigger).__module__}")

# CronTrigger fields are in trigger.fields list — pull by name
field_map = {f.name: f for f in trigger.fields}
print(f"  day_of_week field:   {field_map.get('day_of_week')}")
print(f"  hour field:          {field_map.get('hour')}")
print(f"  minute field:        {field_map.get('minute')}")
print(f"  Full trigger repr:   {trigger!r}")
print(f"  Full trigger str:    {trigger}")

# ─── Step 7: next_run_time ──────────────────────────────────────────
print("\n[NEXT RUN TIME]")
try:
    next_run = job.next_run_time
    print(f"  job.next_run_time (pre-start): {next_run}")
except AttributeError:
    # In APScheduler 3.x, next_run_time is only populated after the job is
    # added to a STARTED scheduler. Tentatively-added jobs lack the attribute.
    print(f"  job.next_run_time:             <not set — scheduler not started>")

# Compute manually from trigger — this works without start()
manual_next = trigger.get_next_fire_time(None, datetime.now(timezone.utc))
print(f"  trigger.get_next_fire_time:    {manual_next}")
if manual_next:
    delta = manual_next - datetime.now(manual_next.tzinfo)
    print(f"  Time until next run:           {delta}")

# ─── Step 9: Test get_lookback_days ─────────────────────────────────
print("\n[LOOKBACK LOGIC TESTS]")

# Suppress noisy logger output during tests
import logging
logging.getLogger("agents.base_platform_agent").setLevel(logging.WARNING)

# Build a TikTokAgent-like object without invoking __init__ (avoids supabase call)
from agents.base_platform_agent import BasePlatformAgent

class FakeTikTok:
    PLATFORM = "tiktok"
    # Bind method from the class
    get_lookback_days = BasePlatformAgent.get_lookback_days

class FakeReddit:
    PLATFORM = "reddit"
    get_lookback_days = BasePlatformAgent.get_lookback_days

tiktok = FakeTikTok()
reddit = FakeReddit()

# Save original BACKFILL_MODE
orig_backfill = os.environ.get("BACKFILL_MODE")

# Case (a): BACKFILL_MODE=1, no first_scraped_at
os.environ["BACKFILL_MODE"] = "1"
product_a = {"name": "test_product_a"}  # no first_scraped_at, no backfill_completed
days_a = tiktok.get_lookback_days(product_a)
print(f"  (a) BACKFILL_MODE=1, no first_scraped_at  -> {days_a}  {'PASS' if days_a == 365 else 'FAIL (expected 365)'}")

# Case (b): no backfill flag, no first_scraped_at
os.environ.pop("BACKFILL_MODE", None)
product_b = {"name": "test_product_b"}
days_b = tiktok.get_lookback_days(product_b)
print(f"  (b) first run (no first_scraped_at)        -> {days_b}  {'PASS' if days_b == 365 else 'FAIL (expected 365)'}")

# Case (c): no backfill, with first_scraped_at + backfill_completed=True
product_c = {
    "name": "test_product_c",
    "first_scraped_at": "2025-01-01T00:00:00Z",
    "backfill_completed": True,
}
days_c = tiktok.get_lookback_days(product_c)
print(f"  (c) weekly run (already backfilled)        -> {days_c}  {'PASS' if days_c == 7 else 'FAIL (expected 7)'}")

# Case (d): Reddit always 90 — test in all three contexts
os.environ["BACKFILL_MODE"] = "1"
days_d1 = reddit.get_lookback_days(product_a)
os.environ.pop("BACKFILL_MODE", None)
days_d2 = reddit.get_lookback_days(product_b)
days_d3 = reddit.get_lookback_days(product_c)
ok_d = (days_d1 == 90 and days_d2 == 90 and days_d3 == 90)
print(f"  (d) Reddit always 90: backfill={days_d1}, first={days_d2}, weekly={days_d3}  {'PASS' if ok_d else 'FAIL'}")

# Restore env
if orig_backfill is not None:
    os.environ["BACKFILL_MODE"] = orig_backfill
else:
    os.environ.pop("BACKFILL_MODE", None)

print("\n" + "=" * 70)
print("VERIFICATION COMPLETE — scheduler.start() was NOT called")
print("=" * 70)
