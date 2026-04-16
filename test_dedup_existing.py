"""
Test that dedup works correctly using EXISTING DB data — NO new Apify calls.
Pulls 20 existing reddit comments from DB, re-feeds them through write_comments_to_db,
and verifies they are all detected as duplicates.
"""
import os
import sys
import uuid
import logging
from pathlib import Path

# ─── Setup paths and env ───────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

ENV_PATH = PROJECT_ROOT / "agents" / ".env"
with open(ENV_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip surrounding quotes if present
        if value and value[0] in ('"', "'") and value[-1] == value[0]:
            value = value[1:-1]
        os.environ.setdefault(key, value)

# ─── Imports (must come AFTER env is loaded) ───────────────────────
from agents.agent_reddit import RedditAgent

# ─── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("agents.base_platform_agent").setLevel(logging.INFO)
logging.getLogger("agents.base_agent").setLevel(logging.INFO)

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"


def section(title: str):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def count_rows(supabase, table: str, product_id: str, platform: str) -> int:
    """Return count of rows in `table` for product_id+platform. Uses Prefer:count=exact."""
    # postgrest-py supports .execute(count="exact") via select; simpler: fetch ids and len()
    # but ids may exceed default limit. Use head request via count param.
    try:
        resp = (
            supabase.table(table)
            .select("id", count="exact")
            .eq("product_id", product_id)
            .eq("platform", platform)
            .limit(1)
            .execute()
        )
        return resp.count or 0
    except TypeError:
        # Fallback: paginate by id
        total = 0
        page = 0
        page_size = 1000
        while True:
            r = (
                supabase.table(table)
                .select("id")
                .eq("product_id", product_id)
                .eq("platform", platform)
                .range(page * page_size, (page + 1) * page_size - 1)
                .execute()
            )
            rows = r.data or []
            total += len(rows)
            if len(rows) < page_size:
                break
            page += 1
        return total


def main():
    section("STEP 1-4: Initialize agent")
    agent = RedditAgent()
    agent.run_id = str(uuid.uuid4())
    print(f"Agent created. run_id = {agent.run_id}")
    print(f"Platform = {agent.PLATFORM}")
    supabase = agent.supabase

    # ─── Step 5: Baseline counts ───────────────────────────────────
    section("STEP 5: Baseline counts BEFORE test")
    posts_before = count_rows(supabase, "posts", PRODUCT_ID, "reddit")
    comments_before = count_rows(supabase, "comments", PRODUCT_ID, "reddit")
    print(f"posts (product={PRODUCT_ID}, platform=reddit):    {posts_before}")
    print(f"comments (product={PRODUCT_ID}, platform=reddit): {comments_before}")

    # ─── Step 6: Pull 20 existing reddit comments ──────────────────
    section("STEP 6: Pull 20 existing reddit comment-rows from DB")
    resp = (
        supabase.table("posts")
        .select("id,reddit_id,post_body,intent_level,sentiment_score,data_type,platform,product_id")
        .eq("product_id", PRODUCT_ID)
        .eq("platform", "reddit")
        .eq("data_type", "comment")
        .not_.is_("reddit_id", "null")
        .limit(20)
        .execute()
    )
    rows = resp.data or []
    print(f"Pulled {len(rows)} existing comment-rows from posts table")
    if not rows:
        print("ERROR: no existing rows to test against. Aborting.")
        return 1
    for i, r in enumerate(rows[:5], 1):
        body_preview = (r.get("post_body") or "")[:60].replace("\n", " ")
        print(f"  [{i}] reddit_id={r['reddit_id']}  body='{body_preview}...'")
    if len(rows) > 5:
        print(f"  ... and {len(rows) - 5} more")

    # ─── Step 7: Reformat into write_comments_to_db format ─────────
    section("STEP 7: Reformat rows for write_comments_to_db")
    reformatted = []
    for r in rows:
        intent_level = r.get("intent_level") or 1
        reformatted.append({
            "text": r.get("post_body") or "",
            "id": r["reddit_id"],
            "cid": r["reddit_id"],
            "_intent_level": intent_level,
            "_intent_score": intent_level * 0.2,
            "_sentiment": r.get("sentiment_score") or 0,
            "_is_purchase": False,
            "_is_negative": False,
            "_is_question": False,
            "_weight": 1.0,
        })
    print(f"Reformatted {len(reformatted)} comments")

    # ─── Step 8: Call write_comments_to_db ─────────────────────────
    section("STEP 8: Call write_comments_to_db (expect ALL skipped as duplicates)")
    written = agent.write_comments_to_db(reformatted, PRODUCT_ID)
    print(f"write_comments_to_db returned: {written}")

    # ─── Step 9: Inspect last_dedup_stats ──────────────────────────
    section("STEP 9: agent.last_dedup_stats")
    stats = getattr(agent, "last_dedup_stats", None)
    if stats is None:
        print("ERROR: agent.last_dedup_stats not set!")
    else:
        print(f"  written:           {stats.get('written')}")
        print(f"  batch_skipped:     {stats.get('batch_skipped')}")
        print(f"  constraint_skipped:{stats.get('constraint_skipped')}")
        print(f"  null_id_skipped:   {stats.get('null_id_skipped')}")
        print(f"  total_skipped:     {stats.get('total_skipped')}")

    # ─── Step 10: Counts AFTER test ────────────────────────────────
    section("STEP 10: Counts AFTER test (deltas should be 0)")
    posts_after = count_rows(supabase, "posts", PRODUCT_ID, "reddit")
    comments_after = count_rows(supabase, "comments", PRODUCT_ID, "reddit")
    posts_delta = posts_after - posts_before
    comments_delta = comments_after - comments_before
    print(f"posts:    before={posts_before}  after={posts_after}  delta={posts_delta:+d}")
    print(f"comments: before={comments_before}  after={comments_after}  delta={comments_delta:+d}")

    if posts_delta == 0 and comments_delta == 0:
        print("PASS: no new rows inserted")
    else:
        print("FAIL: dedup did not block all writes")

    # ─── Step 11: Test dedup_check_batch directly ──────────────────
    section("STEP 11: Test dedup_check_batch with mix of real + fake IDs")
    real_ids = [r["reddit_id"] for r in rows[:10]]
    fake_ids = [f"fake_id_{i}" for i in range(1, 6)]
    combined = real_ids + fake_ids
    print(f"Calling dedup_check_batch with {len(real_ids)} real + {len(fake_ids)} fake = {len(combined)} IDs")
    print(f"  Real IDs: {real_ids}")
    print(f"  Fake IDs: {fake_ids}")
    found = agent.dedup_check_batch(combined, "reddit", PRODUCT_ID)
    print(f"\nReturned {len(found)} IDs detected as existing:")
    for fid in sorted(found):
        is_fake = fid.startswith("fake_id_")
        marker = "  <-- FAKE (BUG!)" if is_fake else ""
        print(f"  - {fid}{marker}")

    real_found = found & set(real_ids)
    fake_found = found & set(fake_ids)
    print(f"\nReal IDs detected as existing: {len(real_found)}/{len(real_ids)}")
    print(f"Fake IDs detected as existing: {len(fake_found)}/{len(fake_ids)} (should be 0)")
    if len(real_found) == len(real_ids) and len(fake_found) == 0:
        print("PASS: dedup_check_batch correctly identified real vs fake IDs")
    else:
        print("FAIL: dedup_check_batch result is incorrect")

    section("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
