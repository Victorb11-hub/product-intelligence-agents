"""
Smoke test for BasePlatformAgent.compute_comment_tiers() via TikTokAgent.
NO Apify calls — pure logic test.
"""
import os
import sys
import random
from pathlib import Path

# Force UTF-8 stdout/stderr so emdashes and arrows don't crash on Windows cp1252
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ─── 1. Load agents/.env manually (utf-8) and add project root to sys.path ───
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

ENV_PATH = PROJECT_ROOT / "agents" / ".env"
if ENV_PATH.exists():
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            # Don't clobber existing env vars already set by the shell
            os.environ.setdefault(key, val)
    print(f"[env] Loaded env from: {ENV_PATH}")
else:
    print(f"[env] WARNING — no .env at {ENV_PATH}")


def _show_tier_result(label: str, tiers: dict):
    print(f"\n--- {label} ---")
    print(f"tier1 posts: {len(tiers['tier1'])}  limit/post: {tiers['tier1_limit']}")
    print(f"tier2 posts: {len(tiers['tier2'])}  limit/post: {tiers['tier2_limit']}")
    print(f"tier3 posts: {len(tiers['tier3'])}  limit/post: {tiers['tier3_limit']}")
    print(f"total comment limit: {tiers['total_limit']:,}")
    print("breakdown:", tiers.get("breakdown"))
    for tname in ("tier1", "tier2", "tier3"):
        ids = [p.get("id") for p in tiers[tname]]
        print(f"  {tname} post ids: {ids}")


def build_synthetic():
    random.seed(42)
    posts = []
    for i in range(20):
        posts.append({
            "id": f"vid_{i}",
            "webVideoUrl": f"https://tiktok.com/@x/video/{i}",
            "playCount": random.randint(10_000, 5_000_000),
            "diggCount": random.randint(100, 500_000),
            "commentCount": random.randint(10, 2_000),
        })
    posts.sort(key=lambda p: p["playCount"], reverse=True)
    return posts


def main():
    # ─── Import the agent (triggers BaseAgent.__init__ → get_supabase) ───
    from agents.agent_tiktok import TikTokAgent
    agent = TikTokAgent()
    print(f"[agent] instantiated: {agent.__class__.__name__}  PLATFORM={agent.PLATFORM}")

    # ═══ Test 1: default thresholds (0.10, 0.30) with 20 posts ═══
    print("\n" + "=" * 70)
    print("TEST 1 — Default tiers (PASS2_TIER1=0.10, PASS2_TIER2=0.30), 20 posts")
    print("=" * 70)

    # Force defaults by clearing any tier env overrides
    for key in ("PASS2_TIERS_ENABLED", "PASS2_TIER1_THRESHOLD",
                "PASS2_TIER2_THRESHOLD", "PASS2_COMMENTS_TIER1",
                "PASS2_COMMENTS_TIER2", "PASS2_COMMENTS_TIER3",
                "PASS2_COMMENTS_PER_POST"):
        os.environ.pop(key, None)

    posts = build_synthetic()
    print("Posts by views (desc):")
    for p in posts:
        print(f"  {p['id']}: {p['playCount']:>10,} views")

    tiers = agent.compute_comment_tiers(posts, lambda p: p["playCount"])
    _show_tier_result("Default tiering result", tiers)

    # Sanity check expected: tier1=2, tier2=4, tier3=14, total = 2*1000+4*500+14*200 = 6800
    expected_total = 2 * 1000 + 4 * 500 + 14 * 200
    assert len(tiers["tier1"]) == 2, f"expected 2 tier1, got {len(tiers['tier1'])}"
    assert len(tiers["tier2"]) == 4, f"expected 4 tier2, got {len(tiers['tier2'])}"
    assert len(tiers["tier3"]) == 14, f"expected 14 tier3, got {len(tiers['tier3'])}"
    assert tiers["total_limit"] == expected_total, \
        f"expected total {expected_total}, got {tiers['total_limit']}"
    print(f"\nPASS — 2/4/14 split, total_limit={tiers['total_limit']} matches expected {expected_total}")

    # ═══ Test 2: tiers disabled → everyone in tier3 with flat limit ═══
    print("\n" + "=" * 70)
    print("TEST 2 -- PASS2_TIERS_ENABLED=0 -> flat limit for all")
    print("=" * 70)
    os.environ["PASS2_TIERS_ENABLED"] = "0"
    os.environ["PASS2_COMMENTS_PER_POST"] = "50"
    posts = build_synthetic()
    tiers = agent.compute_comment_tiers(posts, lambda p: p["playCount"])
    _show_tier_result("Disabled tiering result", tiers)
    assert len(tiers["tier1"]) == 0
    assert len(tiers["tier2"]) == 0
    assert len(tiers["tier3"]) == 20
    assert tiers["tier3_limit"] == 50
    assert tiers["total_limit"] == 50 * 20
    assert tiers["breakdown"]["tiers_enabled"] is False
    print("\nPASS — all 20 posts in tier3, flat limit 50, total 1000")

    # Reset
    os.environ.pop("PASS2_TIERS_ENABLED", None)
    os.environ.pop("PASS2_COMMENTS_PER_POST", None)

    # ═══ Test 3: edge case — empty list ═══
    print("\n" + "=" * 70)
    print("TEST 3 — Edge case: empty list")
    print("=" * 70)
    tiers = agent.compute_comment_tiers([], lambda p: p["playCount"])
    _show_tier_result("Empty list result", tiers)
    assert len(tiers["tier1"]) == 0
    assert len(tiers["tier2"]) == 0
    assert len(tiers["tier3"]) == 0
    assert tiers["total_limit"] == 0
    print("\nPASS — empty input returns 0/0/0 tiers and total_limit=0")

    # ═══ Test 4: edge case — single post ═══
    print("\n" + "=" * 70)
    print("TEST 4 — Edge case: single post (should land in tier1)")
    print("=" * 70)
    single = [{"id": "solo_0", "playCount": 123456}]
    tiers = agent.compute_comment_tiers(single, lambda p: p["playCount"])
    _show_tier_result("Single post result", tiers)
    assert len(tiers["tier1"]) == 1, f"expected 1 tier1, got {len(tiers['tier1'])}"
    assert len(tiers["tier2"]) == 0
    assert len(tiers["tier3"]) == 0
    assert tiers["tier1"][0]["id"] == "solo_0"
    print("\nPASS — single post routed to tier1 (max(1, int(1*0.10))=1)")

    # ═══ Test 5: custom thresholds 0.25 / 0.50 ═══
    print("\n" + "=" * 70)
    print("TEST 5 — Custom thresholds PASS2_TIER1_THRESHOLD=0.25, PASS2_TIER2_THRESHOLD=0.50")
    print("=" * 70)
    os.environ["PASS2_TIER1_THRESHOLD"] = "0.25"
    os.environ["PASS2_TIER2_THRESHOLD"] = "0.50"
    posts = build_synthetic()
    tiers = agent.compute_comment_tiers(posts, lambda p: p["playCount"])
    _show_tier_result("Custom thresholds result", tiers)
    # Expected: tier1 = int(20*0.25)=5, tier2 = int(20*0.50)-5 = 10-5=5, tier3 = 10
    assert len(tiers["tier1"]) == 5, f"expected 5 tier1, got {len(tiers['tier1'])}"
    assert len(tiers["tier2"]) == 5, f"expected 5 tier2, got {len(tiers['tier2'])}"
    assert len(tiers["tier3"]) == 10, f"expected 10 tier3, got {len(tiers['tier3'])}"
    expected_total_c = 5 * 1000 + 5 * 500 + 10 * 200
    assert tiers["total_limit"] == expected_total_c
    print(f"\nPASS — 5/5/10 split at thresholds 0.25/0.50, total_limit={tiers['total_limit']}")

    os.environ.pop("PASS2_TIER1_THRESHOLD", None)
    os.environ.pop("PASS2_TIER2_THRESHOLD", None)

    print("\n" + "=" * 70)
    print("ALL SMOKE TESTS PASSED")
    print("=" * 70)


if __name__ == "__main__":
    main()
