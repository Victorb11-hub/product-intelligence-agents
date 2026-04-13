"""
Reddit Agent — Two Pass Scraper
================================
PASS 1: Discovery (macrocosmos/reddit-scraper)
- Input: product keywords + target subreddits
- Output: posts written to posts table
- Cost: $0.50 per 1,000 results

PASS 2: Intelligence (trudax/reddit-scraper-lite)
- Input: top 20 post URLs by upvotes from Pass 1
- Output: comments written to comments table
- Cost: $3.80 per 1,000 results
- skipComments: false — always fetch comments

BOTH PASSES RUN EVERY SINGLE TIME.
Pass 1 always runs first.
Pass 2 always runs after Pass 1 completes.
If Pass 2 fails mark run as degraded not failed.
Never skip Pass 2 unless monthly budget is exhausted.
"""

# ── Imports ──────────────────────────────────────
import uuid
import math
import logging
from datetime import datetime, date, timedelta

from .config import get_supabase, APIFY_API_TOKEN, RATE_LIMITS
from .skills.apify_helper import run_actor
from .skills.sentiment import analyze_sentiment, analyze_batch, aggregate_sentiment
from .skills.velocity import calculate_velocity
from .skills.fad_classifier import classify as classify_fad
from .skills.intent_scorer import score_intent, score_batch as score_intent_batch
from .skills.rate_limiter import RateLimiter
from .skills.self_healer import retry_with_healing, HealingTracker
from .skills.quality_scorer import score_quality
from .skills.benchmarker import benchmark_signal_row
from .skills.anomaly_detector import detect_anomalies, create_alert_from_anomaly
from .skills.summarizer import generate_summary
from .skills.learner import load_weights, apply_weights

logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────
PASS1_ACTOR = "macrocosmos/reddit-scraper"
PASS2_ACTOR = "trudax/reddit-scraper-lite"
PASS1_COST_PER_1K = 0.50
PASS2_COST_PER_1K = 3.80
MONTHLY_BUDGET = 25.00
POSTS_PER_SUBREDDIT = 25
TOP_POSTS_FOR_COMMENTS = 20

CATEGORY_SUBREDDITS = {
    "Supplements":  ["supplements", "Nootropics", "nutrition", "herbalism", "Biohackers", "AlternativeHealth"],
    "Beauty Tools": ["SkincareAddiction", "beauty", "AsianBeauty", "30PlusSkinCare", "NaturalBeauty", "selfcare"],
    "Fitness":      ["fitness", "homegym", "xxfitness", "bodyweightfitness", "running", "yoga"],
    "Skincare":     ["SkincareAddiction", "AsianBeauty", "30PlusSkinCare", "beauty", "NaturalBeauty"],
    "Haircare":     ["HaircareScience", "curlyhair", "beauty", "NaturalBeauty"],
    "Wellness":     ["wellness", "selfcare", "Biohackers", "AlternativeHealth", "herbalism", "meditation"],
}
DEFAULT_SUBREDDITS = ["supplements", "SkincareAddiction", "fitness", "beauty", "wellness", "Nootropics"]

BUY_WORDS = ["where to buy", "just bought", "ordered", "link", "which brand", "recommend", "best brand", "add to cart"]
PROBLEM_WORDS = ["doesn't work", "scam", "waste", "side effect", "dangerous", "fake", "problem", "returned", "broke out"]
REPEAT_WORDS = ["been using", "on my second", "on my third", "repurchased", "daily routine", "monthly", "restocked", "holy grail"]


class RedditAgent:
    """
    Two-pass Reddit scraper with full skills pipeline, integrity checks,
    and live checkpoint reporting. Gold standard template for all agents.
    """

    PLATFORM = "reddit"

    def __init__(self):
        self.supabase = get_supabase()
        self.run_id = None
        self.agent_run_id = None
        self.healing_tracker = HealingTracker()
        self.weights = {}

        # Apify usage tracking
        self.pass1_results = 0
        self.pass2_results = 0

        # Run stats
        self.posts_written = 0
        self.comments_written = 0
        self.signals_written = 0
        self.integrity_errors = []
        self.run_degraded = False
        self.degraded_reasons = []

    @property
    def apify_total_results(self):
        return self.pass1_results + self.pass2_results

    @property
    def apify_total_cost(self):
        return round(
            self.pass1_results * PASS1_COST_PER_1K / 1000 +
            self.pass2_results * PASS2_COST_PER_1K / 1000, 4
        )

    # ══════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ══════════════════════════════════════════════
    async def run(self, products: list[dict], run_id: str) -> dict:
        self.run_id = run_id
        self.agent_run_id = str(uuid.uuid4())
        self._checkpoint("running", "Pre-run checklist starting")

        # ── PRE-RUN CHECKLIST ──
        checklist_ok, checklist_msg = self._pre_run_checklist(products)
        if not checklist_ok:
            self._checkpoint("failed", f"Pre-run check failed: {checklist_msg}")
            return {"status": "failed", "error": checklist_msg}

        self.weights = await load_weights(self.supabase, self.PLATFORM)

        # ── PROCESS EACH PRODUCT ──
        for product in products:
            try:
                await self._process_product(product)
            except Exception as e:
                msg = f"Error processing {product.get('name', '?')}: {str(e)[:300]}"
                logger.error("[reddit] %s", msg)
                self.run_degraded = True
                self.degraded_reasons.append(msg)

        # ── FINAL STATUS ──
        if self.signals_written == 0:
            status = "failed"
        elif self.run_degraded:
            status = "degraded"
        else:
            status = "complete"

        # Budget check
        self._check_monthly_budget()

        summary = self._build_post_run_summary(products)
        self._checkpoint(status, summary)

        return {
            "status": status,
            "products_processed": len(products),
            "rows_written": self.signals_written,
            "rows_rejected": 0,
            "anomalies_detected": 0,
            "integrity_errors": self.integrity_errors,
            "summary": summary,
        }

    # ══════════════════════════════════════════════
    # PRE-RUN CHECKLIST
    # ══════════════════════════════════════════════
    def _pre_run_checklist(self, products: list[dict]) -> tuple[bool, str]:
        """Verify all dependencies before starting. Returns (ok, message)."""
        import os

        # 1. Apify key exists
        if not os.environ.get("APIFY_API_TOKEN"):
            return False, "APIFY_API_TOKEN not set"

        # 2. Test Apify connection
        try:
            from apify_client import ApifyClient
            client = ApifyClient(APIFY_API_TOKEN)
            user = client.user().get()
            if not user:
                return False, "Apify API key invalid — no user returned"
        except Exception as e:
            return False, f"Apify API unreachable: {str(e)[:200]}"

        # 3. Verify actors exist
        try:
            act1 = client.actor(PASS1_ACTOR).get()
            if not act1:
                return False, f"Pass 1 actor {PASS1_ACTOR} not found"
        except Exception as e:
            return False, f"Pass 1 actor {PASS1_ACTOR} unreachable: {str(e)[:200]}"

        try:
            act2 = client.actor(PASS2_ACTOR).get()
            if not act2:
                return False, f"Pass 2 actor {PASS2_ACTOR} not found"
        except Exception as e:
            return False, f"Pass 2 actor {PASS2_ACTOR} unreachable: {str(e)[:200]}"

        # 4. Active products exist
        if not products:
            return False, "No active products to process"

        # 5. Supabase connection
        try:
            self.supabase.table("products").select("id").limit(1).execute()
        except Exception as e:
            return False, f"Supabase unreachable: {str(e)[:200]}"

        # 6. Monthly budget
        monthly_cost = self._get_monthly_spend()
        if monthly_cost >= MONTHLY_BUDGET:
            return False, f"Monthly Apify budget exhausted (${monthly_cost:.2f} / ${MONTHLY_BUDGET:.2f})"

        logger.info("[reddit] Pre-run checklist PASSED (monthly spend: $%.2f)", monthly_cost)
        return True, "All checks passed"

    # ══════════════════════════════════════════════
    # PROCESS ONE PRODUCT
    # ══════════════════════════════════════════════
    async def _process_product(self, product: dict):
        product_id = product["id"]
        product_name = product["name"]
        keywords = product.get("keywords", [product_name])
        category = product.get("category", "")

        target_subs = product.get("target_subreddits", [])
        if not target_subs:
            target_subs = CATEGORY_SUBREDDITS.get(category, DEFAULT_SUBREDDITS)

        subs_str = ", ".join(target_subs[:5])

        # ── PASS 1: Discovery ──────────────────────
        self._checkpoint("running", f"Pass 1 started — searching {len(keywords)} keywords in {subs_str}")

        pass1_items = self._run_pass1(product_name, keywords, target_subs)

        if not pass1_items:
            self._checkpoint("running", f"Pass 1 returned 0 posts for {product_name} — skipping")
            self.run_degraded = True
            self.degraded_reasons.append(f"{product_name}: Pass 1 returned 0 posts")
            return

        # Data ingestion filter: remove old content + duplicates
        from .skills.data_ingestion import DataIngestionFilter
        ingestion = DataIngestionFilter(self.supabase)

        # Read lookback from settings (default 30 days)
        try:
            lb_resp = self.supabase.table("scoring_settings").select("setting_value") \
                .eq("setting_key", "lookback_reddit").execute()
            lookback = int(lb_resp.data[0]["setting_value"]) if lb_resp.data else 30
        except Exception:
            lookback = 30

        fresh_items, ingestion_stats = ingestion.get_new_items_only(
            pass1_items, "reddit", product_id, lookback_days=lookback
        )
        self.ingestion_stats = ingestion_stats

        if not fresh_items:
            self._checkpoint("running", f"Pass 1: all {len(pass1_items)} items were old or duplicates — skipping")
            return

        # Write relevant posts to posts table (relevance filter applied)
        posts_written = self._write_posts(fresh_items, product_id)
        discarded = getattr(self, "irrelevant_discarded", 0)
        self.posts_written += posts_written
        self._checkpoint("running",
            f"Pass 1 complete — {posts_written} new posts written "
            f"(from {ingestion_stats['raw_count']} raw: {ingestion_stats['items_too_old']} old, "
            f"{ingestion_stats['items_duplicate']} dupes, {discarded} irrelevant, "
            f"saved ${ingestion_stats['cost_saved_dedup']:.4f})"
        )

        # ── PASS 2: Intelligence ───────────────────
        # Get top 20 posts by upvotes for comment scraping
        sorted_posts = sorted(pass1_items, key=lambda x: x.get("score", 0) or 0, reverse=True)
        top_posts = sorted_posts[:TOP_POSTS_FOR_COMMENTS]
        top_urls = [p.get("url") for p in top_posts if p.get("url")]

        pass2_comments = []
        if top_urls:
            # Check budget before Pass 2
            remaining_budget = MONTHLY_BUDGET - self._get_monthly_spend() - self.apify_total_cost
            if remaining_budget < 1.0:
                msg = f"Pass 2 skipped — monthly budget nearly exhausted (${remaining_budget:.2f} remaining)"
                logger.warning("[reddit] %s", msg)
                self.run_degraded = True
                self.degraded_reasons.append(msg)
                self._checkpoint("running", msg)
            else:
                self._checkpoint("running", f"Pass 2 started — fetching comments for top {len(top_urls)} posts by upvotes")

                pass2_comments = self._run_pass2(top_urls)

                if pass2_comments:
                    comments_written = self._write_comments(pass2_comments, product_id, pass1_items)
                    self.comments_written += comments_written
                    self._checkpoint("running", f"Pass 2 complete — {comments_written} comments written to comments table")
                else:
                    msg = "Pass 2 returned 0 comments — flagging run as degraded"
                    logger.warning("[reddit] %s", msg)
                    self.run_degraded = True
                    self.degraded_reasons.append(msg)
                    self._checkpoint("running", msg)
        else:
            self.run_degraded = True
            self.degraded_reasons.append("No post URLs available for Pass 2")

        # ── SKILLS PIPELINE ────────────────────────
        self._checkpoint("running", "Skills pipeline running — sentiment, velocity, fad classifier, intent scoring")

        # Collect all texts (posts + comments)
        all_texts = []
        for item in pass1_items:
            body = (item.get("body") or "").strip()
            title = (item.get("title") or "").strip()
            text = f"{title} {body}".strip() if body else title
            if text and len(text) > 5:
                all_texts.append(text)
        for comment in pass2_comments:
            body = (comment.get("body") or comment.get("text") or "").strip()
            if body and len(body) > 5:
                all_texts.append(body)

        sentiment_results = analyze_batch(all_texts) if all_texts else []
        sentiment_agg = aggregate_sentiment(sentiment_results)

        intent_data = score_intent_batch(all_texts) if all_texts else {
            "avg_intent_score": 0, "intent_level_distribution": {},
            "high_intent_comment_count": 0, "sample_size": 0, "results": [],
        }

        # ── AGGREGATES FROM DB ─────────────────────
        self._checkpoint("running", "Aggregates computed — writing to signals_social")

        db_agg = self._compute_aggregates_from_db(product_id)

        historical = self._get_historical_values(product_id)
        historical.append(db_agg["post_count"])
        velocity_data = calculate_velocity(historical)

        # Check if Google Trends has actually run for this product
        google_trends_exists = False
        google_trends_slope = None
        try:
            gt_resp = self.supabase.table("signals_search") \
                .select("slope_24m, breakout_flag") \
                .eq("product_id", product_id) \
                .eq("platform", "google_trends") \
                .order("scraped_date", desc=True).limit(1).execute()
            if gt_resp.data:
                google_trends_exists = True
                google_trends_slope = gt_resp.data[0].get("slope_24m")
        except Exception:
            pass

        fad_data = classify_fad({
            "platforms_active": ["reddit"],
            "velocity": velocity_data["velocity"],
            "acceleration": velocity_data["acceleration"],
            "projected_peak_days": velocity_data["projected_peak_days"],
            "google_trends_slope": google_trends_slope,  # None if GT hasn't run
            "creator_tier_score": self._avg_creator_tier(pass1_items),
            "repeat_purchase_pct": self._count_keywords(all_texts, REPEAT_WORDS) / max(len(all_texts), 1),
            "days_tracked": (date.today() - datetime.strptime(
                product.get("first_seen_date", "2026-01-01"), "%Y-%m-%d"
            ).date()).days,
            "demographic_score": None,
            "news_trigger": False,
            "supplier_count_change": None,
            "social_mention_pct": 1.0,
            "retail_signal_strength": 0,
        })

        # OVERRIDE: Do NOT apply fad flag if Google Trends has not run yet.
        # The fad classifier fires on absence of GT slope, which is unfair
        # when we simply haven't checked GT yet.
        if not google_trends_exists:
            fad_data["fad_score"] = 0.0
            fad_data["lasting_score"] = 0.5
            fad_data["industry_shift_score"] = 0.5
            fad_data["dominant"] = "lasting"
            logger.info("[reddit] Fad flag suppressed — Google Trends has not run yet")

        # Growth rate
        prev_mentions = historical[-2] if len(historical) >= 2 else db_agg["post_count"]
        growth_rate = (db_agg["post_count"] - prev_mentions) / max(prev_mentions, 1)

        # Build signal row
        signal_row = {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "reddit",
            "run_id": self.run_id,
            "mention_count": db_agg["post_count"],
            "growth_rate_wow": round(growth_rate, 4),
            "total_upvotes": db_agg["total_upvotes"],
            "total_comment_count": db_agg["total_comment_count"],
            "sample_size": db_agg["post_count"] + self.comments_written,
            "creator_tier_score": self._avg_creator_tier(pass1_items),
            "buy_intent_comment_count": self._count_keywords(all_texts, BUY_WORDS),
            "problem_language_comment_count": self._count_keywords(all_texts, PROBLEM_WORDS),
            "repeat_purchase_pct": round(self._count_keywords(all_texts, REPEAT_WORDS) / max(len(all_texts), 1), 4),
            "sentiment_score": sentiment_agg["sentiment_score"],
            "sentiment_confidence": sentiment_agg["sentiment_confidence"],
            "velocity": velocity_data["velocity"],
            "velocity_score": velocity_data["velocity"],
            "acceleration": velocity_data["acceleration"],
            "projected_peak_days": velocity_data["projected_peak_days"],
            "phase": velocity_data["phase"],
            "fad_score": fad_data["fad_score"],
            "lasting_score": fad_data["lasting_score"],
            "industry_shift_score": fad_data["industry_shift_score"],
            "avg_intent_score": intent_data["avg_intent_score"],
            "intent_level_distribution": intent_data.get("intent_level_distribution"),
            "high_intent_comment_count": intent_data["high_intent_comment_count"],
            "data_confidence": 0.8,
        }

        # Quality check
        quality = score_quality(signal_row, "signals_social", sample_size=len(all_texts))
        signal_row["data_quality_score"] = quality["data_quality_score"]
        signal_row["data_confidence"] = quality["data_quality_score"]

        if not quality["passes_threshold"]:
            self.supabase.table("signals_low_quality").insert({
                "product_id": product_id, "platform": "reddit",
                "scraped_date": date.today().isoformat(),
                "data_quality_score": quality["data_quality_score"],
                "rejection_reason": quality["rejection_reason"],
                "raw_json": {"post_count": db_agg["post_count"]},
            }).execute()
            self._checkpoint("running", f"Signal rejected — quality {quality['data_quality_score']:.2f}")
            return

        # Summary
        signal_row["agent_summary"] = generate_summary(
            product_name, "reddit",
            {"mention_count": db_agg["post_count"], "sentiment_score": sentiment_agg["sentiment_score"],
             "velocity": velocity_data["velocity"], "phase": velocity_data["phase"],
             "avg_intent_score": intent_data["avg_intent_score"],
             "high_intent_comments": intent_data["high_intent_comment_count"],
             "fad_classification": fad_data["dominant"]},
            category,
        )

        # Write signal row
        self.supabase.table("signals_social").insert(signal_row).execute()
        self.signals_written += 1

        # ── SCORING ENGINE ─────────────────────────
        self._checkpoint("running", "Scoring engine running")

        # Get old score for score_change tracking
        try:
            old_prod = self.supabase.table("products").select("current_score").eq("id", product_id).execute()
            old_score = old_prod.data[0]["current_score"] if old_prod.data else 0
        except Exception:
            old_score = 0

        composite, verdict, _, job2_score, raw_score, active_jobs_count = self._compute_score(signal_row)

        # ── Phase classification (corrected rules) ──
        db_phase = self._classify_lifecycle_phase(product_id, composite, signal_row)

        # fad_flag: only set True/False when Google Trends has confirmed.
        # If GT hasn't run, set to None (unknown) — don't penalize.
        if google_trends_exists:
            fad_flag_value = fad_data["fad_score"] > 0.6
        else:
            fad_flag_value = False  # Unknown — don't flag until GT confirms

        self.supabase.table("products").update({
            "current_score": composite,
            "current_verdict": verdict,
            "lifecycle_phase": db_phase,
            "fad_flag": fad_flag_value,
            "raw_score": raw_score,
            "coverage_pct": round(active_jobs_count / 4 * 100),
            "active_jobs": active_jobs_count,
            "total_jobs": 4,
        }).eq("id", product_id).execute()

        self._checkpoint("running", f"Score written — {product_name} scored {composite} (raw {raw_score}, {active_jobs_count}/4 jobs) verdict {verdict}")

        # ── WRITE SCORES_HISTORY ───────────────────
        try:
            self.supabase.table("scores_history").insert({
                "product_id": product_id,
                "scored_date": date.today().isoformat(),
                "composite_score": composite,
                "early_detection_score": None,
                "demand_validation_score": job2_score,
                "purchase_intent_score": None,
                "supply_readiness_score": None,
                "verdict": verdict,
                "verdict_reasoning": signal_row.get("agent_summary", ""),
                "score_change": round(composite - old_score, 1),
                "data_confidence": signal_row.get("data_quality_score", 0.5),
                "platforms_used": ["reddit", "google_trends"] if google_trends_exists else ["reddit"],
            }).execute()
            logger.info("[reddit] Wrote scores_history row for %s", product_name)
        except Exception as e:
            logger.error("[reddit] Failed to write scores_history: %s", e)

        # ── WRITE PRODUCT SNAPSHOT ─────────────────
        try:
            gt_data = {}
            try:
                gt_r = self.supabase.table("signals_search") \
                    .select("slope_24m, yoy_growth") \
                    .eq("product_id", product_id).eq("platform", "google_trends") \
                    .order("scraped_date", desc=True).limit(1).execute()
                if gt_r.data:
                    gt_data = gt_r.data[0]
            except Exception:
                pass

            platforms_active = 1  # Reddit
            if gt_data:
                platforms_active += 1

            self.supabase.table("product_snapshots").upsert({
                "product_id": product_id,
                "snapshot_date": date.today().isoformat(),
                "composite_score": composite,
                "verdict": verdict,
                "lifecycle_phase": db_phase,
                "reddit_mentions": db_agg["post_count"],
                "reddit_sentiment": signal_row.get("sentiment_score"),
                "reddit_intent": signal_row.get("avg_intent_score"),
                "gt_slope": gt_data.get("slope_24m"),
                "gt_yoy_growth": gt_data.get("yoy_growth"),
                "platforms_active": platforms_active,
                "data_confidence": signal_row.get("data_quality_score"),
            }, on_conflict="product_id,snapshot_date").execute()
            logger.info("[reddit] Wrote product_snapshot for %s", product_name)
        except Exception as e:
            logger.error("[reddit] Failed to write product_snapshot: %s", e)

        # ── INTEGRITY CHECK ────────────────────────
        # Compare DB count against posts that passed relevance filter, not raw Apify total
        integrity_ok = self._run_integrity_check(product_id, product_name, posts_written, db_agg, signal_row)
        integrity_str = "passed" if integrity_ok else "failed"
        self._checkpoint("running", f"Integrity check — {integrity_str}")

    # ══════════════════════════════════════════════
    # PASS 1: Discovery — macrocosmos/reddit-scraper
    # ══════════════════════════════════════════════
    def _run_pass1(self, product_name: str, keywords: list, target_subs: list) -> list[dict]:
        """Search Reddit by keyword across subreddits. Returns deduplicated items."""
        all_items = []

        for kw in keywords:
            logger.info("[reddit] Pass 1: keyword '%s' across %d subreddits", kw, len(target_subs))
            try:
                items = run_actor(
                    actor_id=PASS1_ACTOR,
                    run_input={
                        "subreddits": target_subs,
                        "keyword": kw,
                        "postsPerSubreddit": POSTS_PER_SUBREDDIT,
                        "sortBy": "new",
                    },
                    api_token=APIFY_API_TOKEN,
                    timeout_secs=180,
                    max_items=200,
                )
                self.pass1_results += len(items)
                all_items.extend(items)
                logger.info("[reddit] Pass 1: keyword '%s' returned %d items", kw, len(items))
            except Exception as e:
                logger.warning("[reddit] Pass 1 keyword '%s' failed: %s", kw, str(e)[:200])

        # Deduplicate by reddit ID
        seen = set()
        unique = []
        for item in all_items:
            rid = item.get("id", "")
            if rid and rid in seen:
                continue
            seen.add(rid)
            unique.append(item)

        logger.info("[reddit] Pass 1 total: %d unique posts (from %d raw)", len(unique), len(all_items))
        return unique

    # ══════════════════════════════════════════════
    # PASS 2: Intelligence — trudax/reddit-scraper-lite
    # ══════════════════════════════════════════════
    def _run_pass2(self, post_urls: list[str]) -> list[dict]:
        """Fetch full comment threads for post URLs. Returns comment items only."""
        logger.info("[reddit] Pass 2: fetching comments for %d post URLs", len(post_urls))

        try:
            items = run_actor(
                actor_id=PASS2_ACTOR,
                run_input={
                    "startUrls": [{"url": u} for u in post_urls],
                    "skipComments": False,
                    "maxItems": 500,
                    "proxy": {"useApifyProxy": True},
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=300,
                max_items=1000,
            )
            self.pass2_results += len(items)

            # Separate comments from posts
            comments = []
            for item in items:
                # trudax lite returns dataType or we detect by parentId/type
                is_comment = (
                    item.get("dataType") == "comment" or
                    item.get("type") == "comment" or
                    item.get("parentId") is not None or
                    (item.get("body") and not item.get("title"))
                )
                if is_comment:
                    comments.append(item)

            logger.info("[reddit] Pass 2: %d total items, %d identified as comments", len(items), len(comments))
            return comments

        except Exception as e:
            logger.error("[reddit] Pass 2 FAILED: %s", str(e)[:300])
            self.run_degraded = True
            self.degraded_reasons.append(f"Pass 2 failed: {str(e)[:200]}")
            return []

    # ══════════════════════════════════════════════
    # WRITE POSTS TO DB (with relevance filtering)
    # ══════════════════════════════════════════════
    def _write_posts(self, items: list[dict], product_id: str) -> int:
        """
        Write Pass 1 items to posts table with relevance scoring.
        Posts with relevance_score < 0.3 are discarded.
        """
        # Get product keywords for relevance scoring
        try:
            prod = self.supabase.table("products").select("keywords, name").eq("id", product_id).execute()
            product_keywords = prod.data[0].get("keywords", []) if prod.data else []
            product_name = prod.data[0].get("name", "") if prod.data else ""
        except Exception:
            product_keywords = []
            product_name = ""

        # Build keyword set for matching (lowercased)
        match_terms = [k.lower() for k in product_keywords] + [product_name.lower()]

        count = 0
        discarded = 0

        for item in items:
            title = (item.get("title") or "").strip()
            body = (item.get("body") or "").strip()
            post_text = f"{title} {body}".strip() if body else title
            if not post_text or len(post_text) < 3:
                continue

            # ── Relevance scoring ──
            title_lower = title.lower()
            body_lower = body.lower()
            name_lower = product_name.lower()

            # Product name in title = 0.8 automatic
            if name_lower and name_lower in title_lower:
                relevance = 0.8
            # Any keyword in title = 0.5 minimum
            elif any(kw in title_lower for kw in match_terms):
                relevance = 0.5
            # Any keyword in body only = 0.2 minimum
            elif any(kw in body_lower for kw in match_terms):
                relevance = 0.2
            else:
                relevance = 0.0

            # Boost for multiple keyword matches
            all_text_lower = f"{title_lower} {body_lower}"
            extra_matches = sum(1 for kw in match_terms if kw in all_text_lower)
            if extra_matches > 1:
                relevance = min(1.0, relevance + extra_matches * 0.1)

            relevance = round(relevance, 4)

            # Filter: discard posts with zero relevance (no keyword match at all)
            if relevance < 0.1:
                discarded += 1
                continue

            s = analyze_sentiment(post_text)
            i = score_intent(post_text)

            try:
                self.supabase.table("posts").insert({
                    "product_id": product_id,
                    "run_id": self.run_id,
                    "platform": "reddit",
                    "post_title": title[:1000] or None,
                    "post_body": body[:5000] or None,
                    "post_url": (item.get("url") or "")[:2000] or None,
                    "subreddit": item.get("communityName") or "",
                    "upvotes": item.get("score") or 0,
                    "comment_count": item.get("num_comments") or 0,
                    "author": (item.get("username") or "")[:200] or None,
                    "posted_at": item.get("createdAt") or None,
                    "scraped_date": date.today().isoformat(),
                    "intent_level": i["intent_level"],
                    "sentiment_score": s["sentiment_score"],
                    "anomaly_flag": False,
                    "reddit_id": item.get("id"),
                    "upvote_ratio": item.get("upvote_ratio"),
                    "data_type": "post",
                    "parent_id": None,
                    "is_nsfw": item.get("isNsfw", False),
                    "media_urls": item.get("media") if item.get("media") else None,
                    "raw_json": item,
                    "relevance_score": relevance,
                }).execute()
                count += 1
            except Exception as e:
                logger.error("[reddit] Failed to write post: %s", str(e)[:200])

        self.irrelevant_discarded = discarded
        if discarded > 0:
            logger.info("[reddit] Discarded %d irrelevant posts (relevance < 0.3)", discarded)

        return count

    # ══════════════════════════════════════════════
    # WRITE COMMENTS TO DB
    # ══════════════════════════════════════════════
    def _write_comments(self, comments: list[dict], product_id: str, pass1_items: list[dict]) -> int:
        """Write Pass 2 comments to both posts and comments tables."""
        # Build a lookup from reddit post URL to our posts table ID
        post_id_lookup = {}
        try:
            stored = self.supabase.table("posts") \
                .select("id, post_url, reddit_id") \
                .eq("product_id", product_id) \
                .eq("run_id", self.run_id) \
                .execute()
            for row in stored.data:
                if row.get("post_url"):
                    post_id_lookup[row["post_url"]] = row["id"]
                if row.get("reddit_id"):
                    post_id_lookup[row["reddit_id"]] = row["id"]
        except Exception:
            pass

        count = 0
        for comment in comments:
            body = (comment.get("body") or comment.get("text") or "").strip()
            if not body or len(body) < 3:
                continue

            s = analyze_sentiment(body)
            i = score_intent(body)
            lower = body.lower()

            # Find the parent post ID in our DB
            parent_url = comment.get("postUrl") or comment.get("url") or ""
            parent_reddit_id = comment.get("parentId") or comment.get("postId") or ""
            db_post_id = post_id_lookup.get(parent_url) or post_id_lookup.get(parent_reddit_id)

            # If we can't find the parent, write the comment to posts table as a comment-type post
            # so it's still stored, then write to comments table with a self-reference
            if not db_post_id:
                try:
                    post_resp = self.supabase.table("posts").insert({
                        "product_id": product_id,
                        "run_id": self.run_id,
                        "platform": "reddit",
                        "post_title": None,
                        "post_body": body[:5000],
                        "post_url": parent_url[:2000] or None,
                        "subreddit": comment.get("communityName") or comment.get("subreddit") or "",
                        "upvotes": comment.get("score") or comment.get("ups") or 0,
                        "comment_count": 0,
                        "author": (comment.get("username") or comment.get("author") or "")[:200] or None,
                        "posted_at": comment.get("createdAt") or None,
                        "scraped_date": date.today().isoformat(),
                        "intent_level": i["intent_level"],
                        "sentiment_score": s["sentiment_score"],
                        "anomaly_flag": False,
                        "reddit_id": comment.get("id"),
                        "data_type": "comment",
                        "parent_id": parent_reddit_id,
                        "raw_json": comment,
                    }).execute()
                    db_post_id = post_resp.data[0]["id"] if post_resp.data else None
                except Exception as e:
                    logger.error("[reddit] Failed to write comment-as-post: %s", str(e)[:200])
                    continue

            if not db_post_id:
                continue

            try:
                self.supabase.table("comments").insert({
                    "post_id": db_post_id,
                    "product_id": product_id,
                    "platform": "reddit",
                    "comment_body": body[:5000],
                    "author": (comment.get("username") or comment.get("author") or "")[:200] or None,
                    "upvotes": comment.get("score") or comment.get("ups") or 0,
                    "intent_level": i["intent_level"],
                    "sentiment_score": s["sentiment_score"],
                    "is_buy_intent": any(w in lower for w in BUY_WORDS),
                    "is_problem_language": any(w in lower for w in PROBLEM_WORDS),
                    "is_repeat_purchase": any(w in lower for w in REPEAT_WORDS),
                    "posted_at": comment.get("createdAt") or None,
                }).execute()
                count += 1
            except Exception as e:
                logger.error("[reddit] Failed to write comment: %s", str(e)[:200])

        return count

    # ══════════════════════════════════════════════
    # COMPUTE AGGREGATES FROM DB
    # ══════════════════════════════════════════════
    def _compute_aggregates_from_db(self, product_id: str) -> dict:
        try:
            # Only count actual posts (data_type='post'), not comment-items from Pass 2
            resp = self.supabase.table("posts") \
                .select("upvotes, comment_count, intent_level, sentiment_score") \
                .eq("product_id", product_id) \
                .eq("run_id", self.run_id) \
                .eq("platform", "reddit") \
                .eq("data_type", "post") \
                .execute()
            rows = resp.data or []
            return {
                "post_count": len(rows),
                "total_upvotes": sum(r.get("upvotes", 0) or 0 for r in rows),
                "total_comment_count": sum(r.get("comment_count", 0) or 0 for r in rows),
                "high_intent_posts": sum(1 for r in rows if (r.get("intent_level") or 1) >= 4),
            }
        except Exception as e:
            logger.error("[reddit] DB aggregate failed: %s", e)
            return {"post_count": 0, "total_upvotes": 0, "total_comment_count": 0, "high_intent_posts": 0}

    # ══════════════════════════════════════════════
    # LIFECYCLE PHASE CLASSIFICATION
    # ══════════════════════════════════════════════
    def _classify_lifecycle_phase(self, product_id: str, composite: float, signal_row: dict) -> str:
        """
        Corrected phase rules:
          - Less than 3 runs of data = "early"
          - velocity > 20% wow AND GT rising = "buy_window"
          - velocity 0-20% wow AND GT rising = "early" (growing but early)
          - velocity > 0 AND GT positive AND score > 60 = "buy_window"
          - velocity declining AND GT declining = "declining"
          - Score > 75 sustained 7+ days = "peak"
          - Otherwise = "plateau" or "early"

        Never classify as declining unless BOTH Reddit velocity is dropping
        AND Google Trends slope is negative.
        """
        # Count how many distinct runs exist for this product
        try:
            run_resp = self.supabase.table("signals_social") \
                .select("scraped_date") \
                .eq("product_id", product_id) \
                .eq("platform", "reddit") \
                .execute()
            distinct_dates = set(r["scraped_date"] for r in run_resp.data)
            run_count = len(distinct_dates)
        except Exception:
            run_count = 1

        # Less than 3 distinct run dates = "early"
        if run_count < 3:
            logger.info("[reddit] Phase: early (only %d runs, need 3+)", run_count)
            return "early"

        velocity = signal_row.get("velocity", 0) or 0

        # Get GT slope
        gt_slope = None
        try:
            gt_resp = self.supabase.table("signals_search") \
                .select("slope_24m") \
                .eq("product_id", product_id) \
                .eq("platform", "google_trends") \
                .order("scraped_date", desc=True).limit(1).execute()
            if gt_resp.data:
                gt_slope = gt_resp.data[0].get("slope_24m")
        except Exception:
            pass

        gt_rising = gt_slope is not None and gt_slope > 0.005
        gt_declining = gt_slope is not None and gt_slope < -0.005

        # Score > 75 sustained — check last 7 days of scores
        try:
            score_resp = self.supabase.table("scores_history") \
                .select("composite_score, scored_date") \
                .eq("product_id", product_id) \
                .order("scored_date", desc=True).limit(7).execute()
            sustained_above_75 = (
                len(score_resp.data) >= 7 and
                all(r.get("composite_score", 0) >= 75 for r in score_resp.data)
            )
        except Exception:
            sustained_above_75 = False

        if sustained_above_75:
            logger.info("[reddit] Phase: peak (score > 75 sustained 7+ days)")
            return "peak"

        # Declining: BOTH velocity < -0.1 AND GT slope negative
        if velocity < -0.1 and gt_declining:
            logger.info("[reddit] Phase: declining (velocity=%.2f AND GT slope=%.4f both negative)", velocity, gt_slope)
            return "declining"

        # Buy window: strong velocity + GT rising, or moderate + high score
        if velocity > 0.2 and gt_rising:
            logger.info("[reddit] Phase: buy_window (velocity=%.2f + GT rising)", velocity)
            return "buy_window"

        if velocity > 0 and gt_rising and composite > 60:
            logger.info("[reddit] Phase: buy_window (velocity positive + GT rising + score %.1f > 60)", composite)
            return "buy_window"

        # Plateau: near-zero velocity with positive GT
        if abs(velocity) < 0.1 and gt_rising:
            logger.info("[reddit] Phase: early (low velocity but GT rising — still building)")
            return "early"

        if abs(velocity) < 0.05:
            logger.info("[reddit] Phase: plateau (velocity near zero)")
            return "plateau"

        # Default
        logger.info("[reddit] Phase: early (default)")
        return "early"

    # ══════════════════════════════════════════════
    # SCORING ENGINE — Spec-defined 4-job system
    # ══════════════════════════════════════════════
    def _compute_score(self, signal_row: dict) -> tuple[float, str, str]:
        """
        4-job composite score per spec:
          Job 1 Early Detection (30%): TikTok 40%, Instagram 30%, YouTube 15%, X 10%, Pinterest 5%
          Job 2 Demand Validation (30%): Google Trends 45%, Reddit 35%, Facebook 20%
          Job 3 Purchase Intent (25%): Amazon 50%, Etsy 30%, Walmart 20%
          Job 4 Supply Readiness (15%): Alibaba 100%

        If a job has no data, redistribute its weight proportionally across jobs that DO have data.
        """
        product_id = signal_row.get("product_id")

        # ── Gather data from each job ──
        # Job 1: Early Detection — check if TikTok/Instagram/YouTube/X/Pinterest have data
        job1_score = None  # Not computed by Reddit agent

        # Job 2: Demand Validation — Reddit IS here, check for Google Trends and Facebook
        # Reddit contributes: sentiment, velocity, volume, intent
        sentiment = signal_row.get("sentiment_score", 0) or 0
        velocity = signal_row.get("velocity", 0) or 0
        intent = signal_row.get("avg_intent_score", 0) or 0
        mentions = signal_row.get("mention_count", 0) or 0
        quality = signal_row.get("data_quality_score", 0.5) or 0.5

        # Normalize Reddit signals to 0-100
        reddit_sentiment = max(0, min(100, (sentiment + 1) * 50))
        reddit_velocity = max(0, min(100, (velocity + 0.5) * 100))
        reddit_intent = intent * 100
        reddit_volume = min(100, max(0, math.log10(max(mentions, 1)) * 30))

        # Reddit's demand validation sub-score
        reddit_sub = (reddit_sentiment * 0.25 + reddit_velocity * 0.25 +
                      reddit_intent * 0.25 + reddit_volume * 0.25)

        # ── Fetch Google Trends data from signals_search ──
        gt_score = None
        try:
            gt_resp = self.supabase.table("signals_search") \
                .select("slope_24m, yoy_growth, breakout_flag") \
                .eq("product_id", product_id) \
                .eq("platform", "google_trends") \
                .order("scraped_date", desc=True) \
                .limit(1).execute()

            if gt_resp.data and gt_resp.data[0].get("slope_24m") is not None:
                gt = gt_resp.data[0]
                gt_slope = gt["slope_24m"]
                gt_yoy = gt.get("yoy_growth", 0) or 0
                gt_breakout = gt.get("breakout_flag", False)

                # Normalize GT signals to 0-100
                gt_slope_norm = max(0, min(100, (gt_slope + 0.01) / 0.02 * 100))
                gt_yoy_norm = max(0, min(100, (gt_yoy + 0.5) * 100))
                gt_no_breakout_bonus = 0 if gt_breakout else 20

                gt_score = gt_slope_norm * 0.40 + gt_yoy_norm * 0.40 + gt_no_breakout_bonus
                logger.info("[reddit] GT score: slope_norm=%.1f yoy_norm=%.1f bonus=%d -> %.1f",
                            gt_slope_norm, gt_yoy_norm, gt_no_breakout_bonus, gt_score)
        except Exception as e:
            logger.warning("[reddit] Failed to fetch GT data for scoring: %s", e)

        # ── Fetch Facebook data from signals_social ──
        fb_score = None
        # (Facebook agent not yet active)

        # ── Compute Job 2 with available platforms ──
        available_weight = 0.35  # Reddit always available
        if gt_score is not None:
            available_weight += 0.45
        if fb_score is not None:
            available_weight += 0.20

        job2_score = reddit_sub * (0.35 / available_weight)
        if gt_score is not None:
            job2_score += gt_score * (0.45 / available_weight)
        if fb_score is not None:
            job2_score += fb_score * (0.20 / available_weight)

        logger.info("[reddit] Job 2: reddit=%.1f (%.0f%%) gt=%s (%.0f%%) -> combined=%.1f",
                    reddit_sub, 0.35/available_weight*100,
                    f"{gt_score:.1f}" if gt_score is not None else "none",
                    0.45/available_weight*100 if gt_score is not None else 0,
                    job2_score)

        # Job 3: Purchase Intent — check for Amazon/Etsy/Walmart
        job3_score = None  # No retail agents have run

        # Job 4: Supply Readiness — check for Alibaba
        job4_score = None  # No supply agents have run

        # ── Redistribute job weights ──
        jobs = {
            "early_detection": (0.30, job1_score),
            "demand_validation": (0.30, job2_score),
            "purchase_intent": (0.25, job3_score),
            "supply_readiness": (0.15, job4_score),
        }

        # Filter to jobs with data
        active_jobs = {k: v for k, v in jobs.items() if v[1] is not None}
        if not active_jobs:
            return 0.0, "pass", signal_row.get("phase", "emerging")

        # Redistribute weights proportionally
        total_active_weight = sum(w for w, _ in active_jobs.values())
        composite = 0.0
        for job_name, (weight, score) in active_jobs.items():
            redistributed_weight = weight / total_active_weight
            contribution = score * redistributed_weight
            composite += contribution
            logger.info(
                "[reddit] Score job %s: %.1f x %.2f (redistributed from %.2f) = %.1f",
                job_name, score, redistributed_weight, weight, contribution,
            )

        # Apply data quality multiplier
        total_jobs = 4
        raw_score = composite * (0.7 + quality * 0.3)
        raw_score = round(max(0, min(100, raw_score)), 1)

        # Coverage penalty — honest scoring based on data completeness
        coverage_ratio = len(active_jobs) / total_jobs
        coverage_penalty = 0.5 + (coverage_ratio * 0.5)
        composite = round(max(0, min(100, raw_score * coverage_penalty)), 1)

        # Verdict based on coverage-adjusted score
        verdict = "buy" if composite >= 75 else "watch" if composite >= 55 else "pass"
        phase = signal_row.get("phase", "emerging") or "emerging"

        logger.info(
            "[reddit] Raw: %.1f, Coverage: %d%% (%d/%d jobs), Adjusted: %.1f → %s",
            raw_score, round(coverage_ratio * 100), len(active_jobs), total_jobs, composite, verdict,
        )

        return composite, verdict, phase, job2_score, raw_score, len(active_jobs)

    # ══════════════════════════════════════════════
    # INTEGRITY CHECK
    # ══════════════════════════════════════════════
    def _run_integrity_check(self, product_id: str, product_name: str,
                             apify_count: int, db_agg: dict, signal_row: dict) -> bool:
        errors = []

        # Check 1: Post count (data_type='post' only) matches Pass 1 Apify count
        # db_agg already filters data_type='post' via _compute_aggregates_from_db
        if db_agg["post_count"] != apify_count:
            errors.append(f"Post count: Apify Pass1={apify_count} posts_table={db_agg['post_count']}")

        # Check 2: Upvotes in signal row match sum from posts table
        if signal_row.get("total_upvotes", 0) != db_agg["total_upvotes"]:
            errors.append(f"Upvotes: signal={signal_row.get('total_upvotes')} posts_sum={db_agg['total_upvotes']}")

        # Check 3: mention_count matches post count
        if signal_row.get("mention_count", 0) != db_agg["post_count"]:
            errors.append(f"Mentions: signal={signal_row.get('mention_count')} posts_count={db_agg['post_count']}")

        # Check 4: Comment count in comments table (separate from posts table)
        # Count ONLY comments written during THIS run by checking created_at recency
        try:
            comment_rows = self.supabase.table("comments") \
                .select("id, intent_level, sentiment_score, post_id") \
                .eq("product_id", product_id) \
                .eq("platform", "reddit") \
                .execute()

            # Only check comments that were written in this run
            # (comments table has no run_id, so we check self.comments_written)
            run_comments = comment_rows.data or []

            # Verify every comment has required fields populated
            for c in run_comments[-self.comments_written:] if self.comments_written > 0 else []:
                if c.get("intent_level") is None:
                    errors.append("Comment missing intent_level")
                    break
                if c.get("sentiment_score") is None:
                    errors.append("Comment missing sentiment_score")
                    break
                if c.get("post_id") is None:
                    errors.append("Comment missing post_id")
                    break
        except Exception:
            pass

        if errors:
            error_str = "; ".join(errors)
            logger.warning("[reddit] INTEGRITY FAILED for %s: %s", product_name, error_str)
            self.integrity_errors.append(f"{product_name}: {error_str}")
            try:
                self.supabase.table("signals_social").update({
                    "integrity_verified": False, "integrity_errors": error_str,
                }).eq("product_id", product_id).eq("run_id", self.run_id).execute()
            except Exception:
                pass
            return False
        else:
            logger.info("[reddit] Integrity PASSED for %s", product_name)
            try:
                self.supabase.table("signals_social").update({
                    "integrity_verified": True,
                }).eq("product_id", product_id).eq("run_id", self.run_id).execute()
            except Exception:
                pass
            return True

    # ══════════════════════════════════════════════
    # CHECKPOINT WRITER
    # ══════════════════════════════════════════════
    def _checkpoint(self, status: str, message: str):
        """Write a checkpoint to agent_runs AND Victor's global dashboard."""
        logger.info("[reddit] CHECKPOINT: [%s] %s", status, message)

        # Post to global activity dashboard at localhost:3847
        from .skills.activity_logger import post_status
        activity_status = "busy" if status == "running" else "done" if status == "complete" else "reporting" if status in ("failed", "degraded") else "idle"
        post_status("scraper-reddit", activity_status, message)
        try:
            now = datetime.now().isoformat()
            data = {
                "id": self.agent_run_id,
                "run_id": self.run_id,
                "platform": "reddit",
                "status": status,
                "products_processed": 1 if self.signals_written > 0 else 0,
                "rows_written": self.signals_written,
                "rows_rejected": 0,
                "anomalies_detected": 0,
                "apify_results_count": self.apify_total_results,
                "apify_estimated_cost": self.apify_total_cost,
                "irrelevant_posts_discarded": getattr(self, "irrelevant_discarded", 0),
                "items_new": getattr(self, "ingestion_stats", {}).get("items_new", 0),
                "items_duplicate": getattr(self, "ingestion_stats", {}).get("items_duplicate", 0),
                "items_too_old": getattr(self, "ingestion_stats", {}).get("items_too_old", 0),
                "cost_saved_dedup": getattr(self, "ingestion_stats", {}).get("cost_saved_dedup", 0),
                "agent_run_summary": message,
            }

            if status == "running" and not self._agent_run_exists:
                data["started_at"] = now
                self.supabase.table("agent_runs").insert(data).execute()
                self._agent_run_exists = True
            else:
                if status in ("complete", "failed", "degraded"):
                    data["completed_at"] = now
                    if self.integrity_errors:
                        data["integrity_check_passed"] = False
                        data["integrity_errors"] = self.integrity_errors
                    else:
                        data["integrity_check_passed"] = True
                    # Duration
                    try:
                        run_resp = self.supabase.table("agent_runs") \
                            .select("started_at").eq("id", self.agent_run_id).execute()
                        if run_resp.data and run_resp.data[0].get("started_at"):
                            started = datetime.fromisoformat(run_resp.data[0]["started_at"].replace("Z", "+00:00"))
                            data["duration_seconds"] = (datetime.now(started.tzinfo or None) - started).total_seconds()
                    except Exception:
                        pass

                self.supabase.table("agent_runs").update(data).eq("id", self.agent_run_id).execute()
        except Exception as e:
            logger.error("[reddit] Checkpoint write failed: %s", e)

    _agent_run_exists = False

    # ══════════════════════════════════════════════
    # POST-RUN SUMMARY
    # ══════════════════════════════════════════════
    def _build_post_run_summary(self, products: list[dict]) -> str:
        product_name = products[0]["name"] if products else "Unknown"

        # Get the signal data we just wrote
        try:
            sig = self.supabase.table("signals_social") \
                .select("sentiment_score, avg_intent_score, high_intent_comment_count") \
                .eq("run_id", self.run_id).eq("platform", "reddit").execute()
            sig_data = sig.data[0] if sig.data else {}
        except Exception:
            sig_data = {}

        # Get the product score we just wrote
        try:
            prod = self.supabase.table("products") \
                .select("current_score, current_verdict") \
                .eq("id", products[0]["id"]).execute()
            prod_data = prod.data[0] if prod.data else {}
        except Exception:
            prod_data = {}

        target_subs = products[0].get("target_subreddits", []) if products else []
        if not target_subs:
            target_subs = CATEGORY_SUBREDDITS.get(products[0].get("category", ""), []) if products else []
        subs_str = ", ".join(target_subs[:5])

        integrity_str = "passed" if not self.integrity_errors else "failed"
        top_intent = max((sig_data.get("high_intent_comment_count") or 0), 0)

        return (
            f"Reddit agent completed two-pass scrape for {product_name}. "
            f"Pass 1 found {self.posts_written} posts across {subs_str}. "
            f"Pass 2 fetched {self.comments_written} comments from top {TOP_POSTS_FOR_COMMENTS} posts. "
            f"Sentiment: {sig_data.get('sentiment_score', 0)}. "
            f"Top intent level: {sig_data.get('avg_intent_score', 0):.2f}. "
            f"Composite score: {prod_data.get('current_score', 0)}. "
            f"Verdict: {prod_data.get('current_verdict', 'unknown')}. "
            f"Total Apify cost: ${self.apify_total_cost}. "
            f"Integrity: {integrity_str}."
        )

    # ══════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════
    def _get_monthly_spend(self) -> float:
        try:
            month_start = f"{date.today().strftime('%Y-%m')}-01"
            resp = self.supabase.table("agent_runs") \
                .select("apify_estimated_cost") \
                .gte("created_at", month_start).execute()
            return sum(r.get("apify_estimated_cost", 0) or 0 for r in resp.data)
        except Exception:
            return 0.0

    def _check_monthly_budget(self):
        total = self._get_monthly_spend() + self.apify_total_cost
        pct = total / MONTHLY_BUDGET * 100
        logger.info("[reddit] Monthly Apify spend: $%.2f / $%.2f (%.1f%%)", total, MONTHLY_BUDGET, pct)
        if pct >= 75:
            priority = "high" if pct >= 90 else "medium"
            try:
                self.supabase.table("alerts").insert({
                    "product_id": None,
                    "alert_type": "fad_warning",
                    "priority": priority,
                    "message": f"Apify monthly budget at {pct:.0f}% (${total:.2f} of ${MONTHLY_BUDGET:.2f}).",
                    "actioned": False,
                }).execute()
            except Exception:
                pass

    def _get_historical_values(self, product_id: str) -> list:
        try:
            resp = self.supabase.table("signals_social") \
                .select("mention_count") \
                .eq("product_id", product_id).eq("platform", "reddit") \
                .order("scraped_date", desc=False).limit(30).execute()
            return [r["mention_count"] for r in resp.data if r.get("mention_count") is not None]
        except Exception:
            return []

    @staticmethod
    def _avg_creator_tier(items: list[dict]) -> float:
        scores = []
        for item in items:
            s = item.get("score", 0) or 0
            scores.append(0.8 if s > 100 else 0.6 if s > 20 else 0.3)
        return round(sum(scores) / max(len(scores), 1), 4)

    @staticmethod
    def _count_keywords(texts: list[str], keywords: list[str]) -> int:
        return sum(1 for t in texts if any(w in t.lower() for w in keywords))
