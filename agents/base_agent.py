"""
Base Agent — All platform agents inherit from this.

Pipeline flow (POSTS-FIRST architecture):
  1. Scrape platform → get raw items
  2. Write EVERY raw item to posts table (zero data loss)
  3. Write comments to comments table
  4. Compute aggregates FROM stored posts/comments in Supabase
  5. Run skills (sentiment, velocity, fad, intent) on stored data
  6. Write aggregated signal row to signals table
  7. Run integrity check — verify aggregates match stored rows
  8. If integrity fails → mark run as degraded
"""
import uuid
import json
import logging
from datetime import datetime, date
from abc import ABC, abstractmethod

from .config import get_supabase, SIGNAL_TABLES
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


class BaseAgent(ABC):
    """Base class for all platform agents."""

    PLATFORM: str = ""
    SIGNAL_TABLE: str = ""
    REQUIRED_CREDENTIALS: list[str] = []

    def __init__(self):
        self.supabase = get_supabase()
        self.rate_limiter = None
        self.healing_tracker = HealingTracker()
        self.weights = {}
        self.run_id = None
        self.agent_run_id = None

        # Stats
        self.products_processed = 0
        self.rows_written = 0
        self.rows_rejected = 0
        self.anomalies_detected = 0
        self.integrity_errors = []
        self.errors = []

    def has_credentials(self) -> bool:
        """Check if required API credentials are configured."""
        import os
        for cred in self.REQUIRED_CREDENTIALS:
            if not os.environ.get(cred):
                return False
        return True

    async def run(self, products: list[dict], run_id: str) -> dict:
        """Execute the full agent pipeline for all products."""
        self.run_id = run_id
        self.SIGNAL_TABLE = SIGNAL_TABLES.get(self.PLATFORM, "signals_social")

        self.agent_run_id = str(uuid.uuid4())
        self._update_status("running")

        logger.info("[%s] Starting run %s for %d products", self.PLATFORM, run_id, len(products))

        if not self.has_credentials():
            msg = f"{self.PLATFORM} agent requires credentials: {self.REQUIRED_CREDENTIALS}"
            logger.warning(msg)
            self._update_status("failed", error_message=msg)
            return {"status": "failed", "error": msg}

        try:
            self.weights = await load_weights(self.supabase, self.PLATFORM)

            from .config import RATE_LIMITS
            rl_config = RATE_LIMITS.get(self.PLATFORM, {})
            self.rate_limiter = RateLimiter(
                self.PLATFORM,
                mean_delay=rl_config.get("mean_delay", 1.0),
                rpm=rl_config.get("rpm", 60),
                safe_pct=rl_config.get("safe_pct", 0.8),
            )

            products = self.rate_limiter.randomize_order(products)

            for product in products:
                try:
                    await self._process_product(product)
                    self.products_processed += 1
                except Exception as e:
                    logger.error("[%s] Error processing %s: %s", self.PLATFORM, product.get("name", "?"), e)
                    self.errors.append(f"{product.get('name', '?')}: {str(e)[:200]}")

            summary = self._generate_run_summary()

            if self.products_processed == 0:
                status = "failed"
            elif self.errors or self.integrity_errors:
                status = "degraded"
            else:
                status = "complete"

            self._update_status(status, summary=summary)

            return {
                "status": status,
                "products_processed": self.products_processed,
                "rows_written": self.rows_written,
                "rows_rejected": self.rows_rejected,
                "anomalies_detected": self.anomalies_detected,
                "integrity_errors": self.integrity_errors,
                "summary": summary,
            }

        except Exception as e:
            error_msg = f"Agent run failed: {str(e)[:500]}"
            logger.error("[%s] %s", self.PLATFORM, error_msg)
            self._update_status("failed", error_message=error_msg)
            return {"status": "failed", "error": error_msg}

    async def _process_product(self, product: dict):
        """
        POSTS-FIRST pipeline for a single product.

        Flow:
          1. Scrape → raw items from Apify
          2. Write every raw item to posts table (ZERO data loss)
          3. Compute aggregates FROM the posts table
          4. Run skills on aggregates
          5. Write signal row
          6. Integrity check
        """
        product_id = product["id"]
        product_name = product["name"]
        keywords = product.get("keywords", [])
        category = product.get("category", "")

        logger.info("[%s] Processing: %s", self.PLATFORM, product_name)

        # ── STEP 1: Scrape ──
        result = retry_with_healing(
            primary_fn=lambda: self.scrape(product_name, keywords, product),
            adapted_fn=lambda: self.scrape_adapted(product_name, keywords, product),
            fallback_fn=lambda: self.scrape_fallback(product_name, keywords, product),
            product_name=product_name,
            platform=self.PLATFORM,
        )
        self.healing_tracker.record(result)

        if not result.success or not result.data:
            logger.warning("[%s] All strategies failed for %s", self.PLATFORM, product_name)
            return

        raw_data = result.data
        raw_items = raw_data.get("raw_items", [])
        apify_item_count = len(raw_items)

        if apify_item_count == 0:
            logger.warning("[%s] No raw items for %s", self.PLATFORM, product_name)
            return

        # ── STEP 2: Write EVERY raw item to posts table ──
        posts_written = self._write_all_posts(raw_items, product_id)

        # ── STEP 3: Compute aggregates FROM stored posts ──
        db_agg = self._compute_aggregates_from_db(product_id)

        # If no posts were written (e.g. all filtered out), skip signal write
        if db_agg["post_count"] == 0 and posts_written == 0:
            logger.warning("[%s] No posts written for %s — skipping signal row", self.PLATFORM, product_name)
            return

        # ── STEP 4: Run skills on the text content ──
        # Collect all texts from raw items for sentiment/intent (universal field names)
        texts = []
        for item in raw_items:
            body = (item.get("body") or item.get("text") or item.get("desc") or
                    item.get("description") or item.get("caption") or "").strip()
            title = (item.get("title") or "").strip()
            text = f"{title} {body}".strip() if title and body else (title or body)
            if text and len(text) > 5:
                texts.append(text)

        sentiment_results = analyze_batch(texts) if texts else []
        sentiment_agg = aggregate_sentiment(sentiment_results)

        intent_data = score_intent_batch(texts) if texts else {
            "avg_intent_score": 0, "intent_level_distribution": {},
            "high_intent_comment_count": 0, "sample_size": 0,
        }

        historical = await self._get_historical_values(product_id, "mention_count")
        historical.append(db_agg["post_count"])
        velocity_data = calculate_velocity(historical)

        fad_data = classify_fad({
            "platforms_active": [self.PLATFORM],
            "velocity": velocity_data["velocity"],
            "acceleration": velocity_data["acceleration"],
            "projected_peak_days": velocity_data["projected_peak_days"],
            "google_trends_slope": raw_data.get("google_trends_slope"),
            "creator_tier_score": raw_data.get("creator_tier_score"),
            "repeat_purchase_pct": raw_data.get("repeat_purchase_pct", 0),
            "days_tracked": (date.today() - datetime.strptime(
                product.get("first_seen_date", "2026-01-01"), "%Y-%m-%d"
            ).date()).days,
            "demographic_score": raw_data.get("demographic_score"),
            "news_trigger": raw_data.get("news_trigger", False),
            "supplier_count_change": raw_data.get("supplier_count_change"),
            "social_mention_pct": 1.0 if self.SIGNAL_TABLE == "signals_social" else 0.0,
            "retail_signal_strength": raw_data.get("retail_signal_strength", 0),
        })

        weighted_metrics = apply_weights({
            "mention_count": db_agg["post_count"],
            "sentiment_score": sentiment_agg["sentiment_score"],
            "velocity_score": velocity_data["velocity"],
            "buy_intent_score": intent_data["avg_intent_score"],
        }, self.weights)

        # ── STEP 5: Build signal row FROM DB aggregates ──
        signal_row = self.build_signal_row(raw_data, product_id)

        # Override aggregated fields with values computed FROM stored posts
        signal_row.update({
            "run_id": self.run_id,
            "mention_count": db_agg["post_count"],
            "total_upvotes": db_agg["total_upvotes"],
            "total_comment_count": db_agg["total_comment_count"],
            "sample_size": db_agg["post_count"],
            "sentiment_score": sentiment_agg["sentiment_score"],
            "sentiment_confidence": sentiment_agg["sentiment_confidence"],
            "velocity": velocity_data["velocity"],
            "acceleration": velocity_data["acceleration"],
            "projected_peak_days": velocity_data["projected_peak_days"],
            "phase": velocity_data["phase"],
            "fad_score": fad_data["fad_score"],
            "lasting_score": fad_data["lasting_score"],
            "industry_shift_score": fad_data["industry_shift_score"],
            "avg_intent_score": intent_data["avg_intent_score"],
            "intent_level_distribution": intent_data.get("intent_level_distribution"),
            "high_intent_comment_count": intent_data["high_intent_comment_count"],
        })

        # Quality check
        quality = score_quality(
            signal_row, self.SIGNAL_TABLE,
            sample_size=db_agg["post_count"],
            data_dates=raw_data.get("data_dates", []),
        )
        signal_row["data_quality_score"] = quality["data_quality_score"]
        signal_row["data_confidence"] = quality["data_quality_score"]

        if not quality["passes_threshold"]:
            self.supabase.table("signals_low_quality").insert({
                "product_id": product_id,
                "platform": self.PLATFORM,
                "scraped_date": date.today().isoformat(),
                "data_quality_score": quality["data_quality_score"],
                "rejection_reason": quality["rejection_reason"],
                "raw_json": {"post_count": db_agg["post_count"]},
            }).execute()
            self.rows_rejected += 1
            logger.info("[%s] Rejected %s — quality %.2f", self.PLATFORM, product_name, quality["data_quality_score"])
            return

        # Benchmark
        try:
            cat_data = await self._get_category_data(category)
            benchmark = benchmark_signal_row(signal_row, cat_data, ["mention_count", "sentiment_score", "velocity"])
            signal_row["relative_strength"] = benchmark["relative_strength"]
            signal_row["above_category_average"] = benchmark["above_category_average"]
        except Exception:
            pass

        # Anomaly detection
        hist_data = await self._get_historical_signals(product_id)
        anomalies = detect_anomalies(signal_row, hist_data, platform=self.PLATFORM)
        if anomalies:
            signal_row["anomaly_flag"] = True
            signal_row["anomaly_type"] = anomalies[0]["anomaly_type"]
            signal_row["anomaly_description"] = anomalies[0]["anomaly_description"]
            self.anomalies_detected += len(anomalies)
            for anomaly in anomalies:
                try:
                    alert = create_alert_from_anomaly(anomaly, product_id, self.PLATFORM)
                    self.supabase.table("alerts").insert(alert).execute()
                except Exception as e:
                    logger.error("Failed to create anomaly alert: %s", e)

        # Summary
        summary_metrics = {
            "mention_count": db_agg["post_count"],
            "sentiment_score": sentiment_agg["sentiment_score"],
            "velocity": velocity_data["velocity"],
            "phase": velocity_data["phase"],
            "avg_intent_score": intent_data["avg_intent_score"],
            "high_intent_comments": intent_data["high_intent_comment_count"],
            "fad_classification": fad_data["dominant"],
            "relative_strength": signal_row.get("relative_strength"),
        }
        signal_row["agent_summary"] = generate_summary(product_name, self.PLATFORM, summary_metrics, category)

        # ── STEP 6: Write signal row ──
        self.supabase.table(self.SIGNAL_TABLE).insert(signal_row).execute()
        self.rows_written += 1

        # ── STEP 7: Update products table with latest score and verdict ──
        self._update_product_score(product_id, signal_row)

        # ── STEP 8: Integrity check ──
        # Compare posts written THIS run (not total DB posts) to what DB aggregates found
        self._run_integrity_check(product_id, product_name, posts_written, db_agg, signal_row)

    # ──────────────────────────────────────────────
    # POSTS-FIRST: Write every Apify item to posts table
    # ──────────────────────────────────────────────
    def _write_all_posts(self, raw_items: list[dict], product_id: str) -> int:
        """
        Write EVERY raw Apify item to the posts table.
        Stores all Apify fields — nothing is dropped.
        Items with parentId are also written to comments table.
        Returns count of posts written.
        """
        post_count = 0
        comment_count = 0

        for item in raw_items:
            # Universal field extraction — handle all platform field names
            title = (item.get("title") or "").strip()
            body = (item.get("body") or item.get("text") or item.get("desc") or
                    item.get("description") or item.get("caption") or "").strip()
            post_text = f"{title} {body}".strip() if title and body else (title or body)

            if not post_text or len(post_text) < 3:
                continue

            # Score individual item
            item_sentiment = analyze_sentiment(post_text)
            item_intent = score_intent(post_text)

            # Determine if this is a comment (has parentId) or a post
            data_type = item.get("dataType", "post")
            parent_id = item.get("parentId")
            if parent_id:
                data_type = "comment"

            # Universal URL extraction
            post_url = (item.get("url") or item.get("webVideoUrl") or
                        item.get("shortUrl") or item.get("permalink") or "")
            # Universal engagement extraction
            upvotes = (item.get("score") or item.get("diggCount") or
                       item.get("likesCount") or item.get("likes") or
                       item.get("upvotes") or 0)
            comment_count = (item.get("num_comments") or item.get("commentCount") or
                            item.get("commentsCount") or item.get("numComments") or 0)
            # Universal author extraction
            author_meta = item.get("authorMeta") or item.get("author") or {}
            if isinstance(author_meta, dict):
                author = author_meta.get("name") or author_meta.get("uniqueId") or ""
            else:
                author = item.get("username") or item.get("ownerUsername") or str(author_meta) or ""
            # Universal date extraction
            posted_at = (item.get("createdAt") or item.get("createTimeISO") or
                        item.get("timestamp") or item.get("posted_at") or None)
            # Convert unix timestamps to ISO
            if isinstance(posted_at, (int, float)) and posted_at > 1_000_000_000:
                from datetime import datetime as dt, timezone as tz
                posted_at = dt.fromtimestamp(posted_at, tz=tz.utc).isoformat()

            try:
                post_row = {
                    "product_id": product_id,
                    "run_id": self.run_id,
                    "platform": self.PLATFORM,
                    "post_title": title[:1000] if title else None,
                    "post_body": body[:5000] if body else None,
                    "post_url": post_url[:2000] if post_url else None,
                    "subreddit": (item.get("communityName") or item.get("subreddit") or ""),
                    "upvotes": upvotes,
                    "comment_count": comment_count,
                    "author": author[:200] if author else None,
                    "posted_at": posted_at,
                    "scraped_date": date.today().isoformat(),
                    "intent_level": item_intent["intent_level"],
                    "sentiment_score": item_sentiment["sentiment_score"],
                    "anomaly_flag": False,
                    # NEW: every Apify field stored
                    "reddit_id": item.get("id"),
                    "upvote_ratio": item.get("upvote_ratio"),
                    "data_type": data_type,
                    "parent_id": parent_id,
                    "is_nsfw": item.get("isNsfw", False),
                    "media_urls": item.get("media") if item.get("media") else None,
                    "raw_json": item,  # Full backup — NOTHING lost
                }

                post_resp = self.supabase.table("posts").insert(post_row).execute()
                post_count += 1

                # If this item is a comment (has parentId), also write to comments table
                # Use the same expanded keyword lists + tie-break logic as score_comments()
                # to keep flags consistent across scrape pipeline and re-scoring.
                if data_type == "comment" and post_resp.data:
                    post_db_id = post_resp.data[0]["id"]
                    # Lazy-import to avoid circular dependency
                    from .base_platform_agent import (
                        PURCHASE_SIGNALS, NEGATIVE_SIGNALS,
                        EMOJI_PURCHASE, EMOJI_NEGATIVE,
                        _normalize_text, _count_emojis,
                    )
                    normalized = _normalize_text(post_text)
                    purchase_matches = sum(1 for w in PURCHASE_SIGNALS if w in normalized)
                    negative_matches = sum(1 for w in NEGATIVE_SIGNALS if w in normalized)
                    purchase_matches += _count_emojis(post_text, EMOJI_PURCHASE)
                    negative_matches += _count_emojis(post_text, EMOJI_NEGATIVE)
                    # Priority: tie → purchase
                    is_buy = is_neg = False
                    if purchase_matches > 0 or negative_matches > 0:
                        if purchase_matches >= negative_matches:
                            is_buy = True
                        else:
                            is_neg = True
                    repeat_words = ["been using", "on my second", "on my third", "repurchased",
                                    "daily routine", "monthly", "restocked", "holy grail",
                                    "keep buying", "buying again"]
                    is_repeat = any(w in normalized for w in repeat_words)

                    self.supabase.table("comments").insert({
                        "post_id": post_db_id,
                        "product_id": product_id,
                        "platform": self.PLATFORM,
                        "comment_body": post_text[:5000],
                        "author": (item.get("username") or item.get("author") or "")[:200] or None,
                        "upvotes": item.get("score") or 0,
                        "intent_level": item_intent["intent_level"],
                        "sentiment_score": item_sentiment["sentiment_score"],
                        "is_buy_intent": is_buy,
                        "is_problem_language": is_neg,
                        "is_repeat_purchase": is_repeat,
                        "posted_at": item.get("createdAt") or None,
                    }).execute()
                    comment_count += 1

            except Exception as e:
                err_str = str(e)[:200]
                # DB-level dedup backstop — unique constraint violation = duplicate, count + skip
                if "duplicate" in err_str.lower() or "23505" in err_str or "unique" in err_str.lower():
                    if not hasattr(self, "_dedup_skip_count"):
                        self._dedup_skip_count = 0
                    self._dedup_skip_count += 1
                else:
                    logger.error("[%s] Failed to write post: %s", self.PLATFORM, err_str)

        skip_count = getattr(self, "_dedup_skip_count", 0)
        if skip_count > 0:
            logger.info("[%s] Wrote %d posts, %d comments, skipped %d duplicates",
                        self.PLATFORM, post_count, comment_count, skip_count)
        else:
            logger.info("[%s] Wrote %d posts, %d comments to DB", self.PLATFORM, post_count, comment_count)
        return post_count

    # ──────────────────────────────────────────────
    # Compute aggregates FROM stored data in Supabase
    # ──────────────────────────────────────────────
    def _compute_aggregates_from_db(self, product_id: str) -> dict:
        """
        Query the posts table to compute aggregated metrics.
        This guarantees signals_social numbers match what's in posts.
        """
        try:
            resp = self.supabase.table("posts") \
                .select("upvotes, comment_count, intent_level, sentiment_score") \
                .eq("product_id", product_id) \
                .eq("run_id", self.run_id) \
                .eq("platform", self.PLATFORM) \
                .execute()

            rows = resp.data or []
            post_count = len(rows)
            total_upvotes = sum(r.get("upvotes", 0) or 0 for r in rows)
            total_comment_count = sum(r.get("comment_count", 0) or 0 for r in rows)
            high_intent = sum(1 for r in rows if (r.get("intent_level") or 1) >= 4)
            sentiments = [r["sentiment_score"] for r in rows if r.get("sentiment_score") is not None]
            avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0

            return {
                "post_count": post_count,
                "total_upvotes": total_upvotes,
                "total_comment_count": total_comment_count,
                "high_intent_posts": high_intent,
                "avg_sentiment": round(avg_sentiment, 4),
            }
        except Exception as e:
            logger.error("[%s] Failed to compute DB aggregates: %s", self.PLATFORM, e)
            return {
                "post_count": 0, "total_upvotes": 0,
                "total_comment_count": 0, "high_intent_posts": 0,
                "avg_sentiment": 0,
            }

    # ──────────────────────────────────────────────
    # Integrity check — verify aggregates match stored rows
    # ──────────────────────────────────────────────
    def _run_integrity_check(
        self, product_id: str, product_name: str,
        posts_written: int, db_agg: dict, signal_row: dict,
    ):
        """
        Verify data integrity after pipeline completes.
        Compares posts written THIS run (via run_id filter) to DB aggregates.
        Checks:
          1. Row count in posts (this run) matches posts_written count
          2. total_upvotes in signal row matches sum from posts
          3. total_comment_count matches actual rows
          4. If any check fails → log warning, flag as degraded
        """
        errors = []

        # Check 1: Row count — posts written this run vs DB aggregate for this run
        if db_agg["post_count"] != posts_written:
            errors.append(
                f"Row count mismatch: wrote {posts_written} posts, "
                f"but DB query for run_id returned {db_agg['post_count']}"
            )

        # Check 2: Upvotes
        signal_upvotes = signal_row.get("total_upvotes", 0)
        if signal_upvotes != db_agg["total_upvotes"]:
            errors.append(
                f"Upvote mismatch: signal_row has {signal_upvotes}, "
                f"posts table sum is {db_agg['total_upvotes']}"
            )

        # Check 3: Comment count
        signal_comments = signal_row.get("total_comment_count", 0)
        if signal_comments != db_agg["total_comment_count"]:
            errors.append(
                f"Comment count mismatch: signal_row has {signal_comments}, "
                f"posts table sum is {db_agg['total_comment_count']}"
            )

        # Check 4: mention_count matches post_count
        signal_mentions = signal_row.get("mention_count", 0)
        if signal_mentions != db_agg["post_count"]:
            errors.append(
                f"Mention count mismatch: signal_row has {signal_mentions}, "
                f"posts table count is {db_agg['post_count']}"
            )

        if errors:
            error_str = "; ".join(errors)
            logger.warning("[%s] INTEGRITY CHECK FAILED for %s: %s", self.PLATFORM, product_name, error_str)
            self.integrity_errors.append(f"{product_name}: {error_str}")

            # Update the signal row with integrity status
            try:
                self.supabase.table(self.SIGNAL_TABLE) \
                    .update({
                        "integrity_verified": False,
                        "integrity_errors": error_str,
                    }) \
                    .eq("product_id", product_id) \
                    .eq("run_id", self.run_id) \
                    .eq("platform", self.PLATFORM) \
                    .execute()
            except Exception:
                pass
        else:
            logger.info("[%s] Integrity check PASSED for %s", self.PLATFORM, product_name)
            try:
                self.supabase.table(self.SIGNAL_TABLE) \
                    .update({"integrity_verified": True}) \
                    .eq("product_id", product_id) \
                    .eq("run_id", self.run_id) \
                    .eq("platform", self.PLATFORM) \
                    .execute()
            except Exception:
                pass

    # ──────────────────────────────────────────────
    # Update product score after signal row is written
    # ──────────────────────────────────────────────
    def _update_product_score(self, product_id: str, signal_row: dict):
        """
        Update the products table with the latest composite score and verdict.
        Uses the signal row data to compute a composite score and determine verdict.

        Scoring weights (from spec):
          Early detection 30%, Demand validation 30%, Purchase intent 25%, Supply readiness 15%

        For a single-platform run, the signal row IS the score.
        The composite score is derived from sentiment, velocity, intent, and data quality.
        """
        try:
            # Build a composite score from available signals
            sentiment = signal_row.get("sentiment_score", 0) or 0
            velocity = signal_row.get("velocity", 0) or 0
            intent = signal_row.get("avg_intent_score", 0) or 0
            quality = signal_row.get("data_quality_score", 0.5) or 0.5
            fad_score = signal_row.get("fad_score", 0) or 0
            lasting_score = signal_row.get("lasting_score", 0) or 0
            mention_count = signal_row.get("mention_count", 0) or 0
            high_intent = signal_row.get("high_intent_comment_count", 0) or 0

            # Normalize each component to 0-100
            # Sentiment: -1 to 1 → 0 to 100
            sentiment_norm = max(0, min(100, (sentiment + 1) * 50))

            # Velocity: positive is good, scale -0.5 to 0.5 → 0 to 100
            velocity_norm = max(0, min(100, (velocity + 0.5) * 100))

            # Intent: 0 to 1 → 0 to 100
            intent_norm = intent * 100

            # Volume signal: log scale, 10 mentions = 30, 100 = 60, 500+ = 90
            import math
            volume_norm = min(100, max(0, math.log10(max(mention_count, 1)) * 30))

            # Lasting vs fad bonus
            lasting_bonus = lasting_score * 20 - fad_score * 15

            # Composite: weighted average
            composite = (
                sentiment_norm * 0.20 +
                velocity_norm * 0.15 +
                intent_norm * 0.30 +
                volume_norm * 0.20 +
                quality * 100 * 0.15
            ) + lasting_bonus

            composite = max(0, min(100, composite))

            # Determine verdict
            if composite >= 75:
                verdict = "buy"
            elif composite >= 55:
                verdict = "watch"
            else:
                verdict = "pass"

            # Map velocity phase to DB constraint values
            # DB allows: early, buy_window, peak, plateau, declining
            # Velocity skill outputs: emerging, accelerating, peaking, plateau, declining
            phase_map = {
                "emerging": "early",
                "accelerating": "buy_window",
                "peaking": "peak",
                "plateau": "plateau",
                "declining": "declining",
            }
            raw_phase = signal_row.get("phase", "early") or "early"
            phase = phase_map.get(raw_phase, "early")

            # Update products table
            self.supabase.table("products").update({
                "current_score": round(composite, 1),
                "current_verdict": verdict,
                "lifecycle_phase": phase,
                "fad_flag": fad_score > 0.6,
            }).eq("id", product_id).execute()

            logger.info(
                "[%s] Updated product score: %.1f → %s (phase: %s)",
                self.PLATFORM, composite, verdict, phase,
            )

        except Exception as e:
            logger.error("[%s] Failed to update product score: %s", self.PLATFORM, e)

    # ──────────────────────────────────────────────
    # Abstract methods for subclasses
    # ──────────────────────────────────────────────
    @abstractmethod
    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        """
        Primary scraping method. Must return a dict with:
        - raw_items: list[dict] — original Apify items (REQUIRED, nothing dropped)
        - texts: list[str] — extracted text for skills processing
        - mention_count: int
        - data_dates: list[str]
        - Any platform-specific fields
        """
        raise NotImplementedError

    def scrape_adapted(self, product_name: str, keywords: list, product: dict) -> dict:
        """Strategy 2: adapted scraping (reduced scope)."""
        return self.scrape(product_name, keywords[:1], product)

    def scrape_fallback(self, product_name: str, keywords: list, product: dict) -> dict:
        """Strategy 3: fallback scraping (alternative data source)."""
        raise NotImplementedError(f"No fallback for {self.PLATFORM}")

    @abstractmethod
    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        """Build the signal table row from raw scraped data."""
        raise NotImplementedError

    # ──────────────────────────────────────────────
    # Historical data helpers
    # ──────────────────────────────────────────────
    async def _get_historical_values(self, product_id: str, metric: str) -> list:
        try:
            resp = self.supabase.table(self.SIGNAL_TABLE) \
                .select(f"scraped_date, {metric}") \
                .eq("product_id", product_id) \
                .eq("platform", self.PLATFORM) \
                .order("scraped_date", desc=False) \
                .limit(30).execute()
            return [r[metric] for r in resp.data if r.get(metric) is not None]
        except Exception:
            return []

    async def _get_historical_signals(self, product_id: str) -> list[dict]:
        try:
            resp = self.supabase.table(self.SIGNAL_TABLE) \
                .select("*") \
                .eq("product_id", product_id) \
                .eq("platform", self.PLATFORM) \
                .order("scraped_date", desc=False) \
                .limit(30).execute()
            return resp.data
        except Exception:
            return []

    async def _get_category_data(self, category: str) -> dict:
        try:
            products = self.supabase.table("products") \
                .select("id").eq("category", category).eq("active", True).execute()
            product_ids = [p["id"] for p in products.data]
            if not product_ids:
                return {}
            resp = self.supabase.table(self.SIGNAL_TABLE) \
                .select("mention_count, sentiment_score, velocity") \
                .in_("product_id", product_ids) \
                .eq("platform", self.PLATFORM) \
                .limit(300).execute()
            result = {"mention_count": [], "sentiment_score": [], "velocity": []}
            for row in resp.data:
                for key in result:
                    val = row.get(key)
                    if val is not None:
                        result[key].append(float(val))
            return result
        except Exception:
            return {}

    def _update_status(self, status: str, error_message: str = None, summary: str = None):
        try:
            now = datetime.now().isoformat()
            data = {
                "id": self.agent_run_id,
                "run_id": self.run_id,
                "platform": self.PLATFORM,
                "status": status,
                "products_processed": self.products_processed,
                "rows_written": self.rows_written,
                "rows_rejected": self.rows_rejected,
                "anomalies_detected": self.anomalies_detected,
            }

            # Include Apify usage if agent tracks it
            apify_count = getattr(self, "apify_results_total", 0)
            if apify_count > 0:
                data["apify_results_count"] = apify_count
                data["apify_estimated_cost"] = round(apify_count * 0.50 / 1000, 4)

            if status == "running":
                data["started_at"] = now
                self.supabase.table("agent_runs").insert(data).execute()
            else:
                data["completed_at"] = now
                if error_message:
                    data["error_message"] = error_message
                if summary:
                    data["agent_run_summary"] = summary
                if self.integrity_errors:
                    data["integrity_check_passed"] = False
                    data["integrity_errors"] = self.integrity_errors
                else:
                    data["integrity_check_passed"] = True

                run_resp = self.supabase.table("agent_runs") \
                    .select("started_at").eq("id", self.agent_run_id).execute()
                if run_resp.data and run_resp.data[0].get("started_at"):
                    started = datetime.fromisoformat(run_resp.data[0]["started_at"].replace("Z", "+00:00"))
                    data["duration_seconds"] = (datetime.now(started.tzinfo or None) - started).total_seconds()

                self.supabase.table("agent_runs").update(data).eq("id", self.agent_run_id).execute()

                # Check monthly Apify budget after run completes
                if apify_count > 0:
                    self._check_monthly_budget()

        except Exception as e:
            logger.error("Failed to update agent run status: %s", e)

    def _check_monthly_budget(self, budget_limit: float = 25.0):
        """
        Sum all apify_estimated_cost from agent_runs this month.
        If approaching budget, create an alert.
        """
        try:
            current_month = date.today().strftime("%Y-%m")
            month_start = f"{current_month}-01"

            resp = self.supabase.table("agent_runs") \
                .select("apify_estimated_cost") \
                .gte("created_at", month_start) \
                .execute()

            total_cost = sum(r.get("apify_estimated_cost", 0) or 0 for r in resp.data)
            pct_used = (total_cost / budget_limit * 100) if budget_limit > 0 else 0

            logger.info(
                "[%s] Monthly Apify spend: $%.2f / $%.2f (%.1f%%)",
                self.PLATFORM, total_cost, budget_limit, pct_used,
            )

            # Alert at 75% and 90%
            if pct_used >= 75:
                priority = "high" if pct_used >= 90 else "medium"
                self.supabase.table("alerts").insert({
                    "product_id": None,
                    "alert_type": "fad_warning",  # Reuse existing type
                    "priority": priority,
                    "message": (
                        f"Apify monthly budget at {pct_used:.0f}% "
                        f"(${total_cost:.2f} of ${budget_limit:.2f}). "
                        f"{'Approaching limit — reduce scrape frequency.' if pct_used < 90 else 'NEAR LIMIT — may exhaust before month end.'}"
                    ),
                    "actioned": False,
                }).execute()
                logger.warning("[%s] Budget alert triggered at %.0f%%", self.PLATFORM, pct_used)

        except Exception as e:
            logger.error("Failed to check monthly budget: %s", e)

    def _generate_run_summary(self) -> str:
        healing_stats = self.healing_tracker.get_stats()
        parts = [
            f"{self.PLATFORM} agent processed {self.products_processed} products.",
            f"{self.rows_written} signals written, {self.rows_rejected} rejected.",
        ]
        if self.anomalies_detected:
            parts.append(f"{self.anomalies_detected} anomalies detected.")
        if self.integrity_errors:
            parts.append(f"INTEGRITY WARNINGS: {len(self.integrity_errors)} products had data mismatches.")
        if self.errors:
            parts.append(f"{len(self.errors)} products had errors.")
        if healing_stats.get("total", 0) > 0:
            parts.append(f"Retry success rate: {healing_stats['success_rate']:.0%}.")
        return " ".join(parts)
