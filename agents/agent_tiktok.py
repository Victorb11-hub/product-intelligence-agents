"""
TikTok Agent — Two-pass architecture using BasePlatformAgent.
  Pass 1: clockworks/tiktok-scraper — metadata only (maxCommentsPerPost=0)
  Pass 2: clockworks/tiktok-comments-scraper — deep comments on top posts
All thresholds from .env. Hashtags from product_hashtags table.
Feeds Job 1 — Early Detection (40% weight).

Field mappings confirmed from actor discovery:
  id, webVideoUrl, createTime (unix int), createTimeISO,
  playCount, diggCount, commentCount, shareCount,
  text (caption), hashtags [{name: ...}], authorMeta {fans, name, ...}
"""
import math
import logging
import time
from datetime import date, datetime, timedelta, timezone

from .base_platform_agent import BasePlatformAgent, _env_int, _env_float
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor

logger = logging.getLogger(__name__)

CREATOR_TIERS = [
    (1_000_000, 0.90, "mega"),
    (100_000,   0.75, "macro"),
    (10_000,    0.50, "micro"),
    (0,         0.20, "nano"),
]


class TikTokAgent(BasePlatformAgent):
    PLATFORM = "tiktok"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        """Main entry point — called by BaseAgent pipeline."""
        start = time.time()

        hashtags = self.get_hashtags(product)
        if not hashtags:
            hashtags = [product_name.lower().replace(" ", "")]
            logger.warning("[tiktok] No hashtags in DB — using product name: %s", hashtags)

        lookback = self.get_lookback_days(product)
        is_backfill = not product.get("backfill_completed")

        # ═══════════════════════════════════════
        # PASS 1 — Metadata discovery (no comments)
        # ═══════════════════════════════════════
        try:
            pass1_items = self.run_pass1(product, hashtags, lookback)
        except Exception as e:
            logger.error("[tiktok] Pass 1 FAILED: %s", str(e)[:300])
            return self._error_result(hashtags, str(e), time.time() - start)

        filtered = self.filter_pass1(pass1_items, lookback)
        top_n = _env_int("PASS2_POST_LIMIT", 20)
        top_posts = filtered[:top_n]

        self.log_run(1, {
            "total_found": len(pass1_items),
            "passed_filter": len(filtered),
            "kept": len(top_posts),
        })

        if not top_posts:
            logger.warning("[tiktok] No posts passed filters — skipping Pass 2")
            return self._empty_result(hashtags, len(pass1_items), time.time() - start)

        # Aggregate Pass 1 metrics
        total_views = sum(p.get("playCount") or 0 for p in top_posts)
        total_likes = sum(p.get("diggCount") or 0 for p in top_posts)
        total_comments_count = sum(p.get("commentCount") or 0 for p in top_posts)
        total_shares = sum(p.get("shareCount") or 0 for p in top_posts)

        # Creator tier scoring
        creator_scores = []
        for p in pass1_items:
            author = p.get("authorMeta") or {}
            fans = author.get("fans") or 0
            for threshold, score, _ in CREATOR_TIERS:
                if fans >= threshold:
                    creator_scores.append(score)
                    break
        avg_creator = sum(creator_scores) / max(len(creator_scores), 1) if creator_scores else 0.3

        # ═══════════════════════════════════════
        # PASS 2 — Deep comments on winners (tiered by engagement)
        # ═══════════════════════════════════════
        posts_with_urls = [p for p in top_posts if p.get("webVideoUrl")]

        try:
            comments, tier_breakdown = self.run_pass2(posts_with_urls, product)
        except Exception as e:
            logger.error("[tiktok] Pass 2 FAILED: %s", str(e)[:300])
            comments = []
            tier_breakdown = {}

        # Score comments with parent virality weighting
        all_scored = []
        post_results = []
        for post in top_posts:
            views = post.get("playCount") or 1
            parent_virality = math.log10(max(views, 1) + 1)

            post_url = post.get("webVideoUrl") or ""
            post_comments = [c for c in comments if
                             (c.get("videoWebUrl") or c.get("submittedVideoUrl") or "") == post_url]

            scored = self.score_comments(post_comments, parent_virality)

            likes = post.get("diggCount") or 0
            cmts = post.get("commentCount") or 0
            shares = post.get("shareCount") or 0
            eng_rate = (likes + cmts + shares) / max(views, 1) * 100

            top_comment = ""
            if scored["scored_comments"]:
                top_c = max(scored["scored_comments"], key=lambda c: c.get("_intent_score", 0))
                top_comment = (top_c.get("text") or "")[:150]

            post_results.append({
                "url": post_url,
                "views": views,
                "likes": likes,
                "comment_count": cmts,
                "shares": shares,
                "engagement_rate": round(eng_rate, 2),
                "caption_snippet": (post.get("text") or "")[:100],
                "top_comment": top_comment,
                "purchase_signals": scored["purchase_signal_count"],
                "negative_signals": scored["negative_signal_count"],
            })

            all_scored.extend(scored["scored_comments"])

        # Aggregate all scored comments
        total_stats = self.score_comments(
            [{"text": c.get("text") or c.get("body") or c.get("comment_body") or "", **c}
             for c in all_scored if c.get("text") or c.get("body") or c.get("comment_body")],
            parent_virality=1.0
        )

        # Write comments to DB with dedup
        written = self.write_comments_to_db(all_scored, product["id"])
        logger.info("[tiktok] Wrote %d comments to DB", written)

        self.log_run(2, {
            "comment_count_total": total_stats["comment_count_total"],
            "posts_enriched": len(top_posts),
            "purchase_signal_count": total_stats["purchase_signal_count"],
            "negative_signal_count": total_stats["negative_signal_count"],
            "question_signal_count": total_stats["question_signal_count"],
        })

        self.update_confidence(product["id"])
        self.update_product_scrape_tracking(product)

        elapsed = time.time() - start

        # Texts for BaseAgent pipeline
        texts = [(p.get("text") or "")[:200] for p in top_posts if p.get("text")]

        return {
            "texts": texts,
            "raw_items": top_posts,
            "data_dates": [date.today().isoformat()],
            "mention_count": len(top_posts),
            "platform": "tiktok",
            "pass1_total": len(pass1_items),
            "pass1_passed": len(filtered),
            "pass2_posts": len(top_posts),
            "pass2_comments": total_stats["comment_count_total"],
            "purchase_signals": total_stats["purchase_signal_count"],
            "negative_signals": total_stats["negative_signal_count"],
            "question_signals": total_stats["question_signal_count"],
            "avg_weighted_intent": round(total_stats["weighted_comment_intent"], 4),
            "weighted_sentiment": round(total_stats["weighted_sentiment"], 4),
            "high_intent_count": total_stats["high_intent_count"],
            "hashtags_searched": hashtags,
            "top_posts": post_results[:10],
            "duration_seconds": round(elapsed, 1),
            "error": None,
            # Metrics for signal row / scoring engine
            "total_views": total_views,
            "total_likes": total_likes,
            "total_comments": total_comments_count,
            "total_shares": total_shares,
            "creator_tier_score": round(avg_creator, 4),
            "buy_intent_comment_count": total_stats["purchase_signal_count"],
            "problem_language_comment_count": total_stats["negative_signal_count"],
            "growth_rate_wow": 0,
            "avg_view_velocity": 0,
            "repeat_purchase_pct": 0,
            # Pass 2 tier audit
            "pass2_tier_breakdown": tier_breakdown,
            "pass2_total_comment_limit": tier_breakdown.get("total_limit", 0),
        }

    # ─── Pass 1: Metadata discovery (no comments) ───

    def run_pass1(self, product: dict, hashtags: list[str], lookback_days: int) -> list[dict]:
        """Pull lightweight metadata — no comments."""
        max_hashtags = _env_int("TIKTOK_MAX_HASHTAGS", 15)
        results_per_page = _env_int("TIKTOK_RESULTS_PER_PAGE", 50)
        timeout = _env_int("TIKTOK_PASS1_TIMEOUT", 240)
        max_items = _env_int("TIKTOK_PASS1_MAX_ITEMS", 200)

        tags = hashtags[:max_hashtags]

        logger.info("[tiktok pass1] Searching %d hashtags, %d results/page, no comments",
                    len(tags), results_per_page)

        try:
            items = run_actor(
                actor_id=APIFY_ACTORS["tiktok"],
                run_input={
                    "hashtags": tags,
                    "resultsPerPage": results_per_page,
                    "maxCommentsPerPost": 0,
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=timeout,
                max_items=max_items,
            )
        except Exception as e:
            logger.error("[tiktok pass1] Actor failed: %s", str(e)[:300])
            raise

        # Dedup by video ID
        seen = set()
        unique = []
        for item in items:
            vid = item.get("id", "")
            if vid and vid in seen:
                continue
            seen.add(vid)
            unique.append(item)

        logger.info("[tiktok pass1] %d items returned, %d unique after dedup", len(items), len(unique))
        return unique

    def filter_pass1(self, items: list[dict], lookback_days: int) -> list[dict]:
        """Filter by date, views, comment count. Sort by engagement."""
        min_views = _env_int("MIN_VIEWS_TIKTOK", 50000)
        cutoff = self.get_lookback_cutoff(lookback_days)

        filtered = []
        for item in items:
            # Date filter
            ct = item.get("createTime")
            if ct and isinstance(ct, (int, float)) and ct > 1_000_000_000:
                created = datetime.fromtimestamp(ct, tz=timezone.utc)
                if created < cutoff:
                    continue
            elif item.get("createTimeISO"):
                try:
                    created = datetime.fromisoformat(item["createTimeISO"].replace("Z", "+00:00"))
                    if created < cutoff:
                        continue
                except Exception:
                    pass

            views = item.get("playCount") or 0
            comments = item.get("commentCount") or 0

            if views < min_views:
                continue
            if comments <= 0:
                continue

            # Skip ads/sponsored
            if item.get("isAd") or item.get("isSponsored"):
                continue

            filtered.append(item)

        # Sort: primary by commentCount desc, secondary by engagement rate
        def sort_key(p):
            views = max(p.get("playCount") or 1, 1)
            likes = p.get("diggCount") or 0
            cmts = p.get("commentCount") or 0
            shares = p.get("shareCount") or 0
            eng_rate = (likes + cmts + shares) / views
            return (cmts, eng_rate)

        filtered.sort(key=sort_key, reverse=True)

        logger.info("[tiktok pass1] %d passed filters (min %d views, comments > 0, %d day window)",
                    len(filtered), min_views, lookback_days)
        return filtered

    # ─── Pass 2: Deep comments on winners ───

    def run_pass2(self, top_posts: list[dict], product: dict):
        """Pull comments from top videos. Returns (comments, tier_breakdown).
        Uses tiered comment limits based on engagement — makes up to 3 actor calls."""
        if not top_posts:
            return [], {}

        # TikTok engagement = views (playCount)
        def eng(p):
            return p.get("playCount") or 0

        tiers = self.compute_comment_tiers(top_posts, eng)

        replies_per = _env_int("PASS2_REPLIES_PER_COMMENT", 10)
        timeout = _env_int("TIKTOK_PASS2_TIMEOUT", 300)

        all_comments = []

        for tier_name, posts, per_post_limit in [
            ("tier1", tiers["tier1"], tiers["tier1_limit"]),
            ("tier2", tiers["tier2"], tiers["tier2_limit"]),
            ("tier3", tiers["tier3"], tiers["tier3_limit"]),
        ]:
            if not posts:
                continue
            urls = [p.get("webVideoUrl") for p in posts if p.get("webVideoUrl")]
            if not urls:
                continue

            # max_items = per-post limit × number of posts + headroom
            max_items = per_post_limit * len(urls) + 100

            logger.info("[tiktok pass2][%s] Fetching comments from %d videos (%d/post max)",
                        tier_name, len(urls), per_post_limit)

            try:
                items = run_actor(
                    actor_id=APIFY_ACTORS["tiktok_comments"],
                    run_input={
                        "postURLs": urls,
                        "commentsPerPost": per_post_limit,
                        "maxRepliesPerComment": replies_per,
                    },
                    api_token=APIFY_API_TOKEN,
                    timeout_secs=timeout,
                    max_items=max_items,
                )
                valid = [item for item in items if item.get("text") and not item.get("error")]
                all_comments.extend(valid)
                logger.info("[tiktok pass2][%s] %d items returned, %d valid", tier_name, len(items), len(valid))
            except Exception as e:
                logger.error("[tiktok pass2][%s] Actor failed: %s", tier_name, str(e)[:300])
                # Continue with other tiers — one failure doesn't kill the whole pass

        logger.info("[tiktok pass2] TOTAL: %d valid comments across %d tiers",
                    len(all_comments), sum(1 for t in ("tier1", "tier2", "tier3") if tiers[t]))

        return all_comments, tiers["breakdown"]

    # ─── Signal row builder ───

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "tiktok",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0,
            "creator_tier_score": raw_data.get("creator_tier_score", 0),
            "buy_intent_comment_count": raw_data.get("buy_intent_comment_count", 0),
            "problem_language_comment_count": raw_data.get("problem_language_comment_count", 0),
            "high_intent_comment_count": raw_data.get("high_intent_count", 0),
            "avg_intent_score": raw_data.get("avg_weighted_intent", 0),
            "total_upvotes": raw_data.get("total_likes", 0),
            "total_comment_count": raw_data.get("total_comments", 0),
            "total_views": raw_data.get("total_views", 0),
            "sample_size": raw_data.get("mention_count", 0),
            "purchase_signal_count": raw_data.get("purchase_signals", 0),
            "negative_signal_count": raw_data.get("negative_signals", 0),
            "question_signal_count": raw_data.get("question_signals", 0),
            "comment_count_total": raw_data.get("pass2_comments", 0),
            "weighted_comment_intent": raw_data.get("avg_weighted_intent", 0),
            "weighted_sentiment": raw_data.get("weighted_sentiment", 0),
            "lookback_days": self.get_lookback_days({"backfill_completed": True}),
            "is_backfill": False,
            "pass2_tier_breakdown": raw_data.get("pass2_tier_breakdown"),
            "pass2_total_comment_limit": raw_data.get("pass2_total_comment_limit", 0),
        }

    # ─── Helpers ───

    def _error_result(self, hashtags, error_msg, elapsed):
        return {
            "texts": [], "raw_items": [], "data_dates": [],
            "mention_count": 0, "platform": "tiktok",
            "pass1_total": 0, "pass1_passed": 0,
            "pass2_posts": 0, "pass2_comments": 0,
            "purchase_signals": 0, "negative_signals": 0, "question_signals": 0,
            "avg_weighted_intent": 0, "weighted_sentiment": 0, "high_intent_count": 0,
            "hashtags_searched": hashtags, "top_posts": [],
            "duration_seconds": round(elapsed, 1), "error": error_msg,
            "total_views": 0, "total_likes": 0, "total_comments": 0, "total_shares": 0,
            "creator_tier_score": 0, "buy_intent_comment_count": 0,
            "problem_language_comment_count": 0, "growth_rate_wow": 0,
            "avg_view_velocity": 0, "repeat_purchase_pct": 0,
            "pass2_tier_breakdown": {}, "pass2_total_comment_limit": 0,
        }

    def _empty_result(self, hashtags, total_found, elapsed):
        result = self._error_result(hashtags, None, elapsed)
        result["pass1_total"] = total_found
        return result
