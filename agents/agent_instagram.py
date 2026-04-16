"""
Instagram Agent — Two-pass architecture using BasePlatformAgent.
  Pass 1: apify/instagram-hashtag-scraper — metadata only
  Pass 2: apify/instagram-comment-scraper — deep comments on top posts
All thresholds from .env. Hashtags from product_hashtags table.
Feeds Job 1 — Early Detection (30% weight).

Field mappings confirmed from actor discovery:
  Pass 1: id, url, shortCode, timestamp (ISO), likesCount, commentsCount,
          type (Image/Video/Sidecar), productType (feed/clips/igtv),
          caption, hashtags (list[str])
  Pass 2: id, text, postUrl (join key!), likesCount, timestamp,
          repliesCount, owner.is_verified
  Note: Pass 1 has NO reel view counts — use engagement instead
  Note: Pass 2 returns error placeholder rows — filter on "error" not in item
"""
import math
import logging
import time
from datetime import date, datetime, timedelta, timezone

from .base_platform_agent import BasePlatformAgent, _env_int, _env_float
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor

logger = logging.getLogger(__name__)


class InstagramAgent(BasePlatformAgent):
    PLATFORM = "instagram"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        """Main entry point — called by BaseAgent pipeline."""
        start = time.time()

        hashtags = self.get_hashtags(product)
        if not hashtags:
            hashtags = [product_name.lower().replace(" ", "")]
            logger.warning("[instagram] No hashtags in DB — using product name: %s", hashtags)

        lookback = self.get_lookback_days(product)

        # ═══════════════════════════════════════
        # PASS 1 — Metadata discovery
        # ═══════════════════════════════════════
        try:
            pass1_items = self.run_pass1(product, hashtags, lookback)
        except Exception as e:
            logger.error("[instagram] Pass 1 FAILED: %s", str(e)[:300])
            return self._error_result(hashtags, str(e), time.time() - start)

        filtered = self.filter_pass1(pass1_items, lookback)
        top_n = _env_int("PASS2_POST_LIMIT", 20)
        top_posts = filtered[:top_n]

        # Count reels vs photos in final selection
        reels = sum(1 for p in top_posts if self._is_reel(p))
        photos = len(top_posts) - reels
        reel_pct = round(reels / max(len(top_posts), 1) * 100, 1)
        if reel_pct < 60 and len(top_posts) >= 5:
            logger.warning("[instagram pass1] Reel percentage %.1f%% below 60%% target", reel_pct)

        logger.info("[instagram pass1] %d posts found, %d passed filters (%d reels, %d photos). "
                    "Reel pct: %.1f%%. Keeping top %d for Pass 2.",
                    len(pass1_items), len(filtered), reels, photos, reel_pct, len(top_posts))

        if not top_posts:
            logger.warning("[instagram] No posts passed filters — skipping Pass 2")
            return self._empty_result(hashtags, len(pass1_items), time.time() - start)

        # Aggregate Pass 1 metrics
        total_likes = sum(p.get("likesCount") or 0 for p in top_posts)
        total_comments_count = sum(p.get("commentsCount") or 0 for p in top_posts)

        # ═══════════════════════════════════════
        # PASS 2 — Deep comments on winners (tiered by engagement)
        # ═══════════════════════════════════════
        # Only fetch comments for posts that actually have comments
        posts_for_pass2 = [p for p in top_posts if p.get("url") and (p.get("commentsCount") or 0) > 0]

        try:
            comments, tier_breakdown = (self.run_pass2(posts_for_pass2, product) if posts_for_pass2 else ([], {}))
        except Exception as e:
            logger.error("[instagram] Pass 2 FAILED: %s", str(e)[:300])
            comments = []
            tier_breakdown = {}

        # Score comments grouped by parent post (using postUrl as join key)
        all_scored = []
        post_results = []
        for post in top_posts:
            likes = post.get("likesCount") or 0
            cmts = post.get("commentsCount") or 0
            parent_virality = math.log10(max(likes + cmts, 1) + 1)

            post_url = post.get("url") or ""
            post_comments = [c for c in comments if c.get("postUrl") == post_url]

            scored = self.score_comments(post_comments, parent_virality)

            top_comment = ""
            if scored["scored_comments"]:
                top_c = max(scored["scored_comments"], key=lambda c: c.get("_intent_score", 0))
                top_comment = (top_c.get("text") or "")[:150]

            post_results.append({
                "url": post_url,
                "type": "reel" if self._is_reel(post) else ("video" if post.get("type") == "Video" else "photo"),
                "likes": likes,
                "comment_count": cmts,
                "views": 0,  # not available in Pass 1 actor
                "caption_snippet": (post.get("caption") or "")[:100],
                "top_comment": top_comment,
                "purchase_signals": scored["purchase_signal_count"],
                "negative_signals": scored["negative_signal_count"],
            })

            all_scored.extend(scored["scored_comments"])

            # Log if Pass 2 returned 0 for a post that had comments in Pass 1
            if cmts > 0 and not post_comments:
                logger.info("[instagram] Zero comments returned for post with %d comments in Pass 1. "
                            "Post may have restricted comments: %s", cmts, post_url)

        # Aggregate all scored comments
        total_stats = self.score_comments(
            [{"text": c.get("text") or "", **c} for c in all_scored if c.get("text")],
            parent_virality=1.0
        )

        # Write comments to DB with dedup
        written = self.write_comments_to_db(all_scored, product["id"])
        logger.info("[instagram] Wrote %d comments to DB", written)

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
        texts = [(p.get("caption") or "")[:200] for p in top_posts if p.get("caption")]

        return {
            "texts": texts,
            "raw_items": top_posts,
            "data_dates": [date.today().isoformat()],
            "mention_count": len(top_posts),
            "platform": "instagram",
            "pass1_total": len(pass1_items),
            "pass1_passed": len(filtered),
            "pass1_reels": reels,
            "pass1_photos": photos,
            "reel_percentage": reel_pct,
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
            # Aggregates for signal row / scoring
            "total_likes": total_likes,
            "total_comments": total_comments_count,
            "creator_tier_score": 0.5,  # Hashtag scraper doesn't expose follower data
            "buy_intent_comment_count": total_stats["purchase_signal_count"],
            "problem_language_comment_count": total_stats["negative_signal_count"],
            "growth_rate_wow": 0,
            "repeat_purchase_pct": 0,
            # Pass 2 tier audit
            "pass2_tier_breakdown": tier_breakdown,
            "pass2_total_comment_limit": tier_breakdown.get("total_limit", 0),
        }

    # ─── Pass 1: Metadata discovery ───

    def run_pass1(self, product: dict, hashtags: list[str], lookback_days: int) -> list[dict]:
        """Pull lightweight post metadata — no comments."""
        max_hashtags = _env_int("INSTAGRAM_MAX_HASHTAGS", 10)
        results_per = _env_int("INSTAGRAM_RESULTS_LIMIT", 150)
        timeout = _env_int("INSTAGRAM_PASS1_TIMEOUT", 240)
        max_items = _env_int("INSTAGRAM_PASS1_MAX_ITEMS", 200)

        tags = hashtags[:max_hashtags]

        logger.info("[instagram pass1] Searching %d hashtags, %d results/hashtag", len(tags), results_per)

        try:
            items = run_actor(
                actor_id=APIFY_ACTORS["instagram"],
                run_input={
                    "hashtags": tags,
                    "resultsLimit": results_per,
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=timeout,
                max_items=max_items,
            )
        except Exception as e:
            logger.error("[instagram pass1] Actor failed: %s", str(e)[:300])
            raise

        # Filter out error placeholder rows
        items = [i for i in items if not i.get("error")]

        # Dedup by post ID
        seen = set()
        unique = []
        for item in items:
            pid = item.get("id") or item.get("shortCode") or ""
            if pid and pid in seen:
                continue
            seen.add(pid)
            unique.append(item)

        logger.info("[instagram pass1] %d items returned, %d unique after dedup", len(items), len(unique))
        return unique

    def filter_pass1(self, items: list[dict], lookback_days: int) -> list[dict]:
        """Filter by date + engagement, sort by comments desc / likes desc."""
        min_reel_views = _env_int("MIN_VIEWS_INSTAGRAM_REEL", 30000)  # used as comment threshold for reels
        min_photo_likes = _env_int("MIN_LIKES_INSTAGRAM_PHOTO", 500)
        min_photo_comments = _env_int("MIN_COMMENTS_INSTAGRAM_PHOTO", 100)
        min_engagement = _env_int("INSTAGRAM_ENGAGEMENT_THRESHOLD", 20)
        cutoff = self.get_lookback_cutoff(lookback_days)

        filtered = []
        for item in items:
            # Date filter
            ts = item.get("timestamp") or item.get("taken_at") or ""
            if ts:
                try:
                    if isinstance(ts, str):
                        created = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    elif isinstance(ts, (int, float)):
                        created = datetime.fromtimestamp(ts, tz=timezone.utc)
                    else:
                        created = None
                    if created and created < cutoff:
                        continue
                except Exception:
                    pass

            likes = item.get("likesCount") or 0
            comments = item.get("commentsCount") or 0

            # Must have comments to be useful for Pass 2
            if comments <= 0:
                continue

            # Reel vs photo thresholds
            if self._is_reel(item):
                # Reels: comments >= 200 OR engagement above threshold
                # (no view count available from this actor, so use engagement)
                if comments < 200 and (likes + comments) < min_engagement:
                    continue
            else:
                # Photos/videos: likes >= threshold OR comments >= threshold
                if likes < min_photo_likes and comments < min_photo_comments:
                    if (likes + comments) < min_engagement:
                        continue

            filtered.append(item)

        # Sort: primary commentsCount desc, secondary likesCount desc
        filtered.sort(
            key=lambda p: ((p.get("commentsCount") or 0), (p.get("likesCount") or 0)),
            reverse=True
        )

        logger.info("[instagram pass1] %d passed engagement+date filters", len(filtered))
        return filtered

    # ─── Pass 2: Deep comments ───

    def run_pass2(self, top_posts: list[dict], product: dict):
        """Pull comments from top posts. Returns (comments, tier_breakdown).
        Uses tiered comment limits based on engagement — makes up to 3 actor calls."""
        if not top_posts:
            return [], {}

        # Instagram engagement = likes + comments (no view count available from hashtag scraper)
        def eng(p):
            return (p.get("likesCount") or 0) + (p.get("commentsCount") or 0)

        tiers = self.compute_comment_tiers(top_posts, eng)

        timeout = _env_int("INSTAGRAM_PASS2_TIMEOUT", 300)
        all_comments = []

        for tier_name, posts, per_post_limit in [
            ("tier1", tiers["tier1"], tiers["tier1_limit"]),
            ("tier2", tiers["tier2"], tiers["tier2_limit"]),
            ("tier3", tiers["tier3"], tiers["tier3_limit"]),
        ]:
            if not posts:
                continue
            urls = [p.get("url") for p in posts if p.get("url")]
            if not urls:
                continue

            max_items = per_post_limit * len(urls) + 100

            logger.info("[instagram pass2][%s] Fetching comments from %d posts (%d/post max)",
                        tier_name, len(urls), per_post_limit)

            try:
                items = run_actor(
                    actor_id=APIFY_ACTORS["instagram_comments"],
                    run_input={
                        "directUrls": urls,
                        "resultsLimit": per_post_limit,
                    },
                    api_token=APIFY_API_TOKEN,
                    timeout_secs=timeout,
                    max_items=max_items,
                )
                valid = [item for item in items if item.get("text") and not item.get("error")]
                unavailable = [i for i in items if i.get("error")]
                for u in unavailable:
                    logger.info("[instagram] Post unavailable: %s", u.get("url") or u.get("inputUrl"))
                all_comments.extend(valid)
                logger.info("[instagram pass2][%s] %d items returned, %d valid, %d unavailable",
                            tier_name, len(items), len(valid), len(unavailable))
            except Exception as e:
                logger.error("[instagram pass2][%s] Actor failed: %s", tier_name, str(e)[:300])

        logger.info("[instagram pass2] TOTAL: %d valid comments across %d tiers",
                    len(all_comments), sum(1 for t in ("tier1", "tier2", "tier3") if tiers[t]))

        return all_comments, tiers["breakdown"]

    # ─── Helpers ───

    def _is_reel(self, post: dict) -> bool:
        """Detect if post is a reel based on productType or type."""
        return (post.get("productType") == "clips" or
                post.get("type") == "Video")

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "instagram",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0,
            "creator_tier_score": raw_data.get("creator_tier_score", 0.5),
            "buy_intent_comment_count": raw_data.get("buy_intent_comment_count", 0),
            "problem_language_comment_count": raw_data.get("problem_language_comment_count", 0),
            "high_intent_comment_count": raw_data.get("high_intent_count", 0),
            "avg_intent_score": raw_data.get("avg_weighted_intent", 0),
            "total_upvotes": raw_data.get("total_likes", 0),
            "total_comment_count": raw_data.get("total_comments", 0),
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

    def _error_result(self, hashtags, error_msg, elapsed):
        return {
            "texts": [], "raw_items": [], "data_dates": [],
            "mention_count": 0, "platform": "instagram",
            "pass1_total": 0, "pass1_passed": 0, "pass1_reels": 0, "pass1_photos": 0,
            "reel_percentage": 0,
            "pass2_posts": 0, "pass2_comments": 0,
            "purchase_signals": 0, "negative_signals": 0, "question_signals": 0,
            "avg_weighted_intent": 0, "weighted_sentiment": 0, "high_intent_count": 0,
            "hashtags_searched": hashtags, "top_posts": [],
            "duration_seconds": round(elapsed, 1), "error": error_msg,
            "total_likes": 0, "total_comments": 0,
            "creator_tier_score": 0.5, "buy_intent_comment_count": 0,
            "problem_language_comment_count": 0, "growth_rate_wow": 0,
            "repeat_purchase_pct": 0,
            "pass2_tier_breakdown": {}, "pass2_total_comment_limit": 0,
        }

    def _empty_result(self, hashtags, total_found, elapsed):
        result = self._error_result(hashtags, None, elapsed)
        result["pass1_total"] = total_found
        return result
