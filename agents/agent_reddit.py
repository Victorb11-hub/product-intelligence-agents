"""
Reddit Agent — Two-pass architecture using BasePlatformAgent.
  Pass 1: macrocosmos/reddit-scraper — lightweight metadata (subreddit search)
  Pass 2: trudax/reddit-scraper-lite — deep comments on top posts
All thresholds from .env. Subreddits from product_hashtags table.
Feeds Job 2 — Demand Validation (35% weight).
"""
import math
import logging
import time
from datetime import date, datetime, timedelta, timezone

from .base_platform_agent import BasePlatformAgent, _env_int, _env_float
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor

logger = logging.getLogger(__name__)


class RedditAgent(BasePlatformAgent):
    PLATFORM = "reddit"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        """Main entry point — called by BaseAgent pipeline."""
        start = time.time()

        # Load subreddits from DB (stored as hashtags with platform='reddit')
        subreddits = self.get_hashtags(product)
        if not subreddits:
            subreddits = ["SkincareAddiction", "AsianBeauty", "beauty"]
            logger.warning("[reddit] No subreddits in DB — using defaults")

        # Reddit always uses 90-day lookback regardless of backfill
        lookback = _env_int("REDDIT_LOOKBACK_DAYS", 90)

        # ═══════════════════════════════════════
        # PASS 1 — Metadata discovery
        # ═══════════════════════════════════════
        try:
            pass1_items = self.run_pass1(product, subreddits, lookback)
        except Exception as e:
            logger.error("[reddit] Pass 1 FAILED: %s", str(e)[:300])
            return self._error_result(product, subreddits, str(e), time.time() - start)

        # Filter and rank
        filtered = self.filter_pass1(pass1_items, lookback)
        top_n = _env_int("PASS2_POST_LIMIT", 20)
        top_posts = filtered[:top_n]

        self.log_run(1, {
            "total_found": len(pass1_items),
            "passed_filter": len(filtered),
            "kept": len(top_posts),
        })

        if not top_posts:
            logger.warning("[reddit] No posts passed filters — skipping Pass 2")
            return self._empty_result(product, subreddits, len(pass1_items), time.time() - start)

        # ═══════════════════════════════════════
        # PASS 2 — Deep comments on winners (tiered by engagement)
        # ═══════════════════════════════════════
        # Keep full post dicts so we can tier them by upvotes × log10(comments+1)
        posts_for_pass2 = [p for p in top_posts
                           if p.get("url") or p.get("permalink")]

        try:
            comments, tier_breakdown = self.run_pass2(posts_for_pass2, product)
        except Exception as e:
            logger.error("[reddit] Pass 2 FAILED: %s", str(e)[:300])
            comments = []
            tier_breakdown = {}

        # Score comments with parent virality weighting
        all_scored = []
        post_results = []
        for post in top_posts:
            post_score = post.get("score") or post.get("upVotes") or 0
            parent_virality = math.log10(max(post_score, 1) + 1)

            # Find comments belonging to this post
            post_url = post.get("url") or f"https://www.reddit.com{post.get('permalink', '')}"
            post_comments = [c for c in comments if
                             (c.get("postUrl") or c.get("url") or "") == post_url or
                             c.get("postId") == post.get("id")]
            # If we can't match by URL, distribute evenly (best effort)
            if not post_comments and comments:
                chunk = max(1, len(comments) // len(top_posts))
                idx = top_posts.index(post)
                post_comments = comments[idx * chunk:(idx + 1) * chunk]

            scored = self.score_comments(post_comments, parent_virality)

            # Build post result
            top_comment = ""
            if scored["scored_comments"]:
                top_c = max(scored["scored_comments"], key=lambda c: c.get("_intent_score", 0))
                top_comment = (top_c.get("body") or top_c.get("text") or "")[:150]

            upvotes = post.get("score") or post.get("upVotes") or 0
            comment_count = post.get("num_comments") or post.get("numComments") or post.get("numberOfComments") or 0
            disc_score = upvotes * math.log10(comment_count + 1)

            post_results.append({
                "title": (post.get("title") or "")[:200],
                "url": post_url,
                "subreddit": post.get("communityName") or post.get("parsedCommunityName") or post.get("subreddit") or "",
                "upvotes": upvotes,
                "comment_count": comment_count,
                "discussion_score": round(disc_score, 1),
                "top_comment": top_comment,
                "purchase_signals": scored["purchase_signal_count"],
                "negative_signals": scored["negative_signal_count"],
            })

            all_scored.extend(scored["scored_comments"])

        # Aggregate all comment scores
        total_stats = self.score_comments(
            [{"text": c.get("body") or c.get("text") or c.get("comment_body") or "", **c}
             for c in all_scored if c.get("body") or c.get("text") or c.get("comment_body")],
            parent_virality=1.0
        )

        # Write comments to DB with dedup
        written = self.write_comments_to_db(all_scored, product["id"])
        logger.info("[reddit] Wrote %d comments to DB", written)

        self.log_run(2, {
            "comment_count_total": total_stats["comment_count_total"],
            "posts_enriched": len(top_posts),
            "purchase_signal_count": total_stats["purchase_signal_count"],
            "negative_signal_count": total_stats["negative_signal_count"],
            "question_signal_count": total_stats["question_signal_count"],
        })

        # Update confidence and tracking
        self.update_confidence(product["id"])
        self.update_product_scrape_tracking(product)

        elapsed = time.time() - start

        # Build texts for BaseAgent pipeline (sentiment/intent batch processing)
        texts = []
        for post in top_posts:
            title = post.get("title") or ""
            if title:
                texts.append(title)

        return {
            "texts": texts,
            "raw_items": top_posts,
            "data_dates": [date.today().isoformat()],
            "mention_count": len(top_posts),
            "platform": "reddit",
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
            "subreddits_searched": subreddits,
            "top_posts": post_results[:10],
            "duration_seconds": round(elapsed, 1),
            "error": None,
            # Fields for BaseAgent signal row
            "buy_intent_comment_count": total_stats["purchase_signal_count"],
            "problem_language_comment_count": total_stats["negative_signal_count"],
            "growth_rate_wow": 0,
            "creator_tier_score": 0,
            "repeat_purchase_pct": 0,
            # Pass 2 tier audit
            "pass2_tier_breakdown": tier_breakdown,
            "pass2_total_comment_limit": tier_breakdown.get("total_limit", 0),
        }

    # ─── Pass 1: Metadata discovery ───

    def run_pass1(self, product: dict, hashtags: list[str], lookback_days: int) -> list[dict]:
        """Pull lightweight post metadata from subreddits."""
        subreddits = hashtags
        posts_per = _env_int("REDDIT_POSTS_PER_SUBREDDIT", 25)
        timeout = _env_int("REDDIT_PASS1_TIMEOUT", 180)
        all_terms = [product.get("name", "")] + (product.get("keywords") or [])
        search_terms = [t for t in all_terms[:3] if t]

        logger.info("[reddit pass1] Searching %d subreddits for %s (lookback=%d days)",
                    len(subreddits), search_terms, lookback_days)

        all_items = []
        # Build Reddit search URLs — guarantees subreddit scoping
        # Format: https://www.reddit.com/r/{sub}/search?q={term}&sort=top&t=month
        from urllib.parse import quote_plus
        start_urls = []
        for sub in subreddits:
            for term in search_terms[:2]:  # Top 2 keywords per subreddit
                # Use t=year to cover 90-day lookback window (Reddit only has month/year/all)
                url = f"https://www.reddit.com/r/{sub}/search?q={quote_plus(term)}&sort=top&t=year&restrict_sr=1"
                start_urls.append({"url": url})

        logger.info("[reddit pass1] Querying %d search URLs across %d subreddits",
                    len(start_urls), len(subreddits))

        # Use trudax/reddit-scraper-lite for Pass 1 — it supports startUrls
        # (macrocosmos/reddit-scraper ignores startUrls and uses its own defaults)
        try:
            items = run_actor(
                actor_id=APIFY_ACTORS.get("reddit_p2", "trudax/reddit-scraper-lite"),
                run_input={
                    "startUrls": start_urls,
                    "skipComments": True,
                    "maxItems": posts_per * len(subreddits),
                    "proxy": {"useApifyProxy": True},
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=timeout,
                max_items=posts_per * len(subreddits) + 50,
            )
            all_items.extend(items)
        except Exception as e:
            logger.error("[reddit pass1] Actor failed: %s", str(e)[:300])
            raise

        # Dedup by reddit ID
        seen = set()
        unique = []
        for item in all_items:
            rid = item.get("id", "")
            if rid and rid in seen:
                continue
            seen.add(rid)
            unique.append(item)

        logger.info("[reddit pass1] %d items returned, %d unique after dedup", len(all_items), len(unique))
        return unique

    def filter_pass1(self, items: list[dict], lookback_days: int) -> list[dict]:
        """Filter by date, engagement, and sort by discussion_score."""
        min_upvotes = _env_int("MIN_UPVOTES_REDDIT", 20)
        min_comments = _env_int("MIN_COMMENTS_REDDIT", 5)
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        filtered = []
        for item in items:
            # Date filter
            created = item.get("createdAt") or item.get("created_utc") or ""
            if created:
                try:
                    if isinstance(created, (int, float)):
                        dt = datetime.fromtimestamp(created, tz=timezone.utc)
                    else:
                        dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
                    if dt < cutoff:
                        continue
                except Exception:
                    pass

            score = item.get("score") or item.get("ups") or item.get("upVotes") or 0
            comments = item.get("num_comments") or item.get("numComments") or item.get("numberOfComments") or 0

            if score < min_upvotes:
                continue
            if comments < min_comments:
                continue

            filtered.append(item)

        # Sort by discussion_score = upvotes * log10(comments + 1)
        def _upvotes(p):
            return p.get("score") or p.get("ups") or p.get("upVotes") or 0
        def _comments(p):
            return p.get("num_comments") or p.get("numComments") or p.get("numberOfComments") or 0
        filtered.sort(
            key=lambda p: _upvotes(p) * math.log10(_comments(p) + 1),
            reverse=True
        )

        logger.info("[reddit pass1] %d passed filters (min %d upvotes, %d comments, %d day window)",
                    len(filtered), min_upvotes, min_comments, lookback_days)
        return filtered

    # ─── Pass 2: Deep comments ───

    def run_pass2(self, top_posts: list[dict], product: dict):
        """Pull full comment threads from top posts using tiered batching.
        Returns (comments, tier_breakdown). Splits posts by engagement into 3 tiers;
        each tier gets its own actor call with a proportional maxItems budget."""
        if not top_posts:
            return [], {}

        # Reddit engagement = upvotes × log10(num_comments + 1) — discussion density
        def eng(p):
            upvotes = p.get("score") or p.get("upVotes") or p.get("ups") or 0
            cmts = p.get("num_comments") or p.get("numComments") or p.get("numberOfComments") or 0
            return upvotes * math.log10(cmts + 1)

        tiers = self.compute_comment_tiers(top_posts, eng)

        timeout = _env_int("REDDIT_PASS2_TIMEOUT", 300)
        all_comments = []

        for tier_name, posts, per_post_limit in [
            ("tier1", tiers["tier1"], tiers["tier1_limit"]),
            ("tier2", tiers["tier2"], tiers["tier2_limit"]),
            ("tier3", tiers["tier3"], tiers["tier3_limit"]),
        ]:
            if not posts:
                continue
            urls = [p.get("url") or f"https://www.reddit.com{p.get('permalink', '')}" for p in posts]
            urls = [u for u in urls if u and "reddit.com" in u]
            if not urls:
                continue

            # Reddit actor uses global maxItems — multiply per-post limit × post count
            # Add 200 headroom for post rows mixed in with comment rows
            tier_max = per_post_limit * len(urls) + 200

            logger.info("[reddit pass2][%s] Fetching comments from %d posts (max %d total)",
                        tier_name, len(urls), tier_max)

            try:
                items = run_actor(
                    actor_id=APIFY_ACTORS.get("reddit_p2", APIFY_ACTORS.get("reddit")),
                    run_input={
                        "startUrls": [{"url": u} for u in urls],
                        "skipComments": False,
                        "maxItems": tier_max,
                        "proxy": {"useApifyProxy": True},
                    },
                    api_token=APIFY_API_TOKEN,
                    timeout_secs=timeout,
                    max_items=tier_max + 100,
                )
                tier_comments = []
                for item in items:
                    is_comment = (
                        item.get("dataType") == "comment" or
                        item.get("type") == "comment" or
                        item.get("parentId") is not None or
                        (item.get("body") and not item.get("title"))
                    )
                    if is_comment:
                        tier_comments.append(item)
                all_comments.extend(tier_comments)
                logger.info("[reddit pass2][%s] %d items returned, %d comments",
                            tier_name, len(items), len(tier_comments))
            except Exception as e:
                logger.error("[reddit pass2][%s] Actor failed: %s", tier_name, str(e)[:300])

        logger.info("[reddit pass2] TOTAL: %d comments across %d tiers",
                    len(all_comments), sum(1 for t in ("tier1", "tier2", "tier3") if tiers[t]))

        return all_comments, tiers["breakdown"]

    # ─── Signal row builder ───

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "reddit",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0,
            "creator_tier_score": 0,
            "buy_intent_comment_count": raw_data.get("buy_intent_comment_count", 0),
            "problem_language_comment_count": raw_data.get("problem_language_comment_count", 0),
            "high_intent_comment_count": raw_data.get("high_intent_count", 0),
            "avg_intent_score": raw_data.get("avg_weighted_intent", 0),
            "total_upvotes": sum(p.get("upvotes", 0) for p in raw_data.get("top_posts", [])),
            "total_comment_count": raw_data.get("pass2_comments", 0),
            "sample_size": raw_data.get("pass1_passed", 0),
            "purchase_signal_count": raw_data.get("purchase_signals", 0),
            "negative_signal_count": raw_data.get("negative_signals", 0),
            "question_signal_count": raw_data.get("question_signals", 0),
            "comment_count_total": raw_data.get("pass2_comments", 0),
            "weighted_comment_intent": raw_data.get("avg_weighted_intent", 0),
            "weighted_sentiment": raw_data.get("weighted_sentiment", 0),
            "lookback_days": _env_int("REDDIT_LOOKBACK_DAYS", 90),
            "is_backfill": False,
            "pass2_tier_breakdown": raw_data.get("pass2_tier_breakdown"),
            "pass2_total_comment_limit": raw_data.get("pass2_total_comment_limit", 0),
        }

    # ─── Helper methods ───

    def _error_result(self, product, subreddits, error_msg, elapsed):
        return {
            "texts": [], "raw_items": [], "data_dates": [],
            "mention_count": 0, "platform": "reddit",
            "pass1_total": 0, "pass1_passed": 0,
            "pass2_posts": 0, "pass2_comments": 0,
            "purchase_signals": 0, "negative_signals": 0, "question_signals": 0,
            "avg_weighted_intent": 0, "weighted_sentiment": 0, "high_intent_count": 0,
            "subreddits_searched": subreddits, "top_posts": [],
            "duration_seconds": round(elapsed, 1), "error": error_msg,
            "buy_intent_comment_count": 0, "problem_language_comment_count": 0,
            "growth_rate_wow": 0, "creator_tier_score": 0, "repeat_purchase_pct": 0,
            "pass2_tier_breakdown": {}, "pass2_total_comment_limit": 0,
        }

    def _empty_result(self, product, subreddits, total_found, elapsed):
        result = self._error_result(product, subreddits, None, elapsed)
        result["pass1_total"] = total_found
        return result
