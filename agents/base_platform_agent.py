"""
BasePlatformAgent — Two-pass architecture base class.

All social platform agents (TikTok, Instagram, Reddit) extend this.
Provides:
  - Lookback window logic (backfill vs weekly)
  - Two-pass pipeline: run_pass1() → filter → run_pass2()
  - Comment scoring with purchase/negative/question signals
  - Dedup checking on all inserts
  - Confidence level calculation
  - All thresholds read from environment variables

Does NOT replace BaseAgent — extends it. BaseAgent's run() pipeline
still handles posts-first writes, skills, signal rows, and integrity checks.
"""
import os
import math
import logging
from datetime import date, datetime, timedelta, timezone
from abc import abstractmethod

from .base_agent import BaseAgent
from .config import get_supabase, APIFY_API_TOKEN
from .skills.apify_helper import run_actor
from .skills.sentiment import analyze_sentiment
from .skills.intent_scorer import score_intent

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════
# Purchase signal word lists — load from .env or use expanded defaults
# ═══════════════════════════════════════════════════
DEFAULT_PURCHASE_POSITIVE = [
    "where to buy", "link please", "just ordered", "bought this",
    "repurchasing", "on my 3rd", "subscribe and save", "added to cart",
    "worth every penny", "game changer", "holy grail", "buying again",
    "perfect gift", "highly recommend", "already ordered",
    "just purchased", "need this in my life", "shut up and take my money",
    "obsessed with this", "need this", "want one", "need it", "want this",
    "must have", "have to try", "dying to try", "need to try",
    "putting this in my cart", "ordering this", "about to order",
    "this is on my list", "adding this to my list",
    "where is this from", "where did you get this", "what brand is this",
    "send me the link", "drop the link", "link in bio", "what is this called",
    "someone send me this", "i need to find this",
    "does anyone know where i can get this",
    "this is a repurchase", "i keep buying this", "i buy this every month",
    "cant live without", "can't live without", "life changing", "changed my skin",
    "worth it", "worth the money", "worth every cent",
    "best purchase ever", "best thing ive bought", "best thing i've bought",
    "10 out of 10", "would recommend",
    "telling all my friends", "gifting this", "stocking up",
    "bought 3", "bought in bulk", "this sold me", "just bought",
    "on my way to buy", "on my second", "restocked", "keep buying", "auto-subscribe",
]

DEFAULT_PURCHASE_NEGATIVE = [
    "doesn't work", "doesnt work", "did not work", "didn't work",
    "waste of money", "wasted my money", "not worth it",
    "broke me out", "broke out", "caused breakouts",
    "gave me a rash", "burned my face", "burned my skin",
    "allergic reaction", "irritated my skin",
    "returned it", "returning this", "sent it back",
    "fake product", "so disappointed", "no difference",
    "do not buy", "stay away", "scam", "total scam",
    "dont waste your money", "don't waste your money", "save your money",
    "overrated", "not worth the hype", "overhyped",
    "made my skin worse", "skin got worse",
    "pilled on my skin", "pills up", "doesnt absorb", "doesn't absorb",
    "sticky residue", "smells weird", "smells bad",
    "too expensive for what it is", "not worth the price",
    "packaging broke", "leaked everywhere",
    "customer service was terrible", "never again",
    "worst purchase", "would not recommend",
    "1 star", "one star", "negative review",
    "side effect", "dangerous",
]

DEFAULT_PURCHASE_QUESTION = [
    "does this work", "where can i find this", "what brand is this",
    "how much does this cost", "is it worth it", "has anyone tried this",
    "recommendations for", "which one should i get",
    "does anyone know where", "is this good", "is this worth buying",
    "should i buy this", "anyone tried this", "have you tried this",
    "what do you think of this", "honest review",
    "is this better than", "how does this compare",
    "whats the difference between", "what's the difference between",
    "which is better", "does this really work",
    "how long does it take", "how often do you use this",
    "can i use this if", "is this safe for", "good for sensitive skin",
    "good for dry skin", "good for oily skin",
    "does this help with", "will this work for",
    "looking for something like this", "looking for a good",
    "need a recommendation", "can anyone recommend", "what should i use",
]

DEFAULT_NEUTRAL_HIGH_INTENT = [
    "love this", "love it", "amazing", "incredible",
    "absolutely love", "cant get enough", "can't get enough",
    "favorite", "my favorite", "been using this for years",
    "use this every day", "daily use", "part of my routine",
    "in my routine", "never going back", "switched to this",
    "converted", "this converted me", "my holy grail",
    "underrated", "hidden gem", "so underrated",
    "everyone should try this", "everyone needs this",
    "this is the one", "found my new favorite",
    "glowing skin", "my skin loves this", "skin feels amazing",
]


def _env_list(key: str, default: list) -> list:
    """Read comma-separated list from environment, fall back to default."""
    val = os.environ.get(key)
    if val:
        return [t.strip().lower() for t in val.split(",") if t.strip()]
    return [t.lower() for t in default]


PURCHASE_SIGNALS = _env_list("PURCHASE_SIGNALS_POSITIVE", DEFAULT_PURCHASE_POSITIVE)
NEGATIVE_SIGNALS = _env_list("PURCHASE_SIGNALS_NEGATIVE", DEFAULT_PURCHASE_NEGATIVE)
QUESTION_SIGNALS = _env_list("PURCHASE_SIGNALS_QUESTION", DEFAULT_PURCHASE_QUESTION)
NEUTRAL_HIGH_INTENT_SIGNALS = _env_list("PURCHASE_SIGNALS_NEUTRAL", DEFAULT_NEUTRAL_HIGH_INTENT)


# Emoji signal sets
EMOJI_PURCHASE = {"🛒", "🛍️", "🛍", "💳"}
EMOJI_POSITIVE = {"😍", "🤩", "✨", "💖", "💕", "❤️", "❤", "💯"}
EMOJI_NEGATIVE = {"🙅", "❌", "👎", "🤮", "😡"}
EMOJI_QUESTION = {"🤔", "❓", "❔"}


# Phrase proximity pairs: (word1, word2, max_words_apart, signal_type)
PROXIMITY_PAIRS = [
    ("bought", "again", 3, "purchase"),
    ("can't", "without", 3, "purchase"),
    ("cant", "without", 3, "purchase"),
    ("tell", "friends", 3, "purchase"),
    ("told", "friends", 3, "purchase"),
    ("ordered", "another", 3, "purchase"),
    ("buying", "more", 3, "purchase"),
    ("save", "money", 3, "negative"),
    ("waste", "money", 3, "negative"),
    ("not", "work", 3, "negative"),
]


def _normalize_text(text: str) -> str:
    """Lowercase, expand contractions, normalize punctuation."""
    if not text:
        return ""
    t = text.lower()
    # Expand common contractions (write back without apostrophe)
    contractions = {
        "won't": "will not", "wont": "will not",
        "can't": "cannot", "cant": "cannot",
        "don't": "do not", "dont": "do not",
        "doesn't": "does not", "doesnt": "does not",
        "didn't": "did not", "didnt": "did not",
        "i've": "i have", "ive": "i have",
        "i'm": "i am",
        "it's": "it is",
        "you're": "you are",
        "they're": "they are",
        "we're": "we are",
    }
    for short, full in contractions.items():
        t = t.replace(short, full)
    # Strip most punctuation but keep apostrophes for the "don't" → "do not" pattern above
    import re
    t = re.sub(r"[^\w\s']", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _check_proximity(words: list, w1: str, w2: str, max_distance: int) -> bool:
    """Check if w1 appears within max_distance words of w2."""
    positions_w1 = [i for i, w in enumerate(words) if w1 in w]
    positions_w2 = [i for i, w in enumerate(words) if w2 in w]
    for p1 in positions_w1:
        for p2 in positions_w2:
            if abs(p1 - p2) <= max_distance:
                return True
    return False


def _count_emojis(text: str, emoji_set: set) -> int:
    """Count occurrences of emojis from the set in text."""
    return sum(1 for char in text if char in emoji_set)


def _length_weight(text: str) -> float:
    """Get length-based multiplier for a comment."""
    short_max = _env_int("COMMENT_LENGTH_SHORT_MAX", 5)
    long_min = _env_int("COMMENT_LENGTH_LONG_MIN", 20)
    short_w = _env_float("COMMENT_WEIGHT_SHORT", 0.5)
    medium_w = _env_float("COMMENT_WEIGHT_MEDIUM", 1.0)
    long_w = _env_float("COMMENT_WEIGHT_LONG", 1.2)
    word_count = len(text.split()) if text else 0
    if word_count <= short_max: return short_w
    if word_count >= long_min: return long_w
    return medium_w


def _env_int(key, default):
    """Read an int from environment, falling back to default."""
    val = os.environ.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _env_float(key, default):
    """Read a float from environment, falling back to default."""
    val = os.environ.get(key)
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


class BasePlatformAgent(BaseAgent):
    """
    Two-pass platform agent base class.

    Subclasses must implement:
      - run_pass1(product, hashtags, lookback_days) → list[dict]
      - filter_pass1(items, lookback_days) → list[dict]
      - run_pass2(top_posts, product) → list[dict]
      - build_signal_row(raw_data, product_id) → dict

    Inherited from BaseAgent (do not override):
      - run() — the main pipeline entry point
      - _write_all_posts() — posts-first DB writes
      - _compute_aggregates_from_db() — DB aggregate queries
      - _run_integrity_check() — data integrity verification
      - _update_status() — agent_runs table tracking
    """

    # Subclasses set these
    PASS1_ACTOR: str = ""
    PASS2_ACTOR: str = ""
    COST_PER_1K: float = 0.0

    def get_hashtags(self, product: dict) -> list[str]:
        """Load hashtags from product_hashtags table, sorted by priority. Falls back to keywords."""
        try:
            resp = self.supabase.table("product_hashtags") \
                .select("hashtag, priority") \
                .eq("product_id", product["id"]) \
                .eq("platform", self.PLATFORM) \
                .eq("active", True) \
                .order("priority") \
                .execute()
            if resp.data:
                hashtags = [r["hashtag"] for r in resp.data]
                logger.info("[hashtags] Loaded %d hashtags for %s on %s from database",
                            len(hashtags), product.get("name", "?"), self.PLATFORM)
                return hashtags
        except Exception:
            pass

        # Fallback: generate from product name + keywords
        all_terms = [product.get("name", "")] + (product.get("keywords") or [])
        fallback = list(set(
            t.lower().replace(" ", "").replace("-", "").replace("&", "")
            for t in all_terms[:10] if t and len(t) > 3
        ))
        logger.info("[hashtags] Fallback: generated %d hashtags for %s on %s from keywords",
                    len(fallback), product.get("name", "?"), self.PLATFORM)
        return fallback

    def get_lookback_days(self, product: dict, backfill: bool = False) -> int:
        """
        Determine lookback window. Three cases:
          1. Backfill mode (--backfill flag or BACKFILL_MODE env): 365 days
          2. First run of new product (no first_scraped_at): 365 days
          3. Regular weekly run: 7 days
        Reddit always uses REDDIT_LOOKBACK_DAYS (90) regardless of case.
        """
        # Reddit-specific override — always 90 days
        if self.PLATFORM == "reddit":
            days = _env_int("REDDIT_LOOKBACK_DAYS", 90)
            name = product.get("name", "?")
            logger.info("[%s][lookback] %s: using %d days (Reddit always uses 90-day window)",
                        self.PLATFORM, name, days)
            return days

        # Case 1: --backfill flag explicitly set OR BACKFILL_MODE env var
        env_backfill = os.environ.get("BACKFILL_MODE") == "1"
        is_backfill = backfill or env_backfill

        # Case 2: first run (no first_scraped_at recorded)
        is_first_run = not product.get("first_scraped_at") and not product.get("backfill_completed")

        if is_backfill or is_first_run:
            days = _env_int("LOOKBACK_DAYS_BACKFILL", 365)
            mode = "backfill flag" if env_backfill else ("--backfill" if backfill else "first run")
            logger.info("[%s][lookback] %s: using %d days (%s)",
                        self.PLATFORM, product.get("name", "?"), days, mode)
            return days

        # Case 3: regular weekly run
        days = _env_int("LOOKBACK_DAYS_WEEKLY", 7)
        logger.info("[%s][lookback] %s: using %d days (weekly run, previously scraped)",
                    self.PLATFORM, product.get("name", "?"), days)
        return days

    def get_lookback_cutoff(self, lookback_days: int) -> datetime:
        """Return the UTC datetime cutoff for the lookback window."""
        return datetime.now(timezone.utc) - timedelta(days=lookback_days)

    # ─── Pass 1 & 2 (subclasses implement) ───

    @abstractmethod
    def run_pass1(self, product: dict, hashtags: list[str], lookback_days: int) -> list[dict]:
        """Pull lightweight metadata. No comments. Return raw items."""
        ...

    @abstractmethod
    def filter_pass1(self, items: list[dict], lookback_days: int) -> list[dict]:
        """Filter and sort Pass 1 items. Return top N for Pass 2."""
        ...

    @abstractmethod
    def run_pass2(self, top_posts: list[dict], product: dict) -> list[dict]:
        """Pull deep comments on the top posts from Pass 1. Return comment items."""
        ...

    # ─── Comment scoring ───

    def score_comments(self, comments: list[dict], parent_virality: float = 1.0) -> dict:
        """
        Score a batch of comments with expanded signal detection:
          - Normalized text matching (contractions expanded, punctuation stripped)
          - Emoji signal detection (purchase/positive/negative/question)
          - Phrase proximity matching
          - Comment length weighting (short = 0.5x, long = 1.2x)
          - Parent virality weighting (log10 of post engagement)

        Returns aggregate stats dict.
        """
        purchase_count = 0
        negative_count = 0
        question_count = 0
        neutral_high_intent_count = 0
        emoji_purchase_count = 0
        emoji_positive_count = 0
        emoji_negative_count = 0
        emoji_question_count = 0
        intent_scores = []
        sentiment_scores = []
        weighted_intents = []
        weighted_sentiments = []
        scored_comments = []

        virality = max(parent_virality, 1.0)

        for comment in comments:
            raw_body = (comment.get("text") or comment.get("comment") or
                        comment.get("body") or comment.get("reviewDescription") or
                        comment.get("comment_body") or "").strip()
            if not raw_body or len(raw_body) < 3:
                continue

            # Normalize for keyword matching
            normalized = _normalize_text(raw_body)
            words = normalized.split()

            # Intent + sentiment (run on raw text)
            i = score_intent(raw_body)
            s = analyze_sentiment(raw_body)

            # Length weight × parent virality
            length_w = _length_weight(raw_body)
            total_weight = virality * length_w

            intent_scores.append(i["intent_score"])
            sentiment_scores.append(s["sentiment_score"])
            weighted_intents.append(i["intent_score"] * total_weight)
            weighted_sentiments.append(s["sentiment_score"] * total_weight)

            # Keyword-based signal detection — COUNT matches per category
            # (was: any() returning bool; now: sum() returning int for priority resolution)
            purchase_matches = sum(1 for w in PURCHASE_SIGNALS if w in normalized)
            negative_matches = sum(1 for w in NEGATIVE_SIGNALS if w in normalized)
            question_matches = sum(1 for w in QUESTION_SIGNALS if w in normalized)
            is_neutral_high = any(w in normalized for w in NEUTRAL_HIGH_INTENT_SIGNALS)

            # Phrase proximity matching adds to match counts
            for w1, w2, dist, sig_type in PROXIMITY_PAIRS:
                if _check_proximity(words, w1, w2, dist):
                    if sig_type == "purchase":
                        purchase_matches += 1
                    elif sig_type == "negative":
                        negative_matches += 1

            # Emoji detection (on raw text — preserves emoji)
            ep = _count_emojis(raw_body, EMOJI_PURCHASE)
            epp = _count_emojis(raw_body, EMOJI_POSITIVE)
            en = _count_emojis(raw_body, EMOJI_NEGATIVE)
            eq = _count_emojis(raw_body, EMOJI_QUESTION)
            emoji_purchase_count += ep
            emoji_positive_count += epp
            emoji_negative_count += en
            emoji_question_count += eq

            # Emoji adds to match counts (purchase/negative/question)
            purchase_matches += ep
            negative_matches += en
            question_matches += eq

            # PRIORITY RULE: when both purchase and negative matches exist,
            # whichever has more matches wins. Tie → purchase (benefit of doubt).
            is_purchase = False
            is_negative = False
            is_question = question_matches > 0
            if purchase_matches > 0 or negative_matches > 0:
                if purchase_matches >= negative_matches:
                    is_purchase = True
                else:
                    is_negative = True

            if is_purchase:
                purchase_count += 1
            if is_negative:
                negative_count += 1
            if is_question:
                question_count += 1
            if is_neutral_high:
                neutral_high_intent_count += 1

            scored_comments.append({
                **comment,
                "_intent_level": i["intent_level"],
                "_intent_score": i["intent_score"],
                "_sentiment": s["sentiment_score"],
                "_is_purchase": is_purchase,
                "_is_negative": is_negative,
                "_is_question": is_question,
                "_is_neutral_high": is_neutral_high,
                "_emoji_purchase": ep,
                "_emoji_positive": epp,
                "_emoji_negative": en,
                "_emoji_question": eq,
                "_weight": total_weight,
                "_length_weight": length_w,
                "_virality_weight": virality,
            })

        total = len(intent_scores)
        weight_sum = sum(c["_weight"] for c in scored_comments) if scored_comments else 1

        return {
            "comment_count_total": total,
            "purchase_signal_count": purchase_count,
            "negative_signal_count": negative_count,
            "question_signal_count": question_count,
            "neutral_high_intent_count": neutral_high_intent_count,
            "emoji_purchase_count": emoji_purchase_count,
            "emoji_positive_count": emoji_positive_count,
            "emoji_negative_count": emoji_negative_count,
            "emoji_question_count": emoji_question_count,
            "avg_intent": sum(intent_scores) / max(total, 1) if intent_scores else 0,
            "avg_sentiment": sum(sentiment_scores) / max(total, 1) if sentiment_scores else 0,
            "weighted_comment_intent": sum(weighted_intents) / max(weight_sum, 1),
            "weighted_sentiment": sum(weighted_sentiments) / max(weight_sum, 1),
            "high_intent_count": sum(1 for s in intent_scores if s >= 0.8),
            "scored_comments": scored_comments,
        }

    def generate_signal_report(self, product_id: str, platform: str) -> dict:
        """
        Generate a signal quality report for a product/platform from existing DB comments.
        Used by dashboard scorecard and weekly email.
        """
        try:
            comments_resp = self.supabase.table("comments") \
                .select("comment_body, sentiment_score, intent_level, is_buy_intent, is_problem_language, is_repeat_purchase, posts(post_url, upvotes)") \
                .eq("product_id", product_id) \
                .eq("platform", platform) \
                .execute()
            db_comments = comments_resp.data or []
        except Exception as e:
            logger.error("[%s] Failed to load comments for report: %s", platform, str(e)[:200])
            return self._empty_signal_report(platform)

        if not db_comments:
            return self._empty_signal_report(platform)

        # Re-score with current logic
        comments_for_scoring = []
        for c in db_comments:
            comments_for_scoring.append({
                "text": c.get("comment_body") or "",
                "_db_post_url": (c.get("posts") or {}).get("post_url", "") if c.get("posts") else "",
                "_db_post_upvotes": (c.get("posts") or {}).get("upvotes", 0) if c.get("posts") else 0,
            })
        stats = self.score_comments(comments_for_scoring, parent_virality=1.0)

        # Top comments by signal type
        top_purchase = sorted(
            [c for c in stats["scored_comments"] if c.get("_is_purchase")],
            key=lambda c: c.get("_intent_score", 0), reverse=True
        )[:5]
        top_negative = sorted(
            [c for c in stats["scored_comments"] if c.get("_is_negative")],
            key=lambda c: -c.get("_sentiment", 0)
        )[:5]

        # Negative ratio + penalty
        purch = stats["purchase_signal_count"]
        neg = stats["negative_signal_count"]
        ratio = neg / purch if purch > 0 else 0
        penalty = self._negative_penalty(ratio)

        # Quality assessment
        if purch > 50 and ratio < 0.10:
            quality = "high"
        elif purch > 10 and ratio < 0.25:
            quality = "medium"
        else:
            quality = "low"

        return {
            "platform": platform,
            "total_comments": stats["comment_count_total"],
            "purchase_signals": purch,
            "negative_signals": neg,
            "question_signals": stats["question_signal_count"],
            "neutral_high_intent": stats["neutral_high_intent_count"],
            "emoji_purchase": stats["emoji_purchase_count"],
            "emoji_positive": stats["emoji_positive_count"],
            "emoji_negative": stats["emoji_negative_count"],
            "negative_ratio": round(ratio, 4),
            "penalty_applied": penalty,
            "weighted_intent": round(stats["weighted_comment_intent"], 4),
            "top_purchase_comments": [
                {
                    "text": (c.get("text") or "")[:200],
                    "score": c.get("_intent_score", 0),
                    "post_url": c.get("_db_post_url", ""),
                    "virality_weight": c.get("_virality_weight", 1),
                } for c in top_purchase
            ],
            "top_negative_comments": [
                {
                    "text": (c.get("text") or "")[:200],
                    "score": c.get("_sentiment", 0),
                    "post_url": c.get("_db_post_url", ""),
                } for c in top_negative
            ],
            "signal_quality": quality,
        }

    def _empty_signal_report(self, platform: str) -> dict:
        return {
            "platform": platform, "total_comments": 0,
            "purchase_signals": 0, "negative_signals": 0,
            "question_signals": 0, "neutral_high_intent": 0,
            "emoji_purchase": 0, "emoji_positive": 0,
            "emoji_negative": 0, "negative_ratio": 0,
            "penalty_applied": 1.0, "weighted_intent": 0,
            "top_purchase_comments": [], "top_negative_comments": [],
            "signal_quality": "low",
        }

    def _negative_penalty(self, ratio: float) -> float:
        """Compute the score multiplier based on negative/purchase ratio."""
        severe_thresh = _env_float("NEGATIVE_RATIO_SEVERE", 0.50)
        moderate_thresh = _env_float("NEGATIVE_RATIO_MODERATE", 0.25)
        mild_thresh = _env_float("NEGATIVE_RATIO_MILD", 0.10)
        if ratio > severe_thresh:
            return _env_float("NEGATIVE_PENALTY_SEVERE", 0.70)
        if ratio > moderate_thresh:
            return _env_float("NEGATIVE_PENALTY_MODERATE", 0.85)
        if ratio > mild_thresh:
            return _env_float("NEGATIVE_PENALTY_MILD", 0.95)
        return 1.0

    # ─── Dedup ───

    def dedup_check(self, platform_post_id: str, platform: str, product_id: str) -> bool:
        """
        Check if a post/comment already exists by (product_id, platform, post_id).
        Returns True if exists (should skip), False if new.
        Null post_id always returns False (cannot dedup reliably).
        """
        if not platform_post_id:
            return False
        try:
            resp = self.supabase.table("posts") \
                .select("id") \
                .eq("product_id", product_id) \
                .eq("platform", platform) \
                .eq("reddit_id", platform_post_id) \
                .limit(1) \
                .execute()
            return len(resp.data or []) > 0
        except Exception:
            return False

    def dedup_check_batch(self, post_ids: list, platform: str, product_id: str) -> set:
        """
        Batch dedup — returns set of post_ids that ALREADY exist in DB.
        Much more efficient than per-record dedup_check() for large batches.
        """
        if not post_ids:
            return set()
        # Filter out null/empty IDs (can't dedup those)
        valid_ids = [pid for pid in post_ids if pid]
        if not valid_ids:
            return set()
        try:
            # PostgREST has limits on .in_() size — chunk to 100
            existing = set()
            for i in range(0, len(valid_ids), 100):
                chunk = valid_ids[i:i+100]
                resp = self.supabase.table("posts") \
                    .select("reddit_id") \
                    .eq("product_id", product_id) \
                    .eq("platform", platform) \
                    .in_("reddit_id", chunk) \
                    .execute()
                for row in (resp.data or []):
                    if row.get("reddit_id"):
                        existing.add(row["reddit_id"])
            return existing
        except Exception as e:
            logger.warning("[%s] Batch dedup failed, falling back to per-record: %s",
                           platform, str(e)[:200])
            return set()

    # ─── Write comments to DB ───

    def write_comments_to_db(self, scored_comments: list[dict], product_id: str) -> int:
        """Write scored comments to both posts and comments tables.
        Uses batch dedup at start, then catches DB constraint violations as final backstop.
        Returns count written. Tracks self.last_dedup_skips for reporting.
        """
        written = 0
        skipped = 0
        null_id_skipped = 0
        constraint_skipped = 0

        # Batch dedup: pre-check which comment IDs already exist in DB
        comment_ids = [c.get("cid") or c.get("id") or c.get("reviewId") or "" for c in scored_comments]
        existing_ids = self.dedup_check_batch(comment_ids, self.PLATFORM, product_id)
        if existing_ids:
            logger.info("[%s][dedup] Pre-check found %d existing comments — will skip",
                        self.PLATFORM, len(existing_ids))

        for c in scored_comments:
            body = (c.get("text") or c.get("comment") or c.get("body") or
                    c.get("comment_body") or "").strip()
            if not body or len(body) < 3:
                continue

            comment_id = c.get("cid") or c.get("id") or c.get("reviewId") or ""

            # Edge case 1: null/missing ID — log and skip (can't dedup reliably)
            if not comment_id:
                null_id_skipped += 1
                continue

            # Already-known duplicate from batch check
            if comment_id in existing_ids:
                skipped += 1
                continue

            # Write to posts table as comment-type
            try:
                post_resp = self.supabase.table("posts").insert({
                    "product_id": product_id,
                    "run_id": self.run_id,
                    "platform": self.PLATFORM,
                    "post_body": body[:5000],
                    "post_url": (c.get("videoWebUrl") or c.get("postUrl") or
                                 c.get("inputUrl") or c.get("reviewUrl") or "")[:2000] or None,
                    "upvotes": c.get("diggCount") or c.get("likesCount") or
                               c.get("score") or c.get("likes") or 0,
                    "comment_count": 0,
                    "author": (c.get("uniqueId") or c.get("username") or
                               c.get("ownerUsername") or c.get("author") or "")[:200] or None,
                    "posted_at": c.get("createTimeISO") or c.get("timestamp") or
                                 c.get("createdAt") or c.get("date") or None,
                    "scraped_date": date.today().isoformat(),
                    "intent_level": c.get("_intent_level", 1),
                    "sentiment_score": c.get("_sentiment", 0),
                    "anomaly_flag": False,
                    "data_type": "comment",
                    "reddit_id": comment_id or None,
                }).execute()
                db_post_id = post_resp.data[0]["id"] if post_resp.data else None
            except Exception as e:
                err_str = str(e)[:200]
                # DB-level dedup backstop: unique constraint violation = duplicate
                if "duplicate" in err_str.lower() or "23505" in err_str or "unique" in err_str.lower():
                    constraint_skipped += 1
                else:
                    logger.error("[%s] Failed to write comment-as-post: %s", self.PLATFORM, err_str)
                continue

            if not db_post_id:
                continue

            # Write to comments table
            try:
                lower = body.lower()
                self.supabase.table("comments").insert({
                    "post_id": db_post_id,
                    "product_id": product_id,
                    "platform": self.PLATFORM,
                    "comment_body": body[:5000],
                    "author": (c.get("uniqueId") or c.get("username") or
                               c.get("ownerUsername") or c.get("author") or "")[:200] or None,
                    "upvotes": c.get("diggCount") or c.get("likesCount") or
                               c.get("score") or c.get("likes") or 0,
                    "intent_level": c.get("_intent_level", 1),
                    "sentiment_score": c.get("_sentiment", 0),
                    "is_buy_intent": c.get("_is_purchase", False),
                    "is_problem_language": c.get("_is_negative", False),
                    "is_repeat_purchase": any(w in lower for w in [
                        "repurchas", "on my second", "on my third", "restocked",
                        "holy grail", "keep buying", "buying again",
                    ]),
                    "posted_at": c.get("createTimeISO") or c.get("timestamp") or
                                 c.get("createdAt") or c.get("date") or None,
                }).execute()
                written += 1
            except Exception as e:
                err_str = str(e)[:200]
                if "duplicate" in err_str.lower() or "23505" in err_str or "unique" in err_str.lower():
                    constraint_skipped += 1
                else:
                    logger.error("[%s] Failed to write comment: %s", self.PLATFORM, err_str)

        # Surface dedup stats for callers (used by pipeline_runs reporting)
        self.last_dedup_stats = {
            "written": written,
            "batch_skipped": skipped,
            "constraint_skipped": constraint_skipped,
            "null_id_skipped": null_id_skipped,
            "total_skipped": skipped + constraint_skipped + null_id_skipped,
        }

        if skipped or constraint_skipped or null_id_skipped:
            logger.info("[%s][dedup] Wrote %d, skipped %d (batch:%d constraint:%d null_id:%d)",
                        self.PLATFORM, written, self.last_dedup_stats["total_skipped"],
                        skipped, constraint_skipped, null_id_skipped)

        return written

    # ─── Confidence calculation ───

    def update_confidence(self, product_id: str):
        """Calculate and store confidence level based on comment volume, platform coverage,
        purchase signals, and negative ratio. Thresholds from .env."""
        try:
            # Thresholds from .env
            high_comments = _env_int("CONFIDENCE_HIGH_COMMENTS", 5000)
            high_platforms = _env_int("CONFIDENCE_HIGH_PLATFORMS", 3)
            high_purchase = _env_int("CONFIDENCE_HIGH_PURCHASE", 100)
            high_neg_ratio = _env_float("CONFIDENCE_HIGH_NEGATIVE_RATIO", 0.10)
            med_comments = _env_int("CONFIDENCE_MEDIUM_COMMENTS", 1000)
            med_platforms = _env_int("CONFIDENCE_MEDIUM_PLATFORMS", 2)

            # Count total comments scored for this product
            comments_resp = self.supabase.table("comments") \
                .select("id") \
                .eq("product_id", product_id) \
                .execute()
            total_comments = len(comments_resp.data or [])

            # Count distinct active platforms across signals_social/retail/search/supply
            active = set()
            for table in ("signals_social", "signals_retail", "signals_search", "signals_supply"):
                try:
                    r = self.supabase.table(table).select("platform") \
                        .eq("product_id", product_id).execute()
                    for row in (r.data or []):
                        if row.get("platform"):
                            active.add(row["platform"])
                except Exception:
                    pass
            active_platforms = len(active)

            # Purchase + negative signal counts
            purchase_resp = self.supabase.table("comments") \
                .select("id") \
                .eq("product_id", product_id) \
                .eq("is_buy_intent", True) \
                .execute()
            purchase_count = len(purchase_resp.data or [])

            negative_resp = self.supabase.table("comments") \
                .select("id") \
                .eq("product_id", product_id) \
                .eq("is_problem_language", True) \
                .execute()
            negative_count = len(negative_resp.data or [])

            negative_ratio = (negative_count / purchase_count) if purchase_count > 0 else 0

            # Determine level using exact rule spec
            high_meets_all = (
                total_comments >= high_comments and
                active_platforms >= high_platforms and
                purchase_count > high_purchase and
                negative_ratio < high_neg_ratio
            )
            medium_meets_all = (
                total_comments >= med_comments and
                active_platforms >= med_platforms
            )

            if high_meets_all:
                level = "high"
                reason = (f"High: {total_comments:,} comments across {active_platforms} platforms, "
                          f"{purchase_count} purchase signals, {negative_ratio*100:.1f}% negative ratio")
            elif medium_meets_all:
                level = "medium"
                missing = []
                if total_comments < high_comments:
                    missing.append(f"{high_comments - total_comments:,} more comments")
                if active_platforms < high_platforms:
                    missing.append(f"{high_platforms - active_platforms} more platform(s)")
                if purchase_count <= high_purchase:
                    missing.append(f"{high_purchase - purchase_count + 1} more purchase signals")
                if negative_ratio >= high_neg_ratio:
                    missing.append(f"reduce negative ratio (currently {negative_ratio*100:.1f}%)")
                reason = (f"Medium: {total_comments:,} comments across {active_platforms} platforms. "
                          f"Need {' and '.join(missing[:2])} for High.")
            else:
                level = "low"
                missing = []
                if total_comments < med_comments:
                    missing.append(f"{med_comments - total_comments:,}+ comments")
                if active_platforms < med_platforms:
                    missing.append(f"{med_platforms - active_platforms} more platform(s)")
                reason = (f"Low: Only {total_comments:,} comments across {active_platforms} platform(s). "
                          f"Need {' and '.join(missing) or 'more data'} for Medium.")

            self.supabase.table("products").update({
                "confidence_level": level,
                "confidence_reason": reason,
                "total_comments_scored": total_comments,
                "active_platform_count": active_platforms,
            }).eq("id", product_id).execute()

            logger.info("[%s] Confidence: %s — %s", self.PLATFORM, level, reason)

        except Exception as e:
            logger.error("[%s] Failed to update confidence: %s", self.PLATFORM, str(e)[:200])

    # ─── Product scrape tracking ───

    def update_product_scrape_tracking(self, product: dict):
        """Update first_scraped_at, last_scraped_at, total_runs, backfill_completed."""
        try:
            updates = {
                "last_scraped_at": datetime.now(timezone.utc).isoformat(),
                "total_runs": (product.get("total_runs") or 0) + 1,
            }
            if not product.get("first_scraped_at"):
                updates["first_scraped_at"] = datetime.now(timezone.utc).isoformat()
            if not product.get("backfill_completed"):
                updates["backfill_completed"] = True

            self.supabase.table("products").update(updates).eq("id", product["id"]).execute()
        except Exception as e:
            logger.error("[%s] Failed to update product tracking: %s", self.PLATFORM, str(e)[:200])

    # ─── Run logging ───

    def log_run(self, pass_num: int, stats: dict):
        """Log pass-level statistics."""
        prefix = f"[{self.PLATFORM} pass{pass_num}]"
        if pass_num == 1:
            logger.info("%s Found %d posts, %d passed filter, keeping top %d for Pass 2",
                        prefix, stats.get("total_found", 0),
                        stats.get("passed_filter", 0),
                        stats.get("kept", 0))
        elif pass_num == 2:
            logger.info("%s Pulled %d comments from %d posts. "
                        "Purchase signals: %d. Negative signals: %d. Questions: %d.",
                        prefix, stats.get("comment_count_total", 0),
                        stats.get("posts_enriched", 0),
                        stats.get("purchase_signal_count", 0),
                        stats.get("negative_signal_count", 0),
                        stats.get("question_signal_count", 0))
