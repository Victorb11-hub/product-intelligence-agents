"""
Discovery Agent — Reddit
Mines subreddits for emerging product keywords without a predefined product list.
Uses spaCy NLP to extract noun phrases, tracks frequency growth week over week.
"""
import logging
import re
import time
from datetime import date, datetime
from collections import Counter

from .config import get_supabase, APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor
from .skills.activity_logger import post_status

logger = logging.getLogger(__name__)


def _load_settings(db):
    resp = db.table("discovery_settings").select("setting_key, setting_value").execute()
    return {r["setting_key"]: r["setting_value"] for r in (resp.data or [])}


def _calc_confidence(mention_count, growth_rate, sentiment, signal_count):
    c = (
        min(1.0, mention_count / 100) * 0.25 +
        min(1.0, growth_rate / 5) * 0.25 +
        min(1.0, signal_count / 3) * 0.30 +
        ((sentiment or 0) + 1) / 2 * 0.20
    )
    return round(min(1.0, max(0, c)), 4)


class RedditDiscoveryAgent:
    PLATFORM = "discovery_reddit"

    async def run(self, run_id=None):
        post_status("discovery-reddit", "busy", "Mining subreddits for emerging keywords")
        start = time.time()
        db = get_supabase()
        settings = _load_settings(db)

        subreddits = [s.strip() for s in settings.get("reddit_subreddits", "supplements").split(",")]
        min_mentions = int(settings.get("min_mention_count", "10"))
        min_growth = float(settings.get("min_growth_rate", "0.5"))
        exclude_kw = set(k.strip().lower() for k in settings.get("exclude_keywords", "").split(","))

        # Get existing products to exclude
        existing = set()
        try:
            prods = db.table("products").select("name").execute()
            existing = set(p["name"].lower() for p in (prods.data or []))
        except Exception:
            pass

        # Get previously dismissed candidates
        dismissed = set()
        try:
            dis = db.table("discovery_candidates").select("keyword").eq("status", "dismissed").execute()
            dismissed = set(d["keyword"].lower() for d in (dis.data or []))
        except Exception:
            pass

        # Pull posts from subreddits via Apify
        all_titles = []
        sub_map = {}  # keyword -> set of subreddits

        for sub in subreddits[:10]:
            try:
                items = run_actor(
                    actor_id=APIFY_ACTORS.get("reddit", "macrocosmos/reddit-scraper"),
                    run_input={"subreddits": [sub], "keyword": "", "postsPerSubreddit": 50, "sortBy": "new"},
                    api_token=APIFY_API_TOKEN,
                    timeout_secs=120, max_items=100,
                )
                for item in items:
                    title = (item.get("title") or "").strip()
                    if title:
                        all_titles.append(title)
                        # Track subreddit per title for later
                        for phrase in _extract_noun_phrases(title):
                            sub_map.setdefault(phrase, set()).add(sub)
            except Exception as e:
                logger.warning("[discovery_reddit] Failed on r/%s: %s", sub, str(e)[:100])

        if not all_titles:
            post_status("discovery-reddit", "idle", "No posts found")
            return {"candidates_found": 0}

        # Extract noun phrases with spaCy
        phrases = []
        for title in all_titles:
            phrases.extend(_extract_noun_phrases(title))

        counts = Counter(phrases)

        # Get last week's counts from existing candidates
        last_week = {}
        try:
            existing_cands = db.table("discovery_candidates").select("keyword, mention_count_this_week") \
                .eq("source", "reddit").execute()
            for c in (existing_cands.data or []):
                last_week[c["keyword"].lower()] = c["mention_count_this_week"]
        except Exception:
            pass

        # Filter and score candidates
        candidates_found = 0
        candidates_new = 0

        for phrase, count in counts.most_common(200):
            if count < min_mentions:
                continue
            if phrase.lower() in existing or phrase.lower() in dismissed or phrase.lower() in exclude_kw:
                continue
            if len(phrase) < 4 or len(phrase.split()) < 2:
                continue

            prev_count = last_week.get(phrase.lower(), 0)
            growth = (count - prev_count) / max(prev_count, 1) if prev_count > 0 else 1.0

            if growth < min_growth and prev_count > 0:
                continue

            # Get example posts
            examples = [t for t in all_titles if phrase.lower() in t.lower()][:3]
            subs_found = list(sub_map.get(phrase, set()))[:5]

            confidence = _calc_confidence(count, growth, 0.1, 1)

            # Upsert candidate
            try:
                existing_row = db.table("discovery_candidates").select("id, mention_count_this_week, signal_count, mention_count_history, growth_rate_history") \
                    .eq("keyword", phrase).eq("source", "reddit").execute()

                if existing_row.data:
                    row = existing_row.data[0]
                    hist = (row.get("mention_count_history") or [])[-12:]
                    hist.append({"date": date.today().isoformat(), "count": count})
                    g_hist = (row.get("growth_rate_history") or [])[-12:]
                    g_hist.append({"date": date.today().isoformat(), "rate": round(growth, 4)})

                    db.table("discovery_candidates").update({
                        "mention_count_last_week": row["mention_count_this_week"],
                        "mention_count_this_week": count,
                        "growth_rate": round(growth, 4),
                        "mention_count_history": hist,
                        "growth_rate_history": g_hist,
                        "last_updated": date.today().isoformat(),
                        "example_posts": examples,
                        "reddit_subreddits": subs_found,
                        "confidence_score": confidence,
                    }).eq("id", row["id"]).execute()
                else:
                    db.table("discovery_candidates").insert({
                        "keyword": phrase,
                        "display_name": phrase.title(),
                        "source": "reddit",
                        "source_detail": ", ".join(subs_found),
                        "mention_count_this_week": count,
                        "growth_rate": round(growth, 4),
                        "example_posts": examples,
                        "reddit_subreddits": subs_found,
                        "confidence_score": confidence,
                        "signal_count": 1,
                        "status": "new",
                    }).execute()
                    candidates_new += 1

                candidates_found += 1
                logger.info("[discovery_reddit] Candidate: '%s' growth=%.0f%% mentions=%d conf=%.2f subs=%s",
                            phrase, growth * 100, count, confidence, ",".join(subs_found[:3]))
            except Exception as e:
                logger.error("[discovery_reddit] Failed to write candidate '%s': %s", phrase, str(e)[:100])

        # Log run
        duration = int(time.time() - start)
        db.table("discovery_runs").insert({
            "source": "reddit", "candidates_found": candidates_found,
            "candidates_new": candidates_new, "runtime_seconds": duration,
        }).execute()

        post_status("discovery-reddit", "done", f"Found {candidates_found} candidates ({candidates_new} new)")
        post_status("discovery-reddit", "idle", f"Last run: {candidates_found} candidates")
        logger.info("[discovery_reddit] Found %d candidates (%d new) in %ds", candidates_found, candidates_new, duration)
        return {"candidates_found": candidates_found, "candidates_new": candidates_new}


_nlp = None

def _get_nlp():
    global _nlp
    if _nlp is None:
        try:
            import spacy
            _nlp = spacy.load("en_core_web_sm")
        except Exception as e:
            logger.warning("[discovery_reddit] spaCy not available: %s", e)
    return _nlp


def _extract_noun_phrases(text):
    """Extract meaningful noun phrases from text using cached spaCy model."""
    nlp = _get_nlp()
    if nlp:
        try:
            doc = nlp(text[:500])
            phrases = []
            for chunk in doc.noun_chunks:
                clean = re.sub(r"^(my|the|a|an|this|that|these|those|some|any|your|our|their)\s+", "", chunk.text.lower().strip())
                if len(clean) > 3 and len(clean.split()) >= 2 and not any(c.isdigit() for c in clean):
                    phrases.append(clean)
            return phrases
        except Exception as e:
            logger.warning("[discovery_reddit] spaCy extraction failed: %s", e)

    # Fallback: simple bigram extraction
    words = re.findall(r'\b[a-z]{3,}\b', text.lower())
    return [f"{words[i]} {words[i+1]}" for i in range(len(words)-1) if i + 1 < len(words) and len(words[i]) > 3]
