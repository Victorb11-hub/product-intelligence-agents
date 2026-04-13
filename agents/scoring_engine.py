"""
Scoring Engine — Runs after all scrapers complete.
Computes 4-job weighted composite score with recency weighting.
Writes to scores_history and product_snapshots.
"""
import math
import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

# Default settings — overridden by scoring_settings table in Supabase
_settings_cache = None


def _load_settings(db):
    """Load scoring settings from Supabase. Cache for the duration of one pipeline run."""
    global _settings_cache
    if _settings_cache is not None:
        return _settings_cache

    defaults = {
        'job1_weight': 0.30, 'job2_weight': 0.30, 'job3_weight': 0.25, 'job4_weight': 0.15,
        'buy_threshold': 75, 'watch_threshold': 55,
        'recency_0_7': 1.0, 'recency_8_14': 0.8, 'recency_15_21': 0.6, 'recency_22_30': 0.4,
        'lookback_days': 30, 'relevance_threshold': 0.1,
    }

    try:
        resp = db.table("scoring_settings").select("setting_key, setting_value").execute()
        for row in resp.data:
            defaults[row["setting_key"]] = row["setting_value"]
        logger.info("[scoring] Loaded %d settings from Supabase", len(resp.data))
    except Exception as e:
        logger.warning("[scoring] Failed to load settings, using defaults: %s", e)

    _settings_cache = defaults
    return defaults


def _recency_weight(scraped_date_str, settings=None):
    s = settings or {}
    try:
        d = datetime.strptime(scraped_date_str, "%Y-%m-%d").date()
        age = (date.today() - d).days
        if age <= 7: return s.get('recency_0_7', 1.0)
        if age <= 14: return s.get('recency_8_14', 0.8)
        if age <= 21: return s.get('recency_15_21', 0.6)
        if age <= 30: return s.get('recency_22_30', 0.4)
        return 0.0
    except Exception:
        return 0.5


def score_all_products(db, products, run_id):
    global _settings_cache
    _settings_cache = None  # Force reload on each pipeline run
    settings = _load_settings(db)

    for product in products:
        try:
            _score_product(db, product, run_id)
        except Exception as e:
            logger.error("Scoring failed for %s: %s", product["name"], e)


def _score_product(db, product, run_id):
    settings = _settings_cache or {}
    pid = product["id"]
    name = product["name"]

    # Gather signals with recency weighting
    reddit = _get_weighted_signal(db, "signals_social", pid, "reddit")
    tiktok = _get_weighted_signal(db, "signals_social", pid, "tiktok")
    instagram = _get_weighted_signal(db, "signals_social", pid, "instagram")
    gt = _get_weighted_signal(db, "signals_search", pid, "google_trends")
    amazon = _get_weighted_signal(db, "signals_retail", pid, "amazon")
    alibaba = _get_weighted_signal(db, "signals_supply", pid, "alibaba")

    # Job 1: Early Detection (TikTok 40%, Instagram 30%, YouTube 15%, X 10%, Pinterest 5%)
    job1 = None
    j1_parts = {}
    if tiktok:
        mentions = tiktok.get("mention_count", 0) or 0
        # Fallback: if signal shows 0 mentions, check posts table directly
        if mentions == 0:
            try:
                pc = db.table("posts").select("id").eq("product_id", pid).eq("platform", "tiktok").execute()
                mentions = len(pc.data or [])
            except Exception:
                pass
        likes = tiktok.get("total_upvotes", 0) or 0
        comments = tiktok.get("total_comment_count", 0) or 0
        creator = tiktok.get("creator_tier_score", 0.3) or 0.3
        # View velocity — read total_views from signal row directly
        total_views = tiktok.get("total_views", 0) or 0
        views_per_post = total_views / max(mentions, 1)
        # Normalize view velocity (views per post) to 0-100
        if views_per_post > 500_000: vel_norm = 90
        elif views_per_post > 100_000: vel_norm = 75
        elif views_per_post > 10_000: vel_norm = 60
        elif views_per_post > 1_000: vel_norm = 40
        else: vel_norm = 20
        # Creator tier to 0-100
        creator_norm = creator * 100
        # Engagement rate
        eng_rate = (likes + comments) / max(total_views, 1) * 100 if total_views > 0 else 30
        eng_norm = min(100, eng_rate * 10)  # Scale up (IG/TT rates are low single digits)

        tiktok_sub = vel_norm * 0.40 + creator_norm * 0.30 + eng_norm * 0.30
        j1_parts["tiktok"] = (tiktok_sub, 0.40)

    if instagram:
        ig_likes = instagram.get("total_upvotes", 0) or 0
        ig_comments = instagram.get("total_comment_count", 0) or 0
        ig_mentions = instagram.get("mention_count", 0) or 0
        if ig_mentions == 0:
            try:
                pc = db.table("posts").select("id").eq("product_id", pid).eq("platform", "instagram").execute()
                ig_mentions = len(pc.data or [])
            except Exception:
                pass
        ig_intent = (instagram.get("avg_intent_score", 0) or 0) * 100
        # Engagement rate — scale so 20 engagements/post = 100
        ig_eng = min(100, ((ig_likes + ig_comments) / max(ig_mentions, 1)) * 5)
        # Content volume
        ig_vol = min(100, max(0, math.log10(max(ig_mentions, 1)) * 30))
        ig_sub = ig_eng * 0.40 + ig_vol * 0.30 + ig_intent * 0.30
        j1_parts["instagram"] = (ig_sub, 0.30)

    if j1_parts:
        j1_w = sum(w for _, w in j1_parts.values())
        if j1_w > 0:
            job1 = sum(s * (w / j1_w) for s, w in j1_parts.values())
            logger.info("[scoring] Job 1: %s = %.1f", {k: f"{s:.1f}" for k, (s, w) in j1_parts.items()}, job1)

    # Job 2: Demand Validation (Reddit 35%, GT 45%, Facebook 20%)
    job2 = None
    j2_parts = {}
    if reddit:
        sent = max(0, min(100, (reddit.get("sentiment_score", 0) + 1) * 50))
        vel = max(0, min(100, (reddit.get("velocity", 0) + 1.0) * 50))
        intent = (reddit.get("avg_intent_score", 0) or 0) * 100
        vol = min(100, max(0, math.log10(max(reddit.get("mention_count", 1), 1)) * 50))
        j2_parts["reddit"] = (sent * 0.25 + vel * 0.25 + intent * 0.25 + vol * 0.25, 0.35)

    if gt:
        slope = gt.get("slope_24m", 0) or 0
        yoy = gt.get("yoy_growth", 0) or 0
        breakout = gt.get("breakout_flag", False)
        s_norm = max(0, min(100, (slope + 0.01) / 0.02 * 100))
        y_norm = max(0, min(100, (yoy + 0.5) * 100))
        bonus = 0 if breakout else 20
        j2_parts["gt"] = (s_norm * 0.4 + y_norm * 0.4 + bonus, 0.45)

    if j2_parts:
        total_w = sum(w for _, w in j2_parts.values())
        if total_w > 0:
            job2 = sum(score * (w / total_w) for score, w in j2_parts.values())
        else:
            job2 = None

    # Job 3: Purchase Intent (Amazon 50%, Etsy 30%, Walmart 20%)
    job3 = None
    if amazon:
        rank = amazon.get("bestseller_rank") or 500
        review_sent = (amazon.get("review_sentiment", 0) or 0) * 100
        rank_norm = max(0, min(100, (500 - rank) / 5))
        job3 = rank_norm * 0.5 + review_sent * 0.5

    # Job 4: Supply Readiness (Alibaba 100%)
    job4 = None
    if alibaba:
        suppliers = alibaba.get("supplier_listing_count", 0) or 0
        moq = alibaba.get("moq_current", 1000) or 1000
        s_norm = min(100, suppliers * 3)
        m_norm = max(0, min(100, (1000 - moq) / 10))
        job4 = s_norm * 0.5 + m_norm * 0.5

    # Redistribute weights — read from settings
    j1w = settings.get('job1_weight', 0.30)
    j2w = settings.get('job2_weight', 0.30)
    j3w = settings.get('job3_weight', 0.25)
    j4w = settings.get('job4_weight', 0.15)
    buy_thresh = settings.get('buy_threshold', 75)
    watch_thresh = settings.get('watch_threshold', 55)

    total_jobs = 4
    jobs = {"early_detection": (j1w, job1), "demand_validation": (j2w, job2),
            "purchase_intent": (j3w, job3), "supply_readiness": (j4w, job4)}
    active = {k: v for k, v in jobs.items() if v[1] is not None}
    if not active:
        logger.warning("No data for %s — score 0", name)
        return

    total_w = sum(w for w, _ in active.values())
    if total_w <= 0:
        logger.warning("Total weight is 0 for %s — cannot compute score", name)
        return
    raw_score = sum(score * (w / total_w) for w, score in active.values())
    raw_score = round(max(0, min(100, raw_score)), 1)

    # Coverage penalty applied FIRST (reduces score for incomplete data)
    coverage_ratio = len(active) / total_jobs
    coverage_penalty = 0.5 + (coverage_ratio * 0.5)
    adjusted = raw_score * coverage_penalty

    # Quality multiplier applied AFTER coverage
    quality = reddit.get("data_quality_score", 0.5) if reddit else 0.5
    composite = adjusted * (0.7 + quality * 0.3)
    if composite > 100 or composite < 0:
        logger.warning("[scoring] %s: composite out of bounds (%.1f) — clamping", name, composite)
    composite = round(max(0, min(100, composite)), 1)
    data_coverage_pct = round(coverage_ratio * 100)

    verdict = "buy" if composite >= buy_thresh else "watch" if composite >= watch_thresh else "pass"

    # Apply override rules from formula_rules table
    composite, verdict = _apply_override_rules(db, pid, composite, verdict, data_coverage_pct, reddit, gt)

    logger.info("[scoring] %s: raw=%.1f coverage=%d%% adjusted=%.1f → %s",
                name, raw_score, data_coverage_pct, composite, verdict)

    # Phase classification — uses GT slope as primary, Reddit velocity as secondary
    gt_slope = gt.get("slope_24m") if gt else None
    gt_yoy = gt.get("yoy_growth", 0) if gt else None
    reddit_velocity = reddit.get("velocity", 0) if reddit else 0

    # Check Reddit post volume for velocity cross-check
    reddit_dates = 0
    try:
        rd = db.table("posts").select("scraped_date").eq("product_id", pid).eq("platform", "reddit").eq("data_type", "post").execute()
        reddit_dates = len(set(r["scraped_date"] for r in (rd.data or [])))
    except Exception:
        pass
    has_reddit_velocity = reddit_dates >= 2

    # Score sustained above 75 = peak
    if composite >= 75:
        phase = "peak"
    # PRIMARY: GT slope determines phase when available
    elif gt_slope is not None:
        if gt_slope > 0.003 and (gt_yoy or 0) > 0.5:
            # Rising demand — cross-check with Reddit
            if has_reddit_velocity and reddit_velocity < -0.1:
                phase = "plateau"  # GT rising but Reddit falling = conflicting
                logger.info("[scoring] Phase: plateau (GT rising but Reddit velocity negative)")
            else:
                phase = "buy_window"
                logger.info("[scoring] Phase: buy_window (GT slope %.4f, YoY %.0f%%)", gt_slope, (gt_yoy or 0) * 100)
        elif gt_slope > 0 and (gt_yoy or 0) > 0:
            phase = "plateau"  # Positive but not strong enough for buy_window
        elif gt_slope < -0.003:
            if has_reddit_velocity and reddit_velocity > 0.1:
                phase = "plateau"  # GT declining but Reddit rising = conflicting
            else:
                phase = "declining"
                logger.info("[scoring] Phase: declining (GT slope %.4f)", gt_slope)
        else:
            phase = "plateau"  # Flat slope
    # TERTIARY: No GT data and insufficient Reddit history
    else:
        phase = "early"
        logger.info("[scoring] Phase: early (no GT data, %d Reddit dates)", reddit_dates)

    # Fad flag: only set when GT has run (Fix 2)
    fad_flag = None  # null = insufficient data
    if gt:
        fad_flag = gt.get("breakout_flag", False)

    # Get old score for change tracking
    old = db.table("products").select("current_score").eq("id", pid).execute()
    old_score = old.data[0]["current_score"] if old.data else 0

    # Update product with both raw and adjusted scores + coverage
    db.table("products").update({
        "current_score": composite, "current_verdict": verdict,
        "lifecycle_phase": phase, "fad_flag": fad_flag if fad_flag is not None else False,
        "raw_score": raw_score,
        "coverage_pct": data_coverage_pct,
        "active_jobs": len(active),
        "total_jobs": total_jobs,
    }).eq("id", pid).execute()

    # Write scores_history
    db.table("scores_history").insert({
        "product_id": pid, "scored_date": date.today().isoformat(),
        "composite_score": composite,
        "demand_validation_score": job2,
        "purchase_intent_score": job3,
        "supply_readiness_score": job4,
        "verdict": verdict,
        "verdict_reasoning": f"Score {composite} from {len(active)} active jobs. {', '.join(active.keys())}.",
        "score_change": round(composite - old_score, 1),
        "data_confidence": quality,
        "platforms_used": list(j2_parts.keys()) + (["amazon"] if job3 else []) + (["alibaba"] if job4 else []),
    }).execute()

    # Write product snapshot (delete + insert to avoid upsert issues)
    snapshot_data = {
        "product_id": pid, "snapshot_date": date.today().isoformat(),
        "composite_score": composite, "verdict": verdict, "lifecycle_phase": phase,
        "reddit_mentions": reddit.get("mention_count") if reddit else None,
        "reddit_sentiment": reddit.get("sentiment_score") if reddit else None,
        "reddit_intent": reddit.get("avg_intent_score") if reddit else None,
        "gt_slope": gt.get("slope_24m") if gt else None,
        "gt_yoy_growth": gt.get("yoy_growth") if gt else None,
        "platforms_active": len(active),
        "data_confidence": quality,
    }
    try:
        db.table("product_snapshots").delete() \
            .eq("product_id", pid).eq("snapshot_date", date.today().isoformat()).execute()
        db.table("product_snapshots").insert(snapshot_data).execute()
    except Exception as e:
        logger.warning("[scoring] Snapshot write failed: %s", str(e)[:100])

    logger.info("[scoring] %s: %.1f %s (phase=%s, jobs=%d)", name, composite, verdict, phase, len(active))


def _apply_override_rules(db, pid, composite, verdict, coverage_pct, reddit, gt):
    """Read formula_rules from Supabase and apply enabled overrides."""
    try:
        resp = db.table("formula_rules").select("*").eq("enabled", True).execute()
        rules = resp.data or []
    except Exception:
        return composite, verdict

    for rule in rules:
        name = rule.get("rule_name", "")
        rtype = rule.get("rule_type", "")
        thresh = rule.get("threshold_value", 0) or 0
        adj = rule.get("adjustment_value", 0) or 0
        triggered = False

        if name == "Fad Override":
            if gt and gt.get("breakout_flag") and gt.get("slope_24m", 0) < 0.005:
                fad_score = 0.7  # approximate — would need full fad classifier
                if fad_score > thresh:
                    verdict = "pass"
                    triggered = True

        elif name == "Insufficient Data":
            if coverage_pct < thresh:
                if verdict == "buy":
                    verdict = "watch"
                    triggered = True

        elif name == "Reddit Pushback":
            if reddit and (reddit.get("sentiment_score", 0) or 0) < thresh:
                if verdict == "buy":
                    verdict = "watch"
                    triggered = True

        elif name == "Competitor OOS Bonus":
            # Would need competitor data — skip for now
            pass

        elif name == "Multi-Platform Corroboration":
            # Would need cross-platform count — skip for now
            pass

        if rtype == "score_adjustment" and triggered:
            composite = round(min(100, composite + adj), 1)

        if triggered:
            try:
                db.table("formula_rules").update({
                    "last_triggered": datetime.now().isoformat(),
                    "trigger_count": (rule.get("trigger_count", 0) or 0) + 1,
                }).eq("id", rule["id"]).execute()
            except Exception:
                pass

    return composite, verdict


def _get_weighted_signal(db, table, product_id, platform):
    """Get most recent signal row within 30 days with actual data, applying recency weight."""
    thirty_ago = (date.today() - timedelta(days=30)).isoformat()
    try:
        # Get up to 5 recent rows and pick the first one with actual data
        resp = db.table(table).select("*") \
            .eq("product_id", product_id).eq("platform", platform) \
            .gte("scraped_date", thirty_ago) \
            .order("scraped_date", desc=True).limit(5).execute()
        for row in (resp.data or []):
            w = _recency_weight(row.get("scraped_date", ""))
            if w <= 0:
                continue
            # Skip empty signal rows (0 mentions = no actual data)
            mention = row.get("mention_count", 0) or 0
            if mention > 0 or table != "signals_social":
                return row
        # If all rows have 0 mentions, return the most recent anyway
        if resp.data:
            return resp.data[0]
        return None
    except Exception:
        return None
