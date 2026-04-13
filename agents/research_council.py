"""
Research Council — 5 specialist AI agents that deliberate on each product.
Uses Claude API (claude-sonnet-4-20250514). Runs sequentially per product.
Includes Round 2 debate for split decisions and dissent tracking.
"""
import os
import json
import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 1000

AGENTS = [
    {
        "name": "trend_archaeologist",
        "label": "Trend Archaeologist",
        "system": (
            "You are an expert trend analyst with 30 years of experience identifying products before they "
            "break out into mainstream retail. You specialize in reading slope shapes and velocity curves. "
            "A rising slope with accelerating velocity in the pre-peak seasonal window is your strongest "
            "Buy signal. You speak with confidence and authority. When you see a pattern you have seen "
            "before you say so directly — I have seen this before, this matches the signature of X product "
            "in Y year. You never hedge on strong signals. If the composite score undervalues what you are "
            "seeing in the trend shape you say so explicitly and explain why. Vote Buy, Watch, or Pass with "
            "confidence 0-100 and plain English reasoning under 100 words."
        ),
    },
    {
        "name": "demand_validator",
        "label": "Demand Validator",
        "system": (
            "You are a consumer behavior expert who specializes in separating real purchase intent from "
            "social noise. You know that 50 comments where people describe buying or repurchasing a product "
            "is worth more than 500 comments of general awareness. You look for brand-specific buying "
            "language, repeat purchase behavior, and price acceptance signals. You are precise and "
            "evidence-based. You call out weak signals immediately — I need to see X before I vote Buy. "
            "If the data shows strong intent but the composite score does not reflect it, you say so and "
            "explain what the score is missing. Vote Buy, Watch, or Pass with confidence 0-100 and "
            "reasoning under 100 words."
        ),
    },
    {
        "name": "supply_analyst",
        "label": "Supply Chain Analyst",
        "system": (
            "You are a wholesale sourcing expert who evaluates whether the supply side can support a demand "
            "signal at profitable margin. You speak in numbers and margins — at current MOQ this pencils out "
            "to X margin. Healthy supplier count with dropping MOQ and stable pricing is the ideal sourcing "
            "window. Constrained supply with rising MOQ means act now or miss the window. If Alibaba data "
            "does not exist for this product you abstain cleanly and explain what data you need. When supply "
            "conditions are exceptional you say so directly and urgently. Vote Buy, Watch, Pass, or Abstain "
            "with confidence 0-100 and reasoning under 100 words."
        ),
    },
    {
        "name": "fad_detector",
        "label": "Fad Detector",
        "system": (
            "You are a contrarian trend analyst whose entire job is to find reasons a signal is misleading. "
            "You have watched hundreds of products spike on social media and disappear within 90 days. You "
            "look for influencer-driven surges with no organic base, single-platform bubbles that do not "
            "translate to retail demand, and seasonal spikes being mistaken for trend momentum. You never "
            "apologize for voting Pass. You hold your ground when challenged. You respect the other agents "
            "but you need strong multi-platform evidence before you vote Buy. If you think the composite "
            "score is too optimistic you say so directly and explain what is misleading about the signal. "
            "Vote Buy, Watch, or Pass with confidence 0-100 and reasoning under 100 words."
        ),
    },
    {
        "name": "category_strategist",
        "label": "Category Strategist",
        "system": (
            "You are a category lifecycle expert who understands how trends move through adjacent product "
            "categories. When one category peaks the adjacent category is often just entering its growth "
            "phase. You see the big picture and speak in cycles — the adjacent category peaked 6 months "
            "ago which puts this product right in the sweet spot. You identify category windows opening "
            "and closing. When the window is closing you say so urgently — this is a 90-day opportunity "
            "not a 12-month one. If the scoring formula is missing category context you formally recommend "
            "what data would fix it. Vote Buy, Watch, or Pass with confidence 0-100 and reasoning under "
            "100 words."
        ),
    },
]


async def run_council_session(db, products, run_id):
    """Run the full research council for all active products."""
    from agents.skills.activity_logger import post_status

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — council skipped")
        post_status("scraper-council", "reporting", "Skipped — no API key")
        return {}

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # Load council weights
    weights = {}
    try:
        w_resp = db.table("council_weights").select("agent_name, current_weight").execute()
        weights = {r["agent_name"]: r["current_weight"] for r in w_resp.data}
    except Exception:
        pass

    results = {}
    for product in products:
        post_status("scraper-council", "busy", f"Deliberating: {product['name']}")
        try:
            result = await _deliberate_product(db, client, product, run_id, weights)
            results[product["name"]] = result
        except Exception as e:
            logger.error("Council failed for %s: %s", product["name"], e)
            results[product["name"]] = {"error": str(e)[:300]}

    post_status("scraper-council", "done", f"Council complete for {len(products)} products")
    post_status("scraper-council", "idle", "Council idle")
    return results


async def _deliberate_product(db, client, product, run_id, weights):
    pid = product["id"]
    name = product["name"]
    category = product.get("category", "")

    # Gather data context for agents
    context = _build_data_context(db, pid, name, category)
    composite_verdict = product.get("current_verdict", "watch")
    composite_score = product.get("current_score", 0)

    # Round 1: Each agent votes independently
    round1_votes = {}
    recommendations = []

    for agent in AGENTS:
        prompt = (
            f"Product: {name} (Category: {category})\n"
            f"Current composite score: {composite_score}\n"
            f"Current composite verdict: {composite_verdict}\n\n"
            f"DATA:\n{context.get(agent['name'], context.get('general', 'No data available.'))}\n\n"
            f"Based on this data, what is your vote?\n"
            f"Respond in exactly this JSON format:\n"
            f'{{"vote": "Buy|Watch|Pass|Abstain", "confidence": 0-100, "reasoning": "your reasoning under 100 words", '
            f'"dissent_from_composite": true|false, "dissent_reasoning": "why you disagree with composite if you do", '
            f'"formula_recommendation": null|{{"type": "weight_adjustment|threshold_change|override_request|new_signal_needed", '
            f'"current_value": "...", "recommended_value": "...", "reasoning": "..."}}}}'
        )

        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=MAX_TOKENS,
                system=agent["system"],
                messages=[{"role": "user", "content": prompt}],
                timeout=45,
            )
            text = resp.content[0].text.strip()

            # Parse JSON from response
            parsed = _parse_agent_response(text)
            round1_votes[agent["name"]] = parsed

            # Check for formula recommendations
            if parsed.get("formula_recommendation"):
                rec = parsed["formula_recommendation"]
                recommendations.append({
                    "product_id": pid, "run_id": run_id,
                    "agent_name": agent["name"],
                    "recommendation_type": rec.get("type", "weight_adjustment"),
                    "current_value": rec.get("current_value", ""),
                    "recommended_value": rec.get("recommended_value", ""),
                    "reasoning": rec.get("reasoning", ""),
                    "confidence": parsed.get("confidence", 50),
                })

            logger.info("[council] %s voted %s (%.0f%%) for %s",
                        agent["label"], parsed.get("vote", "?"), parsed.get("confidence", 0), name)

        except Exception as e:
            logger.error("[council] %s failed: %s", agent["name"], e)
            round1_votes[agent["name"]] = {"vote": "Abstain", "confidence": 0, "reasoning": f"Error: {str(e)[:100]}"}

    # Tally Round 1 votes
    tally = _tally_votes(round1_votes, weights)

    # Round 2: If not unanimous, run debate
    round2_votes = {}
    if not tally["unanimous"]:
        round2_votes = await _run_round2(client, round1_votes, tally, name, context)

    # Use Round 2 votes if they exist, otherwise Round 1
    final_votes = {}
    for agent_name in round1_votes:
        if agent_name in round2_votes and round2_votes[agent_name].get("vote"):
            final_votes[agent_name] = round2_votes[agent_name]
        else:
            final_votes[agent_name] = round1_votes[agent_name]

    final_tally = _tally_votes(final_votes, weights)

    # Build council_verdicts row
    verdict_row = {
        "product_id": pid, "run_id": run_id,
        "verdict_date": date.today().isoformat(),
        "council_verdict": final_tally["verdict"],
        "council_confidence": final_tally["avg_confidence"],
        "votes_for_buy": final_tally["buy_count"],
        "votes_for_watch": final_tally["watch_count"],
        "votes_for_pass": final_tally["pass_count"],
        "final_verdict": final_tally["verdict"],
    }

    # Add per-agent votes
    for agent in AGENTS:
        n = agent["name"]
        r1 = round1_votes.get(n, {})
        r2 = round2_votes.get(n, {})
        verdict_row[f"{n}_vote"] = r1.get("vote")
        verdict_row[f"{n}_confidence"] = r1.get("confidence")
        verdict_row[f"{n}_reasoning"] = r1.get("reasoning")
        if r2:
            verdict_row[f"{n}_round2_vote"] = r2.get("vote")
            verdict_row[f"{n}_round2_reasoning"] = r2.get("reasoning")

    # Dissent tracking
    any_dissent = any(v.get("dissent_from_composite") for v in round1_votes.values())
    if any_dissent:
        dissent_reasons = [f"{n}: {v.get('dissent_reasoning', '')}"
                          for n, v in round1_votes.items() if v.get("dissent_from_composite")]
        verdict_row["dissent_from_composite"] = True
        verdict_row["dissent_reasoning"] = " | ".join(dissent_reasons)

    db.table("council_verdicts").insert(verdict_row).execute()

    # Write formula recommendations
    for rec in recommendations:
        try:
            db.table("formula_recommendations").insert(rec).execute()
        except Exception as e:
            logger.error("Failed to write recommendation: %s", e)

    logger.info("[council] %s: council=%s (buy=%d watch=%d pass=%d)",
                name, final_tally["verdict"], final_tally["buy_count"],
                final_tally["watch_count"], final_tally["pass_count"])

    return final_tally


async def _run_round2(client, round1_votes, tally, product_name, context):
    """Run Round 2 debate for split decisions."""
    round2 = {}
    majority_vote = tally["verdict"]

    for agent in AGENTS:
        n = agent["name"]
        r1 = round1_votes.get(n, {})
        agent_vote = r1.get("vote", "Abstain")

        if agent_vote.lower() == majority_vote.lower() or agent_vote == "Abstain":
            continue  # Only dissenting agents respond in Round 2

        # Build other agents' reasoning
        others = []
        for other_agent in AGENTS:
            on = other_agent["name"]
            if on == n:
                continue
            ov = round1_votes.get(on, {})
            if ov.get("vote", "").lower() != agent_vote.lower():
                others.append(f"{other_agent['label']} voted {ov.get('vote')} ({ov.get('confidence')}%): {ov.get('reasoning', '')}")

        prompt = (
            f"Product: {product_name}\n"
            f"The council voted {tally['buy_count']} Buy, {tally['watch_count']} Watch, {tally['pass_count']} Pass.\n"
            f"You voted {agent_vote} with {r1.get('confidence')}% confidence.\n\n"
            f"The following agents disagreed with you:\n" + "\n".join(others) + "\n\n"
            f"Do you maintain your position or update your vote based on their reasoning?\n"
            f"Respond in JSON: {{\"vote\": \"Buy|Watch|Pass\", \"confidence\": 0-100, "
            f"\"reasoning\": \"under 100 words\", \"changed_position\": true|false}}"
        )

        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=MAX_TOKENS,
                system=agent["system"],
                messages=[{"role": "user", "content": prompt}],
                timeout=45,
            )
            round2[n] = _parse_agent_response(resp.content[0].text.strip())
            logger.info("[council] Round 2: %s %s -> %s",
                        agent["label"],
                        "changed to" if round2[n].get("changed_position") else "maintains",
                        round2[n].get("vote"))
        except Exception as e:
            logger.error("[council] Round 2 %s failed: %s", n, e)

    return round2


def _tally_votes(votes, weights):
    buy_w = watch_w = pass_w = 0
    buy_c = watch_c = pass_c = 0
    confidences = []
    total_w = 0

    for name, v in votes.items():
        vote = (v.get("vote") or "Abstain").lower()
        if vote == "abstain":
            continue
        w = weights.get(name, 1.0)
        total_w += w
        confidences.append(v.get("confidence", 50))

        if vote == "buy":
            buy_w += w; buy_c += 1
        elif vote == "watch":
            watch_w += w; watch_c += 1
        elif vote == "pass":
            pass_w += w; pass_c += 1

    if total_w == 0:
        return {"verdict": "insufficient_data", "buy_count": 0, "watch_count": 0, "pass_count": 0,
                "avg_confidence": 0, "unanimous": False}

    buy_pct = buy_w / total_w
    watch_pct = watch_w / total_w

    if buy_pct >= 0.60:
        verdict = "buy"
    elif (buy_pct + watch_pct) >= 0.35:
        verdict = "watch"
    else:
        verdict = "pass"

    non_abstain = [v.get("vote", "").lower() for v in votes.values() if v.get("vote", "").lower() not in ("abstain", "")]
    unanimous = len(set(non_abstain)) <= 1 and len(non_abstain) > 0

    return {
        "verdict": verdict, "buy_count": buy_c, "watch_count": watch_c, "pass_count": pass_c,
        "avg_confidence": round(sum(confidences) / max(len(confidences), 1), 1),
        "unanimous": unanimous,
    }


def _build_data_context(db, pid, name, category):
    """Build data context strings for each agent to read."""
    ctx = {}

    # General context
    thirty_ago = (date.today() - timedelta(days=30)).isoformat()

    # Snapshots
    snaps = db.table("product_snapshots").select("*").eq("product_id", pid) \
        .gte("snapshot_date", thirty_ago).order("snapshot_date", desc=True).execute().data

    # Reddit signal
    reddit = db.table("signals_social").select("*").eq("product_id", pid) \
        .eq("platform", "reddit").order("scraped_date", desc=True).limit(1).execute().data
    r = reddit[0] if reddit else {}

    # GT signal
    gt = db.table("signals_search").select("*").eq("product_id", pid) \
        .eq("platform", "google_trends").order("scraped_date", desc=True).limit(1).execute().data
    g = gt[0] if gt else {}

    # Supply signal
    supply = db.table("signals_supply").select("*").eq("product_id", pid) \
        .order("scraped_date", desc=True).limit(1).execute().data
    s = supply[0] if supply else {}

    # High intent comments
    hi_comments = db.table("comments").select("comment_body, intent_level, is_buy_intent, is_repeat_purchase") \
        .eq("product_id", pid).gte("intent_level", 4).order("intent_level", desc=True).limit(10).execute().data

    # Score history
    scores = db.table("scores_history").select("composite_score, scored_date, verdict") \
        .eq("product_id", pid).order("scored_date", desc=True).limit(30).execute().data

    general = (
        f"Reddit: {r.get('mention_count', 0)} mentions, sentiment {r.get('sentiment_score', 0)}, "
        f"velocity {r.get('velocity', 0)}, intent {r.get('avg_intent_score', 0)}, "
        f"buy_intent_count {r.get('buy_intent_comment_count', 0)}, "
        f"repeat_purchase {r.get('repeat_purchase_pct', 0):.1%}\n"
        f"Google Trends: slope {g.get('slope_24m', 'N/A')}, YoY {g.get('yoy_growth', 'N/A')}, "
        f"breakout {g.get('breakout_flag', 'N/A')}, seasonal {g.get('seasonal_pattern', 'N/A')}\n"
        f"Supply: {s.get('supplier_listing_count', 'N/A')} suppliers, MOQ {s.get('moq_current', 'N/A')}, "
        f"trend {s.get('moq_trend', 'N/A')}, price/unit {s.get('price_per_unit', 'N/A')}\n"
        f"Score history (last 5): {[(x['scored_date'], x['composite_score'], x['verdict']) for x in scores[:5]]}\n"
        f"Snapshots (last 5): {[(x['snapshot_date'], x['composite_score']) for x in snaps[:5]]}"
    )

    ctx["general"] = general
    ctx["trend_archaeologist"] = general + f"\nVelocity trend: {[x.get('composite_score') for x in scores[:10]]}"
    ctx["demand_validator"] = general + f"\nHigh intent comments (L4+L5): {json.dumps([c.get('comment_body', '')[:100] for c in hi_comments[:5]])}"
    ctx["supply_analyst"] = general
    ctx["fad_detector"] = general + f"\nPlatform distribution: Reddit only so far. Multi-platform data pending."
    ctx["category_strategist"] = general + f"\nCategory: {category}. Check adjacent product trends."

    return ctx


def _parse_agent_response(text):
    """Extract JSON from agent response, handling markdown code blocks."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON in the text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
        return {"vote": "Abstain", "confidence": 0, "reasoning": text[:200]}
