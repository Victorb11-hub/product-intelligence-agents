"""
Backtesting Engine — Reconstructs historical signal picture and runs council agents.
Input: product keyword + start date + end date
Output: month-by-month council verdicts as JSON
"""
import os
import json
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)


def run_backtest(keyword, start_date, end_date):
    """
    Run a backtest for a product keyword over a date range.
    Returns month-by-month council verdicts.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY not set"}

    # Pull Google Trends historical data
    from pytrends.request import TrendReq
    pt = TrendReq(hl="en-US", tz=360)

    timeframe = f"{start_date} {end_date}"
    try:
        pt.build_payload([keyword], timeframe=timeframe, geo="US")
        df = pt.interest_over_time()
    except Exception as e:
        return {"error": f"PyTrends failed: {str(e)[:200]}"}

    if df.empty:
        return {"error": f"No Google Trends data for '{keyword}' in range"}

    values = df[keyword].tolist()
    dates = [d.strftime("%Y-%m-%d") for d in df.index]

    # Group into months
    months = {}
    for d, v in zip(dates, values):
        month_key = d[:7]  # "2024-01"
        if month_key not in months:
            months[month_key] = []
        months[month_key].append(v)

    # For each month, reconstruct signal and run council
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    from agents.research_council import AGENTS, _parse_agent_response

    results = []
    import numpy as np

    sorted_months = sorted(months.keys())
    for i, month_key in enumerate(sorted_months):
        month_values = months[month_key]
        avg_interest = sum(month_values) / len(month_values)

        # Calculate slope up to this month
        all_values_to_date = []
        for mk in sorted_months[:i + 1]:
            all_values_to_date.extend(months[mk])

        if len(all_values_to_date) > 1:
            x = np.arange(len(all_values_to_date))
            slope = float(np.polyfit(x, all_values_to_date, 1)[0])
        else:
            slope = 0

        # Velocity (month over month change)
        prev_avg = sum(months[sorted_months[i - 1]]) / len(months[sorted_months[i - 1]]) if i > 0 else avg_interest
        velocity = (avg_interest - prev_avg) / max(prev_avg, 1)

        context = (
            f"Product keyword: {keyword}\n"
            f"Month: {month_key}\n"
            f"Google Trends average interest: {avg_interest:.1f}/100\n"
            f"Slope to date: {slope:.4f}\n"
            f"Month-over-month velocity: {velocity:+.2%}\n"
            f"Interest trend: {all_values_to_date[-6:] if len(all_values_to_date) >= 6 else all_values_to_date}\n"
            f"NOTE: This is a backtest. Only Google Trends data is available. "
            f"No Reddit, Amazon, or Alibaba data for this historical period."
        )

        month_votes = {}
        for agent in AGENTS:
            prompt = (
                f"{context}\n\n"
                f"Based on this historical signal, would you have recommended sourcing this product "
                f"during {month_key}? Vote Buy, Watch, or Pass with confidence 0-100 and reasoning."
                f'\nRespond in JSON: {{"vote": "Buy|Watch|Pass", "confidence": 0-100, "reasoning": "under 100 words"}}'
            )

            try:
                resp = client.messages.create(
                    model="claude-sonnet-4-20250514", max_tokens=500,
                    system=agent["system"],
                    messages=[{"role": "user", "content": prompt}],
                )
                parsed = _parse_agent_response(resp.content[0].text.strip())
                month_votes[agent["name"]] = parsed
            except Exception as e:
                month_votes[agent["name"]] = {"vote": "Abstain", "confidence": 0, "reasoning": str(e)[:100]}

        # Tally
        buy_c = sum(1 for v in month_votes.values() if v.get("vote", "").lower() == "buy")
        watch_c = sum(1 for v in month_votes.values() if v.get("vote", "").lower() == "watch")
        pass_c = sum(1 for v in month_votes.values() if v.get("vote", "").lower() == "pass")
        total = buy_c + watch_c + pass_c
        verdict = "buy" if total > 0 and buy_c / total >= 0.6 else "watch" if total > 0 and (buy_c + watch_c) / total >= 0.35 else "pass"

        results.append({
            "month": month_key,
            "avg_interest": round(avg_interest, 1),
            "slope": round(slope, 4),
            "velocity": round(velocity, 4),
            "votes": month_votes,
            "buy_count": buy_c,
            "watch_count": watch_c,
            "pass_count": pass_c,
            "council_verdict": verdict,
        })

    return {"keyword": keyword, "months": results, "total_months": len(results)}
