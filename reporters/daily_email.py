"""
Daily Email Report — HTML email via Gmail SMTP.
Always pulls most recent data before sending.
Sends to all active recipients in email_settings.
"""
import os
import io
import base64
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

VERCEL_URL = "https://product-intelligence-dashboard.vercel.app"


def send_daily_report(db, products=None, run_id=None):
    """Generate and send the daily report. Always pulls fresh data."""
    gmail_addr = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")

    if not gmail_addr or not gmail_pass:
        logger.warning("[email] Gmail credentials not configured — skipping email")
        return 0

    # Get recipients
    recipients = db.table("email_settings").select("email_address, name") \
        .eq("active", True).eq("receive_daily", True).execute().data

    if not recipients:
        logger.info("[email] No active daily recipients")
        return 0

    # Always pull fresh data — never rely on stale passed-in products
    fresh = _load_fresh_data(db, run_id)

    html = _build_html(db, fresh)

    verdicts = {"buy": 0, "watch": 0, "pass": 0}
    high_conf = 0
    for p in fresh["products"]:
        v = p.get("current_verdict", "pass")
        verdicts[v] = verdicts.get(v, 0) + 1
        if p.get("confidence_level") == "high":
            high_conf += 1

    total_products = len(fresh["products"])
    subject = (
        f"[{total_products} products] [{high_conf} HIGH conf] — "
        f"Product Intelligence Daily {date.today().strftime('%b %d')}"
    )

    sent = 0
    failures = 0
    for recipient in recipients:
        if failures >= 3:
            logger.error("[email] 3+ SMTP failures — aborting")
            break
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = gmail_addr
            msg["To"] = recipient["email_address"]
            msg.attach(MIMEText(html, "html"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(gmail_addr, gmail_pass)
                server.sendmail(gmail_addr, recipient["email_address"], msg.as_string())

            sent += 1
            logger.info("[email] Sent to %s", recipient["email_address"])
        except Exception as e:
            failures += 1
            logger.error("[email] Failed to send to %s: %s", recipient["email_address"], e)

    return sent


def _load_fresh_data(db, run_id=None):
    """Pull all current data from Supabase. Never use cached data."""
    # Get the most recent COMPLETED pipeline run
    pipeline = db.table("pipeline_runs").select("*") \
        .eq("status", "complete").order("completed_at", desc=True).limit(1).execute().data
    if not pipeline:
        # Fallback: any recent run
        pipeline = db.table("pipeline_runs").select("*") \
            .order("started_at", desc=True).limit(1).execute().data
    if pipeline:
        run_id = run_id or pipeline[0].get("run_id", "")
        pipeline_time = pipeline[0].get("completed_at") or pipeline[0].get("started_at")
    else:
        pipeline_time = None

    # Fresh products
    products = db.table("products").select("*") \
        .eq("active", True).order("current_score", desc=True).execute().data or []

    # Signals per product
    signals = {}
    for p in products:
        pid = p["id"]
        signals[pid] = {}

        # Social — skip rows with 0 mentions (pick the most recent with actual data)
        social = db.table("signals_social").select("*") \
            .eq("product_id", pid).order("scraped_date", desc=True).limit(20).execute().data or []
        for row in social:
            if row["platform"] not in signals[pid]:
                mention = row.get("mention_count") or 0
                if mention > 0:
                    signals[pid][row["platform"]] = row
        # Fallback: if we still have nothing for a platform, use the latest row anyway
        for row in social:
            if row["platform"] not in signals[pid]:
                signals[pid][row["platform"]] = row

        # Search
        search = db.table("signals_search").select("*") \
            .eq("product_id", pid).order("scraped_date", desc=True).limit(5).execute().data or []
        for row in search:
            if row["platform"] not in signals[pid]:
                signals[pid][row["platform"]] = row

        # Retail
        retail = db.table("signals_retail").select("*") \
            .eq("product_id", pid).order("scraped_date", desc=True).limit(5).execute().data or []
        for row in retail:
            if row["platform"] not in signals[pid]:
                signals[pid][row["platform"]] = row

    # Alerts from today
    today = date.today().isoformat()
    alerts = db.table("alerts").select("*") \
        .gte("triggered_at", today).order("triggered_at", desc=True).limit(20).execute().data or []

    # Previous scores for change tracking
    prev_scores = {}
    for p in products:
        snaps = db.table("product_snapshots").select("composite_score") \
            .eq("product_id", p["id"]).order("snapshot_date", desc=True).limit(2).execute().data or []
        if len(snaps) >= 2:
            prev_scores[p["id"]] = snaps[1]["composite_score"]

    # Apify spend
    month_start = f"{date.today().strftime('%Y-%m')}-01"
    costs = db.table("agent_runs").select("apify_estimated_cost") \
        .gte("created_at", month_start).execute().data or []
    total_cost = sum(r.get("apify_estimated_cost", 0) or 0 for r in costs)

    return {
        "products": products,
        "signals": signals,
        "alerts": alerts,
        "prev_scores": prev_scores,
        "run_id": run_id,
        "pipeline_time": pipeline_time,
        "apify_cost": total_cost,
    }


def _build_html(db, data):
    """Build the full HTML email body."""
    products = data["products"]
    signals = data["signals"]
    alerts = data["alerts"]
    prev_scores = data["prev_scores"]
    today_str = date.today().strftime("%B %d, %Y")
    pipeline_time = data.get("pipeline_time")
    if pipeline_time:
        try:
            pt = datetime.fromisoformat(str(pipeline_time).replace("Z", "+00:00"))
            pipeline_str = pt.strftime("%b %d at %I:%M %p")
        except Exception:
            pipeline_str = str(pipeline_time)[:19]
    else:
        # Fallback: get most recent agent_run completion time
        try:
            latest_run = db.table("agent_runs").select("completed_at") \
                .order("completed_at", desc=True).limit(1).execute().data
            if latest_run and latest_run[0].get("completed_at"):
                pt = datetime.fromisoformat(str(latest_run[0]["completed_at"]).replace("Z", "+00:00"))
                pipeline_str = pt.strftime("%b %d at %I:%M %p")
            else:
                pipeline_str = date.today().strftime("%b %d") + " (manual run)"
        except Exception:
            pipeline_str = date.today().strftime("%b %d") + " (manual run)"

    # Product sections
    product_sections = []
    for p in products[:20]:
        product_sections.append(_product_section(db, p, signals.get(p["id"], {}), prev_scores))

    # Alerts section — always shown
    if alerts:
        alert_rows = ""
        for a in alerts[:10]:
            color = "#dc2626" if a.get("priority") == "critical" else "#d97706" if a.get("priority") == "warning" else "#6b7280"
            alert_rows += f'<div style="padding:8px 12px;margin:4px 0;border-left:3px solid {color};background:#f9fafb;border-radius:4px;font-size:13px;">{a.get("message","")[:200]}</div>'
        alerts_html = f"""
        <div style="background:white;border-radius:12px;padding:20px 24px;margin-bottom:16px;border:1px solid #e5e7eb;">
          <h2 style="font-size:15px;margin:0 0 12px 0;">Alerts ({len(alerts)})</h2>
          {alert_rows}
        </div>"""
    else:
        alerts_html = """
        <div style="background:white;border-radius:12px;padding:20px 24px;margin-bottom:16px;border:1px solid #e5e7eb;">
          <h2 style="font-size:15px;margin:0 0 8px 0;">Alerts</h2>
          <p style="font-size:13px;color:#9ca3af;margin:0;">No alerts this run.</p>
        </div>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="font-family:-apple-system,system-ui,'Segoe UI',sans-serif;max-width:700px;margin:0 auto;padding:0;color:#1f2937;background:#f3f4f6;">

  <!-- Header -->
  <div style="background:#0f0f0f;padding:24px 28px;border-radius:12px 12px 0 0;">
    <h1 style="font-size:20px;margin:0;color:white;font-weight:700;">Product Intelligence</h1>
    <p style="color:#9ca3af;margin:4px 0 0 0;font-size:13px;">Evolution Equities LLC</p>
  </div>

  <!-- Meta bar -->
  <div style="background:white;padding:16px 28px;border-bottom:1px solid #e5e7eb;font-size:13px;color:#6b7280;">
    <strong>Report Date:</strong> {today_str} &nbsp;|&nbsp;
    <strong>Pipeline completed:</strong> {pipeline_str} &nbsp;|&nbsp;
    <strong>Products tracked:</strong> {len(products)} &nbsp;|&nbsp;
    <strong>Apify spend:</strong> ${data['apify_cost']:.2f}/mo
  </div>

  <div style="padding:16px;">

  <!-- Portfolio Summary -->
  <div style="background:white;border-radius:12px;padding:20px 24px;margin-bottom:16px;border:1px solid #e5e7eb;">
    <h2 style="font-size:15px;margin:0 0 12px 0;">Portfolio Summary</h2>
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
      <tr style="border-bottom:2px solid #e5e7eb;background:#f9fafb;">
        <th style="text-align:left;padding:8px;">Product</th>
        <th style="text-align:right;padding:8px;">Score</th>
        <th style="text-align:right;padding:8px;">Change</th>
        <th style="text-align:center;padding:8px;">Verdict</th>
        <th style="text-align:left;padding:8px;">Phase</th>
        <th style="text-align:right;padding:8px;">Coverage</th>
      </tr>
      {"".join(_summary_row(p, prev_scores) for p in products)}
    </table>
  </div>

  <!-- Per-product sections -->
  {"".join(product_sections)}

  {alerts_html}

  {"".join(_score_trend_table(db, p) for p in products[:5])}

  <!-- Footer -->
  <div style="text-align:center;padding:24px 16px;font-size:12px;color:#9ca3af;border-top:1px solid #e5e7eb;margin-top:8px;">
    <a href="{VERCEL_URL}" style="color:#6366f1;text-decoration:none;font-weight:600;font-size:14px;">Open Dashboard</a>
    <br /><br />
    <span style="color:#6b7280;">Product Intelligence System — Evolution Equities LLC</span>
    <br /><br />
    <span style="font-size:11px;">You are receiving this because you are subscribed to Product Intelligence daily reports.</span>
    <br />
    <a href="{VERCEL_URL}/settings" style="color:#9ca3af;text-decoration:underline;font-size:11px;">Manage email preferences</a>
  </div>

  </div>
</body>
</html>"""


def _summary_row(p, prev_scores):
    score = p.get("current_score", 0)
    prev = prev_scores.get(p["id"])
    change = score - prev if prev is not None else 0
    change_color = "#059669" if change > 0 else "#dc2626" if change < 0 else "#6b7280"
    change_arrow = " &#9650;" if change > 0 else " &#9660;" if change < 0 else ""
    change_str = f"{change:+.1f}{change_arrow}" if prev is not None else "—"

    verdict = p.get("current_verdict", "pass")
    vc = {"buy": "#059669", "watch": "#d97706", "pass": "#dc2626"}.get(verdict, "#6b7280")
    phase = (p.get("lifecycle_phase") or "early").replace("_", " ").title()
    coverage = p.get("coverage_pct", 0)

    return f"""<tr style="border-bottom:1px solid #f3f4f6;">
      <td style="padding:8px;font-weight:600;">{p["name"]}</td>
      <td style="padding:8px;text-align:right;font-weight:700;">{score:.1f}</td>
      <td style="padding:8px;text-align:right;color:{change_color};font-weight:600;">{change_str}</td>
      <td style="padding:8px;text-align:center;"><span style="background:{vc};color:white;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600;text-transform:uppercase;">{verdict}</span></td>
      <td style="padding:8px;color:#6b7280;font-size:12px;">{phase}</td>
      <td style="padding:8px;text-align:right;font-size:12px;color:#6b7280;">{coverage}%</td>
    </tr>"""


def _product_section(db, product, platform_signals, prev_scores):
    pid = product["id"]
    name = product["name"]
    score = product.get("current_score", 0)
    raw_score = product.get("raw_score", 0)
    verdict = product.get("current_verdict", "pass")
    phase = (product.get("lifecycle_phase") or "early").replace("_", " ").title()
    coverage = product.get("coverage_pct", 0)
    active_jobs = product.get("active_jobs", 0)
    total_jobs = product.get("total_jobs", 4)

    prev = prev_scores.get(pid)
    change = score - prev if prev is not None else 0
    change_color = "#059669" if change > 0 else "#dc2626" if change < 0 else "#6b7280"
    change_str = f"{change:+.1f}" if prev is not None else "—"

    vc = {"buy": "#059669", "watch": "#d97706", "pass": "#dc2626"}.get(verdict, "#6b7280")

    # Intelligence summary
    summary, signal_level = _intelligence_summary(platform_signals, raw_score, product.get("fad_flag"))
    signal_color = {"positive": "#059669", "mixed": "#d97706", "concerning": "#dc2626"}.get(signal_level, "#6b7280")

    # Confidence
    conf_level = product.get("confidence_level") or "low"
    confidence_reason = product.get("confidence_reason") or ""
    total_comments_scored = product.get("total_comments_scored") or 0
    active_platform_count = product.get("active_platform_count") or 0
    conf_styles = {
        "high":   {"label": "HIGH CONFIDENCE", "bg": "#d1fae5", "fg": "#065f46", "border": "#059669"},
        "medium": {"label": "MED CONFIDENCE",  "bg": "#fef3c7", "fg": "#92400e", "border": "#d97706"},
        "low":    {"label": "LOW DATA",        "bg": "#fee2e2", "fg": "#991b1b", "border": "#dc2626"},
    }
    cs = conf_styles.get(conf_level, conf_styles["low"])
    conf_label = cs["label"]
    conf_bg = cs["bg"]
    conf_fg = cs["fg"]
    conf_border = cs["border"]

    # Platform highlights
    highlights = _platform_highlights(platform_signals, db=db, product_id=pid)

    return f"""
    <div style="background:white;border-radius:12px;padding:20px 24px;margin-bottom:16px;border:1px solid #e5e7eb;border-left:4px solid {vc};">

      <!-- Score header -->
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px;">
        <div>
          <h2 style="font-size:17px;margin:0;">{name}</h2>
          <p style="color:#6b7280;font-size:12px;margin:4px 0 0 0;">{phase} &nbsp;|&nbsp; {coverage}% coverage ({active_jobs}/{total_jobs} jobs) &nbsp;|&nbsp; {total_comments_scored:,} comments &middot; {active_platform_count} platforms</p>
        </div>
        <div style="text-align:right;">
          <span style="font-size:28px;font-weight:700;color:{vc};">{score:.1f}</span>
          <span style="font-size:13px;color:{change_color};font-weight:600;margin-left:8px;">{change_str}</span>
          <br />
          <span style="background:{vc};color:white;padding:2px 12px;border-radius:12px;font-size:12px;font-weight:600;text-transform:uppercase;">{verdict}</span>
          <span style="background:{conf_bg};color:{conf_fg};padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700;margin-left:6px;text-transform:uppercase;">{conf_label}</span>
          {f'<span style="font-size:11px;color:#9ca3af;margin-left:6px;">raw: {raw_score:.1f}</span>' if raw_score != score else ''}
        </div>
      </div>

      <!-- Confidence reason -->
      {f'<div style="background:#f9fafb;border-left:3px solid {conf_border};padding:8px 12px;margin-bottom:12px;border-radius:4px;"><p style="font-size:12px;color:#6b7280;margin:0;line-height:1.5;">{confidence_reason}</p></div>' if confidence_reason else ''}

      <!-- Intelligence Summary -->
      <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:12px 16px;margin-bottom:14px;border-left:3px solid {signal_color};">
        <p style="font-size:11px;font-weight:700;color:#374151;margin:0 0 4px 0;">Intelligence Summary <span style="color:{signal_color};font-weight:600;">({signal_level.title()})</span></p>
        <p style="font-size:13px;color:#374151;margin:0;line-height:1.5;">{summary}</p>
      </div>

      <!-- Platform Highlights -->
      {highlights}

      <div style="text-align:right;margin-top:10px;">
        <a href="{VERCEL_URL}/scorecard/{pid}" style="color:#6366f1;font-size:12px;text-decoration:none;font-weight:600;">View full scorecard &rarr;</a>
      </div>
    </div>"""


def _intelligence_summary(signals, raw_score, fad_flag):
    """Generate plain English summary and signal level."""
    parts = []
    gt = signals.get("google_trends")
    reddit = signals.get("reddit")
    tiktok = signals.get("tiktok")
    amazon = signals.get("amazon")

    if gt and (gt.get("slope_24m") or 0) > 0.003 and (gt.get("yoy_growth") or 0) > 0.5:
        parts.append(f"Google Trends confirms sustained growth with {(gt['yoy_growth'] * 100):.0f}% YoY increase.")

    if tiktok:
        tv = tiktok.get("total_views")
        views = int(tv) if tv is not None and isinstance(tv, (int, float)) and tv > 0 else 0
        if views == 0:
            views = (tiktok.get("total_upvotes") or 0) * 20
        if views > 1_000_000:
            parts.append(f"TikTok shows strong momentum with {views / 1_000_000:.1f}M views.")

    if amazon:
        rc = amazon.get("review_count") or 0
        sat = amazon.get("satisfaction_score") or 0
        if rc > 10_000:
            parts.append(f"Amazon confirms a healthy buyer base with {rc / 1000:.0f}K+ reviews and {sat:.0f}% satisfaction.")

    if reddit:
        intent = reddit.get("avg_intent_score") or 0
        if intent < 0.3 and (reddit.get("mention_count") or 0) > 20:
            parts.append("Reddit shows awareness but purchase intent is still moderate.")
        elif intent >= 0.3:
            parts.append("Reddit shows active purchase consideration with strong intent signals.")

    if not parts:
        parts.append("Limited data across platforms. More pipeline runs needed.")

    # Signal level
    gt_slope = gt.get("slope_24m", 0) if gt else 0
    if raw_score > 60 and gt_slope > 0:
        level = "positive"
    elif raw_score < 45 or fad_flag:
        level = "concerning"
    else:
        level = "mixed"

    return " ".join(parts), level


def _platform_highlights(signals, db=None, product_id=None):
    """Generate per-platform highlight rows."""
    rows = []

    reddit = signals.get("reddit")
    if reddit:
        rows.append(_highlight_row("Reddit", "#f97316", [
            f"Posts: {reddit.get('mention_count', 0)}",
            f"Sentiment: {(reddit.get('sentiment_score') or 0):.2f}",
            f"High intent: {reddit.get('high_intent_comment_count', 0)}",
            f"Buy intent: {reddit.get('buy_intent_comment_count', 0)}",
        ]))

    tiktok = signals.get("tiktok")
    if tiktok:
        # Read total_views directly from the signal row column
        views = 0
        tv = tiktok.get("total_views")
        if tv is not None and isinstance(tv, (int, float)) and tv > 0:
            views = int(tv)
        if views == 0:
            # Only estimate if column is truly missing/zero
            views = (tiktok.get("total_upvotes") or 0) * 20
        rows.append(_highlight_row("TikTok", "#111827", [
            f"Videos: {tiktok.get('mention_count', 0)}",
            f"Views: {views:,.0f}",
            f"Likes: {(tiktok.get('total_upvotes') or 0):,.0f}",
        ]))

    instagram = signals.get("instagram")
    if instagram:
        mentions = instagram.get("mention_count") or 0
        eng = ((instagram.get("total_upvotes") or 0) + (instagram.get("total_comment_count") or 0))
        avg_eng = eng / max(mentions, 1)
        rows.append(_highlight_row("Instagram", "#c026d3", [
            f"Posts: {mentions}",
            f"Avg engagement: {avg_eng:.0f}",
        ]))

    amazon = signals.get("amazon")
    if amazon:
        rating = amazon.get("avg_rating") or 0
        reviews = amazon.get("review_count") or 0
        sat = amazon.get("satisfaction_score") or 0
        five = amazon.get("five_star_pct") or 0
        four = amazon.get("four_star_pct") or 0
        three = amazon.get("three_star_pct") or 0
        two = amazon.get("two_star_pct") or 0
        one = amazon.get("one_star_pct") or 0
        # Check if a previous signals_retail row exists for actual delta
        has_previous = False
        prev_review_count = 0
        if db and product_id:
            try:
                prev_rows = db.table("signals_retail").select("review_count, scraped_date") \
                    .eq("product_id", product_id).eq("platform", "amazon") \
                    .order("scraped_date", desc=True).limit(2).execute().data or []
                if len(prev_rows) >= 2:
                    has_previous = True
                    prev_review_count = prev_rows[1].get("review_count") or 0
            except Exception:
                pass

        if has_previous:
            delta = reviews - prev_review_count
            vel_str = f"New reviews: {delta:+,d}"
        else:
            vel_str = f"Baseline established &mdash; {reviews:,d} total"

        dist_bar = ""
        if five > 0 or one > 0:
            dist_bar = (
                f'<div style="font-size:11px;color:#6b7280;margin-top:6px;">'
                f'5&#9733; {five:.0f}% &nbsp;|&nbsp; 4&#9733; {four:.0f}% &nbsp;|&nbsp; '
                f'3&#9733; {three:.0f}% &nbsp;|&nbsp; 2&#9733; {two:.0f}% &nbsp;|&nbsp; '
                f'1&#9733; {one:.0f}%</div>'
            )

        warning = ""
        if one > 15:
            warning = '<div style="color:#dc2626;font-size:11px;font-weight:600;margin-top:4px;">&#9888; High negative review rate — investigate before sourcing</div>'

        rows.append(f"""
        <div style="padding:10px 0;border-bottom:1px solid #f3f4f6;">
          <div style="margin-bottom:6px;">
            <span style="background:#f59e0b;color:white;padding:1px 8px;border-radius:4px;font-size:11px;font-weight:600;">Amazon</span>
          </div>
          <div style="font-size:13px;color:#374151;line-height:1.8;">
            Rating: {rating:.1f} &#9733; &nbsp;&nbsp;|&nbsp;&nbsp; Reviews: {reviews:,.0f}<br/>
            Satisfaction: {sat:.0f}% &nbsp;&nbsp;|&nbsp;&nbsp; {vel_str}
          </div>
          {dist_bar}
          {warning}
        </div>""")

    gt = signals.get("google_trends")
    if gt:
        slope = gt.get("slope_24m") or 0
        yoy = gt.get("yoy_growth") or 0
        direction = "Rising" if slope > 0.003 else "Flat" if slope > -0.003 else "Declining"
        rows.append(_highlight_row("Google Trends", "#2563eb", [
            f"Slope: {slope:+.4f} ({direction})",
            f"YoY: {yoy * 100:.0f}%",
            f"Pattern: {gt.get('seasonal_pattern', 'N/A')}",
            "Not a fad" if not gt.get("breakout_flag") else "Breakout detected",
        ]))

    if not rows:
        return '<p style="font-size:13px;color:#9ca3af;">No platform data available yet.</p>'

    return f'<div style="margin-bottom:8px;">{"".join(rows)}</div>'


def _highlight_row(platform, color, metrics):
    metrics_html = " &nbsp;&nbsp;|&nbsp;&nbsp; ".join(metrics)
    return f"""
    <div style="padding:10px 0;border-bottom:1px solid #f3f4f6;">
      <div style="margin-bottom:6px;">
        <span style="background:{color};color:white;padding:1px 8px;border-radius:4px;font-size:11px;font-weight:600;">{platform}</span>
      </div>
      <div style="font-size:13px;color:#374151;line-height:1.6;">{metrics_html}</div>
    </div>"""


def _score_trend_table(db, product):
    """Build a text-based 30-day score trend table for email."""
    pid = product["id"]
    name = product["name"]
    thirty_ago = (date.today() - timedelta(days=30)).isoformat()

    try:
        snaps = db.table("product_snapshots") \
            .select("snapshot_date, composite_score, verdict") \
            .eq("product_id", pid) \
            .gte("snapshot_date", thirty_ago) \
            .order("snapshot_date", desc=True) \
            .limit(10) \
            .execute().data or []
    except Exception:
        snaps = []

    if len(snaps) < 2:
        return f"""
        <div style="background:white;border-radius:12px;padding:20px 24px;margin-bottom:16px;border:1px solid #e5e7eb;">
          <h2 style="font-size:15px;margin:0 0 8px 0;">30-Day Score Trend — {name}</h2>
          <p style="font-size:13px;color:#9ca3af;margin:0;">Trend data building — check back after more pipeline runs complete.</p>
        </div>"""

    vc_map = {"buy": "#059669", "watch": "#d97706", "pass": "#dc2626"}

    trend_rows = ""
    for i, snap in enumerate(snaps):
        score = snap.get("composite_score", 0)
        verdict = snap.get("verdict", "pass")
        vc = vc_map.get(verdict, "#6b7280")

        try:
            d = datetime.strptime(snap["snapshot_date"], "%Y-%m-%d")
            date_str = d.strftime("%b %d, %Y")
        except Exception:
            date_str = snap["snapshot_date"]

        # Change from next older row
        if i < len(snaps) - 1:
            prev_score = snaps[i + 1].get("composite_score", 0)
            delta = score - prev_score
            delta_color = "#059669" if delta > 0 else "#dc2626" if delta < 0 else "#6b7280"
            arrow = " &#9650;" if delta > 0 else " &#9660;" if delta < 0 else ""
            change_str = f'<span style="color:{delta_color};font-weight:600;">{delta:+.1f}{arrow}</span>'
        else:
            change_str = '<span style="color:#6b7280;">baseline</span>'

        trend_rows += f"""
        <tr style="border-bottom:1px solid #f3f4f6;">
          <td style="padding:6px 8px;font-size:13px;color:#6b7280;">{date_str}</td>
          <td style="padding:6px 8px;font-size:13px;font-weight:700;text-align:right;">{score:.1f}</td>
          <td style="padding:6px 8px;text-align:center;">
            <span style="background:{vc};color:white;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:600;text-transform:uppercase;">{verdict}</span>
          </td>
          <td style="padding:6px 8px;text-align:right;font-size:13px;">{change_str}</td>
        </tr>"""

    return f"""
    <div style="background:white;border-radius:12px;padding:20px 24px;margin-bottom:16px;border:1px solid #e5e7eb;">
      <h2 style="font-size:15px;margin:0 0 12px 0;">30-Day Score Trend — {name}</h2>
      <table style="width:100%;border-collapse:collapse;">
        <tr style="border-bottom:2px solid #e5e7eb;background:#f9fafb;">
          <th style="text-align:left;padding:6px 8px;font-size:12px;color:#6b7280;font-weight:600;">Date</th>
          <th style="text-align:right;padding:6px 8px;font-size:12px;color:#6b7280;font-weight:600;">Score</th>
          <th style="text-align:center;padding:6px 8px;font-size:12px;color:#6b7280;font-weight:600;">Verdict</th>
          <th style="text-align:right;padding:6px 8px;font-size:12px;color:#6b7280;font-weight:600;">Change</th>
        </tr>
        {trend_rows}
      </table>
    </div>"""


def _generate_score_chart(db, product_id):
    """Generate a 30-day score trend chart as base64 PNG."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        snaps = db.table("product_snapshots").select("snapshot_date, composite_score") \
            .eq("product_id", product_id).order("snapshot_date").execute().data

        if len(snaps) < 2:
            return None

        dates = [datetime.strptime(s["snapshot_date"], "%Y-%m-%d") for s in snaps]
        scores = [s["composite_score"] for s in snaps]

        fig, ax = plt.subplots(figsize=(6, 2.5))
        ax.plot(dates, scores, color="#6366f1", linewidth=2)
        ax.axhline(y=75, color="#059669", linestyle="--", alpha=0.5, linewidth=1)
        ax.axhline(y=55, color="#d97706", linestyle="--", alpha=0.5, linewidth=1)
        ax.fill_between(dates, scores, alpha=0.1, color="#6366f1")
        ax.set_ylim(0, 100)
        ax.set_ylabel("Score", fontsize=10)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.tick_params(labelsize=9)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")
    except Exception as e:
        logger.warning("[email] Chart generation failed: %s", e)
        return None
