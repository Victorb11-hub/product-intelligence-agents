"""
Daily Email Report — HTML email with embedded charts via Gmail SMTP.
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


def send_daily_report(db, products, run_id):
    """Generate and send the daily report. Returns number of emails sent."""
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

    # Build report data
    html = _build_daily_html(db, products, run_id)
    verdicts = {"buy": 0, "watch": 0, "pass": 0}
    for p in products:
        v = p.get("current_verdict", "watch")
        verdicts[v] = verdicts.get(v, 0) + 1

    subject = (
        f"Product Intelligence Daily — {date.today().strftime('%b %d, %Y')} — "
        f"{verdicts['buy']} Buy, {verdicts['watch']} Watch, {verdicts['pass']} Pass"
    )

    sent = 0
    smtp_failures = 0
    for recipient in recipients:
        try:
            if smtp_failures >= 3:
                logger.error("[email] Too many SMTP failures — aborting remaining sends")
                break

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
            smtp_failures += 1
            logger.error("[email] Failed to send to %s: %s", recipient["email_address"], e)
            try:
                db.table("alerts").insert({
                    "alert_type": "fad_warning", "priority": "high",
                    "message": f"Email delivery failed: {str(e)[:200]}",
                    "actioned": False,
                }).execute()
            except Exception:
                pass

    return sent


def _build_daily_html(db, products, run_id):
    """Build the full HTML email body."""
    today_str = date.today().strftime("%B %d, %Y")
    is_monday = date.today().weekday() == 0

    # Sort products by score
    sorted_products = sorted(products, key=lambda p: p.get("current_score", 0), reverse=True)

    # Apify spend this month
    month_start = f"{date.today().strftime('%Y-%m')}-01"
    costs = db.table("agent_runs").select("apify_estimated_cost").gte("created_at", month_start).execute().data
    total_cost = sum(r.get("apify_estimated_cost", 0) or 0 for r in costs)

    # Build score chart for each product (limit to top 20)
    max_products = 20
    product_sections = []
    for product in sorted_products[:max_products]:
        section = _build_product_section(db, product, run_id)
        product_sections.append(section)
    if len(sorted_products) > max_products:
        product_sections.append(f'<div style="text-align:center;color:#9ca3af;font-size:12px;padding:16px;">Showing top {max_products} of {len(sorted_products)} products. View full leaderboard in the dashboard.</div>')

    # Council weekly briefing (Mondays only)
    weekly_section = ""
    if is_monday:
        weekly_section = _build_weekly_briefing(db, run_id)

    verdict_colors = {"buy": "#059669", "watch": "#d97706", "pass": "#dc2626"}

    html = f"""
    <html>
    <body style="font-family: -apple-system, system-ui, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #1f2937; background: #f9fafb;">
      <div style="background: white; border-radius: 12px; padding: 24px; margin-bottom: 20px; border: 1px solid #e5e7eb;">
        <h1 style="font-size: 22px; margin: 0 0 4px 0;">Product Intelligence Daily</h1>
        <p style="color: #6b7280; margin: 0 0 20px 0; font-size: 14px;">{today_str}</p>

        <h2 style="font-size: 16px; margin: 20px 0 12px 0;">Portfolio Summary</h2>
        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
          <tr style="border-bottom: 1px solid #e5e7eb; background: #f9fafb;">
            <th style="text-align: left; padding: 8px;">Product</th>
            <th style="text-align: right; padding: 8px;">Score</th>
            <th style="text-align: center; padding: 8px;">Verdict</th>
            <th style="text-align: left; padding: 8px;">Phase</th>
          </tr>
          {"".join(f'''
          <tr style="border-bottom: 1px solid #f3f4f6;">
            <td style="padding: 8px; font-weight: 600;">{p["name"]}</td>
            <td style="padding: 8px; text-align: right; font-weight: 600;">{p.get("current_score", 0):.1f}</td>
            <td style="padding: 8px; text-align: center;">
              <span style="background: {verdict_colors.get(p.get("current_verdict","watch"), "#d97706")}; color: white; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; text-transform: uppercase;">{p.get("current_verdict","watch")}</span>
            </td>
            <td style="padding: 8px; color: #6b7280; text-transform: capitalize;">{p.get("lifecycle_phase","early").replace("_"," ")}</td>
          </tr>''' for p in sorted_products)}
        </table>

        <p style="color: #6b7280; font-size: 13px; margin-top: 16px;">
          Apify spend this month: <strong>${total_cost:.2f}</strong> of $25.00 ({total_cost/25*100:.0f}%)
        </p>
      </div>

      {"".join(product_sections)}
      {weekly_section}

      <div style="text-align: center; color: #9ca3af; font-size: 12px; padding: 20px;">
        Product Intelligence System — Automated Report
      </div>
    </body>
    </html>
    """
    return html


def _build_product_section(db, product, run_id):
    pid = product["id"]
    name = product["name"]
    score = product.get("current_score", 0)
    verdict = product.get("current_verdict", "watch")

    # Council verdict
    council = db.table("council_verdicts").select("*") \
        .eq("product_id", pid).order("verdict_date", desc=True).limit(1).execute().data
    cv = council[0] if council else {}

    vote_tally = f"{cv.get('votes_for_buy', 0)}-{cv.get('votes_for_watch', 0)}-{cv.get('votes_for_pass', 0)}"

    # Agent reasoning
    agents_html = ""
    for agent_name, label in [("trend_archaeologist", "Trend Archaeologist"),
                               ("demand_validator", "Demand Validator"),
                               ("supply_analyst", "Supply Analyst"),
                               ("fad_detector", "Fad Detector"),
                               ("category_strategist", "Category Strategist")]:
        vote = cv.get(f"{agent_name}_vote", "—")
        reasoning = cv.get(f"{agent_name}_reasoning", "")
        if vote and vote != "—":
            v_color = "#059669" if vote.lower() == "buy" else "#d97706" if vote.lower() == "watch" else "#dc2626"
            agents_html += f'<div style="margin: 4px 0; font-size: 13px;"><span style="color: {v_color}; font-weight: 600;">{vote}</span> — <strong>{label}</strong>: {reasoning[:120]}</div>'

    # Top 3 Reddit comments
    comments = db.table("comments").select("comment_body, intent_level, is_buy_intent") \
        .eq("product_id", pid).order("intent_level", desc=True).limit(3).execute().data
    comments_html = ""
    for c in comments:
        intent_badge = f'L{c.get("intent_level", 1)}'
        buy_badge = ' <span style="background: #d1fae5; color: #059669; padding: 1px 6px; border-radius: 4px; font-size: 11px;">BUY</span>' if c.get("is_buy_intent") else ""
        comments_html += f'<div style="margin: 6px 0; font-size: 13px; padding: 8px; background: #f9fafb; border-radius: 6px; border-left: 3px solid #6366f1;"><span style="font-weight: 600;">{intent_badge}</span>{buy_badge} {(c.get("comment_body") or "")[:120]}</div>'

    # Score chart (matplotlib)
    chart_img = _generate_score_chart(db, pid)

    verdict_colors = {"buy": "#059669", "watch": "#d97706", "pass": "#dc2626"}
    vc = verdict_colors.get(verdict, "#d97706")

    return f"""
    <div style="background: white; border-radius: 12px; padding: 24px; margin-bottom: 16px; border: 1px solid #e5e7eb;">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
        <h2 style="font-size: 18px; margin: 0;">{name}</h2>
        <div>
          <span style="font-size: 24px; font-weight: 700; margin-right: 8px;">{score:.1f}</span>
          <span style="background: {vc}; color: white; padding: 3px 12px; border-radius: 12px; font-size: 13px; font-weight: 600; text-transform: uppercase;">{verdict}</span>
        </div>
      </div>
      <p style="color: #6b7280; font-size: 13px; margin: 0 0 12px 0;">Council: {vote_tally} (Buy-Watch-Pass) — {cv.get("council_verdict", "pending")}</p>
      {f'<img src="data:image/png;base64,{chart_img}" style="width: 100%; border-radius: 8px; margin-bottom: 12px;" />' if chart_img else ''}
      <div style="margin-bottom: 12px;">{agents_html}</div>
      {f'<h3 style="font-size: 14px; margin: 16px 0 8px 0;">Top Reddit Comments</h3>{comments_html}' if comments_html else ''}
    </div>
    """


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


def _build_weekly_briefing(db, run_id):
    """Monday only: Research Council Weekly Briefing."""
    return """
    <div style="background: white; border-radius: 12px; padding: 24px; margin-bottom: 16px; border: 2px solid #6366f1;">
      <h2 style="font-size: 18px; margin: 0 0 12px 0; color: #6366f1;">Research Council Weekly Briefing</h2>
      <p style="color: #6b7280; font-size: 14px;">
        Weekly agent analysis will appear here once the council has accumulated
        a full week of deliberation data. Check the Research Council tab in the
        dashboard for individual agent reasoning and formula recommendations.
      </p>
    </div>
    """
