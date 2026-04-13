"""
Quarterly Report — 3-month macro analysis emailed Jan 1, Apr 1, Jul 1, Oct 1.
"""
import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def send_quarterly_report(db):
    gmail_addr = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not gmail_addr or not gmail_pass:
        return 0

    recipients = db.table("email_settings").select("email_address") \
        .eq("active", True).eq("receive_quarterly", True).execute().data
    if not recipients:
        return 0

    quarter_start = (date.today() - timedelta(days=90)).isoformat()
    quarter_label = f"Q{(date.today().month - 1) // 3 + 1} {date.today().year}"

    snaps = db.table("product_snapshots").select("*, products(name)") \
        .gte("snapshot_date", quarter_start).order("snapshot_date").execute().data

    weights = db.table("council_weights").select("*").execute().data

    html = f"""
    <html><body style="font-family: -apple-system, system-ui, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #1f2937;">
    <div style="background: white; border-radius: 12px; padding: 24px; border: 1px solid #e5e7eb;">
      <h1 style="font-size: 22px;">Quarterly Report — {quarter_label}</h1>
      <p style="color: #6b7280;">3-month macro trend analysis and formula performance review.</p>
      <h2 style="font-size: 16px;">Data Points This Quarter: {len(snaps)}</h2>
      <h2 style="font-size: 16px;">Council Weight Evolution</h2>
      {"".join(f'<p style="font-size: 14px;"><strong>{w["agent_name"].replace("_"," ").title()}</strong>: weight {w["current_weight"]:.2f} (base {w["base_weight"]:.2f}), accuracy {w["accuracy_rate"]:.0%} over {w["total_decisions"]} decisions</p>' for w in weights)}
    </div>
    <div style="text-align: center; color: #9ca3af; font-size: 12px; padding: 20px;">Product Intelligence System — Quarterly Report</div>
    </body></html>
    """

    sent = 0
    for r in recipients:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"Product Intelligence Quarterly — {quarter_label}"
            msg["From"] = gmail_addr
            msg["To"] = r["email_address"]
            msg.attach(MIMEText(html, "html"))
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(gmail_addr, gmail_pass)
                server.sendmail(gmail_addr, r["email_address"], msg.as_string())
            sent += 1
        except Exception as e:
            logger.error("[quarterly] Failed: %s", e)
    return sent
