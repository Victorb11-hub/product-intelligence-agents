"""
Monthly Report — Full month analysis emailed on the 1st of each month.
"""
import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def send_monthly_report(db):
    gmail_addr = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not gmail_addr or not gmail_pass:
        return 0

    recipients = db.table("email_settings").select("email_address") \
        .eq("active", True).eq("receive_monthly", True).execute().data
    if not recipients:
        return 0

    last_month = (date.today().replace(day=1) - timedelta(days=1))
    month_name = last_month.strftime("%B %Y")
    month_start = last_month.replace(day=1).isoformat()
    month_end = date.today().replace(day=1).isoformat()

    # Get all snapshots for the month
    snaps = db.table("product_snapshots").select("*, products(name)") \
        .gte("snapshot_date", month_start).lt("snapshot_date", month_end) \
        .order("snapshot_date").execute().data

    # Council accuracy
    weights = db.table("council_weights").select("agent_name, accuracy_rate, total_decisions, current_weight") \
        .execute().data

    html = f"""
    <html><body style="font-family: -apple-system, system-ui, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #1f2937;">
    <div style="background: white; border-radius: 12px; padding: 24px; border: 1px solid #e5e7eb;">
      <h1 style="font-size: 22px;">Monthly Report — {month_name}</h1>
      <p style="color: #6b7280;">Full month analysis across all tracked products.</p>
      <h2 style="font-size: 16px;">Snapshots This Month: {len(snaps)}</h2>
      <h2 style="font-size: 16px;">Council Agent Performance</h2>
      <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
        <tr style="border-bottom: 1px solid #e5e7eb;">
          <th style="text-align: left; padding: 8px;">Agent</th>
          <th style="text-align: right; padding: 8px;">Weight</th>
          <th style="text-align: right; padding: 8px;">Accuracy</th>
          <th style="text-align: right; padding: 8px;">Decisions</th>
        </tr>
        {"".join(f'<tr style="border-bottom: 1px solid #f3f4f6;"><td style="padding: 8px;">{w["agent_name"].replace("_"," ").title()}</td><td style="padding: 8px; text-align: right;">{w["current_weight"]:.2f}</td><td style="padding: 8px; text-align: right;">{w["accuracy_rate"]:.0%}</td><td style="padding: 8px; text-align: right;">{w["total_decisions"]}</td></tr>' for w in weights)}
      </table>
    </div>
    <div style="text-align: center; color: #9ca3af; font-size: 12px; padding: 20px;">Product Intelligence System — Monthly Report</div>
    </body></html>
    """

    sent = 0
    for r in recipients:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"Product Intelligence Monthly — {month_name}"
            msg["From"] = gmail_addr
            msg["To"] = r["email_address"]
            msg.attach(MIMEText(html, "html"))
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(gmail_addr, gmail_pass)
                server.sendmail(gmail_addr, r["email_address"], msg.as_string())
            sent += 1
        except Exception as e:
            logger.error("[monthly] Failed: %s", e)
    return sent
