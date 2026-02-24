# email_utils.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict
from datetime import datetime


def send_email(
    subject: str,
    plain_body: str,
    html_body: str,
    email_config: Dict[str, str],
    recipients: List[str]
) -> bool:
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = email_config["sender_email"]
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject

        # Attach plain text part
        msg.attach(MIMEText(plain_body, "plain"))

        # Attach HTML part only if provided
        if html_body is not None:
            msg.attach(MIMEText(html_body, "html"))

        server = smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"])
        server.starttls()
        server.login(email_config["sender_email"], email_config["app_password"])
        server.sendmail(email_config["sender_email"], recipients, msg.as_string())
        server.quit()

        print(f"✅ Email sent successfully to {', '.join(recipients)}")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {str(e)}")
        return False
