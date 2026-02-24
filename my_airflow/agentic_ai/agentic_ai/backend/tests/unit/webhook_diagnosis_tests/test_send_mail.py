# import pytest
# import smtplib
# from Webhook_diagnose.send_mail import send_email

# class DummySMTP:
#     def __init__(self, server, port):
#         self.server = server
#         self.port = port
#         self.starttls_called = False
#         self.login_called = False
#         self.login_args = None
#         self.sendmail_called = False
#         self.sendmail_args = None
#         self.quit_called = False

#     def starttls(self):
#         self.starttls_called = True

#     def login(self, user, pwd):
#         self.login_called = True
#         self.login_args = (user, pwd)

#     def sendmail(self, sender, recipients, msg_str):
#         self.sendmail_called = True
#         self.sendmail_args = (sender, recipients, msg_str)

#     def quit(self):
#         self.quit_called = True


# def test_send_email_success_with_html(monkeypatch):
#     captured = {}

#     def factory(server, port):
#         inst = DummySMTP(server, port)
#         captured["inst"] = inst
#         return inst

#     monkeypatch.setattr(smtplib, "SMTP", factory)

#     cfg = {
#         "smtp_server": "smtp.example.com",
#         "smtp_port": 587,
#         "sender_email": "me@example.com",
#         "app_password": "secret",
#     }
#     recipients = ["to@example.com"]
#     ok = send_email("Subject", "plain text", "<b>html</b>", cfg, recipients)

#     assert ok is True
#     inst = captured["inst"]
#     assert inst.starttls_called is True
#     assert inst.login_called is True
#     assert inst.login_args == (cfg["sender_email"], cfg["app_password"])
#     assert inst.sendmail_called is True
#     sender, recips, msg = inst.sendmail_args
#     assert sender == cfg["sender_email"]
#     assert recips == recipients
#     assert "text/html" in msg  # HTML part should be present
#     assert inst.quit_called is True


# def test_send_email_success_without_html(monkeypatch):
#     captured = {}

#     def factory(server, port):
#         inst = DummySMTP(server, port)
#         captured["inst"] = inst
#         return inst

#     monkeypatch.setattr(smtplib, "SMTP", factory)

#     cfg = {
#         "smtp_server": "smtp.example.com",
#         "smtp_port": 587,
#         "sender_email": "me@example.com",
#         "app_password": "secret",
#     }
#     recipients = ["to@example.com"]
#     ok = send_email("Subject", "plain text only", None, cfg, recipients)

#     assert ok is True
#     inst = captured["inst"]
#     assert inst.sendmail_called is True
#     _, _, msg = inst.sendmail_args
#     assert "text/html" not in msg  # No HTML part when html_body is None


# def test_send_email_returns_false_when_smtp_constructor_fails(monkeypatch):
#     def raising_factory(server, port):
#         raise RuntimeError("connection failed")

#     monkeypatch.setattr(smtplib, "SMTP", raising_factory)

#     cfg = {
#         "smtp_server": "bad.example.com",
#         "smtp_port": 587,
#         "sender_email": "me@example.com",
#         "app_password": "secret",
#     }
#     recipients = ["to@example.com"]
#     ok = send_email("S", "p", "<h>h</h>", cfg, recipients)
#     assert ok is False


# def test_send_email_returns_false_when_sendmail_fails(monkeypatch):
#     class BadSMTP(DummySMTP):
#         def sendmail(self, sender, recipients, msg_str):
#             raise RuntimeError("send failed")

#     def factory(server, port):
#         return BadSMTP(server, port)

#     monkeypatch.setattr(smtplib, "SMTP", factory)

#     cfg = {
#         "smtp_server": "smtp.example.com",
#         "smtp_port": 587,
#         "sender_email": "me@example.com",
#         "app_password": "secret",
#     }
#     recipients = ["to@example.com"]
#     ok = send_email("Subj", "plain", "<b>h</b>", cfg, recipients)
#     assert ok is False





from databricks_monitoring.webhook_diagnosis.send_mail import send_email

def test_send_email_success(mocker):
    mock_smtp = mocker.patch("smtplib.SMTP", autospec=True)
    instance = mock_smtp.return_value
    instance.sendmail.return_value = {}

    email_config = {
        "sender_email": "test@example.com",
        "app_password": "password",
        "smtp_server": "smtp.test.com",
        "smtp_port": 587,
    }
    result = send_email(
        subject="Test Subject",
        plain_body="Hello",
        html_body=None,
        email_config=email_config,
        recipients=["receiver@example.com"]
    )
    assert result is True
    instance.sendmail.assert_called_once()

def test_send_email_failure(mocker):
    mocker.patch("smtplib.SMTP", side_effect=Exception("SMTP error"))

    email_config = {
        "sender_email": "test@example.com",
        "app_password": "password",
        "smtp_server": "smtp.test.com",
        "smtp_port": 587,
    }
    result = send_email("Test", "Body", None, email_config, ["x@test.com"])
    assert result is False
