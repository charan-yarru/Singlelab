import smtplib
from email.message import EmailMessage
from typing import Iterable, Optional

from config.settings import alert_config


class EmailNotifier:
    """Utility to send alert emails when listeners hit errors."""

    def __init__(self, config: Optional[dict] = None) -> None:
        self._cfg = config or alert_config or {}
        self.enabled = bool(self._cfg.get("enabled"))

    def _get_recipients(self) -> list:
        recips = self._cfg.get("to") or []
        if isinstance(recips, str):
            recips = [recips]
        return [addr for addr in recips if addr]

    def send(self, subject: str, body: str) -> bool:
        if not self.enabled:
            return False
        recipients = self._get_recipients()
        if not recipients:
            return False

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self._cfg.get("from") or "lims-cluster@example.com"
        msg["To"] = ", ".join(recipients)
        msg.set_content(body)

        host = self._cfg.get("smtp_host")
        port = int(self._cfg.get("smtp_port", 587))
        username = self._cfg.get("username")
        password = self._cfg.get("password")
        use_tls = bool(self._cfg.get("use_tls", True))

        try:
            with smtplib.SMTP(host, port, timeout=10) as smtp:
                if use_tls:
                    smtp.starttls()
                if username and password:
                    smtp.login(username, password)
                smtp.send_message(msg)
            return True
        except Exception as exc:
            print(f"[Alerts] Failed to send email: {exc}")
            return False

    def notify_machine_error(self, machine: str, message: str) -> None:
        subject = f"[LIMS Cluster] Issue on {machine}"
        body = message
        self.send(subject, body)
