"""Email delivery for periodic run-manager notifications."""

from __future__ import annotations

import getpass
import smtplib
import socket
from email.message import EmailMessage
from typing import TYPE_CHECKING

from cybershake.constants import RUN_MANAGER_STATUS_URL

if TYPE_CHECKING:
    from collections.abc import Sequence


class Mailer:
    """Send run-manager messages through the available local SMTP relays."""

    def __init__(
        self,
        *,
        sender: str | None = None,
        hosts: Sequence[str] | None = None,
    ) -> None:
        """Initialize the sender and ordered SMTP relay list.

        Args:
            sender (str | None, optional): Envelope and message sender. Defaults
                to the current user at the local fully qualified hostname.
            hosts (Sequence[str] | None, optional): SMTP relays to try in order.
                Defaults to the legacy local and domain-derived relay names.
        """
        domain = socket.getfqdn()
        self.sender = sender or f"{getpass.getuser()}@{domain}"
        if hosts is None:
            suffix = domain.partition(".")[2]
            candidates = ["localhost"]
            if suffix:
                candidates.append(f"smtp.{suffix}")
            candidates.append(f"smtp.{domain}")
            self.hosts = tuple(dict.fromkeys(candidates))
        else:
            self.hosts = tuple(hosts)

    def send(
        self,
        recipients: Sequence[str],
        subject: str,
        body: str,
    ) -> bool:
        """Send a notification, falling back across configured relays.

        Args:
            recipients (Sequence[str]): Destination email addresses.
            subject (str): Message subject.
            body (str): Plain-text message body.

        Returns:
            bool: ``True`` after a relay accepts the message, otherwise ``False``.
        """
        message = EmailMessage()
        message["From"] = self.sender
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject
        message.set_content(
            f"{body}\n\n"
            "------------------------------------------------------------\n"
            "Automated message from CyberShake RunManager\n"
            f"{RUN_MANAGER_STATUS_URL}\n"
        )

        for host in self.hosts:
            try:
                with smtplib.SMTP(host) as server:
                    server.send_message(message)
            except (OSError, smtplib.SMTPException):
                continue
            return True
        return False
