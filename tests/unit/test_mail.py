"""Tests for run-manager email notifications."""

from unittest.mock import Mock

from pytest_mock import MockerFixture

from cybershake.mail import Mailer


def test_mailer_sends_to_all_recipients(mocker: MockerFixture) -> None:
    smtp = Mock()
    smtp_type = mocker.patch("cybershake.mail.smtplib.SMTP")
    smtp_type.return_value.__enter__.return_value = smtp
    mailer = Mailer(sender="runs@example.org", hosts=("smtp.example.org",))

    sent = mailer.send(("a@example.org", "b@example.org"), "Status", "Body")

    assert sent is True
    message = smtp.send_message.call_args.args[0]
    assert message["From"] == "runs@example.org"
    assert message["To"] == "a@example.org, b@example.org"
    assert message["Subject"] == "Status"
    assert "Body" in message.get_content()


def test_mailer_falls_back_to_next_smtp_host(mocker: MockerFixture) -> None:
    smtp = Mock()
    smtp_type = mocker.patch(
        "cybershake.mail.smtplib.SMTP",
        side_effect=[OSError("offline"), smtp],
    )
    smtp.__enter__ = Mock(return_value=smtp)
    smtp.__exit__ = Mock(return_value=False)
    mailer = Mailer(sender="runs@example.org", hosts=("first", "second"))

    assert mailer.send(("a@example.org",), "Status", "Body") is True
    assert [item.args[0] for item in smtp_type.call_args_list] == ["first", "second"]


def test_mailer_reports_exhausted_smtp_hosts(mocker: MockerFixture) -> None:
    mocker.patch("cybershake.mail.smtplib.SMTP", side_effect=OSError("offline"))
    mailer = Mailer(sender="runs@example.org", hosts=("first", "second"))

    assert mailer.send(("a@example.org",), "Status", "Body") is False
