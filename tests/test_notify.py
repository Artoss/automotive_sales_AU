"""
Tests for Slack notification module.
"""

from __future__ import annotations

from unittest.mock import patch

from motor_vehicles.notify import (
    notify_update_failure,
    notify_update_success,
    send_slack,
)


class TestSendSlack:
    """Tests for the low-level Slack webhook sender."""

    def test_noop_when_no_webhook(self):
        """Returns False and does nothing when SLACK_WEBHOOK_URL is empty."""
        with patch("motor_vehicles.notify.SLACK_WEBHOOK_URL", ""):
            with patch.dict("os.environ", {"SLACK_WEBHOOK_URL": ""}, clear=False):
                result = send_slack("test message")
        assert result is False

    def test_sends_when_webhook_configured(self):
        """Posts to webhook URL and returns True on 200."""
        with patch("motor_vehicles.notify.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"):
            with patch("motor_vehicles.notify.httpx") as mock_httpx:
                mock_httpx.post.return_value.status_code = 200
                result = send_slack("hello")
        assert result is True
        mock_httpx.post.assert_called_once()
        call_args = mock_httpx.post.call_args
        assert call_args[1]["json"] == {"text": "hello"}

    def test_returns_false_on_error(self):
        """Returns False when httpx raises an exception."""
        with patch("motor_vehicles.notify.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"):
            with patch("motor_vehicles.notify.httpx") as mock_httpx:
                mock_httpx.post.side_effect = ConnectionError("timeout")
                result = send_slack("hello")
        assert result is False


class TestNotifyUpdateSuccess:
    """Tests for success notification formatting."""

    def test_formats_summary(self):
        with patch("motor_vehicles.notify.send_slack") as mock_send:
            mock_send.return_value = True
            result = notify_update_success("Pages: 2\nRecords: 100")
        assert result is True
        message = mock_send.call_args[0][0]
        assert "Complete" in message
        assert "Pages: 2" in message


class TestNotifyUpdateFailure:
    """Tests for failure notification formatting."""

    def test_formats_error(self):
        with patch("motor_vehicles.notify.send_slack") as mock_send:
            mock_send.return_value = True
            result = notify_update_failure(RuntimeError("DB down"), step="marklines")
        assert result is True
        message = mock_send.call_args[0][0]
        assert "Failed" in message
        assert "marklines" in message
        assert "DB down" in message

    def test_truncates_long_errors(self):
        with patch("motor_vehicles.notify.send_slack") as mock_send:
            mock_send.return_value = True
            long_error = RuntimeError("x" * 1000)
            notify_update_failure(long_error)
        message = mock_send.call_args[0][0]
        assert "..." in message
