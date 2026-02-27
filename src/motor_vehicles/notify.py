"""Slack webhook notifications for pipeline runs.

Sends success/failure messages to a Slack channel via incoming webhook.
Graceful no-op if SLACK_WEBHOOK_URL is not configured.
"""

from __future__ import annotations

import logging
import os
import traceback

import httpx

logger = logging.getLogger("motor_vehicles.notify")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack(message: str) -> bool:
    """Post a message to Slack via incoming webhook.

    Returns True if sent successfully, False otherwise.
    No-op (returns False) if SLACK_WEBHOOK_URL is not set.
    """
    webhook_url = SLACK_WEBHOOK_URL or os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL not set, skipping notification.")
        return False

    try:
        resp = httpx.post(webhook_url, json={"text": message}, timeout=10)
        if resp.status_code != 200:
            logger.warning("Slack webhook returned %d: %s", resp.status_code, resp.text)
            return False
        return True
    except Exception as exc:
        logger.warning("Failed to send Slack notification: %s", exc)
        return False


def notify_update_success(summary_text: str) -> bool:
    """Send a success notification with the update report summary."""
    message = f":white_check_mark: *Motor Vehicles Monthly Update Complete*\n```{summary_text}```"
    return send_slack(message)


def notify_update_failure(error: Exception, step: str = "") -> bool:
    """Send a failure notification with error details."""
    tb = traceback.format_exception_only(type(error), error)
    error_text = "".join(tb).strip()
    if len(error_text) > 500:
        error_text = error_text[:500] + "..."

    lines = [":x: *Motor Vehicles Monthly Update Failed*"]
    if step:
        lines.append(f"*Step:* {step}")
    lines.append(f"```{error_text}```")
    return send_slack("\n".join(lines))
