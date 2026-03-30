"""Slack通知設定のCRUDエンドポイント。"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.notifications import (
    load_notification_settings,
    save_notification_settings,
    send_slack_message,
)

logger = logging.getLogger("ecommerce_data_extractor.notification_settings")

router = APIRouter(prefix="/notification-settings", tags=["notification-settings"])


class NotificationSettingsBody(BaseModel):
    slack_webhook_url: str = ""
    slack_channel: str = ""
    enabled: bool = True


@router.get("/")
async def get_settings() -> dict:
    """現在の通知設定を返す（Webhook URLはマスク表示）。"""
    settings = load_notification_settings()
    webhook = settings.get("slack_webhook_url", "")
    return {
        "slack_webhook_url_preview": f"...{webhook[-12:]}" if len(webhook) > 12 else ("設定済み" if webhook else ""),
        "slack_channel": settings.get("slack_channel", ""),
        "enabled": settings.get("enabled", False),
        "has_webhook": bool(webhook),
    }


@router.post("/")
async def save_settings(body: NotificationSettingsBody) -> dict:
    """通知設定を保存する。"""
    current = load_notification_settings()

    # If webhook URL is empty, keep existing
    if body.slack_webhook_url:
        current["slack_webhook_url"] = body.slack_webhook_url
    current["slack_channel"] = body.slack_channel
    current["enabled"] = body.enabled

    save_notification_settings(current)
    logger.info("通知設定を保存しました (enabled=%s)", body.enabled)
    return {"ok": True}


@router.post("/test")
async def test_notification() -> dict:
    """テスト通知を送信する。"""
    settings = load_notification_settings()
    webhook = settings.get("slack_webhook_url", "")
    if not webhook:
        return {"ok": False, "error": "Slack Webhook URLが設定されていません"}

    ok = await send_slack_message(
        webhook,
        ":white_check_mark: 【テスト通知】\nEC Data Extractorからのテスト通知です。正常に受信できています。",
    )
    return {"ok": ok, "error": None if ok else "送信に失敗しました"}
