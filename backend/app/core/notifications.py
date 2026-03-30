"""Slack通知とクレデンシャル有効期限チェック。"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from app.core.storage import is_cloud_mode, load_gcs_json, save_gcs_json

logger = logging.getLogger("ecommerce_data_extractor.notifications")

_LOCAL_DIR = Path(__file__).resolve().parent.parent.parent
_GCS_EXPIRY_BLOB = "credential_expiry.json"
_GCS_NOTIF_BLOB = "notification_settings.json"

JST = timezone(timedelta(hours=9))

# Notification thresholds: label → timedelta before expiry
THRESHOLDS = [
    ("7d", timedelta(days=7)),
    ("3d", timedelta(days=3)),
    ("1d", timedelta(days=1)),
    ("6h", timedelta(hours=6)),
]

THRESHOLD_LABELS = {
    "7d": "1週間後",
    "3d": "3日後",
    "1d": "明日",
    "6h": "6時間後",
}


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def load_expiry_records() -> list[dict]:
    if is_cloud_mode():
        data = load_gcs_json(_GCS_EXPIRY_BLOB)
        return data if isinstance(data, list) else []
    path = _LOCAL_DIR / "credential_expiry.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def save_expiry_records(records: list[dict]) -> None:
    if is_cloud_mode():
        save_gcs_json(_GCS_EXPIRY_BLOB, records)
    else:
        path = _LOCAL_DIR / "credential_expiry.json"
        path.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")


def load_notification_settings() -> dict:
    if is_cloud_mode():
        data = load_gcs_json(_GCS_NOTIF_BLOB)
        return data if isinstance(data, dict) else {}
    path = _LOCAL_DIR / "notification_settings.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_notification_settings(settings: dict) -> None:
    if is_cloud_mode():
        save_gcs_json(_GCS_NOTIF_BLOB, settings)
    else:
        path = _LOCAL_DIR / "notification_settings.json"
        path.write_text(json.dumps(settings, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Expiry record helpers
# ---------------------------------------------------------------------------

def upsert_expiry_record(
    platform_id: str,
    credential_type: str,
    label: str,
    expires_at: str,
    *,
    manually_set: bool = True,
) -> dict:
    """Add or update an expiry record. Resets sent_notifications if date changes."""
    records = load_expiry_records()
    existing = None
    for r in records:
        if r["platform_id"] == platform_id and r["credential_type"] == credential_type:
            existing = r
            break

    if existing:
        if existing.get("expires_at") != expires_at:
            existing["sent_notifications"] = {}
        existing["expires_at"] = expires_at
        existing["label"] = label
        existing["manually_set"] = manually_set
    else:
        record = {
            "platform_id": platform_id,
            "credential_type": credential_type,
            "label": label,
            "expires_at": expires_at,
            "manually_set": manually_set,
            "sent_notifications": {},
        }
        records.append(record)
        existing = record

    save_expiry_records(records)
    return existing


# ---------------------------------------------------------------------------
# Slack sender
# ---------------------------------------------------------------------------

async def send_slack_message(webhook_url: str, message: str) -> bool:
    """Send a message to Slack via incoming webhook."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                webhook_url,
                json={"text": message},
                timeout=10,
            )
            if resp.status_code == 200:
                return True
            logger.error("Slack送信失敗: %d %s", resp.status_code, resp.text)
            return False
    except Exception as e:
        logger.error("Slack送信エラー: %s", e)
        return False


# ---------------------------------------------------------------------------
# Check & notify
# ---------------------------------------------------------------------------

async def check_and_send_expiry_notifications() -> dict:
    """Check all expiry records and send staged Slack notifications."""
    settings = load_notification_settings()
    webhook_url = settings.get("slack_webhook_url", "")
    enabled = settings.get("enabled", False)

    if not enabled or not webhook_url:
        return {"skipped": True, "reason": "Slack通知が無効またはWebhook URLが未設定"}

    records = load_expiry_records()
    now = datetime.now(timezone.utc)
    sent_count = 0
    results = []

    for record in records:
        expires_at_str = record.get("expires_at", "")
        if not expires_at_str:
            continue

        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=JST)
            expires_at_utc = expires_at.astimezone(timezone.utc)
        except (ValueError, TypeError):
            continue

        remaining = expires_at_utc - now
        sent_notifs = record.get("sent_notifications", {})

        for label, threshold in THRESHOLDS:
            if label in sent_notifs:
                continue  # Already sent
            if remaining <= threshold:
                # Send notification
                desc = THRESHOLD_LABELS[label]
                expire_display = expires_at.astimezone(JST).strftime("%Y/%m/%d %H:%M")
                remaining_display = _format_remaining(remaining)

                message = (
                    f":warning: 【認証情報の有効期限通知】\n"
                    f"*{record['label']}* の有効期限が{desc}に迫っています。\n\n"
                    f"有効期限: {expire_display} (JST)\n"
                    f"残り: {remaining_display}\n\n"
                    f"早めに更新してください。"
                )

                ok = await send_slack_message(webhook_url, message)
                if ok:
                    sent_notifs[label] = now.isoformat()
                    sent_count += 1
                    results.append(f"{record['label']}: {label} 通知送信")
                else:
                    results.append(f"{record['label']}: {label} 送信失敗")

        record["sent_notifications"] = sent_notifs

    save_expiry_records(records)
    return {"sent": sent_count, "details": results}


def _format_remaining(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    if total_seconds <= 0:
        return "期限切れ"
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    if days > 0:
        return f"{days}日{hours}時間"
    return f"{hours}時間"
