"""クレデンシャル有効期限トラッキングとチェックエンドポイント。"""

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.notifications import (
    check_and_send_expiry_notifications,
    load_expiry_records,
    save_expiry_records,
    upsert_expiry_record,
)

logger = logging.getLogger("ecommerce_data_extractor.expiry")

router = APIRouter(prefix="/expiry", tags=["expiry"])

JST = timezone(timedelta(hours=9))


class RakutenExpiryBody(BaseModel):
    expires_at: str  # ISO date string e.g. "2026-06-15"


@router.get("/")
async def list_expiry_records() -> list[dict]:
    """全てのクレデンシャル有効期限レコードを返す。"""
    records = load_expiry_records()
    # Enrich with remaining time
    now = datetime.now(timezone.utc)
    for r in records:
        try:
            exp = datetime.fromisoformat(r["expires_at"])
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=JST)
            remaining = exp.astimezone(timezone.utc) - now
            r["remaining_hours"] = max(0, int(remaining.total_seconds() / 3600))
            r["expired"] = remaining.total_seconds() <= 0
        except (ValueError, TypeError, KeyError):
            r["remaining_hours"] = None
            r["expired"] = None
    return records


@router.post("/rakuten-license-key")
async def set_rakuten_license_expiry(body: RakutenExpiryBody) -> dict:
    """楽天ライセンスキーの有効期限を設定する。"""
    try:
        # Validate date format
        dt = datetime.fromisoformat(body.expires_at)
        if dt.tzinfo is None:
            # Treat as JST midnight
            dt = dt.replace(tzinfo=JST)
        expires_at_iso = dt.isoformat()
    except ValueError:
        raise HTTPException(status_code=400, detail="日付形式が不正です (例: 2026-06-15)")

    record = upsert_expiry_record(
        platform_id="rakuten",
        credential_type="license_key",
        label="楽天 ライセンスキー",
        expires_at=expires_at_iso,
        manually_set=True,
    )
    logger.info("楽天ライセンスキー有効期限を設定: %s", expires_at_iso)
    return {"ok": True, "record": record}


@router.get("/yahoo-oauth")
async def get_yahoo_oauth_expiry() -> dict:
    """Yahoo OAuthの有効期限情報を返す。"""
    records = load_expiry_records()
    for r in records:
        if r["platform_id"] == "yahoo" and r["credential_type"] == "oauth_token":
            try:
                exp = datetime.fromisoformat(r["expires_at"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=JST)
                remaining = exp.astimezone(timezone.utc) - datetime.now(timezone.utc)
                r["remaining_hours"] = max(0, int(remaining.total_seconds() / 3600))
                r["expired"] = remaining.total_seconds() <= 0
            except (ValueError, TypeError):
                pass
            return r
    return {"platform_id": "yahoo", "credential_type": "oauth_token", "expires_at": None}


@router.delete("/{platform_id}/{credential_type}", status_code=204)
async def delete_expiry_record(platform_id: str, credential_type: str) -> None:
    """有効期限トラッキングを削除する。"""
    records = load_expiry_records()
    new_records = [
        r for r in records
        if not (r["platform_id"] == platform_id and r["credential_type"] == credential_type)
    ]
    if len(new_records) == len(records):
        raise HTTPException(status_code=404, detail="レコードが見つかりません")
    save_expiry_records(new_records)


@router.post("/check")
async def check_expiry() -> dict:
    """有効期限をチェックし、必要に応じてSlack通知を送信する。Cloud Schedulerから呼ばれる。"""
    result = await check_and_send_expiry_notifications()
    logger.info("有効期限チェック完了: %s", result)
    return result
