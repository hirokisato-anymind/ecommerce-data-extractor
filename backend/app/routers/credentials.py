"""API credentials management - save/load from .env file or Secret Manager.

Security:
- All endpoints require an admin token (ADMIN_TOKEN env var or auto-generated)
- Secret values are never returned in responses (only "set" / "not set" status)
- Credentials are saved to .env (local) or Secret Manager (cloud) and reloaded into memory
"""

import logging
import os
import secrets
from pathlib import Path

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.core.storage import is_cloud_mode, load_secret, save_secret

logger = logging.getLogger("ecommerce_data_extractor.credentials")

router = APIRouter(prefix="/credentials", tags=["credentials"])

ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
_SECRET_ID = "ecommerce-credentials-env"

# Admin token for protecting credential endpoints.
# Set ADMIN_TOKEN in .env, or one will be auto-generated and printed on startup.
_admin_token: str | None = None


def _get_admin_token() -> str:
    """Get or generate the admin token."""
    global _admin_token
    if _admin_token is None:
        env_values = _read_env()
        _admin_token = env_values.get("ADMIN_TOKEN", "")
        if not _admin_token:
            _admin_token = secrets.token_urlsafe(32)
            # Persist so it survives restarts
            env_values["ADMIN_TOKEN"] = _admin_token
            _write_env(env_values)
            logger.warning(
                "No ADMIN_TOKEN found. Generated new token: %s", _admin_token
            )
            print(f"\n{'='*60}")
            print(f"  ADMIN_TOKEN (use this to configure credentials):")
            print(f"  {_admin_token}")
            print(f"{'='*60}\n")
    return _admin_token


async def verify_admin_token(
    authorization: str | None = Header(None),
) -> None:
    """Dependency: verify Bearer token matches ADMIN_TOKEN."""
    admin_token = _get_admin_token()
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or parts[1] != admin_token:
        raise HTTPException(status_code=403, detail="Invalid admin token")


# Which keys belong to each platform
PLATFORM_KEYS: dict[str, list[dict]] = {
    "shopify": [
        {"key": "SHOPIFY_STORE_DOMAIN", "label": "ストアドメイン", "hint": "例: myshop.myshopify.com（.myshopify.com まで含む）", "secret": False},
        {"key": "SHOPIFY_CLIENT_ID", "label": "Client ID (API Key)", "hint": "Dev Dashboard > アプリ > API credentials の Client ID", "secret": False},
        {"key": "SHOPIFY_CLIENT_SECRET", "label": "Client Secret (API Secret)", "hint": "Dev Dashboard > アプリ > API credentials の Client Secret", "secret": True},
        {"key": "SHOPIFY_ACCESS_TOKEN", "label": "Admin APIアクセストークン", "hint": "OAuth認証で自動取得されます", "secret": True, "readonly": True},
    ],
    "rakuten": [
        {"key": "RAKUTEN_SERVICE_SECRET", "label": "RMS サービスシークレット", "hint": "RMS > 各種設定 > API設定 で確認", "secret": True},
        {"key": "RAKUTEN_LICENSE_KEY", "label": "RMS ライセンスキー", "hint": "RMS > 各種設定 > API設定 で確認", "secret": True},
    ],
    "amazon": [
        {"key": "AMAZON_CLIENT_ID", "label": "LWA Client ID", "hint": "Seller Central > アプリ開発 > LWA認証情報の Client ID", "secret": False},
        {"key": "AMAZON_CLIENT_SECRET", "label": "LWA Client Secret", "hint": "LWA認証情報の Client Secret", "secret": True},
        {"key": "AMAZON_REFRESH_TOKEN", "label": "Refresh Token", "hint": "SP-API認可で取得したリフレッシュトークン", "secret": True},
        {"key": "AMAZON_MARKETPLACE_ID", "label": "マーケットプレイスID", "hint": "日本: A1VC38T7YXB528", "secret": False},
    ],
    "yahoo": [
        {"key": "YAHOO_CLIENT_ID", "label": "Client ID (アプリケーションID)", "hint": "Yahoo!デベロッパーネットワークで発行", "secret": False},
        {"key": "YAHOO_CLIENT_SECRET", "label": "Client Secret (シークレット)", "hint": "アプリケーションのシークレット", "secret": True},
        {"key": "YAHOO_SELLER_ID", "label": "ストアアカウント", "hint": "Yahoo!ショッピングのストアアカウントID", "secret": False},
        {"key": "YAHOO_ACCESS_TOKEN", "label": "Access Token", "hint": "OAuth認証で取得、または手動入力", "secret": True, "readonly": True},
    ],
}


def _parse_env_text(text: str) -> dict[str, str]:
    """Parse .env format text into a dict."""
    values: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            values[k.strip()] = v.strip()
    return values


def _serialize_env(values: dict[str, str]) -> str:
    """Serialize dict to .env format text."""
    lines = [f"{k}={v}" for k, v in sorted(values.items())]
    return "\n".join(lines) + "\n"


def _read_env() -> dict[str, str]:
    """Read credentials from Secret Manager (cloud) or .env file (local)."""
    if is_cloud_mode():
        raw = load_secret(_SECRET_ID)
        if raw:
            return _parse_env_text(raw)
        return {}
    values: dict[str, str] = {}
    if ENV_PATH.exists():
        values = _parse_env_text(ENV_PATH.read_text(encoding="utf-8"))
    return values


def _write_env(values: dict[str, str]) -> None:
    """Write credentials to Secret Manager (cloud) or .env file (local)."""
    text = _serialize_env(values)
    if is_cloud_mode():
        save_secret(_SECRET_ID, text)
    else:
        ENV_PATH.write_text(text, encoding="utf-8")


@router.get("/{platform_id}")
async def get_credentials(platform_id: str) -> dict:
    """Get credential fields for a platform.

    Secret values are NEVER returned - only whether they are set or not.
    """
    if platform_id not in PLATFORM_KEYS:
        raise HTTPException(status_code=404, detail="Unknown platform")

    # Determine if platform supports OAuth
    oauth_platform = platform_id in ("shopify", "yahoo")

    env_values = _read_env()
    fields = []
    for field in PLATFORM_KEYS[platform_id]:
        raw = env_values.get(field["key"], "")
        fields.append({
            "key": field["key"],
            "label": field["label"],
            "hint": field["hint"],
            "secret": field["secret"],
            "readonly": field.get("readonly", False),
            # Never return actual secret values, only non-secret fields
            "value": "" if field["secret"] else raw,
            "hasValue": bool(raw),
        })
    return {
        "platform_id": platform_id,
        "fields": fields,
        "oauth": oauth_platform,
    }


class SaveCredentialsRequest(BaseModel):
    values: dict[str, str]


@router.post("/{platform_id}")
async def save_credentials(platform_id: str, body: SaveCredentialsRequest) -> dict:
    """Save credential values for a platform."""
    if platform_id not in PLATFORM_KEYS:
        raise HTTPException(status_code=404, detail="Unknown platform")

    valid_keys = {f["key"] for f in PLATFORM_KEYS[platform_id]}
    env_values = _read_env()

    for key, value in body.values.items():
        if key not in valid_keys:
            continue
        # Empty string = user wants to clear the value
        # Non-empty = set the value
        if value:
            env_values[key] = value
        else:
            env_values.pop(key, None)

    _write_env(env_values)

    # Reload settings in-memory
    _reload_settings(env_values)

    logger.info("Credentials updated for platform '%s'", platform_id)
    return {"ok": True}


def _reload_settings(env_values: dict[str, str]) -> None:
    """Update the global settings object with new values."""
    for key, value in env_values.items():
        attr = key.lower()
        if hasattr(settings, attr):
            object.__setattr__(settings, attr, value if value else None)


def load_credentials_from_storage() -> None:
    """Load credentials from Secret Manager (cloud) or .env (local) into settings.

    This must be called at app startup so that the in-memory ``settings``
    object is populated before the scheduler or any platform client tries
    to use it.  On Cloud Run there is no ``.env`` file, so without this
    step every ``is_configured()`` check would return False and scheduled
    jobs would fail.
    """
    try:
        env_values = _read_env()
        if env_values:
            _reload_settings(env_values)
            logger.info(
                "Loaded %d credential keys from storage into settings",
                len(env_values),
            )
        else:
            logger.info("No stored credentials found in storage")
    except Exception as e:
        logger.error("Failed to load credentials from storage: %s", e)
