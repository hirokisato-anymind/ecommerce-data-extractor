"""OAuth callback handlers for platforms that require OAuth authorization."""

import logging

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from app.config import settings
from app.routers.credentials import _read_env, _write_env, _reload_settings

logger = logging.getLogger("ecommerce_data_extractor.oauth")

router = APIRouter(prefix="/oauth", tags=["oauth"])

SHOPIFY_SCOPES = ",".join([
    "read_products",
    "read_orders",
    "read_customers",
    "read_inventory",
])


def _update_setting(key: str, value: str) -> None:
    attr = key.lower()
    if hasattr(settings, attr):
        object.__setattr__(settings, attr, value)


# ── Shopify OAuth ──────────────────────────────────────────────────────────

@router.get("/shopify/authorize")
async def shopify_authorize(request: Request) -> dict:
    """Generate Shopify OAuth authorization URL.

    The user should open this URL in their browser to authorize the app.
    """
    store = settings.shopify_store_domain
    client_id = settings.shopify_client_id
    if not store or not client_id:
        raise HTTPException(
            status_code=400,
            detail="shopify_store_domain and shopify_client_id must be configured first",
        )

    base = str(request.base_url).rstrip("/")
    proto = request.headers.get("x-forwarded-proto", "")
    if proto == "https" and base.startswith("http://"):
        base = "https://" + base[len("http://"):]
    redirect_uri = base + "/api/oauth/shopify/callback"
    auth_url = (
        f"https://{store}/admin/oauth/authorize"
        f"?client_id={client_id}"
        f"&scope={SHOPIFY_SCOPES}"
        f"&redirect_uri={redirect_uri}"
    )
    return {"authorize_url": auth_url, "redirect_uri": redirect_uri}


@router.get("/shopify/callback", response_class=HTMLResponse)
async def shopify_callback(
    code: str = Query(...),
    shop: str = Query(...),
    hmac: str = Query(None),
) -> HTMLResponse:
    """Handle Shopify OAuth callback - exchange code for access token."""
    client_id = settings.shopify_client_id
    client_secret = settings.shopify_client_secret
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Shopify client credentials not configured")

    # Exchange authorization code for permanent access token
    token_url = f"https://{shop}/admin/oauth/access_token"
    async with httpx.AsyncClient() as client:
        resp = await client.post(token_url, json={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
        })

    if resp.status_code != 200:
        logger.error("Shopify token exchange failed: %d %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail="Failed to obtain access token from Shopify")

    data = resp.json()
    access_token = data.get("access_token", "")

    # Save to Secret Manager (cloud) or .env (local) and reload in-memory
    try:
        env_values = _read_env()
        env_values["SHOPIFY_ACCESS_TOKEN"] = access_token
        env_values["SHOPIFY_STORE_DOMAIN"] = shop
        _write_env(env_values)
        _reload_settings(env_values)
        logger.info("Shopify OAuth completed for shop: %s", shop)
    except Exception as e:
        logger.exception("Failed to save Shopify OAuth token: %s", e)
        raise HTTPException(status_code=500, detail=f"トークンの保存に失敗しました: {e}")

    return HTMLResponse(content="""
    <html><body style="font-family:sans-serif;text-align:center;padding:60px">
        <h2>Shopify 認証完了</h2>
        <p>アクセストークンを取得・保存しました。このタブを閉じてアプリに戻ってください。</p>
        <script>window.close()</script>
    </body></html>
    """)


# ── Yahoo Shopping OAuth ───────────────────────────────────────────────────

YAHOO_AUTH_URL = "https://auth.login.yahoo.co.jp/yconnect/v2/authorization"
YAHOO_TOKEN_URL = "https://auth.login.yahoo.co.jp/yconnect/v2/token"


@router.get("/yahoo/authorize")
async def yahoo_authorize(request: Request) -> dict:
    """Generate Yahoo OAuth authorization URL."""
    client_id = settings.yahoo_client_id
    if not client_id:
        raise HTTPException(status_code=400, detail="yahoo_client_id must be configured first")

    base = str(request.base_url).rstrip("/")
    proto = request.headers.get("x-forwarded-proto", "")
    if proto == "https" and base.startswith("http://"):
        base = "https://" + base[len("http://"):]
    redirect_uri = base + "/api/oauth/yahoo/callback"
    # Yahoo Shopping seller API access is granted at the app level in Yahoo
    # Developer Console, not via OAuth scopes. Only openid is needed here.
    auth_url = (
        f"{YAHOO_AUTH_URL}"
        f"?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&scope=openid"
    )
    return {"authorize_url": auth_url, "redirect_uri": redirect_uri}


@router.get("/yahoo/callback", response_class=HTMLResponse)
async def yahoo_callback(
    request: Request,
    code: str = Query(None),
    error: str = Query(None),
    error_description: str = Query(None),
) -> HTMLResponse:
    """Handle Yahoo OAuth callback - exchange code for access token."""
    if error or not code:
        detail = error_description or error or "認証が拒否されました"
        logger.error("Yahoo OAuth error: %s - %s", error, error_description)
        return HTMLResponse(
            status_code=400,
            content=f"""
            <html><body style="font-family:sans-serif;text-align:center;padding:60px">
                <h2>Yahoo!ショッピング 認証エラー</h2>
                <p style="color:red">{detail}</p>
                <p>アプリの設定を確認して再度お試しください。</p>
            </body></html>
            """,
        )
    client_id = settings.yahoo_client_id
    client_secret = settings.yahoo_client_secret
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Yahoo client credentials not configured")

    base = str(request.base_url).rstrip("/")
    proto = request.headers.get("x-forwarded-proto", "")
    if proto == "https" and base.startswith("http://"):
        base = "https://" + base[len("http://"):]
    redirect_uri = base + "/api/oauth/yahoo/callback"
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            YAHOO_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )

    if resp.status_code != 200:
        logger.error("Yahoo token exchange failed: %d %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail="Failed to obtain access token from Yahoo")

    data = resp.json()
    access_token = data.get("access_token", "")
    refresh_token = data.get("refresh_token", "")

    env_values = _read_env()
    env_values["YAHOO_ACCESS_TOKEN"] = access_token
    if refresh_token:
        env_values["YAHOO_REFRESH_TOKEN"] = refresh_token
    _write_env(env_values)
    _update_setting("YAHOO_ACCESS_TOKEN", access_token)
    if refresh_token:
        _update_setting("YAHOO_REFRESH_TOKEN", refresh_token)

    logger.info("Yahoo OAuth completed")

    # Track OAuth token expiry (refresh_token valid for 28 days)
    try:
        from datetime import datetime, timedelta, timezone as tz
        from app.core.notifications import upsert_expiry_record
        expires_at = (datetime.now(tz.utc) + timedelta(days=28)).isoformat()
        upsert_expiry_record(
            platform_id="yahoo",
            credential_type="oauth_token",
            label="Yahoo OAuth トークン",
            expires_at=expires_at,
            manually_set=False,
        )
    except Exception as e:
        logger.warning("Yahoo OAuth有効期限の記録に失敗: %s", e)

    return HTMLResponse(content="""
    <html><body style="font-family:sans-serif;text-align:center;padding:60px">
        <h2>Yahoo!ショッピング 認証完了</h2>
        <p>アクセストークンを取得・保存しました。このタブを閉じてアプリに戻ってください。</p>
        <script>window.close()</script>
    </body></html>
    """)
