"""Google認証エンドポイント（ユーザーログイン用）。"""

import logging
import os
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import httpx
import jwt
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.routers.bigquery import _get_google_oauth_config, _get_base_url

logger = logging.getLogger("ecommerce_data_extractor.auth_router")

router = APIRouter(prefix="/auth", tags=["auth"])

_SCOPES = "openid email profile"
_COOKIE_NAME = "session_token"
_COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 days in seconds


def _get_jwt_secret() -> str:
    """JWT署名用のシークレットキーを取得する。"""
    secret = os.getenv("JWT_SECRET")
    if secret:
        return secret
    from app.config import settings
    if settings.admin_token:
        return settings.admin_token
    raise RuntimeError("JWT_SECRET or ADMIN_TOKEN must be set")


def _is_secure(request: Request) -> bool:
    """リクエストがHTTPS経由かどうかを判定する。"""
    proto = request.headers.get("x-forwarded-proto", "")
    if proto == "https":
        return True
    return str(request.base_url).startswith("https://")


def get_current_user(request: Request) -> dict:
    """session_token cookieからJWTを検証し、ユーザー情報を返す。

    他のルーターからDependencyとして使用可能。
    例: user = Depends(get_current_user)
    """
    token = request.cookies.get(_COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="認証が必要です")
    try:
        payload = jwt.decode(token, _get_jwt_secret(), algorithms=["HS256"])
        return {
            "email": payload.get("email"),
            "name": payload.get("name"),
            "picture": payload.get("picture"),
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="セッションが期限切れです。再ログインしてください。")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="無効なセッションです")


@router.get("/login")
async def login(request: Request) -> dict:
    """Google OAuth認証URLを返す。"""
    oauth_config = _get_google_oauth_config()
    if not oauth_config.get("client_id") or not oauth_config.get("client_secret"):
        raise HTTPException(
            status_code=500,
            detail="Google OAuthクライアント設定が構成されていません",
        )

    base_url = _get_base_url(request)
    redirect_uri = f"{base_url}/api/auth/callback"

    params = {
        "client_id": oauth_config["client_id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": _SCOPES,
        "access_type": "online",
        "prompt": "select_account",
    }
    authorize_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    return {"authorize_url": authorize_url}


@router.get("/callback", response_class=HTMLResponse)
async def callback(request: Request, code: str | None = None, error: str | None = None):
    """Google OAuthコールバック。認証コードをトークンに交換し、JWTセッションCookieを設定する。"""
    if error:
        return HTMLResponse(
            f"<html><body><p>認証エラー: {error}</p>"
            f'<a href="/">トップに戻る</a></body></html>',
            status_code=400,
        )

    if not code:
        raise HTTPException(status_code=400, detail="認証コードがありません")

    oauth_config = _get_google_oauth_config()
    base_url = _get_base_url(request)
    redirect_uri = f"{base_url}/api/auth/callback"

    # 認証コードをトークンに交換
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": oauth_config["client_id"],
                "client_secret": oauth_config["client_secret"],
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )

    if token_resp.status_code != 200:
        logger.error("トークン交換失敗: %s", token_resp.text)
        return HTMLResponse(
            "<html><body><p>トークン交換に失敗しました。</p>"
            '<a href="/">トップに戻る</a></body></html>',
            status_code=500,
        )

    token_data = token_resp.json()

    # ユーザー情報を取得
    async with httpx.AsyncClient() as client:
        userinfo_resp = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {token_data['access_token']}"},
        )

    if userinfo_resp.status_code != 200:
        logger.error("ユーザー情報取得失敗: %s", userinfo_resp.text)
        return HTMLResponse(
            "<html><body><p>ユーザー情報の取得に失敗しました。</p>"
            '<a href="/">トップに戻る</a></body></html>',
            status_code=500,
        )

    userinfo = userinfo_resp.json()
    email = userinfo.get("email", "")
    name = userinfo.get("name", "")
    picture = userinfo.get("picture", "")

    # JWT作成
    payload = {
        "email": email,
        "name": name,
        "picture": picture,
        "exp": datetime.now(timezone.utc) + timedelta(days=30),
    }
    session_token = jwt.encode(payload, _get_jwt_secret(), algorithm="HS256")

    # フロントエンドのコールバックにトークンを渡してリダイレクト
    # フロントエンド側で cookie を設定する（同一ドメインで cookie を管理するため）
    frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
    redirect_url = f"{frontend_url}/api/auth/callback?token={session_token}"
    return HTMLResponse(
        content=f'<html><body><p>ログイン成功。リダイレクト中...</p>'
        f'<script>window.location.href = "{redirect_url}";</script>'
        f'</body></html>'
    )


@router.get("/me")
async def me(request: Request) -> dict:
    """現在のログインユーザー情報を返す。"""
    user = get_current_user(request)
    return user


@router.post("/logout")
async def logout() -> JSONResponse:
    """セッションCookieを削除してログアウトする。"""
    response = JSONResponse(content={"ok": True, "message": "ログアウトしました"})
    response.delete_cookie(
        key=_COOKIE_NAME,
        path="/",
        httponly=True,
        samesite="lax",
    )
    return response
