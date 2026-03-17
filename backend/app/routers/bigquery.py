"""BigQuery接続・Google OAuth認証エンドポイント。"""

import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.core.storage import is_cloud_mode, load_secret_json, save_secret_json

logger = logging.getLogger("ecommerce_data_extractor.bigquery_router")

router = APIRouter(prefix="/bigquery", tags=["bigquery"])

# Google OAuth設定
TOKENS_FILE = Path(__file__).resolve().parent.parent.parent / "google_tokens.json"
OAUTH_CONFIG_FILE = Path(__file__).resolve().parent.parent.parent / "google_oauth_config.json"
_SM_OAUTH_CONFIG = "ecommerce-bq-oauth-config"
_SM_TOKENS = "ecommerce-bq-tokens"
SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/userinfo.email",
]


def _get_google_oauth_config() -> dict:
    """Google OAuth クライアント設定を取得する（Secret Manager → JSONファイル → 環境変数の順で参照）。"""
    import os
    # Cloud: Secret Manager
    if is_cloud_mode():
        config = load_secret_json(_SM_OAUTH_CONFIG)
        if config and config.get("client_id") and config.get("client_secret"):
            return config
    # Local: JSONファイル
    if OAUTH_CONFIG_FILE.exists():
        try:
            config = json.loads(OAUTH_CONFIG_FILE.read_text(encoding="utf-8"))
            if config.get("client_id") and config.get("client_secret"):
                return config
        except (json.JSONDecodeError, OSError):
            pass
    # フォールバック: 環境変数
    client_id = os.getenv("GOOGLE_CLIENT_ID", "")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "")
    return {"client_id": client_id, "client_secret": client_secret}


def _save_oauth_config(client_id: str, client_secret: str) -> None:
    """Google OAuthクライアント設定を保存する。"""
    import os
    data = {"client_id": client_id, "client_secret": client_secret}
    if is_cloud_mode():
        save_secret_json(_SM_OAUTH_CONFIG, data)
    else:
        OAUTH_CONFIG_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.environ["GOOGLE_CLIENT_ID"] = client_id
    os.environ["GOOGLE_CLIENT_SECRET"] = client_secret


def _save_tokens(tokens: dict) -> None:
    """OAuthトークンを保存する。"""
    if is_cloud_mode():
        save_secret_json(_SM_TOKENS, tokens)
    else:
        TOKENS_FILE.write_text(json.dumps(tokens, indent=2), encoding="utf-8")


def _load_tokens() -> dict | None:
    """保存済みOAuthトークンを読み込む。"""
    if is_cloud_mode():
        return load_secret_json(_SM_TOKENS)
    if not TOKENS_FILE.exists():
        return None
    try:
        return json.loads(TOKENS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _get_credentials():
    """保存済みトークンからGoogle認証情報を取得する。"""
    from google.oauth2.credentials import Credentials

    tokens = _load_tokens()
    if not tokens:
        return None

    oauth_config = _get_google_oauth_config()
    if not oauth_config.get("client_id"):
        return None
    creds = Credentials(
        token=tokens.get("access_token"),
        refresh_token=tokens.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=oauth_config["client_id"],
        client_secret=oauth_config["client_secret"],
        scopes=SCOPES,
    )

    # トークンが期限切れの場合はリフレッシュ
    if creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request as GoogleRequest
        creds.refresh(GoogleRequest())
        # 更新後のトークンを保存
        tokens["access_token"] = creds.token
        _save_tokens(tokens)

    return creds


def _get_bq_client(project_id: str):
    """認証済みのBigQueryクライアントを取得する。"""
    from google.cloud import bigquery

    creds = _get_credentials()
    if not creds:
        raise HTTPException(
            status_code=401,
            detail="Googleアカウントで認証が必要です。先に認証を行ってください。",
        )
    return bigquery.Client(project=project_id, credentials=creds)


class OAuthConfigRequest(BaseModel):
    client_id: str
    client_secret: str


@router.get("/oauth-config-status")
async def oauth_config_status() -> dict:
    """Google OAuthクライアント設定の状態を返す。"""
    config = _get_google_oauth_config()
    configured = bool(config.get("client_id") and config.get("client_secret"))
    return {
        "configured": configured,
        "client_id_preview": config["client_id"][:20] + "..." if configured and len(config.get("client_id", "")) > 20 else config.get("client_id", ""),
    }


@router.post("/oauth-config")
async def save_oauth_config(body: OAuthConfigRequest) -> dict:
    """Google OAuthクライアント設定を保存する。"""
    if not body.client_id or not body.client_secret:
        raise HTTPException(status_code=400, detail="Client IDとClient Secretの両方が必要です")
    _save_oauth_config(body.client_id, body.client_secret)
    logger.info("Google OAuthクライアント設定を保存しました")
    return {"ok": True, "message": "OAuth設定を保存しました"}


@router.get("/auth-url")
async def get_auth_url(request: Request) -> dict:
    """Google OAuth認証URLを返す。"""
    oauth_config = _get_google_oauth_config()
    if not oauth_config["client_id"] or not oauth_config["client_secret"]:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_CLIENT_ID と GOOGLE_CLIENT_SECRET が .env に設定されていません",
        )
    from urllib.parse import urlencode

    # コールバックURLを動的に構築
    base_url = str(request.base_url).rstrip("/")
    redirect_uri = f"{base_url}/api/bigquery/callback"

    params = {
        "client_id": oauth_config["client_id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
    }
    authorize_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    return {"authorize_url": authorize_url}


@router.get("/callback", response_class=HTMLResponse)
async def oauth_callback(request: Request, code: str | None = None, error: str | None = None):
    """Google OAuth コールバック。認証コードをトークンに交換する。"""
    if error:
        return HTMLResponse(f"""
        <html><body><script>
            window.opener?.postMessage({{ type: 'bigquery-auth-error', error: '{error}' }}, '*');
            window.close();
        </script><p>認証エラー: {error}</p></body></html>
        """)

    if not code:
        raise HTTPException(status_code=400, detail="認証コードがありません")

    oauth_config = _get_google_oauth_config()
    base_url = str(request.base_url).rstrip("/")
    redirect_uri = f"{base_url}/api/bigquery/callback"

    # 認証コードをトークンに交換
    import httpx

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": oauth_config["client_id"],
                "client_secret": oauth_config["client_secret"],
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )

    if resp.status_code != 200:
        logger.error("トークン交換失敗: %s", resp.text)
        return HTMLResponse(f"""
        <html><body><script>
            window.opener?.postMessage({{ type: 'bigquery-auth-error', error: 'トークン交換に失敗' }}, '*');
            window.close();
        </script><p>トークン交換に失敗しました</p></body></html>
        """)

    token_data = resp.json()

    # ユーザーのメールアドレスを取得
    email = None
    try:
        async with httpx.AsyncClient() as client:
            userinfo_resp = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {token_data['access_token']}"},
            )
            if userinfo_resp.status_code == 200:
                email = userinfo_resp.json().get("email")
    except Exception as e:
        logger.warning("ユーザー情報取得失敗: %s", e)

    # トークンを保存（再認証時にrefresh_tokenが返されない場合は既存のものを保持）
    existing_tokens = _load_tokens()
    refresh_token = token_data.get("refresh_token")
    if not refresh_token and existing_tokens:
        refresh_token = existing_tokens.get("refresh_token")

    _save_tokens({
        "access_token": token_data["access_token"],
        "refresh_token": refresh_token,
        "email": email,
    })

    return HTMLResponse("""
    <html><body><script>
        window.opener?.postMessage({ type: 'bigquery-auth-success' }, '*');
        window.close();
    </script><p>認証成功！このウィンドウは自動的に閉じます。</p></body></html>
    """)


@router.get("/auth-status")
async def auth_status() -> dict:
    """現在のGoogle認証ステータスを返す。"""
    tokens = _load_tokens()
    if not tokens or not tokens.get("access_token"):
        return {"authenticated": False}
    return {
        "authenticated": True,
        "email": tokens.get("email"),
    }


# --- BigQuery操作エンドポイント ---

class ConnectionTestRequest(BaseModel):
    project_id: str
    dataset_id: str


class ConnectionTestResponse(BaseModel):
    ok: bool
    datasets: list[str] = []
    error: str | None = None


class ListTablesRequest(BaseModel):
    project_id: str
    dataset_id: str


class TableInfo(BaseModel):
    table_id: str
    row_count: int
    created: str


class ListTablesResponse(BaseModel):
    tables: list[TableInfo]


class TableSchemaRequest(BaseModel):
    project_id: str
    dataset_id: str
    table_id: str


class ColumnInfo(BaseModel):
    name: str
    type: str
    mode: str


class TableSchemaResponse(BaseModel):
    columns: list[ColumnInfo]


@router.post("/test-connection")
async def test_connection(body: ConnectionTestRequest) -> ConnectionTestResponse:
    """BigQuery接続をテストする。"""
    try:
        client = _get_bq_client(body.project_id)
        datasets = [ds.dataset_id for ds in client.list_datasets()]
        return ConnectionTestResponse(ok=True, datasets=datasets)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("BigQuery接続テスト失敗: %s", e)
        return ConnectionTestResponse(ok=False, error=f"接続に失敗しました: {e}")


@router.post("/tables")
async def list_tables(body: ListTablesRequest) -> ListTablesResponse:
    """データセット内のテーブル一覧を取得する。"""
    try:
        client = _get_bq_client(body.project_id)
        dataset_ref = f"{body.project_id}.{body.dataset_id}"
        tables = []
        for tbl in client.list_tables(dataset_ref):
            full_table = client.get_table(tbl.reference)
            tables.append(
                TableInfo(
                    table_id=tbl.table_id,
                    row_count=full_table.num_rows or 0,
                    created=full_table.created.isoformat() if full_table.created else "",
                )
            )
        return ListTablesResponse(tables=tables)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("テーブル一覧の取得に失敗: %s", e)
        raise HTTPException(
            status_code=500,
            detail=f"テーブル一覧の取得に失敗しました: {e}",
        ) from e


@router.post("/table-schema")
async def get_table_schema(body: TableSchemaRequest) -> TableSchemaResponse:
    """テーブルのスキーマを取得する。"""
    try:
        client = _get_bq_client(body.project_id)
        table_ref = f"{body.project_id}.{body.dataset_id}.{body.table_id}"
        table = client.get_table(table_ref)
        columns = [
            ColumnInfo(name=field.name, type=field.field_type, mode=field.mode)
            for field in table.schema
        ]
        return TableSchemaResponse(columns=columns)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("スキーマの取得に失敗: %s", e)
        raise HTTPException(
            status_code=500,
            detail=f"テーブルスキーマの取得に失敗しました: {e}",
        ) from e
