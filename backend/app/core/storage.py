"""永続化ストレージ抽象化。

ローカル環境: ファイルシステム
Cloud Run環境: Secret Manager (認証情報) + Cloud Storage (設定ファイル)

環境変数 GCS_CONFIG_BUCKET が設定されている場合はクラウドモード。
"""

import json
import logging
import os

logger = logging.getLogger("ecommerce_data_extractor.storage")

GCP_PROJECT = os.getenv("GCP_PROJECT", "")
GCS_CONFIG_BUCKET = os.getenv("GCS_CONFIG_BUCKET", "")


def is_cloud_mode() -> bool:
    return bool(GCS_CONFIG_BUCKET)


# ---------------------------------------------------------------------------
# Secret Manager (認証情報)
# ---------------------------------------------------------------------------

def _sm_client():
    from google.cloud import secretmanager
    return secretmanager.SecretManagerServiceClient()


def _secret_path(secret_id: str) -> str:
    return f"projects/{GCP_PROJECT}/secrets/{secret_id}"


def _secret_version_path(secret_id: str, version: str = "latest") -> str:
    return f"projects/{GCP_PROJECT}/secrets/{secret_id}/versions/{version}"


def load_secret(secret_id: str) -> str | None:
    """Secret Manager からシークレットの最新バージョンを読み込む。"""
    if not is_cloud_mode():
        return None
    try:
        client = _sm_client()
        response = client.access_secret_version(
            name=_secret_version_path(secret_id)
        )
        return response.payload.data.decode("utf-8")
    except Exception as e:
        logger.debug("Secret '%s' の読み込みに失敗: %s", secret_id, e)
        return None


def save_secret(secret_id: str, data: str) -> None:
    """Secret Manager にシークレットを保存する（存在しなければ作成）。"""
    if not is_cloud_mode():
        return
    client = _sm_client()
    parent = f"projects/{GCP_PROJECT}"
    # シークレットの作成（既に存在していればスキップ）
    try:
        client.create_secret(
            parent=parent,
            secret_id=secret_id,
            secret={"replication": {"automatic": {}}},
        )
    except Exception:
        pass  # Already exists
    # 新しいバージョンを追加
    client.add_secret_version(
        parent=_secret_path(secret_id),
        payload={"data": data.encode("utf-8")},
    )
    logger.info("Secret '%s' を保存しました", secret_id)


def load_secret_json(secret_id: str) -> dict | None:
    """Secret Manager からJSONシークレットを読み込む。"""
    raw = load_secret(secret_id)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def save_secret_json(secret_id: str, data: dict) -> None:
    """Secret Manager にJSONシークレットを保存する。"""
    save_secret(secret_id, json.dumps(data, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Cloud Storage (設定ファイル)
# ---------------------------------------------------------------------------

def _gcs_client():
    from google.cloud import storage
    return storage.Client(project=GCP_PROJECT)


def load_gcs_json(blob_name: str) -> list | dict | None:
    """GCS からJSONファイルを読み込む。"""
    if not is_cloud_mode():
        return None
    try:
        client = _gcs_client()
        bucket = client.bucket(GCS_CONFIG_BUCKET)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        raw = blob.download_as_text(encoding="utf-8")
        return json.loads(raw)
    except Exception as e:
        logger.error("GCS '%s' の読み込みに失敗: %s", blob_name, e)
        return None


def save_gcs_json(blob_name: str, data: list | dict) -> None:
    """GCS にJSONファイルを保存する。"""
    if not is_cloud_mode():
        return
    try:
        client = _gcs_client()
        bucket = client.bucket(GCS_CONFIG_BUCKET)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            json.dumps(data, indent=2, ensure_ascii=False),
            content_type="application/json",
        )
        logger.info("GCS '%s' を保存しました", blob_name)
    except Exception as e:
        logger.error("GCS '%s' の保存に失敗: %s", blob_name, e)
        raise
