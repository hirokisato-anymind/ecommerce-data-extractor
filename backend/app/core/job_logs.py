"""ジョブ実行ログのストア。GCS（クラウド）またはファイル（ローカル）に保存。

各ジョブにつき1つのログファイルを保持し、新しい実行のたびに上書きする。
"""

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.core.storage import is_cloud_mode, GCS_CONFIG_BUCKET

logger = logging.getLogger("ecommerce_data_extractor.job_logs")

_LOGS_DIR = Path(__file__).resolve().parent.parent.parent / "job_logs"
_GCS_LOGS_PREFIX = "job_logs/"
_lock = threading.Lock()


def _gcs_blob_name(schedule_id: str) -> str:
    return f"{_GCS_LOGS_PREFIX}{schedule_id}.json"


def _local_path(schedule_id: str) -> Path:
    _LOGS_DIR.mkdir(exist_ok=True)
    return _LOGS_DIR / f"{schedule_id}.json"


def _load_entries(schedule_id: str) -> list[dict]:
    if is_cloud_mode():
        try:
            from google.cloud import storage as gcs
            client = gcs.Client()
            bucket = client.bucket(GCS_CONFIG_BUCKET)
            blob = bucket.blob(_gcs_blob_name(schedule_id))
            if not blob.exists():
                return []
            return json.loads(blob.download_as_text(encoding="utf-8"))
        except Exception:
            return []
    else:
        path = _local_path(schedule_id)
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []


def _save_entries(schedule_id: str, entries: list[dict]) -> None:
    data = json.dumps(entries, ensure_ascii=False)
    if is_cloud_mode():
        try:
            from google.cloud import storage as gcs
            client = gcs.Client()
            bucket = client.bucket(GCS_CONFIG_BUCKET)
            blob = bucket.blob(_gcs_blob_name(schedule_id))
            blob.upload_from_string(data, content_type="application/json")
        except Exception as e:
            logger.warning("ジョブログの保存に失敗 (%s): %s", schedule_id, e)
    else:
        path = _local_path(schedule_id)
        path.write_text(data, encoding="utf-8")


def clear_logs(schedule_id: str) -> None:
    """ジョブのログをクリアする（新しい実行開始時に呼ぶ）。"""
    with _lock:
        _save_entries(schedule_id, [])


def add_log(schedule_id: str, message: str, level: str = "info") -> None:
    """ログエントリを追加する。"""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
    }
    with _lock:
        entries = _load_entries(schedule_id)
        entries.append(entry)
        # Cap at 500 entries
        if len(entries) > 500:
            entries = entries[-500:]
        _save_entries(schedule_id, entries)


def get_logs(schedule_id: str, after_index: int = 0) -> list[dict]:
    """指定インデックス以降のログエントリを返す。"""
    with _lock:
        entries = _load_entries(schedule_id)
        return [
            {"index": i, **entry}
            for i, entry in enumerate(entries)
            if i >= after_index
        ]
