"""スケジュール抽出ジョブの実行とストレージ管理。"""

import asyncio
import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.core.storage import is_cloud_mode, load_gcs_json, save_gcs_json

_schedules_lock = threading.Lock()

logger = logging.getLogger("ecommerce_data_extractor.scheduler")

SCHEDULES_FILE = Path(__file__).resolve().parent.parent.parent / "schedules.json"
_GCS_SCHEDULES_BLOB = "schedules.json"


def _load_schedules_from_file() -> list[dict]:
    """GCS (cloud) またはJSONファイル (local) からスケジュールを直接読み込む。"""
    if is_cloud_mode():
        data = load_gcs_json(_GCS_SCHEDULES_BLOB)
        if data is None:
            return []
        return data if isinstance(data, list) else []
    if not SCHEDULES_FILE.exists():
        return []
    try:
        data = json.loads(SCHEDULES_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError) as e:
        logger.error("スケジュールファイルの読み込みに失敗: %s", e)
        return []


def _save_schedules_to_file(schedules: list[dict]) -> None:
    """GCS (cloud) またはJSONファイル (local) にスケジュールを保存する。"""
    if is_cloud_mode():
        save_gcs_json(_GCS_SCHEDULES_BLOB, schedules)
    else:
        SCHEDULES_FILE.write_text(
            json.dumps(schedules, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )


def _update_schedule_run_status(
    schedule_id: str, status: str, *, last_synced_at: str | None = None,
) -> None:
    """スケジュールのlast_run_atとlast_run_statusを更新する。ロックで排他制御。"""
    try:
        with _schedules_lock:
            schedules = _load_schedules_from_file()
            for s in schedules:
                if s["id"] == schedule_id:
                    s["last_run_at"] = datetime.now(timezone.utc).isoformat()
                    s["last_run_status"] = status
                    if last_synced_at:
                        s["last_synced_at"] = last_synced_at
                    break
            _save_schedules_to_file(schedules)
    except Exception as e:
        logger.error("スケジュール %s の実行ステータス更新に失敗: %s", schedule_id, e)


async def _execute_scheduled_job(schedule_data: dict) -> None:
    """スケジュールされた抽出ジョブを実行する。"""
    schedule_id = schedule_data["id"]

    # 実行時に最新のスケジュールデータを再読み込み
    fresh_schedules = _load_schedules_from_file()
    for s in fresh_schedules:
        if s["id"] == schedule_id:
            schedule_data = s
            break

    platform_id = schedule_data["platform_id"]
    endpoint_id = schedule_data["endpoint_id"]
    columns = schedule_data.get("columns")
    filters_def = schedule_data.get("filters")
    keyword = schedule_data.get("keyword")
    limit = schedule_data.get("limit", 1000)
    destination = schedule_data.get("destination", {})
    last_synced_at = schedule_data.get("last_synced_at")

    from app.core.job_logs import add_log, clear_logs

    clear_logs(schedule_id)
    add_log(schedule_id, f"ジョブ実行開始: {platform_id}/{endpoint_id}")
    # API制限・実行方針をログに記載
    _API_POLICY: dict[str, str] = {
        "amazon": "Amazon SP-API: Orders=Reports API一括取得 (レポート生成→ダウンロード), Finances=0.5req/s | リトライ: 最大5回 指数バックオフ",
        "shopify": "Shopify Admin API: 2req/s burst4 | GraphQL コストベース制御",
        "rakuten": "楽天RMS API: 1req/s | searchOrder最大1,000件/ページ → getOrder100件/バッチ",
        "yahoo": "Yahoo Shopping API: 1req/s",
    }
    if platform_id in _API_POLICY:
        add_log(schedule_id, f"API制限: {_API_POLICY[platform_id]}")
    if last_synced_at:
        add_log(schedule_id, f"実行方針: 増分取得 (前回同期: {last_synced_at})")
    else:
        add_log(schedule_id, "実行方針: 初回全件取得")
    logger.info("スケジュールジョブ実行開始: %s (%s/%s)", schedule_id, platform_id, endpoint_id)
    _update_schedule_run_status(schedule_id, "実行中")

    try:
        from app.platforms.registry import get_client
        from app.core.filters import FilterDefinition, apply_filters
        from app.core.bigquery import write_to_bigquery, TransferMode

        client = get_client(platform_id)
        if not client:
            raise ValueError(f"プラットフォーム '{platform_id}' が見つかりません")

        # On Cloud Run, credentials may not be loaded into settings yet
        # (e.g. after a cold start).  Reload from Secret Manager before
        # giving up.
        if not client.is_configured():
            add_log(schedule_id, "認証情報を再読み込み中...")
            logger.info(
                "プラットフォーム '%s' 未設定 - ストレージから認証情報を再読み込みします",
                platform_id,
            )
            try:
                from app.routers.credentials import load_credentials_from_storage
                load_credentials_from_storage()
            except Exception as reload_err:
                add_log(schedule_id, f"認証情報の再読み込みに失敗: {reload_err}", level="warning")
                logger.warning("認証情報の再読み込みに失敗: %s", reload_err)
            # Re-create the client so it picks up the refreshed settings
            client = get_client(platform_id)
            if not client or not client.is_configured():
                raise ValueError(
                    f"プラットフォーム '{platform_id}' が設定されていません。"
                    "Secret Manager / .env に認証情報が保存されているか確認してください。"
                )

        add_log(schedule_id, "プラットフォーム接続OK")

        # フィルターで参照されるカラムを取得カラムに含める
        filter_columns = set()
        start_date = None
        end_date = None

        # Incremental sync: use last_synced_at as start_date if available
        # Convert to the target platform's timezone before extracting date
        if last_synced_at:
            _JST_PLATFORMS = {"rakuten", "yahoo"}
            try:
                from datetime import timedelta
                # Parse the stored timestamp
                raw = last_synced_at
                if raw.endswith("Z"):
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(raw)
                if platform_id in _JST_PLATFORMS:
                    # Rakuten/Yahoo APIs expect JST
                    jst = timezone(timedelta(hours=9))
                    start_date = dt.astimezone(jst).strftime("%Y-%m-%d")
                else:
                    # Shopify/Amazon APIs expect UTC
                    start_date = dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                start_date = last_synced_at[:10]
            add_log(schedule_id, f"増分取得: start_date={start_date}")
            logger.info("増分取得: last_synced_at=%s → start_date=%s (platform=%s)", last_synced_at, start_date, platform_id)

        if filters_def:
            for f in filters_def:
                col = f.get("column")
                if col:
                    filter_columns.add(col)
                # Convert last_n_days on date columns to start_date (only if no incremental sync)
                _DATE_FILTER_COLUMNS = {
                    "createdAt", "created_at", "updatedAt", "updated_at",
                    "CreatedAfter", "PostedDate", "PurchaseDate", "LastUpdateDate",
                    "orderDatetime", "orderTime", "updateTime",
                    "created", "updated", "publishedAt",
                }
                if not start_date and f.get("operator") == "last_n_days" and col in _DATE_FILTER_COLUMNS:
                    from datetime import timedelta as _td
                    n_days = int(f["value"])
                    start_date = (datetime.now(timezone.utc) - _td(days=n_days)).strftime("%Y-%m-%d")

        # Ensure updatedAt-type columns are fetched for incremental sync tracking
        _UPDATED_AT_KEYS = {"updatedAt", "updated_at", "LastUpdateDate"}
        sync_columns = _UPDATED_AT_KEYS | filter_columns

        if columns:
            selected_columns = set(columns)
            extra_columns = sync_columns - selected_columns
            fetch_columns = list(columns) + sorted(extra_columns)
        else:
            # columns is None or empty [] → fetch all columns
            fetch_columns = None

        # --- チャンク単位でのデータ取得 + BQ書き込み ---
        CHUNK_SIZE = 10_000
        unlimited = (limit == 0)
        effective_limit = float('inf') if unlimited else limit

        transfer_mode = TransferMode(destination.get("transfer_mode", "append"))
        key_columns_bq = destination.get("key_columns") or None
        table_name = f"{destination['project_id']}.{destination['dataset_id']}.{destination['table_id']}"

        add_log(schedule_id, f"データ取得開始 (上限: {'無制限' if unlimited else f'{limit}件'})")
        add_log(schedule_id, f"BQ書き込み先: {table_name} (モード: {transfer_mode.value}, チャンクサイズ: {CHUNK_SIZE}件)")

        # delete_in_advance: ループ前にテーブル全行削除
        if transfer_mode == TransferMode.DELETE_IN_ADVANCE:
            from app.core.bigquery import delete_all_rows
            add_log(schedule_id, "delete_in_advance: 既存データを一括削除中...")
            delete_all_rows(
                project_id=destination["project_id"],
                dataset_id=destination["dataset_id"],
                table_id=destination["table_id"],
                location=destination.get("location", "US"),
            )
            add_log(schedule_id, "delete_in_advance: 削除完了")

        chunk_buffer: list[dict] = []
        total_fetched = 0
        total_written = 0
        chunk_num = 0
        is_first_chunk = True
        new_synced_at: str | None = None
        current_cursor = None
        page_num = 0
        filter_objs = [FilterDefinition(**f) for f in filters_def] if filters_def else None

        # フィルター適用後、ユーザー選択カラムのみに絞り込むための追加カラム
        extra_columns = (sync_columns - set(columns)) if columns else set()

        async def _flush_chunk(buffer: list[dict], *, first: bool) -> int:
            """バッファをフィルター→カラム絞り込み→BQ書き込みし、書き込み件数を返す。"""
            items = buffer

            # フィルター適用
            if filter_objs:
                before = len(items)
                items = apply_filters(items, filter_objs)
                if before != len(items):
                    add_log(schedule_id, f"  フィルター適用: {before}件 → {len(items)}件")

            # ユーザー選択カラム以外を除外
            if columns and extra_columns:
                items = [
                    {k: v for k, v in item.items() if k not in extra_columns}
                    for item in items
                ]

            if not items:
                return 0

            # 転送モード決定
            if first:
                chunk_mode = transfer_mode
            else:
                # 2回目以降: replace/delete_in_advance は append_direct に切り替え
                if transfer_mode in (TransferMode.REPLACE, TransferMode.DELETE_IN_ADVANCE):
                    chunk_mode = TransferMode.APPEND_DIRECT
                else:
                    chunk_mode = transfer_mode

            # delete_in_advance は事前削除済みなので append_direct で書き込み
            if transfer_mode == TransferMode.DELETE_IN_ADVANCE:
                chunk_mode = TransferMode.APPEND_DIRECT

            bq_result = await write_to_bigquery(
                project_id=destination["project_id"],
                dataset_id=destination["dataset_id"],
                table_id=destination["table_id"],
                rows=items,
                mode=chunk_mode,
                key_columns=key_columns_bq,
                location=destination.get("location", "US"),
                platform_id=platform_id,
                endpoint_id=endpoint_id,
            )
            return bq_result["rows_written"]

        # ページネーションループ
        while total_fetched < effective_limit:
            remaining = effective_limit - total_fetched
            page_limit = min(int(remaining), 1000) if remaining != float('inf') else 1000
            result = await client.extract_data(
                endpoint_id=endpoint_id,
                columns=fetch_columns,
                limit=page_limit,
                cursor=current_cursor,
                keyword=keyword,
                start_date=start_date,
                end_date=end_date,
            )
            page_items = result.get("items", [])
            chunk_buffer.extend(page_items)
            total_fetched += len(page_items)
            page_num += 1

            # new_synced_at をフィルター前の生データから追跡
            if page_items and platform_id != "rakuten":
                for date_key in ("updatedAt", "updated_at", "LastUpdateDate"):
                    dates = [item.get(date_key) for item in page_items if item.get(date_key)]
                    if dates:
                        page_max = max(dates)
                        if new_synced_at is None or page_max > new_synced_at:
                            new_synced_at = page_max
                        break

            # ログ出力（10ページごと、または最終ページ）
            next_cursor = result.get("next_cursor")
            is_last_page = not next_cursor or not page_items
            if page_num % 10 == 0 or is_last_page:
                add_log(schedule_id, f"ページ {page_num} 取得完了 (合計: {total_fetched}件)")

            # チャンクサイズに達したらフラッシュ
            if len(chunk_buffer) >= CHUNK_SIZE:
                chunk_num += 1
                add_log(schedule_id, f"チャンク {chunk_num} BQ書き込み中... ({len(chunk_buffer)}件)")
                written = await _flush_chunk(chunk_buffer, first=is_first_chunk)
                total_written += written
                add_log(schedule_id, f"チャンク {chunk_num} 完了: {written}件書き込み (累計: {total_written}件)")
                chunk_buffer = []
                is_first_chunk = False

            if is_last_page:
                break
            current_cursor = next_cursor

        # 残りバッファをフラッシュ
        if chunk_buffer:
            chunk_num += 1
            add_log(schedule_id, f"チャンク {chunk_num} BQ書き込み中... ({len(chunk_buffer)}件)")
            written = await _flush_chunk(chunk_buffer, first=is_first_chunk)
            total_written += written
            add_log(schedule_id, f"チャンク {chunk_num} 完了: {written}件書き込み (累計: {total_written}件)")

        # 楽天は updatedAt がないため実行時刻を使用
        if total_fetched > 0 and platform_id == "rakuten":
            new_synced_at = datetime.now(timezone.utc).isoformat()

        add_log(schedule_id, f"完了: 取得{total_fetched}件, BQ書き込み{total_written}件")
        status_msg = (
            f"成功: {total_written}行を{table_name}に"
            f"書き込み (モード: {transfer_mode.value})"
        )
        logger.info(
            "スケジュールジョブ %s 完了: %d件をBigQueryに書き込み",
            schedule_id,
            total_written,
        )
        _update_schedule_run_status(schedule_id, status_msg, last_synced_at=new_synced_at)

    except Exception as e:
        written_info = ""
        try:
            written_info = f" ({total_written}件書き込み済み)" if total_written > 0 else ""
        except NameError:
            pass
        add_log(schedule_id, f"エラー{written_info}: {e}", level="error")
        logger.exception("スケジュールジョブ %s 失敗: %s", schedule_id, e)
        _update_schedule_run_status(schedule_id, f"エラー{written_info}: {e}")
