"""スケジュール抽出ジョブのAPScheduler統合。"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.storage import is_cloud_mode, load_gcs_json, save_gcs_json

logger = logging.getLogger("ecommerce_data_extractor.scheduler")

SCHEDULES_FILE = Path(__file__).resolve().parent.parent.parent / "schedules.json"
_GCS_SCHEDULES_BLOB = "schedules.json"

scheduler = AsyncIOScheduler()


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


def _update_schedule_run_status(schedule_id: str, status: str) -> None:
    """スケジュールのlast_run_atとlast_run_statusを更新する。"""
    try:
        schedules = _load_schedules_from_file()
        for s in schedules:
            if s["id"] == schedule_id:
                s["last_run_at"] = datetime.now(timezone.utc).isoformat()
                s["last_run_status"] = status
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
    limit = schedule_data.get("limit", 1000)
    destination = schedule_data.get("destination", {})

    logger.info("スケジュールジョブ実行開始: %s (%s/%s)", schedule_id, platform_id, endpoint_id)

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
            logger.info(
                "プラットフォーム '%s' 未設定 - ストレージから認証情報を再読み込みします",
                platform_id,
            )
            try:
                from app.routers.credentials import load_credentials_from_storage
                load_credentials_from_storage()
            except Exception as reload_err:
                logger.warning("認証情報の再読み込みに失敗: %s", reload_err)
            # Re-create the client so it picks up the refreshed settings
            client = get_client(platform_id)
            if not client or not client.is_configured():
                raise ValueError(
                    f"プラットフォーム '{platform_id}' が設定されていません。"
                    "Secret Manager / .env に認証情報が保存されているか確認してください。"
                )

        # フィルターで参照されるカラムを取得カラムに含める
        filter_columns = set()
        if filters_def:
            for f in filters_def:
                col = f.get("column")
                if col:
                    filter_columns.add(col)

        if columns is not None and filter_columns:
            selected_columns = set(columns)
            extra_columns = filter_columns - selected_columns
            fetch_columns = list(columns) + sorted(extra_columns)
        else:
            fetch_columns = columns

        # Paginate through all pages up to limit
        all_items: list[dict] = []
        current_cursor = None
        while len(all_items) < limit:
            page_limit = min(limit - len(all_items), 100)
            result = await client.extract_data(
                endpoint_id=endpoint_id,
                columns=fetch_columns,
                limit=page_limit,
                cursor=current_cursor,
            )
            page_items = result.get("items", [])
            all_items.extend(page_items)

            next_cursor = result.get("next_cursor")
            if not next_cursor or not page_items:
                break
            current_cursor = next_cursor

        items = all_items[:limit]

        # フィルター適用
        if filters_def:
            filter_objs = [FilterDefinition(**f) for f in filters_def]
            items = apply_filters(items, filter_objs)

        # フィルター適用後、ユーザー選択カラムのみに絞り込む
        if columns is not None and filter_columns:
            extra_columns = filter_columns - set(columns)
            if extra_columns:
                items = [
                    {k: v for k, v in item.items() if k not in extra_columns}
                    for item in items
                ]

        # BigQueryにデータを書き込み
        transfer_mode = TransferMode(destination.get("transfer_mode", "append"))
        key_columns = destination.get("key_columns") or None

        bq_result = await write_to_bigquery(
            project_id=destination["project_id"],
            dataset_id=destination["dataset_id"],
            table_id=destination["table_id"],
            rows=items,
            mode=transfer_mode,
            key_columns=key_columns,
            location=destination.get("location", "US"),
        )

        status_msg = (
            f"成功: {bq_result['rows_written']}行を{bq_result['table']}に"
            f"書き込み (モード: {bq_result['mode']})"
        )
        logger.info(
            "スケジュールジョブ %s 完了: %d件をBigQueryに書き込み",
            schedule_id,
            bq_result["rows_written"],
        )
        _update_schedule_run_status(schedule_id, status_msg)

    except Exception as e:
        logger.exception("スケジュールジョブ %s 失敗: %s", schedule_id, e)
        _update_schedule_run_status(schedule_id, f"エラー: {e}")


def add_job(schedule) -> None:
    """Scheduleオブジェクトまたはdictからスケジューラにジョブを追加する。"""
    if hasattr(schedule, "model_dump"):
        data = schedule.model_dump()
    elif isinstance(schedule, dict):
        data = schedule
    else:
        raise TypeError(f"サポートされていないスケジュール型: {type(schedule)}")

    schedule_id = data["id"]

    # schedule_configからCronTriggerを構築
    schedule_config = data.get("schedule_config")
    if schedule_config:
        from app.routers.schedule import ScheduleConfig, schedule_config_to_cron

        if isinstance(schedule_config, dict):
            config_obj = ScheduleConfig(**schedule_config)
        else:
            config_obj = schedule_config

        cron_kwargs = schedule_config_to_cron(config_obj)
        trigger = CronTrigger(**cron_kwargs)
    else:
        # 後方互換: cron_expressionが残っている場合
        cron_expression = data.get("cron_expression", "0 * * * *")
        trigger = _parse_cron(cron_expression)

    scheduler.add_job(
        _execute_scheduled_job,
        trigger=trigger,
        id=schedule_id,
        args=[data],
        replace_existing=True,
        name=data.get("name", schedule_id),
    )
    logger.info("スケジューラジョブ登録: %s", schedule_id)


def _parse_cron(cron_expression: str) -> CronTrigger:
    """cron式文字列をAPSchedulerのCronTriggerにパースする（後方互換用）。"""
    parts = cron_expression.strip().split()
    if len(parts) == 5:
        return CronTrigger(
            minute=parts[0],
            hour=parts[1],
            day=parts[2],
            month=parts[3],
            day_of_week=parts[4],
        )
    elif len(parts) == 6:
        return CronTrigger(
            second=parts[0],
            minute=parts[1],
            hour=parts[2],
            day=parts[3],
            month=parts[4],
            day_of_week=parts[5],
        )
    else:
        raise ValueError(f"不正なcron式です: {cron_expression}")


def remove_job(schedule_id: str) -> None:
    """スケジューラからジョブを削除する。"""
    try:
        scheduler.remove_job(schedule_id)
        logger.info("スケジューラジョブ削除: %s", schedule_id)
    except Exception:
        logger.debug("ジョブ %s がスケジューラに見つかりません（未登録の可能性）", schedule_id)


def load_all_schedules() -> None:
    """JSONファイルから全有効スケジュールを読み込み登録する。"""
    schedules = _load_schedules_from_file()
    count = 0
    for s in schedules:
        if s.get("enabled", True):
            add_job(s)
            count += 1
    logger.info("%d件の有効なスケジュールを読み込みました", count)


def start_scheduler() -> None:
    """APSchedulerを起動し既存スケジュールを読み込む。"""
    load_all_schedules()
    scheduler.start()
    logger.info("スケジューラを起動しました")


def stop_scheduler() -> None:
    """スケジューラを停止する。"""
    scheduler.shutdown(wait=False)
    logger.info("スケジューラを停止しました")
