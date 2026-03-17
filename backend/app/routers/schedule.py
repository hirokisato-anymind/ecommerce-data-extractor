"""スケジュール抽出ジョブのCRUDエンドポイント。"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.core.storage import is_cloud_mode, load_gcs_json, save_gcs_json

logger = logging.getLogger("ecommerce_data_extractor.schedule")

router = APIRouter(prefix="/schedules", tags=["schedules"])

SCHEDULES_FILE = Path(__file__).resolve().parent.parent.parent / "schedules.json"
_GCS_SCHEDULES_BLOB = "schedules.json"


class DestinationConfig(BaseModel):
    type: str = "bigquery"  # 現時点ではBigQueryのみ対応
    project_id: str
    dataset_id: str
    table_id: str
    transfer_mode: str  # append, append_direct, replace, delete_in_advance, upsert
    key_columns: list[str] = []  # upsert, delete_in_advance用
    location: str = "US"  # BigQueryロケーション/リージョン


class ScheduleConfig(BaseModel):
    # cronではなくスロットベースのスケジュール設定
    frequency: str  # "hourly", "daily", "weekly", "monthly"
    hour: int = 9  # 0-23
    minute: int = 0  # 0-59
    day_of_week: int | None = None  # 0=月, 6=日 (weekly用)
    day_of_month: int | None = None  # 1-31 (monthly用)


def schedule_config_to_cron(config: ScheduleConfig) -> dict:
    """スロットベースのスケジュールをAPSchedulerのcronキーワード引数に変換する。"""
    if config.frequency == "hourly":
        return {"minute": config.minute}
    elif config.frequency == "daily":
        return {"hour": config.hour, "minute": config.minute}
    elif config.frequency == "weekly":
        return {
            "day_of_week": config.day_of_week if config.day_of_week is not None else 0,
            "hour": config.hour,
            "minute": config.minute,
        }
    elif config.frequency == "monthly":
        return {
            "day": config.day_of_month or 1,
            "hour": config.hour,
            "minute": config.minute,
        }
    else:
        raise ValueError(f"不正な頻度指定です: {config.frequency}")


class ScheduleCreate(BaseModel):
    name: str
    platform_id: str
    endpoint_id: str
    columns: list[str] | None = None
    filters: list[dict] | None = None
    limit: int = Field(1000, ge=1, le=10000)
    destination: DestinationConfig
    schedule_config: ScheduleConfig
    enabled: bool = True


class ScheduleUpdate(BaseModel):
    name: str | None = None
    platform_id: str | None = None
    endpoint_id: str | None = None
    columns: list[str] | None = None
    filters: list[dict] | None = None
    limit: int | None = Field(None, ge=1, le=10000)
    destination: DestinationConfig | None = None
    schedule_config: ScheduleConfig | None = None
    enabled: bool | None = None


class Schedule(BaseModel):
    id: str
    name: str
    platform_id: str
    endpoint_id: str
    columns: list[str] | None = None
    filters: list[dict] | None = None
    limit: int = 1000
    destination: DestinationConfig = DestinationConfig(
        project_id="", dataset_id="", table_id="", transfer_mode="append",
    )
    schedule_config: ScheduleConfig = ScheduleConfig(frequency="daily")
    enabled: bool = True
    created_at: str = ""
    updated_at: str = ""
    last_run_at: str | None = None
    last_run_status: str | None = None


def _load_schedules() -> list[dict]:
    """GCS (cloud) またはJSONファイル (local) からスケジュールを読み込む。"""
    if is_cloud_mode():
        data = load_gcs_json(_GCS_SCHEDULES_BLOB)
        if data is None:
            return []
        return data if isinstance(data, list) else []
    if not SCHEDULES_FILE.exists():
        return []
    try:
        data = json.loads(SCHEDULES_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        return []
    except (json.JSONDecodeError, OSError) as e:
        logger.error("スケジュールの読み込みに失敗: %s", e)
        return []


def _save_schedules(schedules: list[dict]) -> None:
    """GCS (cloud) またはJSONファイル (local) にスケジュールを保存する。"""
    if is_cloud_mode():
        save_gcs_json(_GCS_SCHEDULES_BLOB, schedules)
        return
    try:
        SCHEDULES_FILE.write_text(
            json.dumps(schedules, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError as e:
        logger.error("スケジュールの保存に失敗: %s", e)
        raise HTTPException(status_code=500, detail="スケジュールデータの保存に失敗しました") from e


@router.get("/")
async def list_schedules() -> list[Schedule]:
    """全スケジュールを一覧表示する。"""
    schedules = _load_schedules()
    return [Schedule(**s) for s in schedules]


@router.post("/", status_code=201)
async def create_schedule(body: ScheduleCreate) -> Schedule:
    """新しいスケジュールを作成する。"""
    schedules = _load_schedules()
    now = datetime.now(timezone.utc).isoformat()

    schedule = Schedule(
        id=str(uuid.uuid4()),
        name=body.name,
        platform_id=body.platform_id,
        endpoint_id=body.endpoint_id,
        columns=body.columns,
        filters=body.filters,
        limit=body.limit,
        destination=body.destination,
        schedule_config=body.schedule_config,
        enabled=body.enabled,
        created_at=now,
        updated_at=now,
    )

    schedules.append(schedule.model_dump())
    _save_schedules(schedules)

    # 有効な場合、スケジューラに登録
    try:
        from app.core.scheduler import add_job

        if schedule.enabled:
            add_job(schedule)
    except Exception as e:
        logger.warning("スケジューラへのジョブ登録に失敗: %s", e)

    logger.info("スケジュール作成: %s (%s)", schedule.id, schedule.name)
    return schedule


@router.get("/{schedule_id}")
async def get_schedule(schedule_id: str) -> Schedule:
    """IDでスケジュールを取得する。"""
    schedules = _load_schedules()
    for s in schedules:
        if s["id"] == schedule_id:
            return Schedule(**s)
    raise HTTPException(status_code=404, detail=f"スケジュール '{schedule_id}' が見つかりません")


@router.put("/{schedule_id}")
async def update_schedule(schedule_id: str, body: ScheduleUpdate) -> Schedule:
    """既存のスケジュールを更新する。"""
    schedules = _load_schedules()
    for i, s in enumerate(schedules):
        if s["id"] == schedule_id:
            update_data = body.model_dump(exclude_unset=True)
            # ネストされたモデルを辞書に変換
            if "destination" in update_data and update_data["destination"] is not None:
                update_data["destination"] = (
                    update_data["destination"]
                    if isinstance(update_data["destination"], dict)
                    else update_data["destination"]
                )
            if "schedule_config" in update_data and update_data["schedule_config"] is not None:
                update_data["schedule_config"] = (
                    update_data["schedule_config"]
                    if isinstance(update_data["schedule_config"], dict)
                    else update_data["schedule_config"]
                )
            s.update(update_data)
            s["updated_at"] = datetime.now(timezone.utc).isoformat()
            schedules[i] = s
            _save_schedules(schedules)

            updated = Schedule(**s)

            # スケジューラを再登録
            try:
                from app.core.scheduler import remove_job, add_job

                remove_job(schedule_id)
                if updated.enabled:
                    add_job(updated)
            except Exception as e:
                logger.warning("スケジューラジョブの更新に失敗: %s", e)

            logger.info("スケジュール更新: %s", schedule_id)
            return updated

    raise HTTPException(status_code=404, detail=f"スケジュール '{schedule_id}' が見つかりません")


@router.delete("/{schedule_id}", status_code=204)
async def delete_schedule(schedule_id: str) -> None:
    """スケジュールを削除する。"""
    schedules = _load_schedules()
    new_schedules = [s for s in schedules if s["id"] != schedule_id]
    if len(new_schedules) == len(schedules):
        raise HTTPException(status_code=404, detail=f"スケジュール '{schedule_id}' が見つかりません")

    _save_schedules(new_schedules)

    # スケジューラから削除
    try:
        from app.core.scheduler import remove_job

        remove_job(schedule_id)
    except Exception as e:
        logger.warning("スケジューラジョブの削除に失敗: %s", e)

    logger.info("スケジュール削除: %s", schedule_id)


@router.post("/{schedule_id}/run")
async def trigger_schedule(schedule_id: str, background_tasks: BackgroundTasks) -> dict:
    """スケジュールを即座に手動実行する。"""
    schedules = _load_schedules()
    schedule_data = None
    for s in schedules:
        if s["id"] == schedule_id:
            schedule_data = s
            break
    if not schedule_data:
        raise HTTPException(status_code=404, detail=f"スケジュール '{schedule_id}' が見つかりません")

    # FastAPI BackgroundTasksを使用（Cloud Runでリクエスト完了後も実行される）
    from app.core.scheduler import _execute_scheduled_job
    background_tasks.add_task(_execute_scheduled_job, schedule_data)

    return {"ok": True, "message": f"ジョブ '{schedule_data.get('name', schedule_id)}' の実行を開始しました"}
