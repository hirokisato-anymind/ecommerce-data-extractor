"""Cloud Scheduler ジョブ管理モジュール。"""

import logging
import os

from google.protobuf import duration_pb2

from app.core.storage import is_cloud_mode

logger = logging.getLogger("ecommerce_data_extractor.cloud_scheduler")

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
CLOUD_RUN_URL = os.environ.get("CLOUD_RUN_URL", "")
CLOUD_SCHEDULER_LOCATION = os.environ.get("CLOUD_SCHEDULER_LOCATION", "asia-northeast1")
SCHEDULER_TIMEZONE = os.environ.get("SCHEDULER_TIMEZONE", "Asia/Tokyo")
SCHEDULER_SERVICE_ACCOUNT = os.environ.get("SCHEDULER_SERVICE_ACCOUNT", "")


def _job_name(schedule_id: str) -> str:
    """Cloud Scheduler ジョブのフルリソース名を返す。"""
    return f"projects/{GCP_PROJECT}/locations/{CLOUD_SCHEDULER_LOCATION}/jobs/ede-{schedule_id}"


def _parent() -> str:
    return f"projects/{GCP_PROJECT}/locations/{CLOUD_SCHEDULER_LOCATION}"


def schedule_config_to_cron(config) -> str:
    """ScheduleConfig を5フィールドのcron文字列に変換する。

    Day of week conversion: app uses 0=Mon..6=Sun, cron uses 0=Sun.
    Formula: (app_dow + 1) % 7
    """
    if config.frequency == "hourly":
        return f"{config.minute} * * * *"
    elif config.frequency == "daily":
        return f"{config.minute} {config.hour} * * *"
    elif config.frequency == "weekly":
        dow = config.day_of_week if config.day_of_week is not None else 0
        cron_dow = (dow + 1) % 7
        return f"{config.minute} {config.hour} * * {cron_dow}"
    elif config.frequency == "monthly":
        dom = config.day_of_month or 1
        return f"{config.minute} {config.hour} {dom} * *"
    else:
        raise ValueError(f"不正な頻度指定です: {config.frequency}")


def create_cloud_scheduler_job(schedule_id: str, schedule_config, enabled: bool) -> None:
    """Cloud Scheduler にジョブを作成する。"""
    if not is_cloud_mode():
        return

    from google.cloud import scheduler_v1

    client = scheduler_v1.CloudSchedulerClient()
    cron = schedule_config_to_cron(schedule_config)
    target_url = f"{CLOUD_RUN_URL}/api/schedules/{schedule_id}/run"

    job = scheduler_v1.Job(
        name=_job_name(schedule_id),
        schedule=cron,
        time_zone=SCHEDULER_TIMEZONE,
        http_target=scheduler_v1.HttpTarget(
            uri=target_url,
            http_method=scheduler_v1.HttpMethod.POST,
            oidc_token=scheduler_v1.OidcToken(
                service_account_email=SCHEDULER_SERVICE_ACCOUNT,
            ),
        ),
        state=scheduler_v1.Job.State.ENABLED if enabled else scheduler_v1.Job.State.PAUSED,
        attempt_deadline=duration_pb2.Duration(seconds=1800),
    )

    client.create_job(
        request=scheduler_v1.CreateJobRequest(
            parent=_parent(),
            job=job,
        )
    )
    logger.info("Cloud Scheduler ジョブ作成: ede-%s (cron=%s, enabled=%s)", schedule_id, cron, enabled)


def update_cloud_scheduler_job(schedule_id: str, schedule_config, enabled: bool) -> None:
    """Cloud Scheduler の既存ジョブを更新する。"""
    if not is_cloud_mode():
        return

    from google.cloud import scheduler_v1
    from google.protobuf import field_mask_pb2

    client = scheduler_v1.CloudSchedulerClient()
    cron = schedule_config_to_cron(schedule_config)
    target_url = f"{CLOUD_RUN_URL}/api/schedules/{schedule_id}/run"

    job = scheduler_v1.Job(
        name=_job_name(schedule_id),
        schedule=cron,
        time_zone=SCHEDULER_TIMEZONE,
        http_target=scheduler_v1.HttpTarget(
            uri=target_url,
            http_method=scheduler_v1.HttpMethod.POST,
            oidc_token=scheduler_v1.OidcToken(
                service_account_email=SCHEDULER_SERVICE_ACCOUNT,
            ),
        ),
        state=scheduler_v1.Job.State.ENABLED if enabled else scheduler_v1.Job.State.PAUSED,
        attempt_deadline=duration_pb2.Duration(seconds=1800),
    )

    client.update_job(
        request=scheduler_v1.UpdateJobRequest(
            job=job,
            update_mask=field_mask_pb2.FieldMask(
                paths=["schedule", "time_zone", "http_target", "state", "attempt_deadline"],
            ),
        )
    )
    logger.info("Cloud Scheduler ジョブ更新: ede-%s (cron=%s, enabled=%s)", schedule_id, cron, enabled)


def delete_cloud_scheduler_job(schedule_id: str) -> None:
    """Cloud Scheduler からジョブを削除する。NotFound は無視する。"""
    if not is_cloud_mode():
        return

    from google.api_core.exceptions import NotFound
    from google.cloud import scheduler_v1

    client = scheduler_v1.CloudSchedulerClient()
    try:
        client.delete_job(
            request=scheduler_v1.DeleteJobRequest(
                name=_job_name(schedule_id),
            )
        )
        logger.info("Cloud Scheduler ジョブ削除: ede-%s", schedule_id)
    except NotFound:
        logger.debug("Cloud Scheduler ジョブ ede-%s が見つかりません（未登録の可能性）", schedule_id)


def sync_all_schedules() -> None:
    """GCS から全スケジュールを読み込み、Cloud Scheduler ジョブを作成する。"""
    if not is_cloud_mode():
        return

    from app.core.scheduler import _load_schedules_from_file
    from app.routers.schedule import ScheduleConfig

    schedules = _load_schedules_from_file()
    count = 0
    for s in schedules:
        schedule_config_data = s.get("schedule_config")
        if not schedule_config_data:
            continue
        config = ScheduleConfig(**schedule_config_data) if isinstance(schedule_config_data, dict) else schedule_config_data
        enabled = s.get("enabled", True)
        try:
            create_cloud_scheduler_job(s["id"], config, enabled)
            count += 1
        except Exception as e:
            logger.warning("Cloud Scheduler ジョブ作成失敗 (%s): %s", s["id"], e)

    logger.info("Cloud Scheduler に %d 件のジョブを同期しました", count)
