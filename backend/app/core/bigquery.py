"""BigQueryへのデータ書き込みモジュール。"""

import logging
import re
from enum import Enum

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger("ecommerce_data_extractor.bigquery")

# +0900 -> +09:00 のような修正が必要なタイムゾーンオフセットパターン
_TZ_OFFSET_RE = re.compile(r"([+-]\d{2})(\d{2})$")


def _normalize_timestamp(value: str) -> str:
    """BQが受け付けるタイムスタンプ形式に正規化する。

    例: 2026-03-17T23:06:39+0900 -> 2026-03-17T23:06:39+09:00
    """
    m = _TZ_OFFSET_RE.search(value)
    if m:
        value = value[: m.start()] + m.group(1) + ":" + m.group(2)
    return value


def _normalize_rows(rows: list[dict], schema: list[bigquery.SchemaField]) -> list[dict]:
    """スキーマのTIMESTAMP/DATETIME型カラムの値をBQ互換形式に変換する。"""
    ts_fields = {
        f.name for f in schema
        if f.field_type in ("TIMESTAMP", "DATETIME", "DATE")
    }
    if not ts_fields:
        return rows
    normalized = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if k in ts_fields and isinstance(v, str) and v:
                new_row[k] = _normalize_timestamp(v)
            else:
                new_row[k] = v
        normalized.append(new_row)
    return normalized


class TransferMode(str, Enum):
    APPEND = "append"
    APPEND_DIRECT = "append_direct"
    REPLACE = "replace"
    DELETE_IN_ADVANCE = "delete_in_advance"
    UPSERT = "upsert"


def _get_client(
    project_id: str,
    location: str = "US",
) -> bigquery.Client:
    """保存済みGoogle OAuthトークンを使ってBigQueryクライアントを取得する。"""
    from app.routers.bigquery import _get_credentials

    creds = _get_credentials()
    if creds:
        return bigquery.Client(project=project_id, credentials=creds, location=location)
    # フォールバック: ADC
    return bigquery.Client(project=project_id, location=location)


_ISO_DATE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


def _infer_field_type(value: object) -> str:
    """単一の値からBigQueryフィールド型を推定する。"""
    if value is None:
        return "STRING"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, dict):
        return "JSON"
    if isinstance(value, list):
        return "JSON"
    if isinstance(value, str) and _ISO_DATE_RE.match(value):
        return "TIMESTAMP" if "T" in value else "DATE"
    return "STRING"


def _infer_schema(rows: list[dict]) -> list[bigquery.SchemaField]:
    """複数行を走査してスキーマを推定する。None以外の最初の値で型を決定する。"""
    # 全行からキーを収集（順序保持）
    seen_keys: dict[str, str] = {}
    for row in rows:
        for key, value in row.items():
            if key not in seen_keys or seen_keys[key] == "STRING":
                inferred = _infer_field_type(value)
                # None由来のSTRINGは仮置き。より具体的な型が見つかれば上書きする
                if key not in seen_keys or (seen_keys[key] == "STRING" and inferred != "STRING"):
                    seen_keys[key] = inferred

    return [
        bigquery.SchemaField(key, field_type, mode="NULLABLE")
        for key, field_type in seen_keys.items()
    ]


def _ensure_dataset_exists(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
    """データセットが存在しない場合は作成する。"""
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        logger.info("データセット %s が存在しないため作成します", dataset_ref)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logger.info("データセット %s を作成しました", dataset_ref)


def _get_platform_bq_schema(
    platform_id: str | None,
    endpoint_id: str | None,
) -> list[bigquery.SchemaField] | None:
    """プラットフォームのスキーマ定義から BigQuery スキーマを構築する。
    bq_type が定義されていない場合は None を返す。"""
    if not platform_id or not endpoint_id:
        return None
    try:
        schemas: dict | None = None
        if platform_id == "rakuten":
            from app.platforms.rakuten.client import SCHEMAS
            schemas = SCHEMAS
        elif platform_id == "shopify":
            from app.platforms.shopify.client import ENDPOINT_SCHEMAS
            schemas = ENDPOINT_SCHEMAS
        elif platform_id == "amazon":
            from app.platforms.amazon.client import ENDPOINT_SCHEMAS as AMAZON_SCHEMAS
            schemas = AMAZON_SCHEMAS
        elif platform_id == "yahoo":
            from app.platforms.yahoo.client import ENDPOINT_SCHEMAS as YAHOO_SCHEMAS
            schemas = YAHOO_SCHEMAS

        if not schemas or endpoint_id not in schemas:
            return None
        schema_def = schemas[endpoint_id]
        fields = schema_def.get("fields", []) if isinstance(schema_def, dict) else schema_def
        if not fields or not any(f.get("bq_type") for f in fields):
            return None
        return [
            bigquery.SchemaField(f["name"], f["bq_type"], mode="NULLABLE")
            for f in fields
            if f.get("bq_type")
        ]
    except Exception as e:
        logger.warning("プラットフォームスキーマの取得に失敗 (%s/%s): %s", platform_id, endpoint_id, e)
        return None


def _ensure_table_exists(
    client: bigquery.Client,
    table_ref: str,
    rows: list[dict],
    location: str = "US",
    platform_id: str | None = None,
    endpoint_id: str | None = None,
) -> bigquery.Table:
    """テーブルが存在しない場合、プラットフォームのスキーマ定義または行データからスキーマを検出して作成する。
    データセットが存在しない場合も自動で作成する。"""
    try:
        existing = client.get_table(table_ref)
        if existing.schema:
            return existing
        # Table exists but has empty schema (broken state) — drop and recreate
        logger.warning("テーブル %s のスキーマが空のため再作成します", table_ref)
        client.delete_table(table_ref)
    except NotFound:
        pass
    logger.info("テーブル %s を作成します", table_ref)

    # 行データからスキーマを推定
    if not rows:
        raise ValueError("テーブルが存在せず、スキーマ推定用のデータもありません")

    # データセットが存在しない場合は作成
    parts = table_ref.split(".")
    if len(parts) == 3:
        _ensure_dataset_exists(client, parts[0], parts[1], location)

    # プラットフォーム定義のスキーマを優先、なければデータから推定
    schema = _get_platform_bq_schema(platform_id, endpoint_id)
    if schema:
        logger.info("プラットフォームスキーマ定義を使用 (%s/%s)", platform_id, endpoint_id)
    else:
        logger.warning("プラットフォームスキーマなし、データから推定します (%s/%s)", platform_id, endpoint_id)
        schema = _infer_schema(rows)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    logger.info("テーブル %s を作成しました（%d カラム）", table_ref, len(schema))
    return client.get_table(table_ref)


def delete_all_rows(
    project_id: str,
    dataset_id: str,
    table_id: str,
    location: str = "US",
) -> None:
    """テーブルの全行を削除する（スキーマは保持）。テーブルが存在しない場合はスキップ。"""
    client = _get_client(project_id, location=location)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)
        client.query(f"DELETE FROM `{table_ref}` WHERE TRUE").result()
        logger.info("テーブル %s の全行を削除しました", table_ref)
    except NotFound:
        logger.info("テーブル %s は存在しないためスキップ", table_ref)


async def write_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: list[dict],
    mode: TransferMode,
    key_columns: list[str] | None = None,
    location: str = "US",
    platform_id: str | None = None,
    endpoint_id: str | None = None,
) -> dict:
    """BigQueryテーブルにデータを書き込む。統計情報の辞書を返す。"""
    if not rows:
        return {
            "rows_written": 0,
            "mode": mode.value,
            "table": f"{project_id}.{dataset_id}.{table_id}",
        }

    if mode in (TransferMode.UPSERT, TransferMode.DELETE_IN_ADVANCE):
        if not key_columns:
            raise ValueError(
                f"転送モード '{mode.value}' にはキーカラムの指定が必要です"
            )

    client = _get_client(project_id, location=location)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    table = _ensure_table_exists(client, table_ref, rows, location=location, platform_id=platform_id, endpoint_id=endpoint_id)

    # タイムスタンプ文字列をBQ互換形式に正規化 (+0900 -> +09:00 等)
    if table.schema:
        rows = _normalize_rows(rows, table.schema)

    def _make_load_config(write_disposition) -> bigquery.LoadJobConfig:
        """既存テーブルのスキーマがあればそれを使い、なければautodetectにする。"""
        if table.schema:
            cfg = bigquery.LoadJobConfig(
                schema=table.schema,
                write_disposition=write_disposition,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            # schema_update_options は WRITE_APPEND でのみ使用可能
            if write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
                cfg.schema_update_options = [
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ]
            return cfg
        return bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

    def _load_to_temp_table(src_rows: list[dict]) -> str:
        """一時テーブルにデータをロードし、テーブル参照を返す。"""
        temp_id = f"{table_id}_tmp_{int(__import__('time').time())}"
        temp_ref = f"{project_id}.{dataset_id}.{temp_id}"
        _ensure_dataset_exists(client, project_id, dataset_id, location)
        # Use schema from the already-ensured table object instead of re-fetching
        schema = table.schema
        if not schema:
            raise ValueError(f"テーブル {table_ref} のスキーマが空です。データをロードできません。")
        temp_tbl = bigquery.Table(temp_ref, schema=schema)
        client.create_table(temp_tbl)
        cfg = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = client.load_table_from_json(src_rows, temp_ref, job_config=cfg)
        job.result()
        return temp_ref

    def _merge_sql(temp_ref: str, on_cols: list[str], *, update: bool) -> str:
        """MERGE SQLを構築する。update=FalseならINSERTのみ（APPEND用）。"""
        all_cols = [f.name for f in table.schema]
        on_clause = " AND ".join(f"T.`{c}` = S.`{c}`" for c in on_cols)
        insert_cols = ", ".join(f"`{c}`" for c in all_cols)
        insert_vals = ", ".join(f"S.`{c}`" for c in all_cols)

        sql = f"MERGE `{table_ref}` T USING `{temp_ref}` S ON {on_clause}\n"
        if update:
            upd_cols = [c for c in all_cols if c not in on_cols] or all_cols
            upd_clause = ", ".join(f"T.`{c}` = S.`{c}`" for c in upd_cols)
            sql += f"WHEN MATCHED THEN UPDATE SET {upd_clause}\n"
        sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        return sql

    def _safe_sql_value(value: object) -> str:
        """値をSQL用にエスケープする。"""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        # 文字列: シングルクォートをエスケープ
        return "'" + str(value).replace("'", "\\'") + "'"

    if mode == TransferMode.REPLACE:
        job_config = _make_load_config(bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

    elif mode == TransferMode.APPEND_DIRECT:
        # 重複チェックなしで単純追加
        job_config = _make_load_config(bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

    elif mode == TransferMode.APPEND:
        # キーカラムが指定されている場合: MERGEで新規行のみINSERT（重複スキップ）
        # キーカラムなし: APPEND_DIRECTと同じ動作
        if key_columns:
            temp_ref = _load_to_temp_table(rows)
            try:
                sql = _merge_sql(temp_ref, key_columns, update=False)
                result_job = client.query(sql)
                result_job.result()
                logger.info("APPEND (重複スキップ) 完了: %s", table_ref)
            finally:
                client.delete_table(temp_ref, not_found_ok=True)
        else:
            job_config = _make_load_config(bigquery.WriteDisposition.WRITE_APPEND)
            job = client.load_table_from_json(rows, table_ref, job_config=job_config)
            job.result()

    elif mode == TransferMode.DELETE_IN_ADVANCE:
        # キーカラムに一致する行を削除してから挿入
        unique_key_values: set[tuple] = set()
        for row in rows:
            unique_key_values.add(tuple(row.get(k) for k in key_columns))

        if len(key_columns) == 1:
            col = key_columns[0]
            vals = ", ".join(
                _safe_sql_value(kv[0]) for kv in unique_key_values
            )
            delete_sql = f"DELETE FROM `{table_ref}` WHERE `{col}` IN ({vals})"
        else:
            conditions = []
            for kv in unique_key_values:
                parts = [
                    f"`{key_columns[i]}` = {_safe_sql_value(kv[i])}"
                    for i in range(len(key_columns))
                ]
                conditions.append(f"({' AND '.join(parts)})")
            delete_sql = f"DELETE FROM `{table_ref}` WHERE " + " OR ".join(conditions)

        client.query(delete_sql).result()
        logger.info("既存データを削除しました: %s", table_ref)

        job_config = _make_load_config(bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

    elif mode == TransferMode.UPSERT:
        # 一時テーブル→MERGEでUPSERT（マッチ時UPDATE、非マッチ時INSERT）
        temp_ref = _load_to_temp_table(rows)
        try:
            sql = _merge_sql(temp_ref, key_columns, update=True)
            client.query(sql).result()
            logger.info("UPSERT完了: %s", table_ref)
        finally:
            client.delete_table(temp_ref, not_found_ok=True)

    result = {
        "rows_written": len(rows),
        "mode": mode.value,
        "table": f"{project_id}.{dataset_id}.{table_id}",
    }
    logger.info("BigQuery書き込み完了: %s", result)
    return result
