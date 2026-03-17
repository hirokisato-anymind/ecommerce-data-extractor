"""BigQueryへのデータ書き込みモジュール。"""

import logging
from enum import Enum

from google.cloud import bigquery

logger = logging.getLogger("ecommerce_data_extractor.bigquery")


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


def _ensure_table_exists(
    client: bigquery.Client,
    table_ref: str,
    rows: list[dict],
) -> bigquery.Table:
    """テーブルが存在しない場合、行データからスキーマを自動検出して作成する。"""
    try:
        return client.get_table(table_ref)
    except Exception:
        logger.info("テーブル %s が存在しないため作成します", table_ref)

    # 行データからスキーマを推定
    if not rows:
        raise ValueError("テーブルが存在せず、スキーマ推定用のデータもありません")

    sample = rows[0]
    schema = []
    for key, value in sample.items():
        if isinstance(value, bool):
            field_type = "BOOLEAN"
        elif isinstance(value, int):
            field_type = "INTEGER"
        elif isinstance(value, float):
            field_type = "FLOAT"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(key, field_type, mode="NULLABLE"))

    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    logger.info("テーブル %s を作成しました", table_ref)
    return client.get_table(table_ref)


async def write_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: list[dict],
    mode: TransferMode,
    key_columns: list[str] | None = None,
    location: str = "US",
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

    _ensure_table_exists(client, table_ref, rows)

    if mode == TransferMode.REPLACE:
        # WRITE_TRUNCATE: テーブルを上書き
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

    elif mode == TransferMode.APPEND_DIRECT:
        # WRITE_APPEND: 重複チェックなしで追加（高速）
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

    elif mode == TransferMode.APPEND:
        # 通常の追加（スキーマ自動検出）
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

    elif mode == TransferMode.DELETE_IN_ADVANCE:
        # キーカラムに一致する行を削除してから挿入
        key_values = set()
        for row in rows:
            key_tuple = tuple(row.get(k) for k in key_columns)
            key_values.add(key_tuple)

        if len(key_columns) == 1:
            col = key_columns[0]
            values_list = [repr(row.get(col)) for row in rows]
            values_str = ", ".join(set(values_list))
            delete_sql = f"DELETE FROM `{table_ref}` WHERE `{col}` IN ({values_str})"
        else:
            conditions = []
            for row in rows:
                parts = [
                    f"`{col}` = {repr(row.get(col))}" for col in key_columns
                ]
                conditions.append(f"({' AND '.join(parts)})")
            # 重複条件を除去
            unique_conditions = list(set(conditions))
            delete_sql = (
                f"DELETE FROM `{table_ref}` WHERE "
                + " OR ".join(unique_conditions)
            )

        client.query(delete_sql).result()
        logger.info("既存データを削除しました: %s", table_ref)

        # 削除後にデータを挿入
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

    elif mode == TransferMode.UPSERT:
        # 一時テーブルにデータを読み込み、MERGE SQLでUPSERT
        temp_table_id = f"{table_id}_temp_{int(__import__('time').time())}"
        temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"

        try:
            # 元テーブルのスキーマをコピーして一時テーブルを作成
            source_table = client.get_table(table_ref)
            temp_table = bigquery.Table(temp_table_ref, schema=source_table.schema)
            client.create_table(temp_table)

            # 一時テーブルにデータをロード
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = client.load_table_from_json(
                rows, temp_table_ref, job_config=job_config
            )
            job.result()

            # MERGE SQL を構築
            on_clause = " AND ".join(
                f"T.`{col}` = S.`{col}`" for col in key_columns
            )

            # 全カラム名を取得
            all_columns = [field.name for field in source_table.schema]
            update_columns = [c for c in all_columns if c not in key_columns]

            update_clause = ", ".join(
                f"T.`{col}` = S.`{col}`" for col in update_columns
            ) if update_columns else ", ".join(
                f"T.`{col}` = S.`{col}`" for col in all_columns
            )

            insert_columns = ", ".join(f"`{col}`" for col in all_columns)
            insert_values = ", ".join(f"S.`{col}`" for col in all_columns)

            merge_sql = f"""
                MERGE `{table_ref}` T
                USING `{temp_table_ref}` S
                ON {on_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """

            client.query(merge_sql).result()
            logger.info("UPSERT完了: %s", table_ref)

        finally:
            # 一時テーブルを削除
            try:
                client.delete_table(temp_table_ref, not_found_ok=True)
            except Exception as e:
                logger.warning("一時テーブル %s の削除に失敗: %s", temp_table_ref, e)

    result = {
        "rows_written": len(rows),
        "mode": mode.value,
        "table": f"{project_id}.{dataset_id}.{table_id}",
    }
    logger.info("BigQuery書き込み完了: %s", result)
    return result
