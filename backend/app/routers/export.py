import logging

from fastapi import APIRouter, HTTPException, Query

from app.core.export import stream_csv, stream_json
from app.platforms.registry import get_client

logger = logging.getLogger("ecommerce_data_extractor.export")

router = APIRouter(prefix="/export", tags=["export"])


@router.get("/{format}")
async def export_data(
    format: str,
    platform_id: str = Query(..., description="Platform ID"),
    endpoint_id: str = Query(..., description="Endpoint ID"),
    columns: str | None = Query(None, description="Comma-separated column names"),
    limit: int = Query(100, ge=1, le=10000, description="Number of records"),
):
    """Export extracted data in CSV or JSON format."""
    if format not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="Format must be 'csv' or 'json'")

    client = get_client(platform_id)
    if not client:
        raise HTTPException(status_code=404, detail=f"Platform '{platform_id}' not found")
    if not client.is_configured():
        raise HTTPException(status_code=400, detail=f"Platform '{platform_id}' is not configured")

    column_list = [c.strip() for c in columns.split(",")] if columns else None

    try:
        result = await client.extract_data(
            endpoint_id=endpoint_id,
            columns=column_list,
            limit=limit,
            cursor=None,
        )
    except Exception as e:
        logger.exception("Export failed for %s/%s", platform_id, endpoint_id)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch data from {platform_id}. Check server logs for details.",
        )

    items = result.get("items", [])
    result_columns = result.get("columns", [])

    if format == "csv":
        return stream_csv(items, result_columns)
    else:
        return stream_json(items)
