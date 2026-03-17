import logging

from fastapi import APIRouter, HTTPException, Query

from app.core.filters import apply_filters, parse_filters
from app.platforms.registry import get_client

logger = logging.getLogger("ecommerce_data_extractor.extract")

router = APIRouter(prefix="/extract", tags=["extract"])


@router.get("/")
async def extract_data(
    platform_id: str = Query(..., description="Platform ID"),
    endpoint_id: str = Query(..., description="Endpoint ID"),
    columns: str | None = Query(None, description="Comma-separated column names"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records"),
    cursor: str | None = Query(None, description="Pagination cursor"),
    filters: str | None = Query(None, description="JSON array of filter definitions"),
) -> dict:
    """Extract data from a specific platform endpoint."""
    client = get_client(platform_id)
    if not client:
        raise HTTPException(status_code=404, detail=f"Platform '{platform_id}' not found")
    if not client.is_configured():
        raise HTTPException(
            status_code=400,
            detail=f"Platform '{platform_id}' is not configured. Set API credentials via settings.",
        )

    column_list = [c.strip() for c in columns.split(",")] if columns else None

    # Parse filters if provided
    filter_list = None
    if filters:
        try:
            filter_list = parse_filters(filters)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid filters: {e}")

    try:
        result = await client.extract_data(
            endpoint_id=endpoint_id,
            columns=column_list,
            limit=limit,
            cursor=cursor,
        )

        # Apply post-extraction filtering
        if filter_list and "items" in result:
            result["items"] = apply_filters(result["items"], filter_list)
            result["filtered_count"] = len(result["items"])

        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Extract failed for %s/%s", platform_id, endpoint_id)
        # Surface the actual API error message to help debugging
        detail = str(e)
        if hasattr(e, "response"):
            try:
                err_body = e.response.json()
                errors = err_body.get("errors", [])
                if errors:
                    detail = "; ".join(
                        f"{err.get('code', '')}: {err.get('message', '')}"
                        for err in errors
                    )
            except Exception:
                detail = e.response.text[:500] if e.response.text else str(e)
        raise HTTPException(
            status_code=502,
            detail=f"{platform_id}/{endpoint_id}: {detail}",
        )
