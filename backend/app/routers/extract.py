import asyncio
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
    limit: int = Query(100, ge=0, description="Number of records (0=unlimited)"),
    cursor: str | None = Query(None, description="Pagination cursor"),
    filters: str | None = Query(None, description="JSON array of filter definitions"),
    keyword: str | None = Query(None, description="Search keyword (used by Yahoo ItemSearch etc.)"),
    fetch_all: bool = Query(False, description="Paginate through ALL pages up to limit"),
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

    # Derive start_date/end_date from filters for API-level filtering
    start_date: str | None = None
    end_date: str | None = None
    if filter_list:
        from datetime import datetime, timedelta, timezone
        for f in filter_list:
            if f.operator == "last_n_days" and f.column in ("createdAt", "created_at", "CreatedAfter"):
                n_days = int(f.value)
                start_date = (datetime.now(timezone.utc) - timedelta(days=n_days)).strftime("%Y-%m-%d")
                break

    effective_limit = float('inf') if limit == 0 else limit

    try:
        if fetch_all:
            # Paginate through all pages up to the requested limit
            all_items: list[dict] = []
            current_cursor = cursor
            result_meta: dict = {}

            while len(all_items) < effective_limit:
                page_limit = min(int(effective_limit - len(all_items)), 100)
                result = await client.extract_data(
                    endpoint_id=endpoint_id,
                    columns=column_list,
                    limit=page_limit,
                    cursor=current_cursor,
                    start_date=start_date,
                    end_date=end_date,
                    keyword=keyword,
                )
                all_items.extend(result.get("items", []))
                result_meta = result

                next_cursor = result.get("next_cursor")
                if not next_cursor or not result.get("items"):
                    break
                current_cursor = next_cursor
                # Rate limiting is handled by per-platform rate limiters

            result_meta["items"] = all_items[:int(effective_limit)] if effective_limit != float('inf') else all_items
            result_meta["next_cursor"] = None if len(all_items) <= effective_limit else result_meta.get("next_cursor")
            result = result_meta
        else:
            result = await client.extract_data(
                endpoint_id=endpoint_id,
                columns=column_list,
                limit=limit,
                cursor=cursor,
                start_date=start_date,
                end_date=end_date,
                keyword=keyword,
            )

        # Apply post-extraction filtering with type-aware comparisons
        if filter_list and "items" in result:
            # Build field type map from schema for correct type coercion
            field_types: dict[str, str] = {}
            try:
                schema = await client.get_schema(endpoint_id)
                fields = schema.get("fields", []) if isinstance(schema, dict) else schema
                field_types = {f.get("name", ""): f.get("type", "string") for f in fields}
            except Exception:
                logger.warning("Could not fetch schema for %s/%s, filtering without type info", platform_id, endpoint_id)
            result["items"] = apply_filters(result["items"], filter_list, field_types)
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
