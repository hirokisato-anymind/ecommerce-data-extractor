from fastapi import APIRouter, HTTPException

from app.platforms.registry import get_client

router = APIRouter(prefix="/platforms", tags=["endpoints"])


@router.get("/{platform_id}/endpoints")
async def list_endpoints(platform_id: str) -> list[dict]:
    """List available endpoints for a platform."""
    client = get_client(platform_id)
    if not client:
        raise HTTPException(status_code=404, detail=f"Platform '{platform_id}' not found")
    return await client.get_endpoints()
