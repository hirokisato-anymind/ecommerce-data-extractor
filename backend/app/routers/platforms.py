from fastapi import APIRouter

from app.platforms.registry import get_all_clients

router = APIRouter(prefix="/platforms", tags=["platforms"])


@router.get("")
async def list_platforms() -> list[dict]:
    """List all available platforms and their configuration status."""
    clients = get_all_clients()
    return [
        {
            "id": c.platform_id,
            "name": c.platform_name,
            "configured": c.is_configured(),
        }
        for c in clients
    ]
