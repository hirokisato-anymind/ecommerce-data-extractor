from fastapi import APIRouter, HTTPException

from app.platforms.registry import get_client

router = APIRouter(prefix="/platforms", tags=["schema"])


@router.get("/{platform_id}/endpoints/{endpoint_id}/schema")
async def get_schema(platform_id: str, endpoint_id: str) -> list[dict]:
    """Get field schema for a specific platform endpoint."""
    client = get_client(platform_id)
    if not client:
        raise HTTPException(status_code=404, detail=f"Platform '{platform_id}' not found")
    schema = await client.get_schema(endpoint_id)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Endpoint '{endpoint_id}' not found")
    # Normalize: some clients return {"fields": [...]}, others return list directly
    if isinstance(schema, dict) and "fields" in schema:
        fields = schema["fields"]
    elif isinstance(schema, list):
        fields = schema
    else:
        fields = []
    # Normalize field format to {key, label, type}
    result = []
    for f in fields:
        result.append({
            "key": f.get("key") or f.get("name", ""),
            "label": f.get("label") or f.get("name") or f.get("key", ""),
            "type": f.get("type", "string"),
            "description": f.get("description", ""),
        })
    return result
