from abc import ABC, abstractmethod
from typing import Any

from app.core.read_only import ReadOnlyHttpClient


class PlatformClient(ABC):
    platform_id: str
    platform_name: str

    @abstractmethod
    async def get_endpoints(self) -> list[dict]:
        """Return list of available endpoints with id, name, description."""
        pass

    @abstractmethod
    async def get_schema(self, endpoint_id: str) -> dict:
        """Return field schema for an endpoint."""
        pass

    @abstractmethod
    async def extract_data(
        self,
        endpoint_id: str,
        columns: list[str] | None,
        limit: int,
        cursor: str | None,
    ) -> dict:
        """Extract data from endpoint. Returns {items, columns, next_cursor, total}."""
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if API credentials are set."""
        pass
