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
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        keyword: str | None = None,
    ) -> dict:
        """Extract data from endpoint. Returns {items, columns, next_cursor, total}.

        Parameters
        ----------
        start_date, end_date : str | None
            ISO-8601 date strings (e.g. ``2024-01-01``) for date-range filtering.
            Platform clients translate these into the appropriate API parameters.
        keyword : str | None
            Search keyword (e.g. for Yahoo ItemSearch query parameter).
        """
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if API credentials are set."""
        pass
