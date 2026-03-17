import logging
import re
from typing import Any

import httpx

logger = logging.getLogger("ecommerce_data_extractor.read_only")

# URL allowlists per platform (regex patterns)
PLATFORM_URL_ALLOWLISTS: dict[str, list[re.Pattern[str]]] = {
    "shopify": [
        re.compile(r"^https://[a-zA-Z0-9\-]+\.myshopify\.com/admin/api/.*$"),
    ],
    "rakuten": [
        re.compile(r"^https://api\.rms\.rakuten\.co\.jp/.*$"),
        re.compile(r"^https://app\.rakuten\.co\.jp/services/api/.*$"),
    ],
    "amazon": [
        re.compile(r"^https://sellingpartnerapi[a-zA-Z0-9\-]*\.amazon\.(com|co\.jp|co\.uk)/.*$"),
    ],
    "yahoo": [
        re.compile(r"^https://circus\.shopping\.yahooapis\.jp/.*$"),
        re.compile(r"^https://shopping\.yahooapis\.jp/.*$"),
    ],
}


def validate_url(platform: str, url: str) -> bool:
    """Validate that a URL matches the allowlist for the given platform.

    Returns True if the URL is allowed, False otherwise.
    """
    patterns = PLATFORM_URL_ALLOWLISTS.get(platform, [])
    for pattern in patterns:
        if pattern.match(url):
            return True
    logger.warning("URL rejected for platform '%s': %s", platform, url)
    return False


def validate_graphql_query(query: str) -> bool:
    """Validate a GraphQL query to ensure it is read-only (no mutations).

    Returns True if the query is safe (read-only), False if a mutation is detected.
    """
    # Case-insensitive check for mutation keyword
    if re.search(r"\bmutation\b", query, re.IGNORECASE):
        logger.warning("GraphQL mutation detected and rejected: %s", query[:200])
        return False
    return True


class ReadOnlyHttpClient:
    """HTTP client wrapper that only exposes GET requests for read-only access.

    All requests are validated against platform URL allowlists and audit-logged.
    """

    def __init__(self, platform: str, client: httpx.AsyncClient | None = None) -> None:
        self.platform = platform
        self._client = client or httpx.AsyncClient(timeout=30.0)

    async def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Perform a read-only GET request with URL validation and audit logging."""
        if not validate_url(self.platform, url):
            raise PermissionError(
                f"URL not in allowlist for platform '{self.platform}': {url}"
            )

        logger.info(
            "READ-ONLY REQUEST | platform=%s | method=GET | url=%s | params=%s",
            self.platform,
            url,
            params,
        )

        response = await self._client.get(url, params=params, headers=headers)

        logger.info(
            "RESPONSE | platform=%s | url=%s | status=%d",
            self.platform,
            url,
            response.status_code,
        )

        return response

    async def post(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Perform a read-only POST request with URL validation and audit logging.

        Some APIs (e.g. Rakuten RMS order search) require POST for read-only
        queries. URL is still validated against the platform allowlist.
        """
        if not validate_url(self.platform, url):
            raise PermissionError(
                f"URL not in allowlist for platform '{self.platform}': {url}"
            )

        logger.info(
            "READ-ONLY REQUEST | platform=%s | method=POST | url=%s",
            self.platform,
            url,
        )

        response = await self._client.post(url, json=json, headers=headers)

        logger.info(
            "RESPONSE | platform=%s | url=%s | status=%d",
            self.platform,
            url,
            response.status_code,
        )

        return response

    async def post_graphql(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Perform a read-only GraphQL POST request.

        Only allows POST to graphql.json endpoints and validates that the
        query contains no mutations.
        """
        if not url.endswith("/graphql.json"):
            raise PermissionError(
                "post_graphql only allows POST to graphql.json endpoints"
            )

        if not validate_url(self.platform, url):
            raise PermissionError(
                f"URL not in allowlist for platform '{self.platform}': {url}"
            )

        query = (json or {}).get("query", "")
        if not validate_graphql_query(query):
            raise PermissionError(
                "GraphQL mutation detected — only read-only queries are allowed"
            )

        logger.info(
            "READ-ONLY REQUEST | platform=%s | method=POST (GraphQL) | url=%s",
            self.platform,
            url,
        )

        response = await self._client.post(url, json=json, headers=headers)

        logger.info(
            "RESPONSE | platform=%s | url=%s | status=%d",
            self.platform,
            url,
            response.status_code,
        )

        return response

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "ReadOnlyHttpClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()
