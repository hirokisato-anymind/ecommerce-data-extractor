import logging
from typing import Any

from app.config import settings
from app.core.rate_limiter import yahoo_limiter, retry_on_429
from app.core.read_only import ReadOnlyHttpClient
from app.platforms.base import PlatformClient

logger = logging.getLogger("ecommerce_data_extractor.yahoo")

# ---------------------------------------------------------------------------
# Endpoint definitions
# ---------------------------------------------------------------------------

ENDPOINT_DEFS: list[dict[str, str]] = [
    {
        "id": "item_search",
        "name": "ItemSearch API - 商品検索",
        "description": "Yahoo!ショッピング商品検索API。キーワードで商品を検索します。",
    },
    {
        "id": "category_ranking",
        "name": "CategoryRanking API - カテゴリランキング",
        "description": "Yahoo!ショッピングカテゴリ別ランキングAPI。",
    },
    {
        "id": "seller_items",
        "name": "StoreItemList API - 出店者商品一覧",
        "description": "出店者の商品一覧を取得します。アクセストークンが必要です。",
    },
    {
        "id": "seller_orders",
        "name": "OrderList API - 出店者注文一覧",
        "description": "出店者の注文一覧を取得します。アクセストークンが必要です。",
    },
]

# ---------------------------------------------------------------------------
# Schema definitions per endpoint
# ---------------------------------------------------------------------------

ENDPOINT_SCHEMAS: dict[str, dict] = {
    "item_search": {
        "fields": [
            {"name": "name", "type": "string", "description": "商品名"},
            {"name": "description", "type": "string", "description": "商品説明"},
            {"name": "price", "type": "integer", "description": "価格"},
            {"name": "url", "type": "string", "description": "商品ページURL"},
            {"name": "imageUrl", "type": "string", "description": "商品画像URL"},
            {"name": "reviewAverage", "type": "number", "description": "レビュー平均評価"},
            {"name": "reviewCount", "type": "integer", "description": "レビュー数"},
            {"name": "shopName", "type": "string", "description": "ストア名"},
            {"name": "shopUrl", "type": "string", "description": "ストアURL"},
            {"name": "janCode", "type": "string", "description": "JANコード"},
            {"name": "brand", "type": "string", "description": "ブランド名"},
        ],
    },
    "category_ranking": {
        "fields": [
            {"name": "rank", "type": "integer", "description": "ランキング順位"},
            {"name": "name", "type": "string", "description": "商品名"},
            {"name": "price", "type": "integer", "description": "価格"},
            {"name": "url", "type": "string", "description": "商品ページURL"},
            {"name": "imageUrl", "type": "string", "description": "商品画像URL"},
            {"name": "reviewAverage", "type": "number", "description": "レビュー平均評価"},
            {"name": "reviewCount", "type": "integer", "description": "レビュー数"},
            {"name": "shopName", "type": "string", "description": "ストア名"},
        ],
    },
    "seller_items": {
        "fields": [
            {"name": "itemCode", "type": "string", "description": "商品コード"},
            {"name": "title", "type": "string", "description": "商品タイトル"},
            {"name": "price", "type": "integer", "description": "販売価格"},
            {"name": "originalPrice", "type": "integer", "description": "定価"},
            {"name": "availability", "type": "string", "description": "在庫状況"},
            {"name": "updateTime", "type": "datetime", "description": "更新日時"},
        ],
    },
    "seller_orders": {
        "fields": [
            {"name": "orderId", "type": "string", "description": "注文ID"},
            {"name": "orderTime", "type": "datetime", "description": "注文日時"},
            {"name": "orderStatus", "type": "string", "description": "注文ステータス"},
            {"name": "totalPrice", "type": "integer", "description": "合計金額"},
            {"name": "paymentMethod", "type": "string", "description": "支払方法"},
            {"name": "shipStatus", "type": "string", "description": "配送ステータス"},
            {"name": "buyerName", "type": "string", "description": "購入者名"},
        ],
    },
}

# ---------------------------------------------------------------------------
# API URL configuration
# ---------------------------------------------------------------------------

_ENDPOINT_URLS: dict[str, str] = {
    "item_search": "https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch",
    "category_ranking": "https://shopping.yahooapis.jp/ShoppingWebService/V3/categoryRanking",
    "seller_items": "https://circus.shopping.yahooapis.jp/ShopWebService/V1/storeItemList",
    "seller_orders": "https://circus.shopping.yahooapis.jp/ShopWebService/V1/orderList",
}

# Endpoints that require seller authentication (Bearer token)
_SELLER_ENDPOINTS = {"seller_items", "seller_orders"}

# Endpoints that use public appid-based authentication
_PUBLIC_ENDPOINTS = {"item_search", "category_ranking"}

# Yahoo API response root keys for extracting item lists
_RESPONSE_ROOT_KEYS: dict[str, list[str]] = {
    "item_search": ["hits"],
    "category_ranking": ["RankingData"],
    "seller_items": ["ResultSet", "Result", "Item"],
    "seller_orders": ["ResultSet", "Result", "Order"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_items(body: dict[str, Any], endpoint_id: str) -> list[dict[str, Any]]:
    """Navigate the Yahoo API JSON response to extract the list of items.

    Yahoo APIs nest results under various keys depending on the endpoint.
    This function walks the configured key path to find the item list.
    """
    keys = _RESPONSE_ROOT_KEYS.get(endpoint_id, [])
    current: Any = body

    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return []

    if isinstance(current, list):
        return current
    if isinstance(current, dict):
        # Some endpoints return a single item as a dict instead of a list
        return [current]
    return []


def _flatten_item(raw: dict[str, Any], endpoint_id: str) -> dict[str, Any]:
    """Flatten a single raw item from the Yahoo API response.

    Yahoo item search results nest item data under an '_item' or 'Item' key
    in some cases. This normalizes the structure to a flat dict with the
    expected field names.
    """
    # Item search results sometimes wrap item data
    if endpoint_id == "item_search":
        # V3 itemSearch returns flat hit objects directly
        return {
            "name": raw.get("name"),
            "description": raw.get("description"),
            "price": raw.get("price"),
            "url": raw.get("url"),
            "imageUrl": raw.get("image", {}).get("small") if isinstance(raw.get("image"), dict) else raw.get("imageUrl"),
            "reviewAverage": raw.get("review", {}).get("rate") if isinstance(raw.get("review"), dict) else raw.get("reviewAverage"),
            "reviewCount": raw.get("review", {}).get("count") if isinstance(raw.get("review"), dict) else raw.get("reviewCount"),
            "shopName": raw.get("seller", {}).get("name") if isinstance(raw.get("seller"), dict) else raw.get("shopName"),
            "shopUrl": raw.get("seller", {}).get("url") if isinstance(raw.get("seller"), dict) else raw.get("shopUrl"),
            "janCode": raw.get("janCode") or raw.get("jan"),
            "brand": raw.get("brand", {}).get("name") if isinstance(raw.get("brand"), dict) else raw.get("brand"),
        }

    if endpoint_id == "category_ranking":
        return {
            "rank": raw.get("rank") or raw.get("_no"),
            "name": raw.get("name"),
            "price": raw.get("price"),
            "url": raw.get("url"),
            "imageUrl": raw.get("image", {}).get("small") if isinstance(raw.get("image"), dict) else raw.get("imageUrl"),
            "reviewAverage": raw.get("review", {}).get("rate") if isinstance(raw.get("review"), dict) else raw.get("reviewAverage"),
            "reviewCount": raw.get("review", {}).get("count") if isinstance(raw.get("review"), dict) else raw.get("reviewCount"),
            "shopName": raw.get("seller", {}).get("name") if isinstance(raw.get("seller"), dict) else raw.get("shopName"),
        }

    if endpoint_id == "seller_items":
        return {
            "itemCode": raw.get("ItemCode") or raw.get("itemCode"),
            "title": raw.get("Title") or raw.get("title"),
            "price": raw.get("Price") or raw.get("price"),
            "originalPrice": raw.get("OriginalPrice") or raw.get("originalPrice"),
            "availability": raw.get("Availability") or raw.get("availability"),
            "updateTime": raw.get("UpdateTime") or raw.get("updateTime"),
        }

    if endpoint_id == "seller_orders":
        return {
            "orderId": raw.get("OrderId") or raw.get("orderId"),
            "orderTime": raw.get("OrderTime") or raw.get("orderTime"),
            "orderStatus": raw.get("OrderStatus") or raw.get("orderStatus"),
            "totalPrice": raw.get("TotalPrice") or raw.get("totalPrice"),
            "paymentMethod": raw.get("PaymentMethod") or raw.get("paymentMethod"),
            "shipStatus": raw.get("ShipStatus") or raw.get("shipStatus"),
            "buyerName": raw.get("BuyerName") or raw.get("buyerName"),
        }

    return raw


# ---------------------------------------------------------------------------
# YahooClient
# ---------------------------------------------------------------------------


class YahooClient(PlatformClient):
    platform_id: str = "yahoo"
    platform_name: str = "Yahoo!ショッピング"

    def __init__(self) -> None:
        self._client_id: str = settings.yahoo_client_id or ""
        self._access_token: str = settings.yahoo_access_token or ""
        self._seller_id: str = settings.yahoo_seller_id or ""
        self._http = ReadOnlyHttpClient(platform="yahoo")

    # -- PlatformClient interface ------------------------------------------

    async def get_endpoints(self) -> list[dict]:
        return list(ENDPOINT_DEFS)

    async def get_schema(self, endpoint_id: str) -> dict:
        schema = ENDPOINT_SCHEMAS.get(endpoint_id)
        if schema is None:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")
        return schema

    async def extract_data(
        self,
        endpoint_id: str,
        columns: list[str] | None,
        limit: int,
        cursor: str | None,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        """Extract data from Yahoo Shopping API.

        1. Build GET request URL and params for the endpoint.
        2. Send request via ReadOnlyHttpClient.get().
        3. Parse JSON response and extract item list.
        4. Flatten items and filter to requested columns.
        5. Return ``{items, columns, next_cursor, total}``.
        """
        if endpoint_id not in _ENDPOINT_URLS:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")

        url = _ENDPOINT_URLS[endpoint_id]
        params = self._build_params(endpoint_id, limit, cursor)
        headers = self._build_headers(endpoint_id)

        # Add date range filtering for seller order endpoints
        if start_date and endpoint_id == "seller_orders":
            params["StartDate"] = start_date
        if end_date and endpoint_id == "seller_orders":
            params["EndDate"] = end_date

        await yahoo_limiter.acquire()

        async def _do_get():
            resp = await self._http.get(url, params=params, headers=headers)
            resp.raise_for_status()
            return resp

        response = await retry_on_429(_do_get)

        body = response.json()

        # Extract raw items from the nested response structure
        raw_items = _extract_items(body, endpoint_id)

        # Flatten each item to the expected schema fields
        records = [_flatten_item(item, endpoint_id) for item in raw_items]

        # Determine available columns from the schema
        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS[endpoint_id]["fields"]]

        # Filter to requested columns
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]
            all_columns = columns

        # Calculate next cursor for offset-based pagination
        start = int(cursor) if cursor else 1
        next_cursor: str | None = None
        if len(raw_items) >= limit:
            next_cursor = str(start + limit)

        return {
            "items": records,
            "columns": all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    def is_configured(self) -> bool:
        """Check if required credentials are set.

        Public APIs (item_search, category_ranking) need yahoo_client_id.
        Seller APIs (seller_items, seller_orders) also need access_token
        and seller_id.
        """
        # At minimum, public APIs require client_id
        if not self._client_id:
            return False
        return True

    def is_endpoint_configured(self, endpoint_id: str) -> bool:
        """Check if a specific endpoint has the required credentials."""
        if endpoint_id in _PUBLIC_ENDPOINTS:
            return bool(self._client_id)
        if endpoint_id in _SELLER_ENDPOINTS:
            return bool(self._client_id and self._access_token and self._seller_id)
        return False

    # -- Private helpers ---------------------------------------------------

    def _build_params(
        self, endpoint_id: str, limit: int, cursor: str | None
    ) -> dict[str, Any]:
        """Build query parameters for a Yahoo API request."""
        params: dict[str, Any] = {"output": "json"}
        start = int(cursor) if cursor else 1

        if endpoint_id == "item_search":
            params["appid"] = self._client_id
            params["results"] = min(limit, 50)
            params["start"] = start

        elif endpoint_id == "category_ranking":
            params["appid"] = self._client_id
            # category_id is optional; could be added via extra params in the future

        elif endpoint_id == "seller_items":
            params["seller_id"] = self._seller_id
            params["start"] = start
            params["results"] = limit

        elif endpoint_id == "seller_orders":
            params["seller_id"] = self._seller_id
            # StartDate and EndDate can be added via extra params in the future

        return params

    def _build_headers(self, endpoint_id: str) -> dict[str, str] | None:
        """Build request headers. Seller endpoints use Bearer auth."""
        if endpoint_id in _SELLER_ENDPOINTS:
            return {"Authorization": f"Bearer {self._access_token}"}
        return None
