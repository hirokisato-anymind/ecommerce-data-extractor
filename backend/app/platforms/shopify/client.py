import logging
from typing import Any

from app.config import settings
from app.core.read_only import ReadOnlyHttpClient
from app.platforms.base import PlatformClient

logger = logging.getLogger("ecommerce_data_extractor.shopify")

# ---------------------------------------------------------------------------
# GraphQL queries keyed by endpoint_id
# ---------------------------------------------------------------------------

GRAPHQL_QUERIES: dict[str, str] = {
    "products": """
query Products($limit: Int!, $cursor: String, $query: String) {
  products(first: $limit, after: $cursor, query: $query) {
    edges {
      node {
        id
        title
        description
        vendor
        productType
        status
        tags
        createdAt
        updatedAt
        variants(first: 10) {
          edges {
            node {
              id
              title
              sku
              price
              inventoryQuantity
            }
          }
        }
      }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""",
    "orders": """
query Orders($limit: Int!, $cursor: String, $query: String) {
  orders(first: $limit, after: $cursor, query: $query) {
    edges {
      node {
        id
        name
        email
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        financialStatus
        fulfillmentStatus
        createdAt
        updatedAt
        lineItems(first: 50) {
          edges {
            node {
              title
              quantity
              sku
            }
          }
        }
      }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""",
    "customers": """
query Customers($limit: Int!, $cursor: String, $query: String) {
  customers(first: $limit, after: $cursor, query: $query) {
    edges {
      node {
        id
        firstName
        lastName
        email
        phone
        ordersCount
        totalSpent
        state
        createdAt
        updatedAt
      }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""",
    "inventory": """
query InventoryItems($limit: Int!, $cursor: String) {
  inventoryItems(first: $limit, after: $cursor) {
    edges {
      node {
        id
        sku
        tracked
        inventoryLevels(first: 10) {
          edges {
            node {
              available
              location {
                name
              }
            }
          }
        }
      }
      cursor
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""",
}

# ---------------------------------------------------------------------------
# Schema definitions per endpoint
# ---------------------------------------------------------------------------

ENDPOINT_SCHEMAS: dict[str, dict] = {
    "products": {
        "fields": [
            {"name": "id", "type": "string", "description": "Product GID"},
            {"name": "title", "type": "string", "description": "Product title"},
            {"name": "description", "type": "string", "description": "Product description"},
            {"name": "vendor", "type": "string", "description": "Product vendor"},
            {"name": "productType", "type": "string", "description": "Product type"},
            {"name": "status", "type": "string", "description": "Product status (ACTIVE, ARCHIVED, DRAFT)"},
            {"name": "tags", "type": "list", "description": "Product tags"},
            {"name": "createdAt", "type": "datetime", "description": "Creation timestamp"},
            {"name": "updatedAt", "type": "datetime", "description": "Last update timestamp"},
            {"name": "variants", "type": "list", "description": "Product variants (id, title, sku, price, inventoryQuantity)"},
        ],
    },
    "orders": {
        "fields": [
            {"name": "id", "type": "string", "description": "Order GID"},
            {"name": "name", "type": "string", "description": "Order name (e.g. #1001)"},
            {"name": "email", "type": "string", "description": "Customer email"},
            {"name": "totalAmount", "type": "number", "description": "Total price amount"},
            {"name": "totalCurrency", "type": "string", "description": "Total price currency code"},
            {"name": "financialStatus", "type": "string", "description": "Financial status"},
            {"name": "fulfillmentStatus", "type": "string", "description": "Fulfillment status"},
            {"name": "createdAt", "type": "datetime", "description": "Creation timestamp"},
            {"name": "updatedAt", "type": "datetime", "description": "Last update timestamp"},
            {"name": "lineItems", "type": "list", "description": "Line items (title, quantity, sku)"},
        ],
    },
    "customers": {
        "fields": [
            {"name": "id", "type": "string", "description": "Customer GID"},
            {"name": "firstName", "type": "string", "description": "First name"},
            {"name": "lastName", "type": "string", "description": "Last name"},
            {"name": "email", "type": "string", "description": "Email address"},
            {"name": "phone", "type": "string", "description": "Phone number"},
            {"name": "ordersCount", "type": "integer", "description": "Total number of orders"},
            {"name": "totalSpent", "type": "number", "description": "Total amount spent"},
            {"name": "state", "type": "string", "description": "Customer account state"},
            {"name": "createdAt", "type": "datetime", "description": "Creation timestamp"},
            {"name": "updatedAt", "type": "datetime", "description": "Last update timestamp"},
        ],
    },
    "inventory": {
        "fields": [
            {"name": "id", "type": "string", "description": "Inventory item GID"},
            {"name": "sku", "type": "string", "description": "SKU"},
            {"name": "tracked", "type": "boolean", "description": "Whether inventory is tracked"},
            {"name": "inventoryLevels", "type": "list", "description": "Inventory levels per location (available, locationName)"},
        ],
    },
}

# ---------------------------------------------------------------------------
# Endpoint definitions
# ---------------------------------------------------------------------------

ENDPOINT_DEFS: list[dict[str, str]] = [
    {
        "id": "products",
        "name": "Products",
        "description": "Shopify products with variants, pricing, and inventory.",
    },
    {
        "id": "orders",
        "name": "Orders",
        "description": "Shopify orders with line items and financial/fulfillment status.",
    },
    {
        "id": "customers",
        "name": "Customers",
        "description": "Shopify customer profiles with order history summary.",
    },
    {
        "id": "inventory",
        "name": "Inventory",
        "description": "Inventory items with stock levels per location.",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _flatten_edges(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten a list of edges/node structures into a flat list of records.

    Nested edges (e.g. variants, lineItems, inventoryLevels) are recursively
    flattened so each nested list becomes a plain list of dicts.
    """
    records: list[dict[str, Any]] = []
    for edge in edges:
        node = dict(edge.get("node", {}))
        # Recursively flatten any nested edges/node structures
        for key, value in node.items():
            if isinstance(value, dict) and "edges" in value:
                node[key] = _flatten_edges(value["edges"])
        records.append(node)
    return records


def _flatten_order_record(record: dict[str, Any]) -> dict[str, Any]:
    """Flatten order-specific nested structures (totalPriceSet)."""
    flat = dict(record)
    total_price_set = flat.pop("totalPriceSet", None)
    if isinstance(total_price_set, dict):
        shop_money = total_price_set.get("shopMoney", {})
        flat["totalAmount"] = shop_money.get("amount")
        flat["totalCurrency"] = shop_money.get("currencyCode")
    return flat


def _flatten_inventory_record(record: dict[str, Any]) -> dict[str, Any]:
    """Flatten inventory-specific nested structures (location → locationName)."""
    flat = dict(record)
    levels = flat.get("inventoryLevels")
    if isinstance(levels, list):
        flattened_levels: list[dict[str, Any]] = []
        for level in levels:
            entry: dict[str, Any] = {"available": level.get("available")}
            location = level.get("location")
            if isinstance(location, dict):
                entry["locationName"] = location.get("name")
            flattened_levels.append(entry)
        flat["inventoryLevels"] = flattened_levels
    return flat


def _post_flatten(endpoint_id: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply endpoint-specific post-flattening transformations."""
    if endpoint_id == "orders":
        return [_flatten_order_record(r) for r in records]
    if endpoint_id == "inventory":
        return [_flatten_inventory_record(r) for r in records]
    return records


# ---------------------------------------------------------------------------
# Root query key lookup  (products → products, orders → orders, etc.)
# ---------------------------------------------------------------------------

_ROOT_KEYS: dict[str, str] = {
    "products": "products",
    "orders": "orders",
    "customers": "customers",
    "inventory": "inventoryItems",
}


# ---------------------------------------------------------------------------
# ShopifyClient
# ---------------------------------------------------------------------------


class ShopifyClient(PlatformClient):
    platform_id: str = "shopify"
    platform_name: str = "Shopify"

    def __init__(self) -> None:
        self._http = ReadOnlyHttpClient(platform="shopify")

    @property
    def _store_domain(self) -> str:
        return settings.shopify_store_domain or ""

    @property
    def _access_token(self) -> str:
        return settings.shopify_access_token or ""

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
        """Extract data from Shopify GraphQL Admin API.

        1. Build the GraphQL query for *endpoint_id*.
        2. Validate the query is read-only (no mutations).
        3. POST via ReadOnlyHttpClient.post_graphql().
        4. Flatten nested edges/nodes into flat records.
        5. Filter to requested *columns* if provided.
        6. Return ``{items, columns, next_cursor, total}``.
        """
        if endpoint_id not in GRAPHQL_QUERIES:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")

        query = GRAPHQL_QUERIES[endpoint_id]
        variables: dict[str, Any] = {"limit": min(limit, 250)}
        if cursor:
            variables["cursor"] = cursor

        # Build Shopify query string for date range filtering
        query_parts: list[str] = []
        if start_date:
            query_parts.append(f"created_at:>={start_date}")
        if end_date:
            query_parts.append(f"created_at:<={end_date}")
        if query_parts:
            variables["query"] = " AND ".join(query_parts)

        url = f"https://{self._store_domain}/admin/api/2024-01/graphql.json"
        headers = {
            "X-Shopify-Access-Token": self._access_token,
            "Content-Type": "application/json",
        }

        response = await self._http.post_graphql(
            url,
            json={"query": query, "variables": variables},
            headers=headers,
        )
        response.raise_for_status()

        body = response.json()

        # Handle GraphQL-level errors
        if "errors" in body:
            error_messages = [e.get("message", str(e)) for e in body["errors"]]
            raise RuntimeError(f"Shopify GraphQL errors: {error_messages}")

        # Navigate to the connection object (e.g. data.products)
        root_key = _ROOT_KEYS[endpoint_id]
        connection = body.get("data", {}).get(root_key, {})

        edges = connection.get("edges", [])
        page_info = connection.get("pageInfo", {})

        # Flatten edges → nodes and apply endpoint-specific transforms
        records = _flatten_edges(edges)
        records = _post_flatten(endpoint_id, records)

        # Determine available columns from the schema
        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS[endpoint_id]["fields"]]

        # Filter to requested columns
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]
            all_columns = columns

        next_cursor = page_info.get("endCursor") if page_info.get("hasNextPage") else None

        return {
            "items": records,
            "columns": all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    def is_configured(self) -> bool:
        return bool(self._store_domain and self._access_token)
