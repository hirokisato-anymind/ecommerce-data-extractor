import logging
from typing import Any

from app.config import settings
from app.core.rate_limiter import shopify_limiter, retry_on_429
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
        descriptionHtml
        handle
        vendor
        productType
        status
        tags
        publishedAt
        createdAt
        updatedAt
        totalInventory
        tracksInventory
        isGiftCard
        hasOnlyDefaultVariant
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
        phone
        cancelledAt
        cancelReason
        closedAt
        confirmationNumber
        discountCode
        discountCodes
        processedAt
        sourceName
        subtotalLineItemsQuantity
        tags
        taxesIncluded
        test
        totalWeight
        note
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        subtotalPriceSet {
          shopMoney {
            amount
          }
        }
        totalShippingPriceSet {
          shopMoney {
            amount
          }
        }
        totalTaxSet {
          shopMoney {
            amount
          }
        }
        totalDiscountsSet {
          shopMoney {
            amount
          }
        }
        totalRefundedSet {
          shopMoney {
            amount
          }
        }
        shippingAddress {
          address1
          address2
          city
          province
          country
          zip
          phone
          name
        }
        billingAddress {
          address1
          city
          province
          country
          zip
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
        displayName
        email
        phone
        locale
        note
        numberOfOrders
        ordersCount
        totalSpent
        verifiedEmail
        taxExempt
        state
        tags
        amountSpent {
          amount
          currencyCode
        }
        defaultAddress {
          address1
          city
          province
          country
          zip
          phone
        }
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
        updatedAt
        inventoryLevels(first: 10) {
          edges {
            node {
              available
              quantities(names: ["available", "committed", "damaged", "incoming", "on_hand", "quality_control", "reserved", "safety_stock"]) {
                name
                quantity
              }
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
            {"name": "descriptionHtml", "type": "string", "description": "Product description in HTML"},
            {"name": "handle", "type": "string", "description": "URL-friendly product handle"},
            {"name": "vendor", "type": "string", "description": "Product vendor"},
            {"name": "productType", "type": "string", "description": "Product type"},
            {"name": "status", "type": "string", "description": "Product status (ACTIVE, ARCHIVED, DRAFT)"},
            {"name": "tags", "type": "list", "description": "Product tags"},
            {"name": "publishedAt", "type": "datetime", "description": "Publication timestamp"},
            {"name": "createdAt", "type": "datetime", "description": "Creation timestamp"},
            {"name": "updatedAt", "type": "datetime", "description": "Last update timestamp"},
            {"name": "totalInventory", "type": "integer", "description": "Total inventory across all variants"},
            {"name": "tracksInventory", "type": "boolean", "description": "Whether inventory is tracked"},
            {"name": "isGiftCard", "type": "boolean", "description": "Whether the product is a gift card"},
            {"name": "hasOnlyDefaultVariant", "type": "boolean", "description": "Whether the product has only the default variant"},
            {"name": "variants", "type": "list", "description": "Product variants (id, title, sku, price, inventoryQuantity)"},
        ],
    },
    "orders": {
        "fields": [
            {"name": "id", "type": "string", "description": "Order GID"},
            {"name": "name", "type": "string", "description": "Order name (e.g. #1001)"},
            {"name": "email", "type": "string", "description": "Customer email"},
            {"name": "phone", "type": "string", "description": "Customer phone"},
            {"name": "cancelledAt", "type": "datetime", "description": "Cancellation timestamp"},
            {"name": "cancelReason", "type": "string", "description": "Reason for cancellation"},
            {"name": "closedAt", "type": "datetime", "description": "Closed timestamp"},
            {"name": "confirmationNumber", "type": "string", "description": "Order confirmation number"},
            {"name": "discountCode", "type": "string", "description": "Primary discount code"},
            {"name": "discountCodes", "type": "list", "description": "All discount codes applied"},
            {"name": "processedAt", "type": "datetime", "description": "Processing timestamp"},
            {"name": "sourceName", "type": "string", "description": "Source of the order (e.g. web, pos)"},
            {"name": "subtotalLineItemsQuantity", "type": "integer", "description": "Total quantity of line items"},
            {"name": "tags", "type": "list", "description": "Order tags"},
            {"name": "taxesIncluded", "type": "boolean", "description": "Whether taxes are included in prices"},
            {"name": "test", "type": "boolean", "description": "Whether this is a test order"},
            {"name": "totalWeight", "type": "number", "description": "Total weight of the order"},
            {"name": "note", "type": "string", "description": "Order note"},
            {"name": "totalAmount", "type": "number", "description": "Total price amount"},
            {"name": "totalCurrency", "type": "string", "description": "Total price currency code"},
            {"name": "subtotalAmount", "type": "number", "description": "Subtotal price amount"},
            {"name": "shippingAmount", "type": "number", "description": "Total shipping price amount"},
            {"name": "taxAmount", "type": "number", "description": "Total tax amount"},
            {"name": "discountAmount", "type": "number", "description": "Total discounts amount"},
            {"name": "refundedAmount", "type": "number", "description": "Total refunded amount"},
            {"name": "shippingName", "type": "string", "description": "Shipping address name"},
            {"name": "shippingAddress1", "type": "string", "description": "Shipping address line 1"},
            {"name": "shippingAddress2", "type": "string", "description": "Shipping address line 2"},
            {"name": "shippingCity", "type": "string", "description": "Shipping city"},
            {"name": "shippingProvince", "type": "string", "description": "Shipping province/state"},
            {"name": "shippingCountry", "type": "string", "description": "Shipping country"},
            {"name": "shippingZip", "type": "string", "description": "Shipping zip/postal code"},
            {"name": "shippingPhone", "type": "string", "description": "Shipping phone number"},
            {"name": "billingAddress1", "type": "string", "description": "Billing address line 1"},
            {"name": "billingCity", "type": "string", "description": "Billing city"},
            {"name": "billingProvince", "type": "string", "description": "Billing province/state"},
            {"name": "billingCountry", "type": "string", "description": "Billing country"},
            {"name": "billingZip", "type": "string", "description": "Billing zip/postal code"},
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
            {"name": "displayName", "type": "string", "description": "Display name"},
            {"name": "email", "type": "string", "description": "Email address"},
            {"name": "phone", "type": "string", "description": "Phone number"},
            {"name": "locale", "type": "string", "description": "Customer locale"},
            {"name": "note", "type": "string", "description": "Note about the customer"},
            {"name": "numberOfOrders", "type": "integer", "description": "Number of orders (string-based count)"},
            {"name": "ordersCount", "type": "integer", "description": "Total number of orders"},
            {"name": "totalSpent", "type": "number", "description": "Total amount spent"},
            {"name": "verifiedEmail", "type": "boolean", "description": "Whether the email is verified"},
            {"name": "taxExempt", "type": "boolean", "description": "Whether the customer is tax exempt"},
            {"name": "state", "type": "string", "description": "Customer account state"},
            {"name": "tags", "type": "list", "description": "Customer tags"},
            {"name": "amountSpent", "type": "number", "description": "Amount spent (from amountSpent MoneyV2)"},
            {"name": "amountSpentCurrency", "type": "string", "description": "Currency of amount spent"},
            {"name": "defaultAddress1", "type": "string", "description": "Default address line 1"},
            {"name": "defaultCity", "type": "string", "description": "Default address city"},
            {"name": "defaultProvince", "type": "string", "description": "Default address province/state"},
            {"name": "defaultCountry", "type": "string", "description": "Default address country"},
            {"name": "defaultZip", "type": "string", "description": "Default address zip/postal code"},
            {"name": "defaultPhone", "type": "string", "description": "Default address phone"},
            {"name": "createdAt", "type": "datetime", "description": "Creation timestamp"},
            {"name": "updatedAt", "type": "datetime", "description": "Last update timestamp"},
        ],
    },
    "inventory": {
        "fields": [
            {"name": "id", "type": "string", "description": "Inventory item GID"},
            {"name": "sku", "type": "string", "description": "SKU"},
            {"name": "tracked", "type": "boolean", "description": "Whether inventory is tracked"},
            {"name": "updatedAt", "type": "datetime", "description": "Last update timestamp"},
            {"name": "inventoryLevels", "type": "list", "description": "Inventory levels per location (available, quantities, locationName)"},
        ],
    },
}

# ---------------------------------------------------------------------------
# Endpoint definitions
# ---------------------------------------------------------------------------

ENDPOINT_DEFS: list[dict[str, str]] = [
    {
        "id": "products",
        "name": "Products - 商品データ",
        "description": "Shopify products with variants, pricing, and inventory.",
    },
    {
        "id": "orders",
        "name": "Orders - 受注データ",
        "description": "Shopify orders with line items and financial/fulfillment status.",
    },
    {
        "id": "customers",
        "name": "Customers - 顧客データ",
        "description": "Shopify customer profiles with order history summary.",
    },
    {
        "id": "inventory",
        "name": "Inventory - 在庫データ",
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


def _extract_shop_money_amount(record: dict[str, Any], key: str) -> Any:
    """Extract the shopMoney.amount value from a MoneySet field."""
    money_set = record.get(key)
    if isinstance(money_set, dict):
        shop_money = money_set.get("shopMoney", {})
        return shop_money.get("amount")
    return None


def _flatten_order_record(record: dict[str, Any]) -> dict[str, Any]:
    """Flatten order-specific nested structures (MoneyV2 sets, addresses)."""
    flat = dict(record)

    # totalPriceSet
    total_price_set = flat.pop("totalPriceSet", None)
    if isinstance(total_price_set, dict):
        shop_money = total_price_set.get("shopMoney", {})
        flat["totalAmount"] = shop_money.get("amount")
        flat["totalCurrency"] = shop_money.get("currencyCode")

    # Other MoneyV2 sets → flat amount fields
    for gql_key, flat_key in [
        ("subtotalPriceSet", "subtotalAmount"),
        ("totalShippingPriceSet", "shippingAmount"),
        ("totalTaxSet", "taxAmount"),
        ("totalDiscountsSet", "discountAmount"),
        ("totalRefundedSet", "refundedAmount"),
    ]:
        money_set = flat.pop(gql_key, None)
        if isinstance(money_set, dict):
            shop_money = money_set.get("shopMoney", {})
            flat[flat_key] = shop_money.get("amount")

    # Shipping address
    shipping = flat.pop("shippingAddress", None)
    if isinstance(shipping, dict):
        flat["shippingName"] = shipping.get("name")
        flat["shippingAddress1"] = shipping.get("address1")
        flat["shippingAddress2"] = shipping.get("address2")
        flat["shippingCity"] = shipping.get("city")
        flat["shippingProvince"] = shipping.get("province")
        flat["shippingCountry"] = shipping.get("country")
        flat["shippingZip"] = shipping.get("zip")
        flat["shippingPhone"] = shipping.get("phone")

    # Billing address
    billing = flat.pop("billingAddress", None)
    if isinstance(billing, dict):
        flat["billingAddress1"] = billing.get("address1")
        flat["billingCity"] = billing.get("city")
        flat["billingProvince"] = billing.get("province")
        flat["billingCountry"] = billing.get("country")
        flat["billingZip"] = billing.get("zip")

    return flat


def _flatten_customer_record(record: dict[str, Any]) -> dict[str, Any]:
    """Flatten customer-specific nested structures (amountSpent, defaultAddress)."""
    flat = dict(record)

    # amountSpent MoneyV2
    amount_spent = flat.pop("amountSpent", None)
    if isinstance(amount_spent, dict):
        flat["amountSpent"] = amount_spent.get("amount")
        flat["amountSpentCurrency"] = amount_spent.get("currencyCode")

    # defaultAddress
    default_addr = flat.pop("defaultAddress", None)
    if isinstance(default_addr, dict):
        flat["defaultAddress1"] = default_addr.get("address1")
        flat["defaultCity"] = default_addr.get("city")
        flat["defaultProvince"] = default_addr.get("province")
        flat["defaultCountry"] = default_addr.get("country")
        flat["defaultZip"] = default_addr.get("zip")
        flat["defaultPhone"] = default_addr.get("phone")

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
    if endpoint_id == "customers":
        return [_flatten_customer_record(r) for r in records]
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

        await shopify_limiter.acquire()

        async def _do_query():
            resp = await self._http.post_graphql(
                url,
                json={"query": query, "variables": variables},
                headers=headers,
            )
            resp.raise_for_status()
            return resp

        response = await retry_on_429(_do_query)

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
