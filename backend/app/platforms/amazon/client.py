import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import settings
from app.core.rate_limiter import amazon_limiter, retry_on_429
from app.core.read_only import ReadOnlyHttpClient
from app.platforms.base import PlatformClient

logger = logging.getLogger("ecommerce_data_extractor.amazon")

SP_API_BASE_URL = "https://sellingpartnerapi-fe.amazon.com"
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
USER_AGENT = "EcommerceDataExtractor/1.0 (Language=Python)"

# ---------------------------------------------------------------------------
# Endpoint definitions
# ---------------------------------------------------------------------------

ENDPOINT_DEFS: list[dict[str, str]] = [
    {
        "id": "orders",
        "name": "Orders - 受注データ",
        "description": "注文＋注文明細（直近30日間）",
    },
    {
        "id": "finances",
        "name": "Financial Events - 売上・手数料明細",
        "description": "出荷イベント別の売上・手数料・ポイント明細（直近30日間）",
    },
    {
        "id": "inventory",
        "name": "FBA Inventory - 在庫サマリー",
        "description": "FBA在庫サマリー（数量内訳付き）",
    },
    {
        "id": "reports",
        "name": "Reports - レポート一覧",
        "description": "直近のレポート一覧（出品・注文・在庫等）",
    },
]

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

ENDPOINT_SCHEMAS: dict[str, dict] = {
    "orders": {
        "fields": [
            # 注文基本
            {"name": "AmazonOrderId", "type": "string", "description": "Amazon注文ID"},
            {"name": "PurchaseDate", "type": "datetime", "description": "購入日時"},
            {"name": "LastUpdateDate", "type": "datetime", "description": "最終更新日時"},
            {"name": "OrderStatus", "type": "string", "description": "注文ステータス (Pending/Unshipped/Shipped/Canceled等)"},
            {"name": "OrderType", "type": "string", "description": "注文タイプ (StandardOrder等)"},
            {"name": "SalesChannel", "type": "string", "description": "販売チャネル"},
            {"name": "FulfillmentChannel", "type": "string", "description": "フルフィルメント (AFN=FBA, MFN=自社出荷)"},
            # 金額
            {"name": "OrderTotalAmount", "type": "number", "description": "注文合計金額"},
            {"name": "OrderTotalCurrency", "type": "string", "description": "通貨コード"},
            {"name": "NumberOfItemsShipped", "type": "integer", "description": "出荷済み商品数"},
            {"name": "NumberOfItemsUnshipped", "type": "integer", "description": "未出荷商品数"},
            # 配送
            {"name": "ShipServiceLevel", "type": "string", "description": "配送サービスレベル"},
            {"name": "EarliestShipDate", "type": "datetime", "description": "最早出荷日"},
            {"name": "LatestShipDate", "type": "datetime", "description": "最終出荷日"},
            {"name": "EarliestDeliveryDate", "type": "datetime", "description": "最早配達日"},
            {"name": "LatestDeliveryDate", "type": "datetime", "description": "最終配達日"},
            {"name": "ShippingPrefecture", "type": "string", "description": "配送先都道府県"},
            {"name": "ShippingPostalCode", "type": "string", "description": "配送先郵便番号"},
            {"name": "AutomatedCarrier", "type": "string", "description": "自動配送キャリア"},
            {"name": "AutomatedShipMethod", "type": "string", "description": "自動配送方法"},
            # 購入者
            {"name": "BuyerEmail", "type": "string", "description": "購入者メール"},
            {"name": "PaymentMethod", "type": "string", "description": "支払方法"},
            # フラグ
            {"name": "IsPrime", "type": "boolean", "description": "Prime注文"},
            {"name": "IsBusinessOrder", "type": "boolean", "description": "ビジネス注文"},
            {"name": "IsGift", "type": "boolean", "description": "ギフト注文"},
            # 注文明細 (OrderItems結合)
            {"name": "ItemASINs", "type": "string", "description": "ASIN一覧"},
            {"name": "ItemSKUs", "type": "string", "description": "出品者SKU一覧"},
            {"name": "ItemTitles", "type": "string", "description": "商品名一覧"},
            {"name": "ItemQuantities", "type": "string", "description": "注文数量一覧"},
            {"name": "ItemPrices", "type": "string", "description": "商品価格一覧"},
            {"name": "ItemTaxes", "type": "string", "description": "商品税額一覧"},
            {"name": "PromotionDiscounts", "type": "string", "description": "プロモーション割引一覧"},
            {"name": "PointsGranted", "type": "string", "description": "付与ポイント一覧"},
        ],
    },
    "finances": {
        "fields": [
            {"name": "AmazonOrderId", "type": "string", "description": "Amazon注文ID"},
            {"name": "PostedDate", "type": "datetime", "description": "計上日時"},
            {"name": "MarketplaceName", "type": "string", "description": "マーケットプレイス"},
            {"name": "SellerSKU", "type": "string", "description": "出品者SKU"},
            {"name": "QuantityShipped", "type": "integer", "description": "出荷数量"},
            # 売上
            {"name": "Principal", "type": "number", "description": "商品売上"},
            {"name": "Tax", "type": "number", "description": "商品税額"},
            {"name": "ShippingCharge", "type": "number", "description": "配送料売上"},
            {"name": "ShippingTax", "type": "number", "description": "配送料税額"},
            {"name": "GiftWrap", "type": "number", "description": "ギフトラッピング売上"},
            {"name": "GiftWrapTax", "type": "number", "description": "ギフトラッピング税額"},
            # 手数料
            {"name": "Commission", "type": "number", "description": "販売手数料"},
            {"name": "FixedClosingFee", "type": "number", "description": "カテゴリー成約料"},
            {"name": "ShippingHB", "type": "number", "description": "配送料手数料"},
            {"name": "GiftwrapCommission", "type": "number", "description": "ギフトラッピング手数料"},
            {"name": "VariableClosingFee", "type": "number", "description": "変動成約料"},
            # ポイント
            {"name": "CostOfPointsGranted", "type": "number", "description": "ポイント原資負担額"},
            # 合計
            {"name": "TotalCharge", "type": "number", "description": "売上合計"},
            {"name": "TotalFee", "type": "number", "description": "手数料合計"},
            {"name": "NetProceeds", "type": "number", "description": "純収益 (売上-手数料-ポイント)"},
            {"name": "EventType", "type": "string", "description": "イベント種別 (Shipment/Refund/GuaranteeClaim)"},
        ],
    },
    "inventory": {
        "fields": [
            {"name": "asin", "type": "string", "description": "ASIN"},
            {"name": "fnSku", "type": "string", "description": "FN SKU (Amazonフルフィルメント番号)"},
            {"name": "sellerSku", "type": "string", "description": "出品者SKU"},
            {"name": "condition", "type": "string", "description": "コンディション"},
            {"name": "totalQuantity", "type": "integer", "description": "合計在庫数"},
            {"name": "fulfillableQuantity", "type": "integer", "description": "出荷可能数"},
            {"name": "inboundWorkingQuantity", "type": "integer", "description": "納品作業中数量"},
            {"name": "inboundShippedQuantity", "type": "integer", "description": "納品輸送中数量"},
        ],
    },
    "reports": {
        "fields": [
            {"name": "reportId", "type": "string", "description": "レポートID"},
            {"name": "reportType", "type": "string", "description": "レポートタイプ"},
            {"name": "processingStatus", "type": "string", "description": "処理ステータス (IN_QUEUE/IN_PROGRESS/DONE等)"},
            {"name": "dataStartTime", "type": "datetime", "description": "データ開始日時"},
            {"name": "dataEndTime", "type": "datetime", "description": "データ終了日時"},
            {"name": "createdTime", "type": "datetime", "description": "レポート作成日時"},
        ],
    },
}

ENDPOINT_PATHS: dict[str, str] = {
    "orders": "/orders/v0/orders",
    "finances": "/finances/v0/financialEvents",
    "inventory": "/fba/inventory/v1/summaries",
    "reports": "/reports/2021-06-30/reports",
}


# ---------------------------------------------------------------------------
# Flatten helpers
# ---------------------------------------------------------------------------

def _extract_amount(obj: Any) -> str | None:
    if isinstance(obj, dict):
        return obj.get("Amount") or obj.get("CurrencyAmount")
    return None


def _flatten_order(order: dict[str, Any], order_items: list[dict] | None = None) -> dict[str, Any]:
    flat: dict[str, Any] = {
        "AmazonOrderId": order.get("AmazonOrderId"),
        "PurchaseDate": order.get("PurchaseDate"),
        "LastUpdateDate": order.get("LastUpdateDate"),
        "OrderStatus": order.get("OrderStatus"),
        "OrderType": order.get("OrderType"),
        "SalesChannel": order.get("SalesChannel"),
        "FulfillmentChannel": order.get("FulfillmentChannel"),
        "NumberOfItemsShipped": order.get("NumberOfItemsShipped"),
        "NumberOfItemsUnshipped": order.get("NumberOfItemsUnshipped"),
        "ShipServiceLevel": order.get("ShipServiceLevel"),
        "EarliestShipDate": order.get("EarliestShipDate"),
        "LatestShipDate": order.get("LatestShipDate"),
        "EarliestDeliveryDate": order.get("EarliestDeliveryDate"),
        "LatestDeliveryDate": order.get("LatestDeliveryDate"),
        "PaymentMethod": order.get("PaymentMethod"),
        "IsPrime": order.get("IsPrime"),
        "IsBusinessOrder": order.get("IsBusinessOrder"),
    }

    # Order total
    order_total = order.get("OrderTotal")
    if isinstance(order_total, dict):
        flat["OrderTotalAmount"] = order_total.get("Amount")
        flat["OrderTotalCurrency"] = order_total.get("CurrencyCode")
    else:
        flat["OrderTotalAmount"] = None
        flat["OrderTotalCurrency"] = None

    # Shipping address
    shipping = order.get("ShippingAddress") or {}
    flat["ShippingPrefecture"] = shipping.get("StateOrRegion")
    flat["ShippingPostalCode"] = shipping.get("PostalCode")

    # Automated shipping
    auto_ship = order.get("AutomatedShippingSettings") or {}
    flat["AutomatedCarrier"] = auto_ship.get("AutomatedCarrier")
    flat["AutomatedShipMethod"] = auto_ship.get("AutomatedShipMethod")

    # Buyer info
    buyer = order.get("BuyerInfo") or {}
    flat["BuyerEmail"] = buyer.get("BuyerEmail")

    # Gift flag from items
    flat["IsGift"] = None

    # Order items
    if order_items:
        flat["ItemASINs"] = " / ".join(i.get("ASIN", "") for i in order_items)
        flat["ItemSKUs"] = " / ".join(i.get("SellerSKU", "") for i in order_items)
        flat["ItemTitles"] = " / ".join(i.get("Title", "") for i in order_items)
        flat["ItemQuantities"] = " / ".join(str(i.get("QuantityOrdered", "")) for i in order_items)
        flat["ItemPrices"] = " / ".join(
            _extract_amount(i.get("ItemPrice")) or "" for i in order_items
        )
        flat["ItemTaxes"] = " / ".join(
            _extract_amount(i.get("ItemTax")) or "" for i in order_items
        )
        flat["PromotionDiscounts"] = " / ".join(
            _extract_amount(i.get("PromotionDiscount")) or "0" for i in order_items
        )
        flat["PointsGranted"] = " / ".join(
            str((i.get("PointsGranted") or {}).get("PointsNumber", 0)) for i in order_items
        )
        # Check gift flag
        gift_flags = [i.get("IsGift") for i in order_items]
        flat["IsGift"] = any(g == "true" or g is True for g in gift_flags)
    else:
        for key in ["ItemASINs", "ItemSKUs", "ItemTitles", "ItemQuantities",
                     "ItemPrices", "ItemTaxes", "PromotionDiscounts", "PointsGranted"]:
            flat[key] = None

    return flat


def _flatten_finance_event(event: dict[str, Any], event_type: str = "Shipment") -> list[dict[str, Any]]:
    """Flatten a shipment/refund/guarantee financial event into per-item rows."""
    rows = []
    order_id = event.get("AmazonOrderId")
    posted = event.get("PostedDate")
    marketplace = event.get("MarketplaceName")

    for item in event.get("ShipmentItemList", []):
        row: dict[str, Any] = {
            "AmazonOrderId": order_id,
            "PostedDate": posted,
            "MarketplaceName": marketplace,
            "SellerSKU": item.get("SellerSKU"),
            "QuantityShipped": item.get("QuantityShipped"),
            "EventType": event_type,
        }

        # Parse charges
        charges: dict[str, float] = {}
        for charge in item.get("ItemChargeList", []):
            charge_type = charge.get("ChargeType", "")
            amount = charge.get("ChargeAmount", {}).get("CurrencyAmount", 0.0)
            charges[charge_type] = float(amount) if amount else 0.0

        row["Principal"] = charges.get("Principal", 0.0)
        row["Tax"] = charges.get("Tax", 0.0)
        row["ShippingCharge"] = charges.get("ShippingCharge", 0.0)
        row["ShippingTax"] = charges.get("ShippingTax", 0.0)
        row["GiftWrap"] = charges.get("GiftWrap", 0.0)
        row["GiftWrapTax"] = charges.get("GiftWrapTax", 0.0)

        # Parse fees
        fees: dict[str, float] = {}
        for fee in item.get("ItemFeeList", []):
            fee_type = fee.get("FeeType", "")
            amount = fee.get("FeeAmount", {}).get("CurrencyAmount", 0.0)
            fees[fee_type] = float(amount) if amount else 0.0

        row["Commission"] = fees.get("Commission", 0.0)
        row["FixedClosingFee"] = fees.get("FixedClosingFee", 0.0)
        row["ShippingHB"] = fees.get("ShippingHB", 0.0)
        row["GiftwrapCommission"] = fees.get("GiftwrapCommission", 0.0)
        row["VariableClosingFee"] = fees.get("VariableClosingFee", 0.0)

        # Points
        points_cost = item.get("CostOfPointsGranted", {})
        row["CostOfPointsGranted"] = float(points_cost.get("CurrencyAmount", 0.0)) if points_cost else 0.0

        # Totals
        total_charge = sum(charges.values())
        total_fee = sum(fees.values())
        row["TotalCharge"] = total_charge
        row["TotalFee"] = total_fee
        row["NetProceeds"] = total_charge + total_fee + row["CostOfPointsGranted"]

        rows.append(row)

    return rows


def _flatten_inventory_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "asin": summary.get("asin"),
        "fnSku": summary.get("fnSku"),
        "sellerSku": summary.get("sellerSku"),
        "condition": summary.get("condition"),
        "totalQuantity": summary.get("totalQuantity"),
        "fulfillableQuantity": summary.get("fulfillableQuantity"),
        "inboundWorkingQuantity": summary.get("inboundWorkingQuantity"),
        "inboundShippedQuantity": summary.get("inboundShippedQuantity"),
    }


def _flatten_report(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "reportId": report.get("reportId"),
        "reportType": report.get("reportType"),
        "processingStatus": report.get("processingStatus"),
        "dataStartTime": report.get("dataStartTime"),
        "dataEndTime": report.get("dataEndTime"),
        "createdTime": report.get("createdTime"),
    }


# ---------------------------------------------------------------------------
# AmazonClient
# ---------------------------------------------------------------------------

class AmazonClient(PlatformClient):
    platform_id: str = "amazon"
    platform_name: str = "Amazon"

    def __init__(self) -> None:
        self._refresh_token: str = settings.amazon_refresh_token or ""
        self._client_id: str = settings.amazon_client_id or ""
        self._client_secret: str = settings.amazon_client_secret or ""
        self._marketplace_id: str = settings.amazon_marketplace_id or "A1VC38T7YXB528"
        self._http = ReadOnlyHttpClient(platform="amazon")
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0
        self._auth_client = httpx.AsyncClient(timeout=30.0)

    # -- LWA auth -------------------------------------------------------------

    async def _ensure_access_token(self) -> str:
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        logger.info("Refreshing LWA access token")
        response = await self._auth_client.post(
            LWA_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        response.raise_for_status()
        token_data = response.json()
        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        self._token_expires_at = time.time() + expires_in - 60
        return self._access_token

    async def _get_headers(self) -> dict[str, str]:
        access_token = await self._ensure_access_token()
        return {
            "x-amz-access-token": access_token,
            "User-Agent": USER_AGENT,
        }

    async def _sp_api_get(
        self, path: str, params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await amazon_limiter.acquire()
        url = f"{SP_API_BASE_URL}{path}"
        headers = await self._get_headers()

        async def _do_get():
            response = await self._http.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

        return await retry_on_429(_do_get)

    # -- PlatformClient interface ---------------------------------------------

    async def get_endpoints(self) -> list[dict]:
        return list(ENDPOINT_DEFS)

    async def get_schema(self, endpoint_id: str) -> dict:
        schema = ENDPOINT_SCHEMAS.get(endpoint_id)
        if schema is None:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")
        return schema

    def is_configured(self) -> bool:
        return bool(self._refresh_token and self._client_id and self._client_secret)

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
        handlers = {
            "orders": self._extract_orders,
            "finances": self._extract_finances,
            "inventory": self._extract_inventory,
            "reports": self._extract_reports,
        }
        handler = handlers.get(endpoint_id)
        if handler is None:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")
        return await handler(columns=columns, limit=limit, cursor=cursor,
                             start_date=start_date, end_date=end_date)

    # -- Orders (with OrderItems) ---------------------------------------------

    async def _extract_orders(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        if start_date:
            created_after = f"{start_date}T00:00:00Z"
        else:
            created_after = (
                datetime.now(timezone.utc) - timedelta(days=30)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        params: dict[str, Any] = {
            "MarketplaceIds": self._marketplace_id,
            "CreatedAfter": created_after,
            "MaxResultsPerPage": min(limit, 100),
        }
        if end_date:
            params["CreatedBefore"] = f"{end_date}T23:59:59Z"
        if cursor:
            params["NextToken"] = cursor

        body = await self._sp_api_get("/orders/v0/orders", params=params)
        payload = body.get("payload", body)
        raw_orders = payload.get("Orders", [])

        # Fetch OrderItems for each order
        records = []
        for order in raw_orders:
            order_id = order.get("AmazonOrderId")
            order_items = []
            try:
                items_resp = await self._sp_api_get(
                    f"/orders/v0/orders/{order_id}/orderItems"
                )
                items_payload = items_resp.get("payload", items_resp)
                order_items = items_payload.get("OrderItems", [])
            except Exception:
                logger.warning("Failed to fetch items for order %s", order_id)

            flat = _flatten_order(order, order_items)
            if columns:
                flat = {k: flat.get(k) for k in columns}
            records.append(flat)

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["orders"]["fields"]]
        next_cursor = payload.get("NextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    # -- Finances (ShipmentEventList) -----------------------------------------

    async def _extract_finances(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        if start_date:
            posted_after = f"{start_date}T00:00:00Z"
        else:
            posted_after = (
                datetime.now(timezone.utc) - timedelta(days=30)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        params: dict[str, Any] = {
            "PostedAfter": posted_after,
            "MaxResultsPerPage": min(limit, 100),
        }
        if end_date:
            params["PostedBefore"] = f"{end_date}T23:59:59Z"
        if cursor:
            params["NextToken"] = cursor

        body = await self._sp_api_get("/finances/v0/financialEvents", params=params)
        payload = body.get("payload", body)
        events = payload.get("FinancialEvents", {})
        event_lists = [
            ("ShipmentEventList", "Shipment"),
            ("RefundEventList", "Refund"),
            ("GuaranteeClaimEventList", "GuaranteeClaim"),
        ]

        records = []
        for list_key, event_type in event_lists:
            for event in events.get(list_key, []):
                rows = _flatten_finance_event(event, event_type=event_type)
                for row in rows:
                    if columns:
                        row = {k: row.get(k) for k in columns}
                    records.append(row)
                    if len(records) >= limit:
                        break
                if len(records) >= limit:
                    break
            if len(records) >= limit:
                break

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["finances"]["fields"]]
        next_cursor = payload.get("NextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    # -- Inventory ------------------------------------------------------------

    async def _extract_inventory(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        params: dict[str, Any] = {
            "marketplaceIds": self._marketplace_id,
            "granularityType": "Marketplace",
            "granularityId": self._marketplace_id,
            "details": "true",
        }
        if cursor:
            params["nextToken"] = cursor

        body = await self._sp_api_get("/fba/inventory/v1/summaries", params=params)
        payload = body.get("payload", body)
        raw_items = payload.get("inventorySummaries", [])

        records = [_flatten_inventory_summary(item) for item in raw_items]
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["inventory"]["fields"]]
        pagination = payload.get("pagination", {})
        next_cursor = pagination.get("nextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    # -- Reports --------------------------------------------------------------

    async def _extract_reports(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        params: dict[str, Any] = {
            "marketplaceIds": self._marketplace_id,
            "pageSize": min(limit, 100),
            "reportTypes": ",".join([
                "GET_MERCHANT_LISTINGS_ALL_DATA",
                "GET_FLAT_FILE_OPEN_LISTINGS_DATA",
                "GET_FLAT_FILE_ORDERS_DATA",
                "GET_FBA_MYI_ALL_INVENTORY_DATA",
            ]),
        }
        if cursor:
            params["nextToken"] = cursor

        body = await self._sp_api_get("/reports/2021-06-30/reports", params=params)
        payload = body.get("payload", body)
        raw_items = payload.get("reports", [])

        records = [_flatten_report(item) for item in raw_items]
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["reports"]["fields"]]
        next_cursor = payload.get("nextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }
