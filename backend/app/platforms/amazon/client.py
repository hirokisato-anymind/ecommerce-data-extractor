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
    {
        "id": "catalog",
        "name": "Catalog Items - 商品カタログ",
        "description": "商品の出品カタログ情報（ASIN・タイトル・ブランド・画像等）",
    },
    {
        "id": "pricing",
        "name": "Product Pricing - 価格情報",
        "description": "商品の価格・Buy Box・オファー情報",
    },
    {
        "id": "sales_metrics",
        "name": "Sales Metrics - 販売指標",
        "description": "日次売上指標（販売数・注文数・平均単価・売上合計）",
    },
    {
        "id": "fba_shipments",
        "name": "FBA Inbound Shipments - FBA納品",
        "description": "FBA納品プランの一覧とステータス",
    },
    {
        "id": "brand_analytics",
        "name": "Brand Analytics Reports - ブランド分析",
        "description": "ブランド分析レポート一覧（検索キーワード・マーケットバスケット等）",
    },
    {
        "id": "direct_fulfillment",
        "name": "Merchant Fulfillment Shipments - 自社出荷",
        "description": "自社出荷（MFN）の出荷ラベル・追跡情報",
    },
    # NOTE: 購入者とのコミュニケーション (Buyer Messaging) and フィードバック依頼
    # (Solicitations) are primarily write operations (sendInvoice, createProductReviewAndSellerFeedbackSolicitation).
    # SP-API does not expose read-only listing endpoints for these, so they are omitted.
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
    "catalog": {
        "fields": [
            {"name": "asin", "type": "string", "description": "ASIN"},
            {"name": "title", "type": "string", "description": "商品タイトル"},
            {"name": "brand", "type": "string", "description": "ブランド名"},
            {"name": "color", "type": "string", "description": "カラー"},
            {"name": "size", "type": "string", "description": "サイズ"},
            {"name": "modelNumber", "type": "string", "description": "モデル番号"},
            {"name": "salesRank", "type": "integer", "description": "売れ筋ランキング"},
            {"name": "imageUrl", "type": "string", "description": "メイン画像URL"},
            {"name": "itemClassification", "type": "string", "description": "商品分類 (BASE_PRODUCT/VARIATION_PARENT等)"},
        ],
    },
    "pricing": {
        "fields": [
            {"name": "asin", "type": "string", "description": "ASIN"},
            {"name": "sellerSku", "type": "string", "description": "出品者SKU"},
            {"name": "listingPrice", "type": "number", "description": "出品価格"},
            {"name": "shippingPrice", "type": "number", "description": "配送料"},
            {"name": "landedPrice", "type": "number", "description": "最終購入価格 (価格+配送料)"},
            {"name": "points", "type": "number", "description": "ポイント付与額"},
            {"name": "buyBoxPrice", "type": "number", "description": "Buy Box価格"},
            {"name": "numberOfOffers", "type": "integer", "description": "オファー数"},
            {"name": "condition", "type": "string", "description": "コンディション (New/Used等)"},
        ],
    },
    "sales_metrics": {
        "fields": [
            {"name": "date", "type": "string", "description": "日付 (YYYY-MM-DD)"},
            {"name": "unitCount", "type": "integer", "description": "販売ユニット数"},
            {"name": "orderItemCount", "type": "integer", "description": "注文アイテム数"},
            {"name": "orderCount", "type": "integer", "description": "注文数"},
            {"name": "averageUnitPrice", "type": "number", "description": "平均単価"},
            {"name": "totalSales_amount", "type": "number", "description": "売上合計金額"},
            {"name": "totalSales_currency", "type": "string", "description": "通貨コード"},
        ],
    },
    "fba_shipments": {
        "fields": [
            {"name": "shipmentId", "type": "string", "description": "納品プランID"},
            {"name": "shipmentName", "type": "string", "description": "納品プラン名"},
            {"name": "shipmentStatus", "type": "string", "description": "ステータス (WORKING/SHIPPED/RECEIVING/CLOSED等)"},
            {"name": "destinationFulfillmentCenterId", "type": "string", "description": "納品先FC ID"},
            {"name": "labelPrepType", "type": "string", "description": "ラベル準備タイプ"},
            {"name": "areCasesRequired", "type": "boolean", "description": "ケース梱包必須"},
        ],
    },
    "brand_analytics": {
        "fields": [
            {"name": "reportId", "type": "string", "description": "レポートID"},
            {"name": "reportType", "type": "string", "description": "レポートタイプ"},
            {"name": "processingStatus", "type": "string", "description": "処理ステータス (IN_QUEUE/IN_PROGRESS/DONE等)"},
            {"name": "dataStartTime", "type": "datetime", "description": "データ開始日時"},
            {"name": "dataEndTime", "type": "datetime", "description": "データ終了日時"},
            {"name": "createdTime", "type": "datetime", "description": "レポート作成日時"},
        ],
    },
    "direct_fulfillment": {
        "fields": [
            {"name": "shipmentId", "type": "string", "description": "出荷ID"},
            {"name": "amazonOrderId", "type": "string", "description": "Amazon注文ID"},
            {"name": "shipmentStatus", "type": "string", "description": "出荷ステータス"},
            {"name": "trackingNumber", "type": "string", "description": "追跡番号"},
            {"name": "carrier", "type": "string", "description": "配送業者"},
            {"name": "shipDate", "type": "datetime", "description": "出荷日"},
            {"name": "deliveryDate", "type": "datetime", "description": "配達日"},
        ],
    },
}

ENDPOINT_PATHS: dict[str, str] = {
    "orders": "/orders/v0/orders",
    "finances": "/finances/v0/financialEvents",
    "inventory": "/fba/inventory/v1/summaries",
    "reports": "/reports/2021-06-30/reports",
    "catalog": "/catalog/2022-04-01/items",
    "pricing": "/products/pricing/v0/price",
    "sales_metrics": "/sales/v1/orderMetrics",
    "fba_shipments": "/fba/inbound/v0/shipments",
    "brand_analytics": "/reports/2021-06-30/reports",
    "direct_fulfillment": "/mfn/v0/shipments",
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


def _flatten_catalog_item(item: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Catalog Items API response item (2022-04-01 version)."""
    asin = item.get("asin")

    # summaries[0] contains title, brand, classification, etc.
    summaries = item.get("summaries") or []
    summary = summaries[0] if summaries else {}

    # images[0] for main image
    images = item.get("images") or []
    image_url = None
    if images:
        # images is a list of image sets per marketplace; take first variant
        image_set = images[0] if images else {}
        image_list = image_set.get("images") or []
        if image_list:
            image_url = image_list[0].get("link")

    # salesRanks
    sales_ranks = item.get("salesRanks") or []
    sales_rank = None
    if sales_ranks:
        rank_list = sales_ranks[0].get("displayGroupRanks") or sales_ranks[0].get("classificationRanks") or []
        if rank_list:
            sales_rank = rank_list[0].get("rank")

    # attributes for color, size, modelNumber
    attributes = item.get("attributes") or {}
    color_attr = attributes.get("color") or []
    size_attr = attributes.get("size") or []
    model_attr = attributes.get("model_number") or attributes.get("part_number") or []

    return {
        "asin": asin,
        "title": summary.get("itemName"),
        "brand": summary.get("brand"),
        "color": color_attr[0].get("value") if color_attr else None,
        "size": size_attr[0].get("value") if size_attr else None,
        "modelNumber": model_attr[0].get("value") if model_attr else None,
        "salesRank": sales_rank,
        "imageUrl": image_url,
        "itemClassification": summary.get("itemClassification"),
    }


def _flatten_pricing(product: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Product Pricing API response product."""
    asin = product.get("ASIN")
    status = product.get("status")

    # The Product element contains offers/pricing
    product_body = product.get("Product", {})
    offers = product_body.get("Offers") or []
    offer = offers[0] if offers else {}

    buying_price = offer.get("BuyingPrice") or {}
    listing_price = buying_price.get("ListingPrice") or {}
    shipping_price = buying_price.get("Shipping") or {}
    landed_price = buying_price.get("LandedPrice") or {}
    points = offer.get("RegularPrice", {})  # fallback

    # Points from offer
    points_obj = buying_price.get("Points") or {}

    # Competitive pricing for BuyBox
    comp_pricing = product_body.get("CompetitivePricing") or {}
    comp_prices = comp_pricing.get("CompetitivePrices") or []
    buy_box_price = None
    for cp in comp_prices:
        if cp.get("belongsToRequester"):
            price_obj = cp.get("Price", {}).get("LandedPrice") or cp.get("Price", {}).get("ListingPrice") or {}
            buy_box_price = price_obj.get("Amount")
            break

    # Number of offer listings
    offer_listings = comp_pricing.get("NumberOfOfferListings") or []
    total_offers = 0
    for ol in offer_listings:
        total_offers += int(ol.get("Count", 0))

    return {
        "asin": asin,
        "sellerSku": offer.get("SellerSKU"),
        "listingPrice": listing_price.get("Amount"),
        "shippingPrice": shipping_price.get("Amount"),
        "landedPrice": landed_price.get("Amount"),
        "points": points_obj.get("PointsMonetaryValue", {}).get("Amount"),
        "buyBoxPrice": buy_box_price,
        "numberOfOffers": total_offers,
        "condition": offer.get("SubCondition") or offer.get("ItemCondition"),
    }


def _flatten_sales_metric(metric: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Sales orderMetrics response entry."""
    interval = metric.get("interval", "")
    # interval is ISO 8601 like "2024-01-01T00:00:00--2024-01-02T00:00:00"
    date_str = interval.split("T")[0] if interval else None

    total_sales = metric.get("totalSales") or {}
    avg_unit_price = metric.get("averageUnitPrice") or {}

    return {
        "date": date_str,
        "unitCount": metric.get("unitCount"),
        "orderItemCount": metric.get("orderItemCount"),
        "orderCount": metric.get("orderCount"),
        "averageUnitPrice": avg_unit_price.get("amount"),
        "totalSales_amount": total_sales.get("amount"),
        "totalSales_currency": total_sales.get("currencyCode"),
    }


def _flatten_fba_shipment(shipment: dict[str, Any]) -> dict[str, Any]:
    """Flatten an FBA Inbound Shipment."""
    return {
        "shipmentId": shipment.get("ShipmentId"),
        "shipmentName": shipment.get("ShipmentName"),
        "shipmentStatus": shipment.get("ShipmentStatus"),
        "destinationFulfillmentCenterId": shipment.get("DestinationFulfillmentCenterId"),
        "labelPrepType": shipment.get("LabelPrepType"),
        "areCasesRequired": shipment.get("AreCasesRequired"),
    }


def _flatten_mfn_shipment(shipment: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Merchant Fulfillment (MFN) shipment."""
    label = shipment.get("Label") or {}
    return {
        "shipmentId": shipment.get("ShipmentId"),
        "amazonOrderId": shipment.get("AmazonOrderId"),
        "shipmentStatus": shipment.get("ShipmentStatus"),
        "trackingNumber": shipment.get("TrackingId"),
        "carrier": shipment.get("ShippingService", {}).get("CarrierName")
                   if isinstance(shipment.get("ShippingService"), dict) else None,
        "shipDate": shipment.get("ShipDate"),
        "deliveryDate": shipment.get("EstimatedDeliveryDate"),
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
            "catalog": self._extract_catalog,
            "pricing": self._extract_pricing,
            "sales_metrics": self._extract_sales_metrics,
            "fba_shipments": self._extract_fba_shipments,
            "brand_analytics": self._extract_brand_analytics,
            "direct_fulfillment": self._extract_direct_fulfillment,
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

    # -- Catalog Items --------------------------------------------------------

    async def _extract_catalog(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        params: dict[str, Any] = {
            "marketplaceIds": self._marketplace_id,
            "includedData": "attributes,identifiers,images,salesRanks,summaries",
            "pageSize": min(limit, 20),
        }
        if cursor:
            params["pageToken"] = cursor

        body = await self._sp_api_get("/catalog/2022-04-01/items", params=params)
        raw_items = body.get("items", [])

        records = [_flatten_catalog_item(item) for item in raw_items]
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["catalog"]["fields"]]
        pagination = body.get("pagination", {})
        next_cursor = pagination.get("nextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    # -- Pricing --------------------------------------------------------------

    async def _extract_pricing(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        # First, fetch ASINs from catalog/inventory to price against
        asins: list[str] = []
        try:
            inv_body = await self._sp_api_get("/fba/inventory/v1/summaries", params={
                "marketplaceIds": self._marketplace_id,
                "granularityType": "Marketplace",
                "granularityId": self._marketplace_id,
            })
            inv_payload = inv_body.get("payload", inv_body)
            for item in inv_payload.get("inventorySummaries", []):
                asin = item.get("asin")
                if asin and asin not in asins:
                    asins.append(asin)
                if len(asins) >= min(limit, 20):
                    break
        except Exception:
            logger.warning("Failed to fetch ASINs from inventory for pricing lookup")

        if not asins:
            return {
                "items": [],
                "columns": columns or [f["name"] for f in ENDPOINT_SCHEMAS["pricing"]["fields"]],
                "next_cursor": None,
                "total": 0,
            }

        params: dict[str, Any] = {
            "MarketplaceId": self._marketplace_id,
            "ItemType": "Asin",
            "Asins": ",".join(asins),
        }

        body = await self._sp_api_get("/products/pricing/v0/price", params=params)
        payload = body.get("payload", body)
        raw_items = payload if isinstance(payload, list) else payload.get("prices", [])

        records = [_flatten_pricing(item) for item in raw_items]
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["pricing"]["fields"]]

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": None,  # Pricing API does not paginate
            "total": len(records),
        }

    # -- Sales Metrics --------------------------------------------------------

    async def _extract_sales_metrics(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        if start_date:
            interval_start = f"{start_date}T00:00:00Z"
        else:
            interval_start = (
                datetime.now(timezone.utc) - timedelta(days=30)
            ).strftime("%Y-%m-%dT00:00:00Z")
        if end_date:
            interval_end = f"{end_date}T23:59:59Z"
        else:
            interval_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z")

        params: dict[str, Any] = {
            "marketplaceIds": self._marketplace_id,
            "interval": f"{interval_start}--{interval_end}",
            "granularity": "Day",
        }

        body = await self._sp_api_get("/sales/v1/orderMetrics", params=params)
        payload = body.get("payload", body)
        raw_items = payload if isinstance(payload, list) else payload.get("orderMetrics", [])

        records = [_flatten_sales_metric(item) for item in raw_items[:limit]]
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["sales_metrics"]["fields"]]

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": None,  # Sales metrics returns all days in the interval
            "total": len(records),
        }

    # -- FBA Inbound Shipments ------------------------------------------------

    async def _extract_fba_shipments(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        params: dict[str, Any] = {
            "MarketplaceId": self._marketplace_id,
            "ShipmentStatusList": "WORKING,SHIPPED,RECEIVING,CLOSED",
            "QueryType": "SHIPMENT",
        }
        if cursor:
            params["NextToken"] = cursor

        body = await self._sp_api_get("/fba/inbound/v0/shipments", params=params)
        payload = body.get("payload", body)
        shipment_data = payload.get("ShipmentData", [])

        records = [_flatten_fba_shipment(s) for s in shipment_data[:limit]]
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["fba_shipments"]["fields"]]
        next_cursor = payload.get("NextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    # -- Brand Analytics (via Reports API) ------------------------------------

    async def _extract_brand_analytics(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        """Fetch brand analytics reports using the Reports API filtered to BA report types."""
        params: dict[str, Any] = {
            "marketplaceIds": self._marketplace_id,
            "pageSize": min(limit, 100),
            "reportTypes": ",".join([
                "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT",
                "GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT",
                "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT",
                "GET_BRAND_ANALYTICS_ALTERNATE_ITEM_REPORT",
                "GET_BRAND_ANALYTICS_ITEM_COMPARISON_REPORT",
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

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["brand_analytics"]["fields"]]
        next_cursor = payload.get("nextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }

    # -- Direct Fulfillment / Merchant Fulfillment ----------------------------

    async def _extract_direct_fulfillment(
        self, *, columns: list[str] | None, limit: int, cursor: str | None,
        start_date: str | None = None, end_date: str | None = None,
    ) -> dict:
        """Fetch Merchant Fulfillment (MFN) shipments for seller direct fulfillment."""
        params: dict[str, Any] = {
            "MarketplaceId": self._marketplace_id,
            "MaxResultsPerPage": min(limit, 100),
        }
        if start_date:
            params["ShipDateCreatedAfter"] = f"{start_date}T00:00:00Z"
        else:
            params["ShipDateCreatedAfter"] = (
                datetime.now(timezone.utc) - timedelta(days=30)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        if end_date:
            params["ShipDateCreatedBefore"] = f"{end_date}T23:59:59Z"
        if cursor:
            params["NextToken"] = cursor

        body = await self._sp_api_get("/mfn/v0/shipments", params=params)
        payload = body.get("payload", body)
        raw_items = payload.get("ShipmentList", [])

        records = [_flatten_mfn_shipment(s) for s in raw_items[:limit]]
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        all_columns = [f["name"] for f in ENDPOINT_SCHEMAS["direct_fulfillment"]["fields"]]
        next_cursor = payload.get("NextToken")

        return {
            "items": records,
            "columns": columns or all_columns,
            "next_cursor": next_cursor,
            "total": len(records),
        }
