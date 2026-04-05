import logging
import xml.etree.ElementTree as ET
from typing import Any

import httpx

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
        "name": "HighRatingTrendRanking API - 高評価トレンドランキング",
        "description": "Yahoo!ショッピング高評価トレンドランキングAPI。",
    },
    {
        "id": "seller_items",
        "name": "MyItemList API - 出店者商品一覧",
        "description": "出店者の商品一覧を取得します。アクセストークンが必要です。",
    },
    {
        "id": "seller_orders",
        "name": "OrderList API - 出店者注文一覧",
        "description": "出店者の注文一覧を取得します。アクセストークンが必要です。",
    },
    {
        "id": "seller_order_items",
        "name": "OrderInfo API - 注文明細",
        "description": "注文の商品明細を取得します。orderListで取得した注文IDごとにorderInfoを呼び出します。",
    },
]

# ---------------------------------------------------------------------------
# Schema definitions per endpoint
# ---------------------------------------------------------------------------

ENDPOINT_SCHEMAS: dict[str, dict] = {
    "item_search": {
        "fields": [
            {"name": "name", "type": "string", "description": "商品名", "bq_type": "STRING"},
            {"name": "description", "type": "string", "description": "商品説明", "bq_type": "STRING"},
            {"name": "price", "type": "integer", "description": "価格", "bq_type": "INTEGER"},
            {"name": "url", "type": "string", "description": "商品ページURL", "bq_type": "STRING"},
            {"name": "imageUrl", "type": "string", "description": "商品画像URL", "bq_type": "STRING"},
            {"name": "reviewAverage", "type": "number", "description": "レビュー平均評価", "bq_type": "FLOAT"},
            {"name": "reviewCount", "type": "integer", "description": "レビュー数", "bq_type": "INTEGER"},
            {"name": "shopName", "type": "string", "description": "ストア名", "bq_type": "STRING"},
            {"name": "shopUrl", "type": "string", "description": "ストアURL", "bq_type": "STRING"},
            {"name": "janCode", "type": "string", "description": "JANコード", "bq_type": "STRING"},
            {"name": "brand", "type": "string", "description": "ブランド名", "bq_type": "STRING"},
        ],
    },
    "category_ranking": {
        "fields": [
            {"name": "rank", "type": "integer", "description": "ランキング順位", "bq_type": "INTEGER"},
            {"name": "name", "type": "string", "description": "商品名", "bq_type": "STRING"},
            {"name": "price", "type": "integer", "description": "価格", "bq_type": "INTEGER"},
            {"name": "url", "type": "string", "description": "商品ページURL", "bq_type": "STRING"},
            {"name": "imageUrl", "type": "string", "description": "商品画像URL", "bq_type": "STRING"},
            {"name": "reviewAverage", "type": "number", "description": "レビュー平均評価", "bq_type": "FLOAT"},
            {"name": "reviewCount", "type": "integer", "description": "レビュー数", "bq_type": "INTEGER"},
            {"name": "shopName", "type": "string", "description": "ストア名", "bq_type": "STRING"},
        ],
    },
    "seller_items": {
        "fields": [
            {"name": "itemCode", "type": "string", "description": "商品コード", "bq_type": "STRING"},
            {"name": "title", "type": "string", "description": "商品タイトル", "bq_type": "STRING"},
            {"name": "price", "type": "integer", "description": "販売価格", "bq_type": "INTEGER"},
            {"name": "originalPrice", "type": "integer", "description": "定価", "bq_type": "INTEGER"},
            {"name": "availability", "type": "string", "description": "在庫状況", "bq_type": "STRING"},
            {"name": "updateTime", "type": "datetime", "description": "更新日時", "bq_type": "TIMESTAMP"},
        ],
    },
    "seller_orders": {
        "fields": [
            # ── 注文基本情報 ──
            {"name": "orderId", "type": "string", "description": "注文ID", "bq_type": "STRING"},
            {"name": "orderTime", "type": "datetime", "description": "注文日時", "bq_type": "TIMESTAMP"},
            {"name": "orderStatus", "type": "integer", "description": "注文ステータス (1:予約中 2:処理中 3:保留 4:キャンセル 5:完了)", "bq_type": "INTEGER"},
            {"name": "lastUpdateTime", "type": "datetime", "description": "最終更新日時", "bq_type": "TIMESTAMP"},
            {"name": "deviceType", "type": "integer", "description": "デバイス種別 (1:PC 2:モバイル 3:スマホ 4:タブレット)", "bq_type": "INTEGER"},
            {"name": "isActive", "type": "boolean", "description": "有効注文フラグ", "bq_type": "BOOLEAN"},
            {"name": "isSplit", "type": "boolean", "description": "分割注文フラグ", "bq_type": "BOOLEAN"},
            {"name": "parentOrderId", "type": "string", "description": "分割元注文ID", "bq_type": "STRING"},
            {"name": "suspect", "type": "integer", "description": "悪戯注文フラグ (0/1/2)", "bq_type": "INTEGER"},
            {"name": "isAffiliate", "type": "boolean", "description": "アフィリエイト注文フラグ", "bq_type": "BOOLEAN"},
            {"name": "isYahooAuctionOrder", "type": "boolean", "description": "ヤフオク注文フラグ", "bq_type": "BOOLEAN"},
            {"name": "printSlipFlag", "type": "boolean", "description": "注文伝票印刷済みフラグ", "bq_type": "BOOLEAN"},
            {"name": "printDeliveryFlag", "type": "boolean", "description": "納品書印刷済みフラグ", "bq_type": "BOOLEAN"},
            {"name": "buyerCommentsFlag", "type": "boolean", "description": "購入者コメント有無フラグ", "bq_type": "BOOLEAN"},
            # ── 金額 ──
            {"name": "totalPrice", "type": "integer", "description": "合計金額", "bq_type": "INTEGER"},
            {"name": "payCharge", "type": "integer", "description": "決済手数料", "bq_type": "INTEGER"},
            {"name": "shipCharge", "type": "integer", "description": "送料", "bq_type": "INTEGER"},
            {"name": "giftWrapCharge", "type": "integer", "description": "ギフト包装料", "bq_type": "INTEGER"},
            {"name": "discount", "type": "integer", "description": "手動値引き額", "bq_type": "INTEGER"},
            {"name": "usePoint", "type": "integer", "description": "利用ポイント数", "bq_type": "INTEGER"},
            {"name": "giftCardDiscount", "type": "integer", "description": "ギフトカード利用額", "bq_type": "INTEGER"},
            {"name": "totalMallCouponDiscount", "type": "integer", "description": "モールクーポン割引合計額", "bq_type": "INTEGER"},
            {"name": "refundTotalPrice", "type": "integer", "description": "返金合計額", "bq_type": "INTEGER"},
            # ── 決済 ──
            {"name": "payStatus", "type": "integer", "description": "支払ステータス (0:未払い 1:支払済み)", "bq_type": "INTEGER"},
            {"name": "settleStatus", "type": "integer", "description": "決済ステータスコード", "bq_type": "INTEGER"},
            {"name": "payMethod", "type": "string", "description": "支払い方法コード", "bq_type": "STRING"},
            {"name": "payMethodName", "type": "string", "description": "支払い方法表示名", "bq_type": "STRING"},
            {"name": "payDate", "type": "string", "description": "入金日", "bq_type": "STRING"},
            {"name": "settleId", "type": "string", "description": "決済ID", "bq_type": "STRING"},
            {"name": "needBillSlip", "type": "boolean", "description": "請求書要否フラグ", "bq_type": "BOOLEAN"},
            {"name": "needReceipt", "type": "boolean", "description": "領収書要否フラグ", "bq_type": "BOOLEAN"},
            # ── 請求先 ──
            {"name": "billFirstName", "type": "string", "description": "請求先名（名）", "bq_type": "STRING"},
            {"name": "billLastName", "type": "string", "description": "請求先名（姓）", "bq_type": "STRING"},
            {"name": "billFirstNameKana", "type": "string", "description": "請求先名カナ（名）", "bq_type": "STRING"},
            {"name": "billLastNameKana", "type": "string", "description": "請求先名カナ（姓）", "bq_type": "STRING"},
            {"name": "billZipCode", "type": "string", "description": "請求先郵便番号", "bq_type": "STRING"},
            {"name": "billPrefecture", "type": "string", "description": "請求先都道府県", "bq_type": "STRING"},
            {"name": "billPhoneNumber", "type": "string", "description": "請求先電話番号", "bq_type": "STRING"},
            {"name": "billMailAddress", "type": "string", "description": "請求先メールアドレス", "bq_type": "STRING"},
            # ── 配送 ──
            {"name": "shipStatus", "type": "integer", "description": "出荷ステータス (0:未出荷 1:出荷済み 2:着荷済み 3:不達 4:返品)", "bq_type": "INTEGER"},
            {"name": "shipMethod", "type": "string", "description": "配送方法コード", "bq_type": "STRING"},
            {"name": "shipMethodName", "type": "string", "description": "配送方法名", "bq_type": "STRING"},
            {"name": "shipDate", "type": "string", "description": "出荷日", "bq_type": "STRING"},
            {"name": "arrivalDate", "type": "string", "description": "着荷日", "bq_type": "STRING"},
            {"name": "shipRequestDate", "type": "string", "description": "配送希望日", "bq_type": "STRING"},
            {"name": "shipRequestTime", "type": "string", "description": "配送希望時間帯", "bq_type": "STRING"},
            {"name": "shipNotes", "type": "string", "description": "配送メモ", "bq_type": "STRING"},
            {"name": "shipCompanyCode", "type": "integer", "description": "配送会社コード", "bq_type": "INTEGER"},
            {"name": "shipInvoiceNumber1", "type": "string", "description": "送り状番号1", "bq_type": "STRING"},
            {"name": "shipInvoiceNumber2", "type": "string", "description": "送り状番号2", "bq_type": "STRING"},
            {"name": "shipUrl", "type": "string", "description": "配送会社追跡URL", "bq_type": "STRING"},
            {"name": "needGiftWrap", "type": "boolean", "description": "ギフト包装要否フラグ", "bq_type": "BOOLEAN"},
            {"name": "needGiftWrapPaper", "type": "boolean", "description": "のし要否フラグ", "bq_type": "BOOLEAN"},
            {"name": "excellentDelivery", "type": "integer", "description": "優良配送フラグ", "bq_type": "INTEGER"},
            {"name": "isEazy", "type": "boolean", "description": "EAZY/置き配フラグ", "bq_type": "BOOLEAN"},
            # ── お届け先 ──
            {"name": "shipFirstName", "type": "string", "description": "お届け先名（名）", "bq_type": "STRING"},
            {"name": "shipLastName", "type": "string", "description": "お届け先名（姓）", "bq_type": "STRING"},
            {"name": "shipFirstNameKana", "type": "string", "description": "お届け先名カナ（名）", "bq_type": "STRING"},
            {"name": "shipLastNameKana", "type": "string", "description": "お届け先名カナ（姓）", "bq_type": "STRING"},
            {"name": "shipPrefecture", "type": "string", "description": "お届け先都道府県", "bq_type": "STRING"},
            # ── 定期購入 ──
            {"name": "isSubscription", "type": "boolean", "description": "定期購入注文フラグ", "bq_type": "BOOLEAN"},
            {"name": "subscriptionId", "type": "string", "description": "定期購入親ID", "bq_type": "STRING"},
            {"name": "subscriptionContinueCount", "type": "integer", "description": "定期購入継続回数", "bq_type": "INTEGER"},
            # ── セラー ──
            {"name": "sellerId", "type": "string", "description": "出店者（ストア）ID", "bq_type": "STRING"},
        ],
    },
    "seller_order_items": {
        "fields": [
            {"name": "orderId", "type": "string", "description": "注文ID", "bq_type": "STRING"},
            {"name": "orderTime", "type": "datetime", "description": "注文日時", "bq_type": "TIMESTAMP"},
            {"name": "lineId", "type": "integer", "description": "明細行ID", "bq_type": "INTEGER"},
            {"name": "itemId", "type": "string", "description": "商品ID", "bq_type": "STRING"},
            {"name": "title", "type": "string", "description": "商品名", "bq_type": "STRING"},
            {"name": "subCode", "type": "string", "description": "商品サブコード", "bq_type": "STRING"},
            {"name": "unitPrice", "type": "integer", "description": "商品単価（税込）", "bq_type": "INTEGER"},
            {"name": "quantity", "type": "integer", "description": "数量", "bq_type": "INTEGER"},
            {"name": "itemTaxRatio", "type": "integer", "description": "商品税率", "bq_type": "INTEGER"},
            {"name": "jan", "type": "string", "description": "JANコード", "bq_type": "STRING"},
            {"name": "productId", "type": "string", "description": "メーカー品番", "bq_type": "STRING"},
            {"name": "categoryId", "type": "integer", "description": "カテゴリコード", "bq_type": "INTEGER"},
            {"name": "couponDiscount", "type": "integer", "description": "クーポン割引額", "bq_type": "INTEGER"},
            {"name": "originalPrice", "type": "integer", "description": "クーポン適用前価格", "bq_type": "INTEGER"},
            {"name": "sellerId", "type": "string", "description": "出店者ID", "bq_type": "STRING"},
        ],
    },
}

# ---------------------------------------------------------------------------
# API URL configuration
# ---------------------------------------------------------------------------

_ENDPOINT_URLS: dict[str, str] = {
    "item_search": "https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch",
    "category_ranking": "https://shopping.yahooapis.jp/ShoppingWebService/V1/highRatingTrendRanking",
    "seller_items": "https://circus.shopping.yahooapis.jp/ShoppingWebService/V1/myItemList",
    "seller_orders": "https://circus.shopping.yahooapis.jp/ShoppingWebService/V1/orderList",
    "seller_order_items": "https://circus.shopping.yahooapis.jp/ShoppingWebService/V1/orderInfo",
}

# Endpoints that require seller authentication (Bearer token)
_SELLER_ENDPOINTS = {"seller_items", "seller_orders", "seller_order_items"}

# Endpoints that use public appid-based authentication
_PUBLIC_ENDPOINTS = {"item_search", "category_ranking"}

# Endpoints that return JSON (public V3 APIs)
_JSON_ENDPOINTS = {"item_search"}

# Endpoints that return JSON (V1 public with output=json)
_JSON_OUTPUT_ENDPOINTS = {"category_ranking"}

# Yahoo API response root keys for extracting item lists from JSON responses
_RESPONSE_ROOT_KEYS: dict[str, list[str]] = {
    "item_search": ["hits"],
    "category_ranking": ["ranking_data"],
}


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------


def _build_order_list_xml(
    seller_id: str,
    fields: list[str],
    *,
    limit: int = 50,
    start: int = 1,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """Build the XML request body for the orderList API.

    The Yahoo orderList API requires a POST with an XML body containing
    search conditions, field selection, and pagination parameters.

    Date format expected by the API: YYYYMMDDHHmmss
    """
    field_csv = ",".join(fields)

    condition_xml = ""
    if start_date:
        # Convert ISO date (2026-03-20) to API format (20260320000000)
        date_str = start_date.replace("-", "") + "000000"
        condition_xml += f"      <OrderTimeFrom>{date_str}</OrderTimeFrom>\n"
    if end_date:
        date_str = end_date.replace("-", "") + "235959"
        condition_xml += f"      <OrderTimeTo>{date_str}</OrderTimeTo>\n"

    # Default condition: if no dates specified, search last 90 days
    if not condition_xml:
        from datetime import datetime, timedelta
        default_from = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d000000")
        default_to = datetime.now().strftime("%Y%m%d235959")
        condition_xml = (
            f"      <OrderTimeFrom>{default_from}</OrderTimeFrom>\n"
            f"      <OrderTimeTo>{default_to}</OrderTimeTo>\n"
        )

    return (
        "<Req>\n"
        "  <Search>\n"
        f"    <Result>{limit}</Result>\n"
        f"    <Start>{start}</Start>\n"
        "    <Sort>+order_time</Sort>\n"
        "    <Condition>\n"
        f"{condition_xml}"
        "    </Condition>\n"
        f"    <Field>{field_csv}</Field>\n"
        "  </Search>\n"
        f"  <SellerId>{seller_id}</SellerId>\n"
        "</Req>"
    )


def _parse_xml_orders(xml_text: str) -> list[dict[str, Any]]:
    """Parse the XML response from orderList and extract order records."""
    root = ET.fromstring(xml_text)

    # Find all Order elements (handle namespace if present)
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    orders: list[dict[str, Any]] = []

    # Yahoo orderList API wraps each order in <OrderInfo>
    for order_el in root.iter(f"{ns}OrderInfo"):
        record: dict[str, Any] = {}
        # OrderInfo may contain nested elements; flatten direct children
        for child in order_el:
            tag = child.tag.replace(ns, "")
            record[tag] = child.text
        if record:
            orders.append(record)

    # Fallback: try <Order> tag if <OrderInfo> yielded nothing
    if not orders:
        for order_el in root.iter(f"{ns}Order"):
            record = {}
            for child in order_el:
                tag = child.tag.replace(ns, "")
                record[tag] = child.text
            if record:
                orders.append(record)

    return orders


def _parse_xml_items(xml_text: str) -> list[dict[str, Any]]:
    """Parse the XML response from myItemList and extract item records."""
    root = ET.fromstring(xml_text)

    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    items: list[dict[str, Any]] = []

    for item_el in root.iter(f"{ns}Item"):
        record: dict[str, Any] = {}
        for child in item_el:
            tag = child.tag.replace(ns, "")
            record[tag] = child.text
        if record:
            items.append(record)

    return items


def _parse_order_info_items(xml_text: str, order_id: str) -> list[dict[str, Any]]:
    """Parse the XML response from orderInfo and extract item records."""
    root = ET.fromstring(xml_text)

    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    # orderInfo のレスポンスから注文レベル情報を取得
    order_time = None
    seller_id = None
    for el in root.iter(f"{ns}OrderTime"):
        order_time = el.text
        break
    for el in root.iter(f"{ns}SellerId"):
        seller_id = el.text
        break

    _MAP = {
        "orderId": order_id,
        "orderTime": order_time,
    }

    _ITEM_FIELD_MAP = {
        "lineId": "LineId",
        "itemId": "ItemId",
        "title": "Title",
        "subCode": "SubCode",
        "unitPrice": "UnitPrice",
        "quantity": "Quantity",
        "itemTaxRatio": "ItemTaxRatio",
        "jan": "Jan",
        "productId": "ProductId",
        "categoryId": "CategoryId",
        "couponDiscount": "CouponDiscount",
        "originalPrice": "OriginalPrice",
    }

    items: list[dict[str, Any]] = []
    for item_el in root.iter(f"{ns}Item"):
        record: dict[str, Any] = dict(_MAP)
        record["sellerId"] = seller_id

        # Item 要素の直接の子要素からフィールドを抽出
        child_map = {}
        for child in item_el:
            tag = child.tag.replace(ns, "")
            child_map[tag] = child.text

        for camel, pascal in _ITEM_FIELD_MAP.items():
            record[camel] = child_map.get(pascal)

        items.append(record)

    return items


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
    """Flatten a single raw item from the Yahoo API response."""
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
            "title": raw.get("Title") or raw.get("title") or raw.get("Name") or raw.get("name"),
            "price": raw.get("Price") or raw.get("price"),
            "originalPrice": raw.get("OriginalPrice") or raw.get("originalPrice"),
            "availability": raw.get("Availability") or raw.get("availability") or raw.get("SubCode") or raw.get("Display"),
            "updateTime": raw.get("UpdateTime") or raw.get("updateTime") or raw.get("EditingTime"),
        }

    if endpoint_id == "seller_orders":
        # Map PascalCase API response keys to camelCase schema field names.
        # Only includes fields available from the orderList API.
        _MAP = {
            # 注文基本
            "orderId": "OrderId", "orderTime": "OrderTime",
            "orderStatus": "OrderStatus", "lastUpdateTime": "LastUpdateTime",
            "deviceType": "DeviceType", "isActive": "IsActive",
            "isSplit": "IsSplit", "parentOrderId": "ParentOrderId",
            "suspect": "Suspect", "isAffiliate": "IsAffiliate",
            "isYahooAuctionOrder": "IsYahooAuctionOrder",
            "printSlipFlag": "PrintSlipFlag", "printDeliveryFlag": "PrintDeliveryFlag",
            "buyerCommentsFlag": "BuyerCommentsFlag",
            # 金額
            "totalPrice": "TotalPrice", "payCharge": "PayCharge",
            "shipCharge": "ShipCharge", "giftWrapCharge": "GiftWrapCharge",
            "discount": "Discount", "usePoint": "UsePoint",
            "giftCardDiscount": "GiftCardDiscount",
            "totalMallCouponDiscount": "TotalMallCouponDiscount",
            "refundTotalPrice": "RefundTotalPrice",
            # 決済
            "payStatus": "PayStatus", "settleStatus": "SettleStatus",
            "payMethod": "PayMethod", "payMethodName": "PayMethodName",
            "payDate": "PayDate", "settleId": "SettleId",
            "needBillSlip": "NeedBillSlip", "needReceipt": "NeedReceipt",
            # 請求先
            "billFirstName": "BillFirstName", "billLastName": "BillLastName",
            "billFirstNameKana": "BillFirstNameKana", "billLastNameKana": "BillLastNameKana",
            "billZipCode": "BillZipCode", "billPrefecture": "BillPrefecture",
            "billPhoneNumber": "BillPhoneNumber", "billMailAddress": "BillMailAddress",
            # 配送
            "shipStatus": "ShipStatus", "shipMethod": "ShipMethod",
            "shipMethodName": "ShipMethodName", "shipDate": "ShipDate",
            "arrivalDate": "ArrivalDate", "shipRequestDate": "ShipRequestDate",
            "shipRequestTime": "ShipRequestTime", "shipNotes": "ShipNotes",
            "shipCompanyCode": "ShipCompanyCode",
            "shipInvoiceNumber1": "ShipInvoiceNumber1",
            "shipInvoiceNumber2": "ShipInvoiceNumber2", "shipUrl": "ShipUrl",
            "needGiftWrap": "NeedGiftWrap", "needGiftWrapPaper": "NeedGiftWrapPaper",
            "excellentDelivery": "ExcellentDelivery", "isEazy": "IsEazy",
            # お届け先
            "shipFirstName": "ShipFirstName", "shipLastName": "ShipLastName",
            "shipFirstNameKana": "ShipFirstNameKana", "shipLastNameKana": "ShipLastNameKana",
            "shipPrefecture": "ShipPrefecture",
            # 定期購入
            "isSubscription": "IsSubscription", "subscriptionId": "SubscriptionId",
            "subscriptionContinueCount": "SubscriptionContinueCount",
            # セラー
            "sellerId": "SellerId",
        }
        return {camel: raw.get(pascal) for camel, pascal in _MAP.items()}

    if endpoint_id == "seller_order_items":
        # orderInfo のレスポンスは既に _parse_order_info_items で camelCase に変換済み
        return raw

    return raw


# ---------------------------------------------------------------------------
# YahooClient
# ---------------------------------------------------------------------------


class YahooClient(PlatformClient):
    platform_id: str = "yahoo"
    platform_name: str = "Yahoo!ショッピング"

    # Order fields requested from the orderList API.
    # NOTE: Item-level fields (LineId, ItemId, Title, etc.) are only
    # available via the orderInfo API, not orderList.
    _ORDER_FIELDS = [
        # 注文基本
        "OrderId", "OrderTime", "OrderStatus", "LastUpdateTime",
        "DeviceType", "IsActive", "IsSplit", "ParentOrderId",
        "Suspect", "IsAffiliate",
        "IsYahooAuctionOrder",
        "PrintSlipFlag", "PrintDeliveryFlag", "BuyerCommentsFlag",
        # 金額
        "TotalPrice", "PayCharge", "ShipCharge", "GiftWrapCharge",
        "Discount", "UsePoint",
        "GiftCardDiscount", "TotalMallCouponDiscount",
        "RefundTotalPrice",
        # 決済
        "PayStatus", "SettleStatus", "PayMethod", "PayMethodName",
        "PayDate", "SettleId", "NeedBillSlip", "NeedReceipt",
        # 請求先
        "BillFirstName", "BillLastName", "BillFirstNameKana", "BillLastNameKana",
        "BillZipCode", "BillPrefecture", "BillPhoneNumber", "BillMailAddress",
        # 配送
        "ShipStatus", "ShipMethod", "ShipMethodName",
        "ShipDate", "ArrivalDate", "ShipRequestDate", "ShipRequestTime",
        "ShipNotes", "ShipCompanyCode",
        "ShipInvoiceNumber1", "ShipInvoiceNumber2", "ShipUrl",
        "NeedGiftWrap", "NeedGiftWrapPaper",
        "ExcellentDelivery", "IsEazy",
        # お届け先
        "ShipFirstName", "ShipLastName", "ShipFirstNameKana", "ShipLastNameKana",
        "ShipPrefecture",
        # 定期購入
        "IsSubscription", "SubscriptionId", "SubscriptionContinueCount",
        # セラー
        "SellerId",
    ]

    _YAHOO_TOKEN_URL = "https://auth.login.yahoo.co.jp/yconnect/v2/token"

    def __init__(self) -> None:
        self._client_id: str = settings.yahoo_client_id or ""
        self._client_secret: str = settings.yahoo_client_secret or ""
        self._access_token: str = settings.yahoo_access_token or ""
        self._refresh_token: str = settings.yahoo_refresh_token or ""
        self._seller_id: str = settings.yahoo_seller_id or ""
        self._http = ReadOnlyHttpClient(platform="yahoo")

    # -- Token refresh -----------------------------------------------------

    async def _ensure_valid_token(self) -> None:
        """Refresh the access token using the refresh token if available.

        Yahoo access tokens expire after ~1 hour. When a 401 occurs, we
        attempt a token refresh before retrying the request.
        """
        if not self._refresh_token or not self._client_id or not self._client_secret:
            return

        logger.info("Refreshing Yahoo access token")
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self._YAHOO_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
            )

        if resp.status_code != 200:
            logger.error("Yahoo token refresh failed: %d %s", resp.status_code, resp.text)
            raise PermissionError(
                "Yahoo アクセストークンの更新に失敗しました。再認証してください。"
            )

        data = resp.json()
        logger.info(
            "Yahoo token response keys=%s, token_type=%s, expires_in=%s, scope=%s",
            list(data.keys()),
            data.get("token_type"),
            data.get("expires_in"),
            data.get("scope"),
        )
        self._access_token = data["access_token"]
        # Update in-memory settings so other code sees the new token
        object.__setattr__(settings, "yahoo_access_token", self._access_token)
        if data.get("refresh_token"):
            self._refresh_token = data["refresh_token"]
            object.__setattr__(settings, "yahoo_refresh_token", self._refresh_token)

        # Persist to storage so scheduled jobs also get the new token
        try:
            from app.routers.credentials import _read_env, _write_env
            env_values = _read_env()
            env_values["YAHOO_ACCESS_TOKEN"] = self._access_token
            if data.get("refresh_token"):
                env_values["YAHOO_REFRESH_TOKEN"] = self._refresh_token
            _write_env(env_values)
        except Exception as e:
            logger.warning("Could not persist refreshed Yahoo token: %s", e)

        logger.info("Yahoo access token refreshed successfully")

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
        keyword: str | None = None,
    ) -> dict:
        """Extract data from Yahoo Shopping API."""
        if endpoint_id not in _ENDPOINT_URLS:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")

        if endpoint_id == "item_search" and not keyword:
            raise ValueError("item_search requires a search keyword (検索キーワードを入力してください)")

        await yahoo_limiter.acquire()

        # Attempt extraction; on 401, refresh token and retry once
        raw_items = await self._extract_with_retry(
            endpoint_id=endpoint_id, limit=limit, cursor=cursor,
            start_date=start_date, end_date=end_date, keyword=keyword,
        )

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
        """Check if required credentials are set."""
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

    # -- Private extraction methods ----------------------------------------

    async def _extract_with_retry(
        self,
        *,
        endpoint_id: str,
        limit: int,
        cursor: str | None,
        start_date: str | None,
        end_date: str | None,
        keyword: str | None,
    ) -> list[dict[str, Any]]:
        """Run extraction, auto-refreshing the token on 401.

        After refreshing, wait a few seconds for Yahoo token propagation,
        then retry up to 2 more times with increasing delays.
        """
        import asyncio

        try:
            return await self._do_extract(
                endpoint_id=endpoint_id, limit=limit, cursor=cursor,
                start_date=start_date, end_date=end_date, keyword=keyword,
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 401 or endpoint_id not in _SELLER_ENDPOINTS:
                raise
            logger.warning(
                "Yahoo seller API returned 401. Response: %s", e.response.text[:1000]
            )

        # 401: refresh token and retry with backoff
        await self._ensure_valid_token()

        for attempt, delay in enumerate([3, 5], start=1):
            await asyncio.sleep(delay)
            logger.info("Token refresh retry %d (waited %ds)", attempt, delay)
            try:
                return await self._do_extract(
                    endpoint_id=endpoint_id, limit=limit, cursor=cursor,
                    start_date=start_date, end_date=end_date, keyword=keyword,
                )
            except httpx.HTTPStatusError as e2:
                if e2.response.status_code == 401 and attempt < 2:
                    logger.warning(
                        "Retry %d still 401. Response: %s",
                        attempt, e2.response.text[:1000],
                    )
                    continue
                if e2.response.status_code == 401:
                    logger.error(
                        "Yahoo seller API 401 after all retries. Response: %s",
                        e2.response.text[:1000],
                    )
                raise

    async def _do_extract(
        self,
        *,
        endpoint_id: str,
        limit: int,
        cursor: str | None,
        start_date: str | None,
        end_date: str | None,
        keyword: str | None,
    ) -> list[dict[str, Any]]:
        """Route to the appropriate extraction method."""
        if endpoint_id == "seller_order_items":
            return await self._extract_order_items(
                limit=limit, cursor=cursor,
                start_date=start_date, end_date=end_date,
            )
        elif endpoint_id == "seller_orders":
            return await self._extract_orders(
                limit=limit, cursor=cursor,
                start_date=start_date, end_date=end_date,
            )
        elif endpoint_id == "seller_items":
            return await self._extract_seller_items(
                limit=limit, cursor=cursor, keyword=keyword,
            )
        else:
            return await self._extract_json(
                endpoint_id=endpoint_id, limit=limit, cursor=cursor,
                keyword=keyword,
            )

    async def _extract_orders(
        self,
        *,
        limit: int,
        cursor: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        """Extract orders via POST + XML body."""
        url = _ENDPOINT_URLS["seller_orders"]
        start = int(cursor) if cursor else 1
        xml_body = _build_order_list_xml(
            seller_id=self._seller_id,
            fields=self._ORDER_FIELDS,
            limit=limit,
            start=start,
            start_date=start_date,
            end_date=end_date,
        )

        logger.info("orderList request XML:\n%s", xml_body)

        async def _do_post():
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/xml; charset=utf-8",
            }
            resp = await self._http.post(
                url, content=xml_body, headers=headers,
            )
            if resp.status_code >= 400:
                logger.error(
                    "orderList error %d. Response body: %s",
                    resp.status_code, resp.text[:2000],
                )
            resp.raise_for_status()
            return resp

        response = await retry_on_429(_do_post)
        logger.debug("orderList response (first 500 chars): %s", response.text[:500])
        orders = _parse_xml_orders(response.text)
        if not orders:
            logger.warning("orderList returned 0 orders. Response: %s", response.text[:1000])
        return orders

    _ORDER_INFO_FIELDS = [
        "OrderId", "OrderTime",
        "LineId", "ItemId", "Title", "SubCode",
        "UnitPrice", "Quantity", "ItemTaxRatio",
        "Jan", "ProductId", "CategoryId",
        "CouponDiscount", "OriginalPrice",
        "SellerId",
    ]

    async def _extract_order_items(
        self,
        *,
        limit: int,
        cursor: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        """orderList で注文ID一覧を取得し、各注文の orderInfo で商品明細を取得する。"""
        import asyncio

        # Step 1: orderList で注文ID一覧を取得
        orders = await self._extract_orders(
            limit=limit, cursor=cursor,
            start_date=start_date, end_date=end_date,
        )
        if not orders:
            return []

        order_ids = list(dict.fromkeys(o.get("OrderId") for o in orders if o.get("OrderId")))
        logger.info("orderInfo: %d件の注文の明細を取得します", len(order_ids))

        # Step 2: 各注文に対して orderInfo を呼び出す (レートリミット遵守)
        url = _ENDPOINT_URLS["seller_order_items"]
        field_csv = ",".join(self._ORDER_INFO_FIELDS)
        all_items: list[dict[str, Any]] = []

        for i, order_id in enumerate(order_ids):
            await yahoo_limiter.acquire()

            xml_body = (
                "<Req>\n"
                "  <Target>\n"
                f"    <OrderId>{order_id}</OrderId>\n"
                f"    <Field>{field_csv}</Field>\n"
                "  </Target>\n"
                f"  <SellerId>{self._seller_id}</SellerId>\n"
                "</Req>"
            )
            async def _do_post(body=xml_body):
                headers = {
                    "Authorization": f"Bearer {self._access_token}",
                    "Content-Type": "application/xml; charset=utf-8",
                }
                resp = await self._http.post(url, content=body, headers=headers)
                if resp.status_code >= 400:
                    logger.error(
                        "orderInfo error %d for order %s. Response: %s",
                        resp.status_code, order_id, resp.text[:1000],
                    )
                resp.raise_for_status()
                return resp

            response = await retry_on_429(_do_post)
            items = _parse_order_info_items(response.text, order_id)
            all_items.extend(items)

            if (i + 1) % 50 == 0:
                logger.info("orderInfo: %d/%d 注文処理済み (%d明細)", i + 1, len(order_ids), len(all_items))

        logger.info("orderInfo: 合計 %d明細を取得 (%d注文)", len(all_items), len(order_ids))
        return all_items

    async def _extract_seller_items(
        self,
        *,
        limit: int,
        cursor: str | None,
        keyword: str | None,
    ) -> list[dict[str, Any]]:
        """Extract seller items via GET with XML response."""
        url = _ENDPOINT_URLS["seller_items"]
        start = int(cursor) if cursor else 1
        headers = {"Authorization": f"Bearer {self._access_token}"}

        params: dict[str, Any] = {
            "seller_id": self._seller_id,
            "start": start,
            "results": limit,
        }
        if keyword:
            params["query"] = keyword

        async def _do_get():
            resp = await self._http.get(url, params=params, headers=headers)
            resp.raise_for_status()
            return resp

        response = await retry_on_429(_do_get)
        return _parse_xml_items(response.text)

    async def _extract_json(
        self,
        *,
        endpoint_id: str,
        limit: int,
        cursor: str | None,
        keyword: str | None,
    ) -> list[dict[str, Any]]:
        """Extract data from JSON-returning endpoints (item_search, category_ranking)."""
        url = _ENDPOINT_URLS[endpoint_id]
        params = self._build_params(endpoint_id, limit, cursor, keyword=keyword)
        headers = self._build_headers(endpoint_id)

        async def _do_get():
            resp = await self._http.get(url, params=params, headers=headers)
            resp.raise_for_status()
            return resp

        response = await retry_on_429(_do_get)
        body = response.json()
        return _extract_items(body, endpoint_id)

    # -- Private helpers ---------------------------------------------------

    def _build_params(
        self, endpoint_id: str, limit: int, cursor: str | None, *, keyword: str | None = None
    ) -> dict[str, Any]:
        """Build query parameters for JSON API endpoints."""
        params: dict[str, Any] = {}
        start = int(cursor) if cursor else 1

        if endpoint_id == "item_search":
            params["appid"] = self._client_id
            params["results"] = min(limit, 50)
            params["start"] = start
            params["query"] = keyword if keyword else ""

        elif endpoint_id == "category_ranking":
            params["appid"] = self._client_id
            params["offset"] = start
            params["limit"] = min(limit, 100)

        return params

    def _build_headers(self, endpoint_id: str) -> dict[str, str] | None:
        """Build request headers. Seller endpoints use Bearer auth."""
        if endpoint_id in _SELLER_ENDPOINTS:
            return {"Authorization": f"Bearer {self._access_token}"}
        return None
