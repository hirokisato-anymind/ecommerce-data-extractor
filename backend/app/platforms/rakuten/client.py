import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.config import settings
from app.core.read_only import ReadOnlyHttpClient
from app.platforms.base import PlatformClient

logger = logging.getLogger("ecommerce_data_extractor.rakuten")

JST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# RMS WEB API URLs (seller-only)
# ---------------------------------------------------------------------------
RMS_ORDER_SEARCH_URL = (
    "https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/"
)
RMS_ORDER_GET_URL = (
    "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/"
)
RMS_ITEMS_SEARCH_URL = (
    "https://api.rms.rakuten.co.jp/es/2.0/items/search"
)
RMS_INVENTORY_BULK_GET_URL = (
    "https://api.rms.rakuten.co.jp/es/2.1/inventories/bulk-get"
)

# ---------------------------------------------------------------------------
# Endpoint metadata
# ---------------------------------------------------------------------------
ENDPOINTS: list[dict[str, str]] = [
    {
        "id": "rms_orders",
        "name": "受注データ",
        "description": "RMS 受注検索API – 自店舗の注文データを取得（直近2年、最大63日間隔）",
    },
    {
        "id": "rms_items",
        "name": "商品管理",
        "description": "RMS 商品API 2.0 – 自店舗の商品一覧を取得",
    },
    {
        "id": "rms_inventory",
        "name": "在庫管理",
        "description": "RMS 在庫API 2.1 – 自店舗の在庫状況を取得（商品一覧から自動取得）",
    },
]

# ---------------------------------------------------------------------------
# Field schemas per endpoint
# ---------------------------------------------------------------------------
SCHEMAS: dict[str, dict[str, Any]] = {
    "rms_orders": {
        "fields": [
            # 基本情報
            {"name": "orderNumber", "type": "string", "description": "受注番号"},
            {"name": "orderDatetime", "type": "datetime", "description": "注文日時"},
            {"name": "orderProgress", "type": "integer", "description": "受注ステータス (100:注文確認待ち〜900:キャンセル確定)"},
            {"name": "orderType", "type": "integer", "description": "注文種別 (1:通常, 4:定期, 5:頒布会, 6:予約)"},
            {"name": "shopOrderCfmDatetime", "type": "datetime", "description": "注文確認日時"},
            {"name": "orderFixDatetime", "type": "datetime", "description": "注文確定日時"},
            {"name": "shippingCmplRptDatetime", "type": "datetime", "description": "発送完了報告日時"},
            {"name": "remarks", "type": "string", "description": "備考"},
            {"name": "memo", "type": "string", "description": "メモ"},
            # 金額
            {"name": "goodsPrice", "type": "integer", "description": "商品合計金額"},
            {"name": "goodsTax", "type": "integer", "description": "商品税額"},
            {"name": "postagePrice", "type": "integer", "description": "送料"},
            {"name": "deliveryPrice", "type": "integer", "description": "代引料"},
            {"name": "paymentCharge", "type": "integer", "description": "決済手数料"},
            {"name": "totalPrice", "type": "integer", "description": "合計金額"},
            {"name": "requestPrice", "type": "integer", "description": "請求金額"},
            {"name": "couponAllTotalPrice", "type": "integer", "description": "クーポン合計"},
            {"name": "couponShopPrice", "type": "integer", "description": "店舗負担クーポン"},
            # 決済
            {"name": "settlementMethod", "type": "string", "description": "決済方法"},
            {"name": "cardName", "type": "string", "description": "カードブランド"},
            {"name": "cardPayType", "type": "integer", "description": "カード支払区分 (0:一括, 1:リボ, 2:分割等)"},
            # 購入者情報
            {"name": "customerName", "type": "string", "description": "購入者氏名"},
            {"name": "customerNameKana", "type": "string", "description": "購入者氏名(カナ)"},
            {"name": "customerPrefecture", "type": "string", "description": "購入者都道府県"},
            {"name": "customerCity", "type": "string", "description": "購入者市区町村"},
            {"name": "customerZipCode", "type": "string", "description": "購入者郵便番号"},
            {"name": "customerPhone", "type": "string", "description": "購入者電話番号"},
            {"name": "customerEmail", "type": "string", "description": "購入者メール"},
            {"name": "customerSex", "type": "string", "description": "購入者性別"},
            # 送付先
            {"name": "deliveryName", "type": "string", "description": "送付先氏名"},
            {"name": "deliveryPrefecture", "type": "string", "description": "送付先都道府県"},
            {"name": "deliveryCity", "type": "string", "description": "送付先市区町村"},
            {"name": "deliveryZipCode", "type": "string", "description": "送付先郵便番号"},
            {"name": "deliveryPhone", "type": "string", "description": "送付先電話番号"},
            # 商品情報 (最初のパッケージの全商品を結合)
            {"name": "itemCount", "type": "integer", "description": "商品点数"},
            {"name": "itemNames", "type": "string", "description": "商品名一覧"},
            {"name": "itemNumbers", "type": "string", "description": "商品番号一覧"},
            {"name": "itemPrices", "type": "string", "description": "商品単価一覧"},
            {"name": "itemUnits", "type": "string", "description": "商品数量一覧"},
            {"name": "selectedChoices", "type": "string", "description": "選択肢一覧"},
            # フラグ
            {"name": "giftCheckFlag", "type": "integer", "description": "ギフトフラグ"},
            {"name": "asurakuFlag", "type": "integer", "description": "あす楽フラグ"},
            {"name": "rakutenMemberFlag", "type": "integer", "description": "楽天会員フラグ"},
            {"name": "usedPoint", "type": "integer", "description": "利用ポイント"},
            # 配送
            {"name": "deliveryMethod", "type": "string", "description": "配送方法"},
            {"name": "noshi", "type": "string", "description": "のし情報"},
        ],
    },
    "rms_items": {
        "fields": [
            {"name": "manageNumber", "type": "string", "description": "商品管理番号"},
            {"name": "title", "type": "string", "description": "商品名"},
            {"name": "genreId", "type": "string", "description": "ジャンルID"},
            {"name": "itemType", "type": "string", "description": "商品タイプ (NORMAL等)"},
            {"name": "standardPrice", "type": "integer", "description": "販売価格（先頭バリアント）"},
            {"name": "hideItem", "type": "boolean", "description": "非表示フラグ"},
            {"name": "created", "type": "datetime", "description": "登録日時"},
            {"name": "updated", "type": "datetime", "description": "更新日時"},
        ],
    },
    "rms_inventory": {
        "fields": [
            {"name": "manageNumber", "type": "string", "description": "商品管理番号"},
            {"name": "variantId", "type": "string", "description": "バリアントID"},
            {"name": "quantity", "type": "integer", "description": "在庫数"},
            {"name": "created", "type": "datetime", "description": "登録日時"},
            {"name": "updated", "type": "datetime", "description": "更新日時"},
        ],
    },
}


def _pick_fields(item: dict[str, Any], columns: list[str] | None) -> dict[str, Any]:
    """Return only the requested columns from *item*, or all keys if columns is None."""
    if columns is None:
        return item
    return {k: v for k, v in item.items() if k in columns}


class RakutenClient(PlatformClient):
    """Read-only client for the Rakuten RMS (seller) APIs."""

    platform_id: str = "rakuten"
    platform_name: str = "楽天市場"

    def __init__(self) -> None:
        self._http = ReadOnlyHttpClient(platform="rakuten")

    # ------------------------------------------------------------------
    # PlatformClient interface
    # ------------------------------------------------------------------

    async def get_endpoints(self) -> list[dict]:
        return list(ENDPOINTS)

    async def get_schema(self, endpoint_id: str) -> dict:
        schema = SCHEMAS.get(endpoint_id)
        if schema is None:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")
        return schema

    def is_configured(self) -> bool:
        """RMS APIs require service_secret and license_key."""
        return bool(
            settings.rakuten_service_secret
            and settings.rakuten_license_key
        )

    async def extract_data(
        self,
        endpoint_id: str,
        columns: list[str] | None,
        limit: int,
        cursor: str | None,
        *,
        keyword: str | None = None,
        genre_id: int | str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        handlers = {
            "rms_orders": self._extract_rms_orders,
            "rms_items": self._extract_rms_items,
            "rms_inventory": self._extract_rms_inventory,
        }
        handler = handlers.get(endpoint_id)
        if handler is None:
            raise ValueError(f"Unknown endpoint: {endpoint_id}")
        return await handler(columns=columns, limit=limit, cursor=cursor,
                             start_date=start_date, end_date=end_date)

    # ------------------------------------------------------------------
    # Auth helper
    # ------------------------------------------------------------------

    def _build_rms_auth_header(self) -> str:
        """Build ESA authorization: ``ESA Base64(serviceSecret:licenseKey)``"""
        if not settings.rakuten_service_secret or not settings.rakuten_license_key:
            raise RuntimeError(
                "RMS credentials (service_secret, license_key) are not configured"
            )
        raw = f"{settings.rakuten_service_secret}:{settings.rakuten_license_key}"
        encoded = base64.b64encode(raw.encode()).decode()
        return f"ESA {encoded}"

    def _rms_headers_get(self) -> dict[str, str]:
        """Headers for GET requests (no Content-Type)."""
        return {
            "Authorization": self._build_rms_auth_header(),
            "Accept": "application/json",
        }

    def _rms_headers_post(self) -> dict[str, str]:
        """Headers for POST requests (with Content-Type)."""
        return {
            "Authorization": self._build_rms_auth_header(),
            "Content-Type": "application/json;charset=utf-8",
            "Accept": "application/json",
        }

    # ------------------------------------------------------------------
    # RMS Order Search (受注検索) - 2-step: searchOrder → getOrder
    # ------------------------------------------------------------------

    async def _extract_rms_orders(
        self,
        *,
        columns: list[str] | None,
        limit: int,
        cursor: str | None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        if not self.is_configured():
            raise RuntimeError("RMS credentials are not configured")

        page = int(cursor) if cursor else 1

        # Use provided date range or default to last 63 days (API max range)
        now = datetime.now(JST)
        if end_date:
            end_dt = f"{end_date}T23:59:59+0900"
        else:
            end_dt = now.strftime("%Y-%m-%dT%H:%M:%S+0900")
        if start_date:
            start_dt = f"{start_date}T00:00:00+0900"
        else:
            start_dt = (now - timedelta(days=63)).strftime("%Y-%m-%dT%H:%M:%S+0900")

        # Step 1: Search order numbers
        search_body: dict[str, Any] = {
            "dateType": 1,  # 注文日
            "startDatetime": start_dt,
            "endDatetime": end_dt,
            "PaginationRequestModel": {
                "requestRecordsAmount": min(limit, 1000),
                "requestPage": page,
            },
        }

        search_resp = await self._http.post(
            RMS_ORDER_SEARCH_URL,
            json=search_body,
            headers=self._rms_headers_post(),
        )
        search_resp.raise_for_status()
        search_data = search_resp.json()

        order_numbers = search_data.get("orderNumberList", [])
        pagination = search_data.get("PaginationResponseModel", {})
        total = pagination.get("totalRecordsAmount")
        total_pages = pagination.get("totalPages", 1)

        available_fields = {f["name"] for f in SCHEMAS["rms_orders"]["fields"]}

        if not order_numbers:
            return {
                "items": [],
                "columns": columns or list(available_fields),
                "next_cursor": None,
                "total": total or 0,
            }

        # Step 2: Get order details (max 100 per getOrder request, batch if needed)
        all_get_data: list[dict] = []
        for i in range(0, len(order_numbers), 100):
            batch = order_numbers[i : i + 100]
            get_resp = await self._http.post(
                RMS_ORDER_GET_URL,
                json={"orderNumberList": batch, "version": 7},
                headers=self._rms_headers_post(),
            )
            get_resp.raise_for_status()
            get_data = get_resp.json()
            all_get_data.extend(get_data.get("OrderModelList", []))

        raw_orders = all_get_data
        items = []
        for order in raw_orders:
            row: dict[str, Any] = {}

            # 基本情報
            row["orderNumber"] = order.get("orderNumber")
            row["orderDatetime"] = order.get("orderDatetime")
            row["orderProgress"] = order.get("orderProgress")
            row["orderType"] = order.get("orderType")
            row["shopOrderCfmDatetime"] = order.get("shopOrderCfmDatetime")
            row["orderFixDatetime"] = order.get("orderFixDatetime")
            row["shippingCmplRptDatetime"] = order.get("shippingCmplRptDatetime")
            row["remarks"] = (order.get("remarks") or "").strip()
            row["memo"] = order.get("memo")

            # 金額
            row["goodsPrice"] = order.get("goodsPrice")
            row["goodsTax"] = order.get("goodsTax")
            row["postagePrice"] = order.get("postagePrice", 0)
            row["deliveryPrice"] = order.get("deliveryPrice", 0)
            row["paymentCharge"] = order.get("paymentCharge", 0)
            row["totalPrice"] = order.get("totalPrice")
            row["requestPrice"] = order.get("requestPrice")
            row["couponAllTotalPrice"] = order.get("couponAllTotalPrice", 0)
            row["couponShopPrice"] = order.get("couponShopPrice", 0)

            # 決済
            settlement = order.get("SettlementModel") or {}
            row["settlementMethod"] = settlement.get("settlementMethod")
            row["cardName"] = settlement.get("cardName")
            row["cardPayType"] = settlement.get("cardPayType")

            # 購入者情報
            orderer = order.get("OrdererModel") or {}
            row["customerName"] = (
                f"{orderer.get('familyName', '')} {orderer.get('firstName', '')}"
            ).strip()
            row["customerNameKana"] = (
                f"{orderer.get('familyNameKana', '')} {orderer.get('firstNameKana', '')}"
            ).strip()
            row["customerPrefecture"] = orderer.get("prefecture")
            row["customerCity"] = orderer.get("city")
            row["customerZipCode"] = (
                f"{orderer.get('zipCode1', '')}-{orderer.get('zipCode2', '')}"
            ) if orderer.get("zipCode1") else None
            row["customerPhone"] = (
                f"{orderer.get('phoneNumber1', '')}-{orderer.get('phoneNumber2', '')}-{orderer.get('phoneNumber3', '')}"
            ) if orderer.get("phoneNumber1") else None
            row["customerEmail"] = orderer.get("emailAddress")
            row["customerSex"] = orderer.get("sex")

            # 送付先・商品情報（PackageModelList）
            package_list = order.get("PackageModelList", [])
            all_items: list[dict] = []
            first_sender: dict = {}
            first_noshi = ""
            for pkg in package_list:
                all_items.extend(pkg.get("ItemModelList", []))
                if not first_sender:
                    first_sender = pkg.get("SenderModel") or {}
                if not first_noshi:
                    first_noshi = pkg.get("noshi") or ""

            row["deliveryName"] = (
                f"{first_sender.get('familyName', '')} {first_sender.get('firstName', '')}"
            ).strip() if first_sender else ""
            row["deliveryPrefecture"] = first_sender.get("prefecture")
            row["deliveryCity"] = first_sender.get("city")
            row["deliveryZipCode"] = (
                f"{first_sender.get('zipCode1', '')}-{first_sender.get('zipCode2', '')}"
            ) if first_sender.get("zipCode1") else None
            row["deliveryPhone"] = (
                f"{first_sender.get('phoneNumber1', '')}-{first_sender.get('phoneNumber2', '')}-{first_sender.get('phoneNumber3', '')}"
            ) if first_sender.get("phoneNumber1") else None

            row["itemCount"] = len(all_items)
            row["itemNames"] = " / ".join(i.get("itemName", "") for i in all_items)
            row["itemNumbers"] = " / ".join(i.get("itemNumber", "") for i in all_items)
            row["itemPrices"] = " / ".join(str(i.get("price", "")) for i in all_items)
            row["itemUnits"] = " / ".join(str(i.get("units", "")) for i in all_items)
            row["selectedChoices"] = " / ".join(
                (i.get("selectedChoice", "") or "")[:100] for i in all_items
            )

            # フラグ
            row["giftCheckFlag"] = order.get("giftCheckFlag")
            row["asurakuFlag"] = order.get("asurakuFlag")
            row["rakutenMemberFlag"] = order.get("rakutenMemberFlag")
            point_model = order.get("PointModel") or {}
            row["usedPoint"] = point_model.get("usedPoint", 0)

            # 配送
            delivery_model = order.get("DeliveryModel") or {}
            row["deliveryMethod"] = delivery_model.get("deliveryName")
            row["noshi"] = first_noshi

            filtered = {k: v for k, v in row.items() if k in available_fields}
            items.append(_pick_fields(filtered, columns))

        next_cursor = str(page + 1) if page < total_pages else None

        return {
            "items": items,
            "columns": columns or list(available_fields),
            "next_cursor": next_cursor,
            "total": total,
        }

    # ------------------------------------------------------------------
    # RMS Item Search (商品検索 v2.0)
    # ------------------------------------------------------------------

    async def _extract_rms_items(
        self,
        *,
        columns: list[str] | None,
        limit: int,
        cursor: str | None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        if not self.is_configured():
            raise RuntimeError("RMS credentials are not configured")

        params: dict[str, Any] = {
            "hits": min(limit, 100),
        }
        if cursor:
            params["cursorMark"] = cursor
        else:
            params["offset"] = 0

        response = await self._http.get(
            RMS_ITEMS_SEARCH_URL, params=params, headers=self._rms_headers_get()
        )
        response.raise_for_status()
        data = response.json()

        raw_results = data.get("results", [])
        total = data.get("numFound")
        next_cursor_mark = data.get("nextCursorMark")

        available_fields = {f["name"] for f in SCHEMAS["rms_items"]["fields"]}

        items = []
        for result in raw_results:
            item_data = result.get("item", result)
            row: dict[str, Any] = {
                "manageNumber": item_data.get("manageNumber"),
                "title": item_data.get("title"),
                "genreId": item_data.get("genreId"),
                "itemType": item_data.get("itemType"),
                "hideItem": item_data.get("hideItem"),
                "created": item_data.get("created"),
                "updated": item_data.get("updated"),
            }
            # Extract price from first variant
            variants = item_data.get("variants", {})
            if variants:
                first_variant = next(iter(variants.values()), {})
                row["standardPrice"] = first_variant.get("standardPrice")
            else:
                row["standardPrice"] = None

            filtered = {k: v for k, v in row.items() if k in available_fields}
            items.append(_pick_fields(filtered, columns))

        return {
            "items": items,
            "columns": columns or list(available_fields),
            "next_cursor": next_cursor_mark,
            "total": total,
        }

    # ------------------------------------------------------------------
    # RMS Inventory API v2.1 (在庫取得)
    # ------------------------------------------------------------------

    async def _extract_rms_inventory(
        self,
        *,
        columns: list[str] | None,
        limit: int,
        cursor: str | None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        if not self.is_configured():
            raise RuntimeError("RMS credentials are not configured")

        # First fetch item list to get manageNumbers for inventory lookup
        items_params: dict[str, Any] = {"hits": min(limit, 100)}
        if cursor:
            items_params["cursorMark"] = cursor
        else:
            items_params["offset"] = 0

        items_resp = await self._http.get(
            RMS_ITEMS_SEARCH_URL, params=items_params, headers=self._rms_headers_get()
        )
        items_resp.raise_for_status()
        items_data = items_resp.json()

        raw_results = items_data.get("results", [])
        next_cursor_mark = items_data.get("nextCursorMark")
        total = items_data.get("numFound")

        if not raw_results:
            available_fields = {f["name"] for f in SCHEMAS["rms_inventory"]["fields"]}
            return {
                "items": [],
                "columns": columns or list(available_fields),
                "next_cursor": None,
                "total": 0,
            }

        # Build inventory request from item manageNumbers + variantIds
        inventory_queries = []
        for result in raw_results:
            item_data = result.get("item", result)
            manage_number = item_data.get("manageNumber")
            variants = item_data.get("variants", {})
            if variants:
                for variant_id in variants:
                    inventory_queries.append({
                        "manageNumber": manage_number,
                        "variantId": variant_id,
                    })
            else:
                inventory_queries.append({
                    "manageNumber": manage_number,
                    "variantId": "",
                })

        inv_resp = await self._http.post(
            RMS_INVENTORY_BULK_GET_URL,
            json={"inventories": inventory_queries},
            headers=self._rms_headers_post(),
        )
        inv_resp.raise_for_status()
        inv_data = inv_resp.json()

        raw_inventories = inv_data.get("inventories", [])
        available_fields = {f["name"] for f in SCHEMAS["rms_inventory"]["fields"]}

        items = []
        for inv in raw_inventories:
            row: dict[str, Any] = {
                "manageNumber": inv.get("manageNumber"),
                "variantId": inv.get("variantId"),
                "quantity": inv.get("quantity"),
                "created": inv.get("created"),
                "updated": inv.get("updated"),
            }
            filtered = {k: v for k, v in row.items() if k in available_fields}
            items.append(_pick_fields(filtered, columns))

        return {
            "items": items,
            "columns": columns or list(available_fields),
            "next_cursor": next_cursor_mark,
            "total": total,
        }
