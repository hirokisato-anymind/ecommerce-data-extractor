"""Platform client registry."""

from app.platforms.base import PlatformClient
from app.platforms.shopify import ShopifyClient
from app.platforms.rakuten import RakutenClient
from app.platforms.amazon import AmazonClient
from app.platforms.yahoo import YahooClient


def get_all_clients() -> list[PlatformClient]:
    return [ShopifyClient(), RakutenClient(), AmazonClient(), YahooClient()]


def get_client(platform_id: str) -> PlatformClient | None:
    for client in get_all_clients():
        if client.platform_id == platform_id:
            return client
    return None
