from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Shopify (OAuth Custom App)
    shopify_store_domain: Optional[str] = None
    shopify_client_id: Optional[str] = None
    shopify_client_secret: Optional[str] = None
    shopify_access_token: Optional[str] = None  # obtained via OAuth

    # Rakuten
    rakuten_app_id: Optional[str] = None  # Ichiba public API
    rakuten_service_secret: Optional[str] = None  # RMS
    rakuten_license_key: Optional[str] = None  # RMS

    # Amazon SP-API
    amazon_client_id: Optional[str] = None  # LWA Client ID
    amazon_client_secret: Optional[str] = None  # LWA Client Secret
    amazon_refresh_token: Optional[str] = None  # SP-API Refresh Token
    amazon_marketplace_id: Optional[str] = "A1VC38T7YXB528"  # JP default

    # Yahoo Shopping (OAuth)
    yahoo_client_id: Optional[str] = None
    yahoo_client_secret: Optional[str] = None
    yahoo_access_token: Optional[str] = None  # obtained via OAuth
    yahoo_refresh_token: Optional[str] = None
    yahoo_seller_id: Optional[str] = None

    # Admin
    admin_token: Optional[str] = None

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
