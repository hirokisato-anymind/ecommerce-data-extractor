import asyncio
import logging
import time

import httpx

logger = logging.getLogger("ecommerce_data_extractor.rate_limiter")


class RateLimiter:
    """Token bucket rate limiter for API request throttling."""

    def __init__(self, tokens_per_second: float, max_tokens: int) -> None:
        self.tokens_per_second = tokens_per_second
        self.max_tokens = max_tokens
        self._tokens = float(max_tokens)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.max_tokens, self._tokens + elapsed * self.tokens_per_second)
        self._last_refill = now

    async def acquire(self) -> None:
        """Wait until a token is available, then consume one."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Calculate how long to wait for the next token
                wait_time = (1.0 - self._tokens) / self.tokens_per_second
            await asyncio.sleep(wait_time)


async def retry_on_429(
    func,
    *args,
    max_retries: int = 5,
    base_delay: float = 3.0,
    **kwargs,
):
    """Call an async function with exponential backoff retry on 429/throttle errors.

    Catches httpx.HTTPStatusError with status 429 and retries up to max_retries
    times with exponential backoff (base_delay * 2^attempt).
    """
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429 and attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "Rate limited (429), retrying in %.1fs (attempt %d/%d)",
                    delay, attempt + 1, max_retries,
                )
                await asyncio.sleep(delay)
            else:
                raise
        except Exception as e:
            # Some APIs return throttling as a different error
            err_str = str(e).lower()
            if ("throttl" in err_str or "too many" in err_str) and attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "Throttled, retrying in %.1fs (attempt %d/%d): %s",
                    delay, attempt + 1, max_retries, e,
                )
                await asyncio.sleep(delay)
            else:
                raise


# Pre-configured rate limiters per platform
# Shopify: 2 req/sec with burst of 4 (matches official limit)
shopify_limiter = RateLimiter(tokens_per_second=2, max_tokens=4)
# Rakuten: 1 req/sec, no burst (conservative for RMS API)
rakuten_limiter = RateLimiter(tokens_per_second=1, max_tokens=1)
# Amazon SP-API: endpoint-specific rate limiters
# getOrders: burst 20, restore 0.0167/s (1/min) — conservative at 0.5/s
amazon_orders_limiter = RateLimiter(tokens_per_second=0.5, max_tokens=4)
# getOrderItems: burst 20, restore 2/s — parallel 2 with rate 2/s
amazon_order_items_limiter = RateLimiter(tokens_per_second=2, max_tokens=10)
# General SP-API endpoints (catalog, inventory, pricing, etc.)
amazon_limiter = RateLimiter(tokens_per_second=0.5, max_tokens=4)
# Yahoo: 1 req/sec, no burst
yahoo_limiter = RateLimiter(tokens_per_second=1, max_tokens=1)
