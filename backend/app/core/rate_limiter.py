import asyncio
import time


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


# Pre-configured rate limiters per platform
shopify_limiter = RateLimiter(tokens_per_second=2, max_tokens=4)
rakuten_limiter = RateLimiter(tokens_per_second=1, max_tokens=1)
amazon_limiter = RateLimiter(tokens_per_second=5, max_tokens=10)
yahoo_limiter = RateLimiter(tokens_per_second=1, max_tokens=1)
