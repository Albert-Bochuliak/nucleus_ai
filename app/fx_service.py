import asyncio
from decimal import Decimal
from time import monotonic
from typing import Mapping

import httpx


class FxService:
    def __init__(
        self,
        client: httpx.AsyncClient,
        ttl_seconds: int = 600,
        max_retries: int = 3,
        retry_base_delay_seconds: float = 1.0,
        mock_rates: Mapping[str, Decimal] | None = None,
    ) -> None:
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._max_retries = max_retries
        self._retry_base_delay_seconds = retry_base_delay_seconds
        self._mock_rates = {key.upper(): value for key, value in (mock_rates or {}).items()}
        self._cache: dict[str, tuple[Decimal, float]] = {}

    async def get_rate(self, currency: str) -> Decimal:
        code = currency.upper()
        if code == "USD":
            return Decimal("1")

        cached = self._cache.get(code)
        now = monotonic()
        if cached and cached[1] > now:
            return cached[0]

        mock_rate = self._mock_rates.get(code)
        if mock_rate is not None:
            self._cache[code] = (mock_rate, now + self._ttl_seconds)
            return mock_rate

        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = await self._client.get(
                    "/latest",
                    params={"from": code, "to": "USD"},
                    timeout=10.0,
                )
                response.raise_for_status()
                payload = response.json()
                rate = Decimal(str(payload["rates"]["USD"]))
                self._cache[code] = (rate, monotonic() + self._ttl_seconds)
                return rate
            except Exception as exc:
                last_error = exc
                if attempt < self._max_retries - 1:
                    delay = self._retry_base_delay_seconds * (2**attempt)
                    await asyncio.sleep(delay)

        raise RuntimeError(f"Failed to fetch FX rate for {code}") from last_error
