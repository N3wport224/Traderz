"""Pydantic request bodies for the FastAPI config and watchlist endpoints."""

from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator


class MomentumConfigUpdate(BaseModel):
    opening_range_minutes: int | None = Field(default=None, ge=1, le=120)
    time_stop_minutes: int | None = Field(default=None, ge=1, le=480)


class SwingConfigUpdate(BaseModel):
    min_touches: int | None = Field(default=None, ge=2, le=10)
    touch_tolerance_pct: float | None = Field(default=None, gt=0, le=0.1)


# Bare stock tickers ("AAPL", "BRK.B") or CCXT-style crypto pairs ("BTC/USDT").
TICKER_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,15}(/[A-Z0-9]{2,10})?$")


class WatchlistUpdate(BaseModel):
    ticker: str = Field(min_length=1, max_length=26)

    @field_validator("ticker")
    @classmethod
    def normalize_and_validate(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not TICKER_PATTERN.fullmatch(normalized):
            raise ValueError(
                "ticker must be a stock symbol (AAPL, BRK.B) or crypto pair (BTC/USDT)"
            )
        return normalized
