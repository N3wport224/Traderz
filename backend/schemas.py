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


# --- Phase 10: copy-trading ---------------------------------------------------


class TraderCreate(BaseModel):
    name: str = Field(min_length=1, max_length=48)
    asset_class: str = Field(default="stock", pattern="^(stock|crypto)$")
    notes: str = Field(default="", max_length=200)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("name cannot be blank")
        return stripped


class TraderFollowUpdate(BaseModel):
    """The auto-follow toggle + per-trade budget. Enabling mirroring without a
    positive budget is rejected — a follow order must have a definite size."""

    auto_follow: bool
    budget_amount: float = Field(default=0.0, ge=0, le=10_000_000)

    @field_validator("budget_amount")
    @classmethod
    def round_budget(cls, value: float) -> float:
        return round(value, 2)


class TraderEventCreate(BaseModel):
    ticker: str = Field(min_length=1, max_length=26)
    action: str = Field(pattern="^(BUY|SELL)$")
    price: float = Field(gt=0, le=1e9)
    note: str = Field(default="", max_length=200)
    source: str = Field(default="manual", pattern="^(manual|webhook)$")

    @field_validator("action", mode="before")
    @classmethod
    def upper_action(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("ticker")
    @classmethod
    def normalize_ticker(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not TICKER_PATTERN.fullmatch(normalized):
            raise ValueError("ticker must be a stock symbol (AAPL) or crypto pair (BTC/USDT)")
        return normalized


class BacktestRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=26)
    strategy: str = Field(pattern="^(momentum|swing)$")
    start_date: str  # ISO date or datetime
    end_date: str
    initial_capital: float = Field(default=100_000.0, gt=0, le=1e9)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not TICKER_PATTERN.fullmatch(normalized):
            raise ValueError("symbol must be a stock ticker (AAPL) or crypto pair (BTC/USDT)")
        return normalized
