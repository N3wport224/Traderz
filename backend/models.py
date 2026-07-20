"""Shared data types used across the data pipeline, strategy engines, and API layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Timeframe(str, Enum):
    ONE_MINUTE = "1m"
    FOUR_HOUR = "4h"


class SignalAction(str, Enum):
    BUY = "buy"
    SELL = "sell"
    SHORT = "short"
    EXIT = "exit"
    ALERT = "alert"


@dataclass(frozen=True, slots=True)
class OHLCVBar:
    symbol: str
    timestamp: datetime
    timeframe: Timeframe
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True, slots=True)
class TradeSignal:
    engine: str
    symbol: str
    action: SignalAction
    price: float
    timestamp: datetime
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)
