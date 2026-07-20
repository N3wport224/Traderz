"""Live-mutable strategy configuration.

Shared between the FastAPI `/api/config` endpoints and the running strategy
engines via a single injected `ConfigStore` instance. Engines read the current
values off the store on every bar (through plain attribute/property access),
so a change submitted through the API takes effect on the very next tick —
no restart required.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace


class ConfigValidationError(ValueError):
    """Raised when a requested config update would leave a value out of range."""


@dataclass(frozen=True, slots=True)
class MomentumConfig:
    opening_range_minutes: int = 5
    time_stop_minutes: int = 20


@dataclass(frozen=True, slots=True)
class SwingConfig:
    min_touches: int = 3
    touch_tolerance_pct: float = 0.005


def _validate_momentum(config: MomentumConfig) -> None:
    if config.opening_range_minutes < 1:
        raise ConfigValidationError("opening_range_minutes must be >= 1")
    if config.time_stop_minutes < 1:
        raise ConfigValidationError("time_stop_minutes must be >= 1")


def _validate_swing(config: SwingConfig) -> None:
    if config.min_touches < 2:
        raise ConfigValidationError("min_touches must be >= 2")
    if not (0 < config.touch_tolerance_pct <= 0.1):
        raise ConfigValidationError("touch_tolerance_pct must be within (0, 0.1]")


class ConfigStore:
    """Task-safe mutable holder for both engines' live-adjustable parameters."""

    def __init__(self, momentum: MomentumConfig | None = None, swing: SwingConfig | None = None) -> None:
        self.momentum = momentum or MomentumConfig()
        self.swing = swing or SwingConfig()
        self._lock = asyncio.Lock()

    async def update_momentum(
        self,
        opening_range_minutes: int | None = None,
        time_stop_minutes: int | None = None,
    ) -> MomentumConfig:
        async with self._lock:
            candidate = replace(
                self.momentum,
                **{
                    k: v
                    for k, v in {
                        "opening_range_minutes": opening_range_minutes,
                        "time_stop_minutes": time_stop_minutes,
                    }.items()
                    if v is not None
                },
            )
            _validate_momentum(candidate)
            self.momentum = candidate
            return self.momentum

    async def update_swing(
        self,
        min_touches: int | None = None,
        touch_tolerance_pct: float | None = None,
    ) -> SwingConfig:
        async with self._lock:
            candidate = replace(
                self.swing,
                **{
                    k: v
                    for k, v in {
                        "min_touches": min_touches,
                        "touch_tolerance_pct": touch_tolerance_pct,
                    }.items()
                    if v is not None
                },
            )
            _validate_swing(candidate)
            self.swing = candidate
            return self.swing
