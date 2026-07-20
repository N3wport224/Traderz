"""Pydantic request bodies for the FastAPI config endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


class MomentumConfigUpdate(BaseModel):
    opening_range_minutes: int | None = Field(default=None, ge=1, le=120)
    time_stop_minutes: int | None = Field(default=None, ge=1, le=480)


class SwingConfigUpdate(BaseModel):
    min_touches: int | None = Field(default=None, ge=2, le=10)
    touch_tolerance_pct: float | None = Field(default=None, gt=0, le=0.1)
