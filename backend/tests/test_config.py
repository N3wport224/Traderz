"""Verification tests for backend/config.py: the live-mutable strategy config store."""

from __future__ import annotations

import pytest

from backend.config import ConfigStore, ConfigValidationError, MomentumConfig, SwingConfig


def test_defaults() -> None:
    store = ConfigStore()
    assert store.momentum == MomentumConfig(opening_range_minutes=5, time_stop_minutes=20)
    assert store.swing == SwingConfig(min_touches=3, touch_tolerance_pct=0.005)


@pytest.mark.asyncio
async def test_update_momentum_partial_update_leaves_other_field_untouched() -> None:
    store = ConfigStore()
    updated = await store.update_momentum(opening_range_minutes=10)
    assert updated.opening_range_minutes == 10
    assert updated.time_stop_minutes == 20  # untouched
    assert store.momentum is updated


@pytest.mark.asyncio
async def test_update_momentum_both_fields() -> None:
    store = ConfigStore()
    updated = await store.update_momentum(opening_range_minutes=15, time_stop_minutes=30)
    assert updated.opening_range_minutes == 15
    assert updated.time_stop_minutes == 30


@pytest.mark.asyncio
async def test_update_swing_partial_update() -> None:
    store = ConfigStore()
    updated = await store.update_swing(min_touches=5)
    assert updated.min_touches == 5
    assert updated.touch_tolerance_pct == 0.005  # untouched


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"opening_range_minutes": 0},
        {"opening_range_minutes": -5},
        {"time_stop_minutes": 0},
    ],
)
async def test_update_momentum_rejects_invalid_values(kwargs: dict[str, int]) -> None:
    store = ConfigStore()
    with pytest.raises(ConfigValidationError):
        await store.update_momentum(**kwargs)
    # A rejected update must not mutate the stored config.
    assert store.momentum == MomentumConfig()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_touches": 1},
        {"touch_tolerance_pct": 0.0},
        {"touch_tolerance_pct": -0.01},
        {"touch_tolerance_pct": 0.2},
    ],
)
async def test_update_swing_rejects_invalid_values(kwargs: dict[str, float]) -> None:
    store = ConfigStore()
    with pytest.raises(ConfigValidationError):
        await store.update_swing(**kwargs)
    assert store.swing == SwingConfig()


@pytest.mark.asyncio
async def test_updates_are_immediately_visible_to_new_reads() -> None:
    """Simulates an engine reading `store.momentum.X` live: a config update made
    between two reads must be visible on the very next read, with no caching."""
    store = ConfigStore()
    assert store.momentum.opening_range_minutes == 5
    await store.update_momentum(opening_range_minutes=8)
    assert store.momentum.opening_range_minutes == 8
