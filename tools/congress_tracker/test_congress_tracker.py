"""Tests for the congressional trade tracker. All network access is mocked —
fixtures reproduce the real feeds' schema quirks (senate MM/DD/YYYY dates and
'Sale (Full)' types vs house YYYY-MM-DD and 'sale_partial', 'Hon.' prefixes,
'--' tickers, option assets)."""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests

import congress_tracker as ct


def senate_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "transaction_date": "01/15/2026",
        "disclosure_date": "02/20/2026",
        "senator": "Thomas H Tuberville",
        "ticker": "AAPL",
        "asset_description": "Apple Inc. Common Stock",
        "type": "Purchase",
        "amount": "$1,001 - $15,000",
        "ptr_link": "https://efdsearch.senate.gov/search/view/ptr/1/",
    }
    row.update(overrides)
    return row


def house_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "transaction_date": "2026-03-02",
        "disclosure_date": "2026-03-30",
        "representative": "Hon. Nancy Pelosi",
        "ticker": "NVDA",
        "asset_description": "NVIDIA Corporation - Common Stock",
        "type": "purchase",
        "amount": "$1,000,001 - $5,000,000",
        "ptr_link": "https://disclosures-clerk.house.gov/ptr/2",
    }
    row.update(overrides)
    return row


class FakeResponse:
    def __init__(self, body: Any, status: int = 200, malformed: bool = False) -> None:
        self._body = body
        self.status_code = status
        self._malformed = malformed
        self.ok = status < 400

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")

    def json(self) -> Any:
        if self._malformed:
            raise ValueError("not json")
        return self._body


class FakeSession:
    """Stands in for requests.Session: URL -> canned response, POSTs recorded."""

    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses = responses
        self.posts: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, timeout: float = 0) -> FakeResponse:
        response = self.responses.get(url)
        if response is None:
            raise requests.ConnectionError(f"no route to {url}")
        return response

    def post(self, url: str, json: dict[str, Any], timeout: float = 0) -> FakeResponse:
        self.posts.append((url, json))
        return FakeResponse({}, 204)


# --- normalization -------------------------------------------------------------


def test_normalization_unifies_both_chambers() -> None:
    df = ct.combine_feeds([senate_row()], [house_row()])
    assert len(df) == 2
    assert set(df.columns) >= {
        "transaction_date", "disclosure_date", "politician", "ticker",
        "transaction_type", "amount", "ptr_link", "disclosure_lag_days",
    }
    pelosi = df[df["politician"] == "Nancy Pelosi"].iloc[0]  # 'Hon.' stripped
    assert pelosi["transaction_type"] == "Purchase"  # lowercase house type mapped
    assert pelosi["disclosure_lag_days"] == 28
    assert pelosi["amount_min"] == 1_000_001.0 and pelosi["amount_max"] == 5_000_000.0
    tuberville = df[df["politician"] == "Thomas H Tuberville"].iloc[0]
    assert tuberville["disclosure_lag_days"] == 36  # senate date format parsed


def test_sale_variants_map_to_sale_and_exchanges_drop() -> None:
    records = [
        senate_row(type="Sale (Full)"),
        senate_row(type="Sale (Partial)"),
        house_row(type="sale_partial"),
        house_row(type="exchange"),  # not actionable for a copycat
    ]
    df = ct.combine_feeds(records[:2], records[2:])
    assert len(df) == 3
    assert set(df["transaction_type"]) == {"Sale"}


def test_non_equities_and_missing_tickers_filtered_out() -> None:
    records = [
        senate_row(),
        senate_row(ticker="--"),                                     # no ticker
        senate_row(ticker=""),                                       # blank
        senate_row(asset_description="AAPL Call Options 06/2026"),   # derivative
        senate_row(asset_description="Municipal Bond Fund", ticker="MUB"),
    ]
    df = ct.normalize_records(records, "senate", "senator")
    assert len(df) == 1  # only the plain common-stock row survives


def test_garbage_rows_are_dropped_not_fatal() -> None:
    records = [senate_row(), {"nonsense": True}, senate_row(transaction_date="not a date")]
    df = ct.normalize_records(records, "senate", "senator")
    assert len(df) == 1


# --- filtering -----------------------------------------------------------------


def test_politician_filter_matches_fragments_case_insensitively() -> None:
    df = ct.combine_feeds([senate_row()], [house_row()])
    assert list(ct.filter_watched(df, ["pelosi"])["politician"]) == ["Nancy Pelosi"]
    assert len(ct.filter_watched(df, ["Tuberville", "Nancy Pelosi"])) == 2
    assert ct.filter_watched(df, ["Mark Green"]).empty


# --- state / new-trade detection ------------------------------------------------


def test_first_run_swallows_history_then_alerts_only_on_new(tmp_path: Any) -> None:
    state_file = str(tmp_path / "state.json")
    df = ct.combine_feeds([senate_row()], [house_row()])

    # First run: history marked seen, nothing alerted.
    state = ct.load_state(state_file)
    assert ct.detect_new_trades(df, state, first_run_alerts=False) == []
    ct.save_state(state_file, state)
    assert len(ct.load_state(state_file)["seen"]) == 2

    # Second run with one genuinely new disclosure: only it is reported.
    df2 = ct.combine_feeds([senate_row(), senate_row(ticker="MSFT")], [house_row()])
    state = ct.load_state(state_file)
    new = ct.detect_new_trades(df2, state, first_run_alerts=False)
    assert [t["ticker"] for t in new] == ["MSFT"]

    # Third run, same data: silence.
    ct.save_state(state_file, state)
    state = ct.load_state(state_file)
    assert ct.detect_new_trades(df2, state, first_run_alerts=False) == []


def test_bootstrap_alert_reports_history_on_first_run(tmp_path: Any) -> None:
    df = ct.combine_feeds([senate_row()], [house_row()])
    state = ct.load_state(str(tmp_path / "s.json"))
    assert len(ct.detect_new_trades(df, state, first_run_alerts=True)) == 2


def test_corrupt_state_file_starts_fresh(tmp_path: Any) -> None:
    path = tmp_path / "state.json"
    path.write_text("{ this is not json")
    state = ct.load_state(str(path))
    assert state["seen"] == {}


# --- payload / webhook ----------------------------------------------------------


def test_payload_shape_is_json_safe() -> None:
    df = ct.combine_feeds([senate_row()], [house_row()])
    payload = ct.to_payload(df.to_dict(orient="records"))
    encoded = json.loads(json.dumps(payload))  # round-trips cleanly
    assert encoded["new_trade_count"] == 2
    trade = encoded["trades"][0]
    assert trade["transaction_date"] == "2026-03-02"  # ISO, newest first
    assert trade["disclosure_lag_days"] == 28
    assert trade["ptr_link"].startswith("https://")


def test_traderz_event_adapter() -> None:
    df = ct.combine_feeds([], [house_row(type="sale_full")])
    payload = ct.to_payload(df.to_dict(orient="records"))
    event = ct.to_traderz_event(payload["trades"][0], price=1234.5)
    assert event["action"] == "SELL"
    assert event["ticker"] == "NVDA"
    assert event["price"] == 1234.5
    assert event["source"] == "webhook"
    assert "Nancy Pelosi" in event["note"] and "28d" in event["note"]
    assert len(event["note"]) <= 200  # Traderz schema limit


def test_discord_webhook_receives_chunked_content() -> None:
    df = ct.combine_feeds([senate_row()], [house_row()])
    payload = ct.to_payload(df.to_dict(orient="records"))
    session = FakeSession({})
    assert ct.send_webhook(session, "https://discord.com/api/webhooks/x", payload) is True  # type: ignore[arg-type]
    assert session.posts and "content" in session.posts[0][1]
    assert "Nancy Pelosi" in session.posts[0][1]["content"]


def test_generic_webhook_receives_raw_payload() -> None:
    payload = ct.to_payload([])
    session = FakeSession({})
    assert ct.send_webhook(session, "https://myapp.example/hook", payload) is True  # type: ignore[arg-type]
    assert session.posts[0][1]["source"] == "congress-tracker"


# --- end-to-end run with mocked feeds -------------------------------------------


def test_run_survives_one_dead_feed_and_reports_new_trades(tmp_path: Any) -> None:
    session = FakeSession(
        {
            ct.SENATE_FEED_URL: FakeResponse([senate_row()]),
            # house URL absent -> ConnectionError -> degraded, not fatal
        }
    )
    state_file = str(tmp_path / "state.json")
    payload = ct.run(
        ["Tuberville"], state_file, None,
        first_run_alerts=True, session=session,  # type: ignore[arg-type]
    )
    assert payload["new_trade_count"] == 1
    assert payload["trades"][0]["politician"] == "Thomas H Tuberville"


def test_run_aborts_when_both_feeds_dead(tmp_path: Any) -> None:
    session = FakeSession({})
    with pytest.raises(ct.FeedError, match="both"):
        ct.run(["Pelosi"], str(tmp_path / "s.json"), None, session=session)  # type: ignore[arg-type]


def test_dry_run_touches_nothing(tmp_path: Any) -> None:
    session = FakeSession(
        {
            ct.SENATE_FEED_URL: FakeResponse([senate_row()]),
            ct.HOUSE_FEED_URL: FakeResponse([house_row()]),
        }
    )
    state_file = tmp_path / "state.json"
    payload = ct.run(
        ["Pelosi"], str(state_file), "https://discord.com/api/webhooks/x",
        dry_run=True, first_run_alerts=True, session=session,  # type: ignore[arg-type]
    )
    assert payload["new_trade_count"] == 1
    assert not state_file.exists()  # no state writes
    assert session.posts == []  # no webhook POST
