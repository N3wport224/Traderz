#!/usr/bin/env python3
"""Congressional stock-trade tracker for a copycat investment application.

Fetches the aggregate transaction feeds published by the Senate Stock Watcher
and House Stock Watcher projects (public S3 buckets of STOCK-Act disclosure
data), normalizes both into one schema, filters to a configurable list of
politicians and to standard equities, detects trades not seen in previous runs
via a local JSON state file, and emits a webhook-ready payload — optionally
POSTing it to Discord/Slack/your own backend.

Reality check baked into the design: disclosures are DELAYED BY LAW — members
have up to 45 days to file, so every trade carries a `disclosure_lag_days`
field and consumers should treat this as a lagging signal, never a real-time
one.

Usage:
    python congress_tracker.py                        # default politicians
    python congress_tracker.py --politicians "Nancy Pelosi,Tuberville"
    python congress_tracker.py --webhook-url https://discord.com/api/webhooks/...
    python congress_tracker.py --dry-run              # no state writes, no POST

Environment variables (CLI flags win):
    CONGRESS_POLITICIANS   comma-separated politician names
    CONGRESS_STATE_FILE    path to the seen-trades state file
    CONGRESS_WEBHOOK_URL   webhook destination

First-run behaviour: the entire history (tens of thousands of rows) counts as
"already seen" so you aren't blasted with years of alerts; only trades that
appear AFTER the first run are reported. Pass --bootstrap-alert to override.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("congress_tracker")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# Public aggregate feeds (unauthenticated S3 objects, ~10-40 MB each).
SENATE_FEED_URL = (
    "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"
)
HOUSE_FEED_URL = (
    "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
)

# Default watchlist — override with --politicians / CONGRESS_POLITICIANS.
# Matching is case-insensitive and substring-based after honorific stripping,
# so "Pelosi" matches "Hon. Nancy Pelosi" and "Tuberville" matches
# "Thomas H Tuberville".
DEFAULT_POLITICIANS = ["Nancy Pelosi", "Mark Green", "Tommy Tuberville", "Dan Crenshaw"]

DEFAULT_STATE_FILE = "congress_tracker_state.json"

REQUEST_TIMEOUT_SECONDS = 120  # the aggregate files are large
STATE_VERSION = 1

# Disclosure `type` values mapped onto the two actions a copycat can act on.
# Exchanges and anything unrecognized are dropped during normalization.
_TRANSACTION_TYPE_MAP = {
    "purchase": "Purchase",
    "sale": "Sale",
    "sale (full)": "Sale",
    "sale (partial)": "Sale",
    "sale_full": "Sale",
    "sale_partial": "Sale",
}

# Values the feeds use for "no ticker" (options on indexes, munis, funds...).
_MISSING_TICKERS = {"", "--", "n/a", "none", "null", "-"}

# Asset descriptions containing these tokens are not plain equities.
_NON_EQUITY_MARKERS = ("option", "call", "put", "warrant", "note", "bond")


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #


def build_session() -> requests.Session:
    """A session with retry/backoff so a flaky connection or transient S3
    hiccup doesn't fail the whole run: 4 retries, exponential backoff, only on
    connection errors and retryable status codes."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=2.0,  # 0s, 2s, 4s, 8s between attempts
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = "congress-tracker/1.0 (copycat research tool)"
    return session


def fetch_feed(session: requests.Session, url: str, chamber: str) -> list[dict[str, Any]]:
    """Downloads one aggregate JSON feed. Raises FeedError on any failure —
    the caller decides whether one chamber failing is fatal (it isn't)."""
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise FeedError(f"{chamber} feed unreachable: {exc}") from exc
    except ValueError as exc:  # not JSON — S3 error page, truncated body...
        raise FeedError(f"{chamber} feed returned malformed JSON: {exc}") from exc
    if not isinstance(payload, list):
        raise FeedError(f"{chamber} feed shape unexpected: got {type(payload).__name__}, wanted list")
    logger.info("%s feed: %d raw records", chamber, len(payload))
    return payload


class FeedError(RuntimeError):
    """A single chamber's feed could not be fetched/parsed."""


# --------------------------------------------------------------------------- #
# Normalization
# --------------------------------------------------------------------------- #


def _clean_name(raw: Any) -> str:
    """'Hon. Nancy Pelosi ' -> 'Nancy Pelosi' (honorifics off, spacing fixed)."""
    name = str(raw or "").strip()
    for honorific in ("Hon.", "Hon ", "Mr.", "Mrs.", "Ms.", "Dr."):
        if name.startswith(honorific):
            name = name[len(honorific):]
    return " ".join(name.split())


def _parse_date(raw: Any) -> pd.Timestamp | None:
    """The feeds mix MM/DD/YYYY (senate) and YYYY-MM-DD (house), with the
    occasional garbage value; unparseable dates return None and the row is
    dropped rather than poisoning the frame."""
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return None


def _parse_amount_bounds(amount: str) -> tuple[float | None, float | None]:
    """'$1,001 - $15,000' -> (1001.0, 15000.0). Disclosures only give ranges —
    exact sizes are never public. Unparseable -> (None, None)."""
    try:
        parts = [p.strip().lstrip("$").replace(",", "") for p in amount.split("-")]
        low = float(parts[0]) if parts and parts[0] else None
        high = float(parts[1]) if len(parts) > 1 and parts[1] else None
        return low, high
    except (ValueError, AttributeError):
        return None, None


def _is_equity(ticker: str, description: str) -> bool:
    """Standard-equity filter: a real ticker symbol AND no option/derivative
    markers in the free-text asset description."""
    if ticker.lower() in _MISSING_TICKERS:
        return False
    lowered = description.lower()
    return not any(marker in lowered for marker in _NON_EQUITY_MARKERS)


def normalize_records(
    records: list[dict[str, Any]], chamber: str, politician_field: str
) -> pd.DataFrame:
    """Maps one chamber's raw rows onto the unified schema:

    transaction_date, disclosure_date, politician, chamber, ticker,
    transaction_type (Purchase|Sale), amount, amount_min, amount_max,
    ptr_link, disclosure_lag_days

    Rows are dropped (and counted) when they are not actionable for a copycat:
    unknown transaction type, missing/derivative asset, unparseable dates.
    """
    rows: list[dict[str, Any]] = []
    dropped = 0
    for raw in records:
        if not isinstance(raw, dict):
            dropped += 1
            continue
        tx_type = _TRANSACTION_TYPE_MAP.get(str(raw.get("type") or "").strip().lower())
        ticker = str(raw.get("ticker") or "").strip().upper()
        description = str(raw.get("asset_description") or raw.get("asset_type") or "")
        tx_date = _parse_date(raw.get("transaction_date"))
        disclosure_date = _parse_date(raw.get("disclosure_date"))
        if tx_type is None or tx_date is None or disclosure_date is None:
            dropped += 1
            continue
        if not _is_equity(ticker, description):
            dropped += 1
            continue
        amount = str(raw.get("amount") or "").strip()
        amount_min, amount_max = _parse_amount_bounds(amount)
        rows.append(
            {
                "transaction_date": tx_date,
                "disclosure_date": disclosure_date,
                "politician": _clean_name(raw.get(politician_field)),
                "chamber": chamber,
                "ticker": ticker,
                "transaction_type": tx_type,
                "amount": amount,
                "amount_min": amount_min,
                "amount_max": amount_max,
                "ptr_link": str(raw.get("ptr_link") or ""),
                # The copycat's most important honesty metric: how stale this
                # information already was the day it became public.
                "disclosure_lag_days": int((disclosure_date - tx_date).days),
            }
        )
    logger.info("%s: normalized %d rows (%d dropped as non-actionable)", chamber, len(rows), dropped)
    return pd.DataFrame(rows)


def combine_feeds(senate: list[dict[str, Any]], house: list[dict[str, Any]]) -> pd.DataFrame:
    """Both chambers -> one DataFrame, newest transactions first."""
    frames = [
        normalize_records(senate, "senate", politician_field="senator"),
        normalize_records(house, "house", politician_field="representative"),
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values("transaction_date", ascending=False).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #


def filter_watched(df: pd.DataFrame, politicians: list[str]) -> pd.DataFrame:
    """Keeps rows whose politician matches any configured name.

    Matching is case-insensitive substring over the honorific-stripped name,
    so config entries can be full names or distinctive fragments ("Pelosi")."""
    if df.empty or not politicians:
        return df.iloc[0:0] if df.empty else df
    wanted = [p.casefold().strip() for p in politicians if p.strip()]

    def matches(name: str) -> bool:
        folded = name.casefold()
        return any(w in folded for w in wanted)

    return df[df["politician"].map(matches)].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# State: new-trade detection across runs
# --------------------------------------------------------------------------- #


def trade_fingerprint(row: dict[str, Any]) -> str:
    """A stable identity for one disclosed trade. The feeds have no row ids,
    so identity is the tuple of everything that makes a disclosure unique.
    (Two identical same-day trades by the same person collapse into one
    alert — an accepted limitation of id-less source data.)"""
    key = "|".join(
        [
            row["politician"],
            row["ticker"],
            str(pd.Timestamp(row["transaction_date"]).date()),
            str(pd.Timestamp(row["disclosure_date"]).date()),
            row["transaction_type"],
            row["amount"],
            row["ptr_link"],
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def load_state(path: str) -> dict[str, Any]:
    """Reads the seen-trades state; a missing or corrupt file starts fresh
    (corrupt state must never crash a scheduled run)."""
    try:
        with open(path, encoding="utf-8") as handle:
            state = json.load(handle)
        if not isinstance(state, dict) or "seen" not in state:
            raise ValueError("unexpected state shape")
        return state
    except FileNotFoundError:
        return {"version": STATE_VERSION, "seen": {}, "last_run": None}
    except (ValueError, OSError) as exc:
        logger.warning("state file %s unreadable (%s) — starting fresh", path, exc)
        return {"version": STATE_VERSION, "seen": {}, "last_run": None}


def save_state(path: str, state: dict[str, Any]) -> None:
    """Atomic write (temp file + rename) so a crash mid-save can never leave a
    truncated state file that would re-alert on the whole history."""
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def detect_new_trades(df: pd.DataFrame, state: dict[str, Any], *, first_run_alerts: bool) -> list[dict[str, Any]]:
    """Splits the filtered frame into seen/new against the state, marking
    everything as seen either way. On the very first run (empty state) the
    default is to swallow history silently — see module docstring."""
    seen: dict[str, str] = state["seen"]
    is_first_run = not seen
    now = datetime.now(timezone.utc).isoformat()

    new_trades: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        fingerprint = trade_fingerprint(row)
        if fingerprint in seen:
            continue
        seen[fingerprint] = now
        if is_first_run and not first_run_alerts:
            continue
        new_trades.append(row)
    if is_first_run and not first_run_alerts:
        logger.info("first run: %d historical trades marked seen without alerting", len(seen))
    return new_trades


# --------------------------------------------------------------------------- #
# Output / notification
# --------------------------------------------------------------------------- #


def to_payload(new_trades: list[dict[str, Any]]) -> dict[str, Any]:
    """The webhook-ready JSON payload. Dates are ISO strings; NaN-free."""
    trades = []
    for row in new_trades:
        trades.append(
            {
                "politician": row["politician"],
                "chamber": row["chamber"],
                "ticker": row["ticker"],
                "transaction_type": row["transaction_type"],
                "transaction_date": str(pd.Timestamp(row["transaction_date"]).date()),
                "disclosure_date": str(pd.Timestamp(row["disclosure_date"]).date()),
                "disclosure_lag_days": row["disclosure_lag_days"],
                "amount": row["amount"],
                "amount_min": row["amount_min"],
                "amount_max": row["amount_max"],
                "ptr_link": row["ptr_link"],
            }
        )
    return {
        "source": "congress-tracker",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "new_trade_count": len(trades),
        "trades": trades,
    }


def to_traderz_event(trade: dict[str, Any], price: float) -> dict[str, Any]:
    """Adapter for the Traderz Copy Trading webhook
    (POST /api/traders/{id}/events). Disclosures contain a dollar RANGE but
    never a share price, so the caller must supply the current market price —
    Traderz sizes copies by your configured budget, the price is the recorded
    reference. Note in the event how stale the disclosure already is."""
    return {
        "ticker": trade["ticker"],
        "action": "BUY" if trade["transaction_type"] == "Purchase" else "SELL",
        "price": price,
        "source": "webhook",
        "note": (
            f"{trade['politician']} ({trade['chamber']}) {trade['amount']}, "
            f"disclosed {trade['disclosure_lag_days']}d after trading"
        )[:200],
    }


def _discord_lines(payload: dict[str, Any]) -> list[str]:
    lines = [f"🏛️ **{payload['new_trade_count']} new congressional trade(s) detected**"]
    for t in payload["trades"]:
        emoji = "🟢" if t["transaction_type"] == "Purchase" else "🔴"
        lines.append(
            f"{emoji} **{t['politician']}** {t['transaction_type'].upper()} "
            f"`{t['ticker']}` {t['amount']} — traded {t['transaction_date']}, "
            f"disclosed {t['disclosure_date']} (**{t['disclosure_lag_days']}d lag**)"
        )
    return lines


def send_webhook(session: requests.Session, url: str, payload: dict[str, Any]) -> bool:
    """Best-effort delivery. Discord/Slack get human-formatted messages
    (chunked under Discord's 2000-char limit); anything else receives the raw
    JSON payload. Returns True when every request was accepted."""
    try:
        if "discord" in url:
            ok = True
            chunk: list[str] = []
            for line in _discord_lines(payload):
                if sum(len(c) + 1 for c in chunk) + len(line) > 1900:
                    response = session.post(url, json={"content": "\n".join(chunk)}, timeout=30)
                    ok = ok and response.ok
                    chunk = []
                chunk.append(line)
            if chunk:
                response = session.post(url, json={"content": "\n".join(chunk)}, timeout=30)
                ok = ok and response.ok
            return ok
        if "slack" in url:
            response = session.post(url, json={"text": "\n".join(_discord_lines(payload))}, timeout=30)
            return response.ok
        response = session.post(url, json=payload, timeout=30)
        return response.ok
    except requests.RequestException as exc:
        logger.error("webhook delivery failed: %s", exc)
        return False


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def run(
    politicians: list[str],
    state_file: str,
    webhook_url: str | None,
    *,
    dry_run: bool = False,
    first_run_alerts: bool = False,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """One full tracking cycle. Returns the payload (empty trades list when
    nothing new). Raises only if BOTH feeds are unreachable — one chamber
    failing degrades to a warning so the other still flows."""
    session = session or build_session()

    feeds: dict[str, list[dict[str, Any]]] = {"senate": [], "house": []}
    failures: list[str] = []
    for chamber, url in (("senate", SENATE_FEED_URL), ("house", HOUSE_FEED_URL)):
        try:
            feeds[chamber] = fetch_feed(session, url, chamber)
        except FeedError as exc:
            logger.warning("%s", exc)
            failures.append(chamber)
    if len(failures) == 2:
        raise FeedError("both Senate and House feeds are unreachable — aborting run")

    combined = combine_feeds(feeds["senate"], feeds["house"])
    logger.info("combined actionable equity trades: %d", len(combined))

    watched = filter_watched(combined, politicians)
    logger.info("after politician filter (%s): %d", ", ".join(politicians), len(watched))

    state = load_state(state_file)
    new_trades = detect_new_trades(watched, state, first_run_alerts=first_run_alerts)
    payload = to_payload(new_trades)

    if new_trades:
        logger.info("NEW trades detected: %d", len(new_trades))
        if webhook_url and not dry_run:
            delivered = send_webhook(session, webhook_url, payload)
            logger.info("webhook delivery: %s", "ok" if delivered else "FAILED")
    else:
        logger.info("no new trades this run")

    if not dry_run:
        save_state(state_file, state)
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--politicians",
        default=os.environ.get("CONGRESS_POLITICIANS", ",".join(DEFAULT_POLITICIANS)),
        help="comma-separated names (or distinctive fragments) to watch",
    )
    parser.add_argument(
        "--state-file",
        default=os.environ.get("CONGRESS_STATE_FILE", DEFAULT_STATE_FILE),
        help="path of the seen-trades JSON state file",
    )
    parser.add_argument(
        "--webhook-url",
        default=os.environ.get("CONGRESS_WEBHOOK_URL") or None,
        help="Discord/Slack/custom webhook to POST new trades to",
    )
    parser.add_argument("--dry-run", action="store_true", help="no webhook POST, no state file writes")
    parser.add_argument(
        "--bootstrap-alert",
        action="store_true",
        help="alert on the entire history during the very first run (default: swallow silently)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    politicians = [p.strip() for p in args.politicians.split(",") if p.strip()]
    try:
        payload = run(
            politicians,
            args.state_file,
            args.webhook_url,
            dry_run=args.dry_run,
            first_run_alerts=args.bootstrap_alert,
        )
    except FeedError as exc:
        logger.error("%s", exc)
        return 2
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
