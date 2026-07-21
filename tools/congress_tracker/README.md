# Congress Tracker

Tracks U.S. Congressional stock trades (STOCK-Act disclosures) for the Traderz
copy-trading feature. Fetches the Senate Stock Watcher and House Stock Watcher
public aggregate feeds, normalizes both chambers into one schema, filters to
your configured politicians and to standard equities, and alerts **only on
trades it hasn't seen before** via a local JSON state file.

## Quick start

```sh
pip install -r requirements.txt
python congress_tracker.py --politicians "Nancy Pelosi,Tuberville" --dry-run
```

- First real (non-dry) run: the entire history is silently marked "seen" so you
  don't get thousands of alerts. From then on, each run reports only new
  disclosures. (`--bootstrap-alert` overrides.)
- Add `--webhook-url https://discord.com/api/webhooks/...` to get formatted
  Discord messages (Slack and plain-JSON endpoints also supported).
- Schedule it (Task Scheduler / cron) every few hours — the feeds only change
  when new disclosures are filed.

## Feeding Traderz Copy Trading

Each payload trade maps onto the Traderz webhook with `to_traderz_event()`:

```python
import requests
from congress_tracker import run, to_traderz_event

payload = run(["Nancy Pelosi"], "state.json", webhook_url=None)
for trade in payload["trades"]:
    price = get_current_market_price(trade["ticker"])  # you supply this
    event = to_traderz_event(trade, price)
    requests.post(f"http://localhost:8000/api/traders/{TRADER_ID}/events", json=event)
```

Add a watched trader in the Copy Trading tab first and use its id. With
auto-follow + a budget enabled, disclosures then mirror as paper trades the
moment this script sees them.

## Honest limitations

- **Disclosures are delayed by law** — members have up to 45 days to file, so
  every trade carries `disclosure_lag_days`. This is a lagging signal for
  research/copycat experiments, not a real-time feed.
- Amounts are disclosed as **ranges** (`$1,001 - $15,000`), never exact sizes,
  and no share price is included (that's why `to_traderz_event` requires one).
- The community-run House feed has had stale periods historically; the script
  degrades gracefully when one chamber's feed is down and aborts only if both
  are unreachable.

## Tests

```sh
python -m pytest test_congress_tracker.py -q
```

All network access is mocked; fixtures reproduce both chambers' real schema
quirks (date formats, `Hon.` prefixes, `--` tickers, option assets,
`Sale (Full)` vs `sale_partial` types).
