"""Landing zone: pull five CoinGecko endpoints and store the raw JSON untouched.

Endpoints
  1. /coins/markets              current snapshot of the top N coins
  2. /coins/{id}/market_chart     daily price, market cap and volume history
  3. /coins/{id}/ohlc             4-hour candles for candlestick analysis
  4. /global                      total market cap and dominance
  5. /search/trending             coins people are searching for right now
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from . import config


class CoinGecko:
    def __init__(self, pause: float = config.SECONDS_BETWEEN_CALLS):
        self.session = requests.Session()
        key = os.getenv("COINGECKO_API_KEY")
        if key:
            self.session.headers["x-cg-demo-api-key"] = key
        self.pause = pause
        self.calls = 0

    def get(self, path: str, **params) -> object:
        for attempt in range(5):
            resp = self.session.get(f"{config.API}{path}", params=params, timeout=30)
            self.calls += 1
            if resp.status_code == 429:          # rate limited: back off and retry
                time.sleep(30 * (attempt + 1))
                continue
            resp.raise_for_status()
            time.sleep(self.pause)
            return resp.json()
        raise RuntimeError(f"gave up on {path} after repeated 429s")


def _write(run_dir: Path, name: str, payload: object) -> None:
    path = run_dir / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def run(top_n: int = config.TOP_N_COINS) -> Path:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = config.LANDING / run_id
    cg = CoinGecko()

    markets = cg.get("/coins/markets", vs_currency=config.VS_CURRENCY,
                     order="market_cap_desc", per_page=top_n, page=1)
    _write(run_dir, "markets", markets)
    _write(run_dir, "global", cg.get("/global"))
    _write(run_dir, "trending", cg.get("/search/trending"))

    for i, coin in enumerate(markets, 1):
        cid = coin["id"]
        _write(run_dir, f"market_chart/{cid}",
               cg.get(f"/coins/{cid}/market_chart", vs_currency=config.VS_CURRENCY,
                      days=config.HISTORY_DAYS))
        _write(run_dir, f"ohlc/{cid}",
               cg.get(f"/coins/{cid}/ohlc", vs_currency=config.VS_CURRENCY, days=config.OHLC_DAYS))
        print(f"[{i}/{len(markets)}] {cid}")

    (run_dir / "_SUCCESS").write_text(json.dumps({"api_calls": cg.calls, "coins": len(markets)}))
    print(f"landed run {run_id}: {cg.calls} API calls")
    return run_dir


if __name__ == "__main__":
    run()
