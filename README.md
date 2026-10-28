# crypto-market-pipeline

A medallion-architecture data pipeline for cryptocurrency market data: five CoinGecko REST endpoints,
50 coins, four Delta Lake layers built with PySpark, data-quality rules with a quarantine table, and
window-function analytics (returns, moving averages, volatility, candlestick patterns, movers).

```
CoinGecko API ──► landing (raw JSON) ──► bronze ──► silver ──► gold ──► platinum
  5 endpoints        one folder/run       Delta     typed +     window     dashboard-
  50 coins                                + lineage  DQ rules   analytics  ready tables
```

## Latest run (28 Oct 2026, local Spark 3.5 + Delta 3.2)

| Layer | Output |
| --- | --- |
| Landing | 50 coins, 139 API calls (incl. rate-limit retries) |
| Bronze | 8,967 daily price rows + 8,973 four-hour candles + markets, global, trending |
| Silver | 8,917 daily prices, 8,973 candles, 50 coins — 0 rows quarantined |
| Gold | 8,917 rows of daily metrics, 8,973 candle signals |
| Platinum | 50-coin scorecard, top-5 gainers/losers, pattern counts |
| Spark time | ~31 s for bronze → platinum |

## Endpoints

1. `/coins/markets` — top 50 coins by market cap
2. `/coins/{id}/market_chart` — 180 days of daily price, market cap, volume
3. `/coins/{id}/ohlc` — 30 days of 4-hour candles
4. `/global` — total market cap, BTC/ETH dominance
5. `/search/trending` — trending coins

The client waits 6.5 s between calls and backs off on HTTP 429 (free tier is ~5–15 calls/min).
Set `COINGECKO_API_KEY` to use a free demo key.

## Layers

- **Bronze** — raw data as Delta tables with `_run_id`, `_source`, `_ingested_at`. Append-only.
- **Silver** — types enforced, duplicates removed with `row_number()` over `(coin_id, date)`,
  and data-quality rules applied. Failing rows go to `silver._quarantine` with the rule name:
  `price_not_null`, `price_positive`, `volume_non_negative`, `ohlc_not_null`, `high_is_highest`, `low_is_lowest`.
- **Gold** — window functions per coin: daily and log returns, 7/30-day moving averages,
  30-day annualised volatility (`stddev(log return) × √365`), price above 30-day average;
  doji, hammer, bullish/bearish engulfing on 4-hour candles. `daily_metrics` is partitioned by `coin_id`.
- **Platinum** — `coin_scorecard` (7/30-day return, volatility, risk band, rank), `movers`, `pattern_counts`.

Re-running is safe: silver reads only the newest bronze run, so nothing is double-counted.

## Run

```bash
uv venv --python 3.11 .venv && uv pip install -r requirements.txt   # needs Java 17
PYTHONPATH=src .venv/bin/python -m crypto_pipeline.pipeline          # fetch + process (~15 min on free tier)
PYTHONPATH=src .venv/bin/python -m crypto_pipeline.pipeline --reuse  # reprocess newest landed run (~30 s)
.venv/bin/python -m pytest -q                                        # 5 tests
```

The same code runs on Databricks: drop `get_spark()` (a session already exists) and point
`config.LAKE` at a DBFS or Unity Catalog volume path.

## Companion project

[de-assistant](https://github.com/mittalmayank842/de-assistant) — an AI assistant (RAG + MCP) that answers
questions about this pipeline: docs, table metadata, PII, lineage, SLA status, and guarded re-runs.
