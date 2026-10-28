"""Bronze: land the raw JSON as Delta tables, plus lineage columns. No cleaning yet."""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config


def _lineage(df: DataFrame, run_id: str, source: str) -> DataFrame:
    return (df.withColumn("_run_id", F.lit(run_id))
              .withColumn("_source", F.lit(source))
              .withColumn("_ingested_at", F.current_timestamp()))


def _f(x):
    """The API returns stablecoin prices as ints (1 instead of 1.0); Spark's DoubleType needs floats."""
    return None if x is None else float(x)


def _rows_from_chart(run_dir: Path) -> list[dict]:
    rows = []
    for f in sorted((run_dir / "market_chart").glob("*.json")):
        data = json.loads(f.read_text())
        caps = {ts: v for ts, v in data.get("market_caps", [])}
        vols = {ts: v for ts, v in data.get("total_volumes", [])}
        for ts, price in data.get("prices", []):
            rows.append({"coin_id": f.stem, "ts_ms": int(ts), "price": _f(price),
                         "market_cap": _f(caps.get(ts)), "volume": _f(vols.get(ts))})
    return rows


def _rows_from_ohlc(run_dir: Path) -> list[dict]:
    rows = []
    for f in sorted((run_dir / "ohlc").glob("*.json")):
        for ts, o, h, l, c in json.loads(f.read_text()):
            rows.append({"coin_id": f.stem, "ts_ms": int(ts), "open": _f(o), "high": _f(h), "low": _f(l), "close": _f(c)})
    return rows


def run(spark: SparkSession, run_dir: Path) -> dict[str, int]:
    run_id = run_dir.name
    counts = {}

    markets = spark.read.json(str(run_dir / "markets.json"), multiLine=True)
    tables = {
        "markets": markets,
        "price_history": spark.createDataFrame(_rows_from_chart(run_dir),
                                               "coin_id string, ts_ms long, price double, market_cap double, volume double"),
        "ohlc": spark.createDataFrame(_rows_from_ohlc(run_dir),
                                      "coin_id string, ts_ms long, open double, high double, low double, close double"),
    }
    glob = json.loads((run_dir / "global.json").read_text())["data"]
    tables["global"] = spark.createDataFrame([{
        "active_cryptocurrencies": glob.get("active_cryptocurrencies"),
        "total_market_cap_usd": _f(glob.get("total_market_cap", {}).get("usd")),
        "total_volume_usd": _f(glob.get("total_volume", {}).get("usd")),
        "btc_dominance": _f(glob.get("market_cap_percentage", {}).get("btc")),
        "eth_dominance": _f(glob.get("market_cap_percentage", {}).get("eth")),
        "market_cap_change_24h_pct": _f(glob.get("market_cap_change_percentage_24h_usd")),
    }])
    trending = json.loads((run_dir / "trending.json").read_text()).get("coins", [])
    tables["trending"] = spark.createDataFrame(
        [{"coin_id": c["item"]["id"], "symbol": c["item"]["symbol"], "trend_rank": i + 1}
         for i, c in enumerate(trending)], "coin_id string, symbol string, trend_rank int")

    for name, df in tables.items():
        out = _lineage(df, run_id, name)
        out.write.format("delta").mode("append").option("mergeSchema", "true").save(str(config.BRONZE / name))
        counts[name] = out.count()
    return counts
