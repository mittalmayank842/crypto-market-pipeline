"""Gold: analytics built with window functions.

  daily_metrics   daily return, 7/30-day moving averages, 30-day annualised volatility
  candle_signals  doji, hammer and bullish/bearish engulfing on 4-hour candles
"""
from __future__ import annotations

import math

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config


def daily_metrics(prices: DataFrame) -> DataFrame:
    by_coin = Window.partitionBy("coin_id").orderBy("date")
    last = lambda n: by_coin.rowsBetween(-(n - 1), 0)
    return (prices
            .withColumn("prev_price", F.lag("price").over(by_coin))
            .withColumn("daily_return", F.col("price") / F.col("prev_price") - 1)
            .withColumn("log_return", F.log(F.col("price") / F.col("prev_price")))
            .withColumn("ma_7", F.avg("price").over(last(7)))
            .withColumn("ma_30", F.avg("price").over(last(30)))
            .withColumn("volatility_30d_ann", F.stddev("log_return").over(last(30)) * math.sqrt(365))
            .withColumn("above_ma_30", F.col("price") > F.col("ma_30"))
            .drop("prev_price"))


def candle_signals(candles: DataFrame) -> DataFrame:
    w = Window.partitionBy("coin_id").orderBy("ts")
    body = F.abs(F.col("close") - F.col("open"))
    rng = F.col("high") - F.col("low")
    lower_wick = F.least("open", "close") - F.col("low")
    upper_wick = F.col("high") - F.greatest("open", "close")
    prev_o, prev_c = F.lag("open").over(w), F.lag("close").over(w)
    bullish = F.col("close") > F.col("open")
    return (candles
            .withColumn("is_doji", (rng > 0) & (body <= 0.1 * rng))
            .withColumn("is_hammer", (body > 0) & (lower_wick >= 2 * body) & (upper_wick <= body))
            .withColumn("bullish_engulfing", bullish & (prev_c < prev_o)
                        & (F.col("open") <= prev_c) & (F.col("close") >= prev_o))
            .withColumn("bearish_engulfing", ~bullish & (prev_c > prev_o)
                        & (F.col("open") >= prev_c) & (F.col("close") <= prev_o)))


def run(spark: SparkSession) -> dict[str, int]:
    s = lambda name: spark.read.format("delta").load(str(config.SILVER / name))
    dm = daily_metrics(s("daily_prices"))
    dm.write.format("delta").mode("overwrite").partitionBy("coin_id").save(str(config.GOLD / "daily_metrics"))
    cs = candle_signals(s("candles"))
    cs.write.format("delta").mode("overwrite").save(str(config.GOLD / "candle_signals"))
    return {"daily_metrics": dm.count(), "candle_signals": cs.count()}
