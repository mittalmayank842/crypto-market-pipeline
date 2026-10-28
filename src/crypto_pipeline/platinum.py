"""Platinum: small, business-ready tables a dashboard can read directly.

  coin_scorecard   one row per coin: latest price, 7d/30d return, volatility, risk band, rank
  movers           top 5 gainers and losers over 7 days
  pattern_counts   how often each candlestick pattern fired per coin in the last 30 days
"""
from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from . import config


def run(spark: SparkSession) -> dict[str, int]:
    g = lambda name: spark.read.format("delta").load(str(config.GOLD / name))
    dm = g("daily_metrics")
    coins = spark.read.format("delta").load(str(config.SILVER / "coins"))

    latest = dm.withColumn("_rn", F.row_number().over(
        Window.partitionBy("coin_id").orderBy(F.col("date").desc()))).where("_rn = 1")
    price_on = lambda days: (dm.alias("d").join(
        latest.select("coin_id", F.date_sub("date", days).alias("t")), "coin_id")
        .where(F.col("d.date") == F.col("t")).select("coin_id", F.col("price").alias(f"price_{days}d_ago")))

    scorecard = (latest.select("coin_id", "date", "price", "ma_7", "ma_30", "volatility_30d_ann", "above_ma_30")
                 .join(price_on(7), "coin_id", "left").join(price_on(30), "coin_id", "left")
                 .withColumn("return_7d", F.col("price") / F.col("price_7d_ago") - 1)
                 .withColumn("return_30d", F.col("price") / F.col("price_30d_ago") - 1)
                 .withColumn("risk_band", F.when(F.col("volatility_30d_ann") < 0.4, "low")
                             .when(F.col("volatility_30d_ann") < 0.8, "medium").otherwise("high"))
                 .join(coins.select("coin_id", "symbol", "name", "market_cap_rank"), "coin_id", "left")
                 .withColumn("return_7d_rank", F.dense_rank().over(Window.orderBy(F.col("return_7d").desc_nulls_last()))))
    scorecard.write.format("delta").mode("overwrite").save(str(config.PLATINUM / "coin_scorecard"))

    ranked = scorecard.where(F.col("return_7d").isNotNull())
    gainers = ranked.orderBy(F.col("return_7d").desc()).limit(5).withColumn("side", F.lit("gainer"))
    losers = ranked.orderBy(F.col("return_7d").asc()).limit(5).withColumn("side", F.lit("loser"))
    movers = gainers.unionByName(losers).select("side", "symbol", "name", "return_7d", "price")
    movers.write.format("delta").mode("overwrite").save(str(config.PLATINUM / "movers"))

    cs = g("candle_signals")
    patterns = (cs.groupBy("coin_id").agg(
        F.sum(F.col("is_doji").cast("int")).alias("doji"),
        F.sum(F.col("is_hammer").cast("int")).alias("hammer"),
        F.sum(F.col("bullish_engulfing").cast("int")).alias("bullish_engulfing"),
        F.sum(F.col("bearish_engulfing").cast("int")).alias("bearish_engulfing")))
    patterns.write.format("delta").mode("overwrite").save(str(config.PLATINUM / "pattern_counts"))

    return {"coin_scorecard": scorecard.count(), "movers": movers.count(), "pattern_counts": patterns.count()}
