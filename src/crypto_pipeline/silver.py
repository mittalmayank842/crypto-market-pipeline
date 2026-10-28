"""Silver: enforce types, deduplicate, and split good rows from bad ones.

Rows that fail a data-quality rule are not dropped silently; they go to a
quarantine table with the name of the rule they broke.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config

# rule name -> function returning the condition a GOOD row must satisfy
# (built lazily, because Spark columns need an active session)
PRICE_RULES = {
    "price_not_null": lambda: F.col("price").isNotNull(),
    "price_positive": lambda: F.col("price") > 0,
    "volume_non_negative": lambda: F.col("volume").isNull() | (F.col("volume") >= 0),
}
OHLC_RULES = {
    "ohlc_not_null": lambda: F.col("open").isNotNull() & F.col("close").isNotNull(),
    "high_is_highest": lambda: F.col("high") >= F.greatest("open", "close", "low"),
    "low_is_lowest": lambda: F.col("low") <= F.least("open", "close", "high"),
}


def latest_run(df: DataFrame) -> DataFrame:
    """Keep only the most recent ingestion for each key (re-runs are safe)."""
    return df.where(F.col("_run_id") == df.agg(F.max("_run_id")).first()[0])


def dedupe(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def split_by_rules(df: DataFrame, rules: dict) -> tuple[DataFrame, DataFrame]:
    failed = reduce(
        lambda acc, kv: F.when(~kv[1](), F.lit(kv[0])).otherwise(acc),
        reversed(list(rules.items())), F.lit(None).cast("string"))
    tagged = df.withColumn("_failed_rule", failed)
    return tagged.where("_failed_rule IS NULL").drop("_failed_rule"), tagged.where("_failed_rule IS NOT NULL")


def to_daily(df: DataFrame) -> DataFrame:
    return (df.withColumn("ts", (F.col("ts_ms") / 1000).cast("timestamp"))
              .withColumn("date", F.to_date("ts")))


def run(spark: SparkSession) -> dict[str, int]:
    counts = {}
    b = lambda name: latest_run(spark.read.format("delta").load(str(config.BRONZE / name)))

    prices = to_daily(b("price_history"))
    prices = dedupe(prices, ["coin_id", "date"])
    good, bad = split_by_rules(prices, PRICE_RULES)
    good.select("coin_id", "date", "ts", "price", "market_cap", "volume") \
        .write.format("delta").mode("overwrite").save(str(config.SILVER / "daily_prices"))
    counts["daily_prices"] = good.count()
    counts["quarantined_prices"] = bad.count()

    ohlc = dedupe(to_daily(b("ohlc")), ["coin_id", "ts_ms"])
    good_o, bad_o = split_by_rules(ohlc, OHLC_RULES)
    good_o.select("coin_id", "ts", "date", "open", "high", "low", "close") \
          .write.format("delta").mode("overwrite").save(str(config.SILVER / "candles"))
    counts["candles"] = good_o.count()
    counts["quarantined_candles"] = bad_o.count()

    quarantine = bad.select("coin_id", "ts_ms", "_failed_rule", "_run_id").unionByName(
        bad_o.select("coin_id", "ts_ms", "_failed_rule", "_run_id"))
    quarantine.write.format("delta").mode("append").save(str(config.SILVER / "_quarantine"))

    markets = latest_run(spark.read.format("delta").load(str(config.BRONZE / "markets")))
    (markets.select(F.col("id").alias("coin_id"), "symbol", "name",
                    F.col("market_cap_rank").cast("int"), F.col("current_price").cast("double"),
                    F.col("market_cap").cast("double"), F.col("total_volume").cast("double"),
                    F.col("price_change_percentage_24h").cast("double").alias("change_24h_pct"),
                    F.col("circulating_supply").cast("double"), F.to_timestamp("last_updated").alias("last_updated"))
            .dropDuplicates(["coin_id"])
            .write.format("delta").mode("overwrite").save(str(config.SILVER / "coins")))
    counts["coins"] = spark.read.format("delta").load(str(config.SILVER / "coins")).count()
    return counts
