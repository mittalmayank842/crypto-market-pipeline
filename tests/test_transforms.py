import datetime as dt
import math

import pytest
from pyspark.sql import SparkSession

from crypto_pipeline.gold import candle_signals, daily_metrics
from crypto_pipeline.silver import PRICE_RULES, OHLC_RULES, split_by_rules


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests") \
        .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    yield s
    s.stop()


def test_daily_return_and_moving_average(spark):
    d0 = dt.date(2026, 1, 1)
    rows = [("btc", d0 + dt.timedelta(days=i), p) for i, p in enumerate([100.0, 110.0, 99.0])]
    out = {r.date: r for r in daily_metrics(spark.createDataFrame(rows, "coin_id string, date date, price double")).collect()}
    assert out[d0].daily_return is None                      # no previous day
    assert out[d0 + dt.timedelta(1)].daily_return == pytest.approx(0.10)
    assert out[d0 + dt.timedelta(2)].daily_return == pytest.approx(-0.10)
    assert out[d0 + dt.timedelta(2)].ma_7 == pytest.approx((100 + 110 + 99) / 3)


def test_volatility_is_annualised(spark):
    d0 = dt.date(2026, 1, 1)
    prices = [100.0, 101.0, 100.0, 101.0, 100.0]
    df = spark.createDataFrame([("x", d0 + dt.timedelta(i), p) for i, p in enumerate(prices)],
                               "coin_id string, date date, price double")
    last = daily_metrics(df).orderBy("date").collect()[-1]
    logs = [math.log(b / a) for a, b in zip(prices, prices[1:])]
    mean = sum(logs) / len(logs)
    sample_sd = math.sqrt(sum((x - mean) ** 2 for x in logs) / (len(logs) - 1))
    assert last.volatility_30d_ann == pytest.approx(sample_sd * math.sqrt(365))


def test_bad_rows_are_quarantined_with_the_rule_name(spark):
    df = spark.createDataFrame([("a", 1.0, 5.0), ("b", -2.0, 5.0), ("c", None, 5.0), ("d", 3.0, -1.0)],
                               "coin_id string, price double, volume double")
    good, bad = split_by_rules(df, PRICE_RULES)
    assert [r.coin_id for r in good.collect()] == ["a"]
    assert {r.coin_id: r._failed_rule for r in bad.collect()} == {
        "b": "price_positive", "c": "price_not_null", "d": "volume_non_negative"}


def test_ohlc_rule_catches_impossible_candle(spark):
    df = spark.createDataFrame([("x", 10.0, 9.0, 8.0, 9.5)], "coin_id string, open double, high double, low double, close double")
    good, bad = split_by_rules(df, OHLC_RULES)
    assert good.count() == 0 and bad.first()._failed_rule == "high_is_highest"


def test_candle_patterns(spark):
    t = lambda h: dt.datetime(2026, 1, 1, h)
    rows = [
        ("x", t(0), 10.0, 10.5, 9.5, 10.02),   # doji: tiny body, wide range
        ("x", t(4), 10.0, 10.1, 9.0, 10.05),   # hammer: long lower wick
        ("x", t(8), 10.5, 10.6, 9.9, 10.0),    # bearish candle
        ("x", t(12), 9.9, 10.8, 9.8, 10.7),    # engulfs the previous one -> bullish engulfing
    ]
    out = candle_signals(spark.createDataFrame(rows, "coin_id string, ts timestamp, open double, high double, low double, close double")).orderBy("ts").collect()
    assert out[0].is_doji and out[1].is_hammer and out[3].bullish_engulfing
    assert not out[2].bullish_engulfing
