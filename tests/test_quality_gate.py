from crypto_pipeline.quality_gate import check

GOOD = {"bronze": {"price_history": 1000}, "silver": {"daily_prices": 990, "quarantined_prices": 0, "candles": 1000,
        "quarantined_candles": 3, "coins": 50}, "platinum": {"coin_scorecard": 50}}


def test_good_run_passes():
    assert all(r["passed"] for r in check(GOOD, 50))


def test_bad_run_fails_the_right_rules():
    bad = {**GOOD, "silver": {**GOOD["silver"], "quarantined_prices": 100, "coins": 30}}
    failed = {r["rule"] for r in check(bad, 50) if not r["passed"]}
    assert failed == {"coins_loaded", "quarantine_rate", "platinum_complete"}
