"""Fail the run (exit 1) when the data looks wrong, so the scheduled job turns red and GitHub emails the owner.

Checks on data/last_run_report.json:
  coins_loaded        silver has at least 90% of the coins requested
  quarantine_rate     at most 2% of price rows and candles were quarantined
  rows_kept           silver kept at least 95% of bronze price rows (dedup should drop very few)
  platinum_complete   every coin has a scorecard row
"""
from __future__ import annotations

import json
import sys

from . import config


def check(report: dict, top_n: int = config.TOP_N_COINS) -> list[dict]:
    b, s, p = report["bronze"], report["silver"], report["platinum"]
    q_rate = (s["quarantined_prices"] + s["quarantined_candles"]) / max(s["daily_prices"] + s["candles"], 1)
    kept = s["daily_prices"] / max(b["price_history"], 1)
    rules = [
        ("coins_loaded", s["coins"] >= 0.9 * top_n, f"{s['coins']} of {top_n}"),
        ("quarantine_rate", q_rate <= 0.02, f"{q_rate:.2%}"),
        ("rows_kept", kept >= 0.95, f"{kept:.1%}"),
        ("platinum_complete", p["coin_scorecard"] == s["coins"], f"{p['coin_scorecard']} scorecards / {s['coins']} coins"),
    ]
    return [{"rule": r, "passed": bool(ok), "value": v} for r, ok, v in rules]


def main() -> None:
    path = config.DATA / "last_run_report.json"
    report = json.loads(path.read_text())
    top_n = int(sys.argv[1]) if len(sys.argv) > 1 else config.TOP_N_COINS
    results = check(report, top_n)
    report["quality_gate"] = {"passed": all(r["passed"] for r in results), "rules": results}
    path.write_text(json.dumps(report, indent=2))
    for r in results:
        print(("PASS " if r["passed"] else "FAIL ") + r["rule"], r["value"])
    if not report["quality_gate"]["passed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
