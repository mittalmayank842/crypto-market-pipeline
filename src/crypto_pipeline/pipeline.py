"""Run the whole pipeline: landing -> bronze -> silver -> gold -> platinum.

  python -m crypto_pipeline.pipeline            # fetch fresh data, then process
  python -m crypto_pipeline.pipeline --reuse    # process the newest landed run again
"""
from __future__ import annotations

import argparse
import json
import time

from . import bronze, config, gold, ingest, platinum, silver
from .spark import get_spark


def newest_run():
    runs = sorted(p for p in config.LANDING.glob("*") if (p / "_SUCCESS").exists())
    if not runs:
        raise SystemExit("no landed runs yet - run without --reuse first")
    return runs[-1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reuse", action="store_true", help="skip the API and reuse the newest landed run")
    ap.add_argument("--top", type=int, default=config.TOP_N_COINS)
    args = ap.parse_args()

    run_dir = newest_run() if args.reuse else ingest.run(args.top)
    spark = get_spark()
    report = {"run": run_dir.name}
    for name, step in [("bronze", lambda: bronze.run(spark, run_dir)), ("silver", lambda: silver.run(spark)),
                       ("gold", lambda: gold.run(spark)), ("platinum", lambda: platinum.run(spark))]:
        t0 = time.time()
        report[name] = step()
        report[name]["_seconds"] = round(time.time() - t0, 1)
        print(name, report[name])
    (config.DATA / "last_run_report.json").write_text(json.dumps(report, indent=2))

    spark.read.format("delta").load(str(config.PLATINUM / "movers")).show(truncate=False)


if __name__ == "__main__":
    main()
