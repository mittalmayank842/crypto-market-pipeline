"""Build a static dashboard (site/index.html + site/data.json) from the platinum tables and the run report.

The scheduled GitHub Actions run publishes it to GitHub Pages, so the page always shows the latest run's
health (rows per layer, timings, quality gate) next to the market numbers.
"""
from __future__ import annotations

import html
import json
from datetime import datetime, timezone

from . import config
from .spark import get_spark

SITE = config.ROOT / "site"


def pct(x):
    return "" if x is None else f"{x * 100:+.1f}%"


def rows(df, cols):
    return [{c: r[c] for c in cols} for r in df.collect()]


def main() -> None:
    spark = get_spark("dashboard")
    load = lambda layer, t: spark.read.format("delta").load(str(layer / t))
    score = rows(load(config.PLATINUM, "coin_scorecard").orderBy("market_cap_rank"),
                 ["market_cap_rank", "symbol", "name", "price", "return_7d", "return_30d", "volatility_30d_ann", "risk_band"])
    movers = rows(load(config.PLATINUM, "movers"), ["side", "symbol", "name", "return_7d", "price"])
    report = json.loads((config.DATA / "last_run_report.json").read_text())
    built = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    SITE.mkdir(exist_ok=True)
    (SITE / "data.json").write_text(json.dumps({"built": built, "report": report, "scorecard": score, "movers": movers},
                                               indent=1, default=str))
    gate = report.get("quality_gate", {})
    e = lambda s: html.escape(str(s))
    tr = lambda cells: "<tr>" + "".join(f"<td>{e(c)}</td>" for c in cells) + "</tr>"
    layers = "".join(tr([k, ", ".join(f"{a}: {b}" for a, b in v.items() if a != "_seconds"), v.get("_seconds", "")])
                     for k, v in report.items() if k in ("bronze", "silver", "gold", "platinum"))
    rules = "".join(tr([r["rule"], "pass" if r["passed"] else "FAIL", r["value"]]) for r in gate.get("rules", []))
    mv = "".join(tr([m["side"], m["symbol"].upper(), m["name"], pct(m["return_7d"]), f"{m['price']:,.4g}"]) for m in movers)
    sc = "".join(tr([s["market_cap_rank"], s["symbol"].upper(), s["name"], f"{s['price']:,.4g}", pct(s["return_7d"]),
                     pct(s["return_30d"]), f"{(s['volatility_30d_ann'] or 0):.0%}", s["risk_band"]]) for s in score)
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crypto Market Pipeline</title><style>
body{{font:14px/1.45 system-ui,sans-serif;margin:0 auto;max-width:960px;padding:16px;color:#1b1f24;background:#fff}}
h1{{font-size:22px;margin:0 0 4px}}h2{{font-size:16px;margin:24px 0 8px}}p{{margin:4px 0;color:#555}}
table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}}td,th{{border-bottom:1px solid #e3e6ea;padding:5px 8px;text-align:left}}
th{{background:#f5f7f9}}.ok{{color:#11703a;font-weight:600}}.bad{{color:#b3261e;font-weight:600}}.wrap{{overflow-x:auto}}
@media (prefers-color-scheme:dark){{body{{background:#14171a;color:#e6e8eb}}th{{background:#1f2328}}td,th{{border-color:#2d333b}}p{{color:#9aa4ae}}}}
</style></head><body>
<h1>Crypto Market Pipeline</h1>
<p>Run <b>{e(report.get('run'))}</b> · page built {built} by a scheduled GitHub Actions job (CoinGecko → bronze → silver → gold → platinum on PySpark + Delta Lake).</p>
<p>Quality gate: <span class="{'ok' if gate.get('passed') else 'bad'}">{'PASSED' if gate.get('passed') else 'FAILED'}</span> · <a href="data.json">data.json</a> · <a href="https://github.com/mittalmayank842/crypto-market-pipeline">source</a></p>
<h2>Run health</h2><div class="wrap"><table><tr><th>Layer</th><th>Rows</th><th>Seconds</th></tr>{layers}</table></div>
<h2>Quality gate</h2><div class="wrap"><table><tr><th>Rule</th><th>Result</th><th>Value</th></tr>{rules}</table></div>
<h2>Top movers (7 days)</h2><div class="wrap"><table><tr><th>Side</th><th>Symbol</th><th>Name</th><th>7d</th><th>Price (USD)</th></tr>{mv}</table></div>
<h2>Coin scorecard</h2><div class="wrap"><table><tr><th>#</th><th>Symbol</th><th>Name</th><th>Price</th><th>7d</th><th>30d</th><th>Volatility</th><th>Risk</th></tr>{sc}</table></div>
</body></html>"""
    (SITE / "index.html").write_text(page)
    print("wrote", SITE / "index.html", len(score), "coins")


if __name__ == "__main__":
    main()
