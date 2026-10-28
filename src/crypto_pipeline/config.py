"""Paths and settings shared by every layer of the pipeline."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
LANDING = DATA / "landing"          # raw JSON exactly as the API returned it
LAKE = DATA / "lake"                # Delta tables, one folder per layer

BRONZE = LAKE / "bronze"
SILVER = LAKE / "silver"
GOLD = LAKE / "gold"
PLATINUM = LAKE / "platinum"

API = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"
TOP_N_COINS = 50
HISTORY_DAYS = 180   # >90 days makes CoinGecko return one point per day
OHLC_DAYS = 30       # 3-30 days gives 4-hour candles

# Free tier allows roughly 5-15 calls a minute without a key; stay well under it.
SECONDS_BETWEEN_CALLS = 6.5
