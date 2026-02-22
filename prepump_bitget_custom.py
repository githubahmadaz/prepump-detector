import requests
import numpy as np
import pandas as pd
import time
import os

BASE_URL = "https://api.bitget.com"

TARGET_SYMBOLS = [
"AAVEUSDT","ACHUSDT","ADAUSDT","ALGOUSDT","APEUSDT","APTUSDT","ARBUSDT",
"ATOMUSDT","AVAXUSDT","AXSUSDT","BLURUSDT","CAKEUSDT","CFXUSDT",
"CHZUSDT","COMPUSDT","CRVUSDT","DOTUSDT","DYDXUSDT","ENSUSDT",
"ETCUSDT","FILUSDT","FLOKIUSDT","GALAUSDT","HBARUSDT","ICPUSDT",
"IMXUSDT","INJUSDT","JASMYUSDT","JUPUSDT","KASUSDT","LINKUSDT",
"LTCUSDT","MANAUSDT","MASKUSDT","MEMEUSDT","MINAUSDT","NEARUSDT",
"OPUSDT","ORCAUSDT","PENDLEUSDT","PEPEUSDT","PYTHUSDT","QNTUSDT",
"RAYUSDT","RENDERUSDT","ROSEUSDT","RSRUSDT","RUNEUSDT","SANDUSDT",
"SEIUSDT","SHIBUSDT","SNXUSDT","STXUSDT","SUIUSDT","TIAUSDT",
"TONUSDT","TRBUSDT","UMAUSDT","UNIUSDT","VETUSDT","WIFUSDT",
"WLDUSDT","XLMUSDT","XTZUSDT","ZECUSDT","ZILUSDT","ZRXUSDT"
]

def get_valid_futures():
    url = BASE_URL + "/api/mix/v1/market/contracts?productType=umcbl"
    r = requests.get(url).json()
    valid = set()
    if r["code"] == "00000":
        for c in r["data"]:
            valid.add(c["symbol"].replace("_UMCBL",""))
    return valid

def fetch_ohlcv(symbol):
    url = BASE_URL + "/api/mix/v1/market/candles"
    params = {
        "symbol": symbol+"_UMCBL",
        "granularity": "5m",
        "limit": 50
    }
    r = requests.get(url, params=params).json()
    if r["code"] != "00000":
        return None
    return r["data"]

def fetch_oi(symbol):
    url = BASE_URL + "/api/mix/v1/market/open-interest"
    params = {"symbol": symbol+"_UMCBL"}
    r = requests.get(url, params=params).json()
    if r["code"] != "00000":
        return None
    return float(r["data"]["openInterest"])

def fetch_funding(symbol):
    url = BASE_URL + "/api/mix/v1/market/current-fundRate"
    params = {"symbol": symbol+"_UMCBL"}
    r = requests.get(url, params=params).json()
    if r["code"] != "00000":
        return None
    return float(r["data"]["fundingRate"])

def fetch_depth(symbol):
    url = BASE_URL + "/api/mix/v1/market/depth"
    params = {"symbol": symbol+"_UMCBL", "limit": 15}
    r = requests.get(url, params=params).json()
    if r["code"] != "00000":
        return None
    return r["data"]

def score_symbol(symbol):

    ohlcv = fetch_ohlcv(symbol)
    if not ohlcv:
        return None

    closes = [float(c[4]) for c in ohlcv]
    highs = [float(c[2]) for c in ohlcv]
    lows = [float(c[3]) for c in ohlcv]
    volumes = [float(c[5]) for c in ohlcv]

    price = closes[-1]

    # Volatility compression
    range_pct = (max(highs[-5:]) - min(lows[-5:])) / price
    score_vol = max(0, 1 - range_pct*20)

    # Volume stability
    vol_now = volumes[-1]
    vol_avg = np.mean(volumes[:-1])
    score_vol_stable = 1 if vol_now < vol_avg*2 else 0

    # OI stability
    oi = fetch_oi(symbol)
    score_oi = 0.5
    if oi:
        score_oi = 1

    # Funding neutrality
    funding = fetch_funding(symbol)
    score_funding = 0
    if funding:
        if abs(funding) < 0.0003:
            score_funding = 1

    # Depth imbalance
    depth = fetch_depth(symbol)
    score_depth = 0
    if depth:
        bids = sum([float(b[1]) for b in depth["bids"]])
        asks = sum([float(a[1]) for a in depth["asks"]])
        if bids > asks:
            score_depth = 1

    total = (
        score_vol*2 +
        score_vol_stable +
        score_oi +
        score_funding +
        score_depth
    )

    return total

def run():

    valid = get_valid_futures()
    symbols = [s for s in TARGET_SYMBOLS if s in valid]

    results = []

    for sym in symbols:
        score = score_symbol(sym)
        if score:
            results.append((sym, score))
        time.sleep(0.2)

    ranked = sorted(results, key=lambda x: x[1], reverse=True)

    print("\n🔥 TOP PRE-PUMP CANDIDATES\n")
    for r in ranked[:5]:
        print(r)

if __name__ == "__main__":
    run()
