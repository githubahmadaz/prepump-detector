# ================================
# PRE PUMP SCANNER v15 QUANT DESK
# ================================

import requests
import time
import os
import math
import json
import logging
from datetime import datetime

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ================================
# CONFIG
# ================================

CONFIG = {

    "min_score_alert": 11,
    "max_alerts_per_run": 15,

    "min_vol_24h": 3000,
    "max_vol_24h": 50_000_000,

    "gate_chg_24h_max": 12,
    "gate_chg_24h_min": -6,

    "gate_rsi_max": 75,

    "min_atr_pct": 0.8,

}

# ================================
# TELEGRAM
# ================================

def send_telegram(msg):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }

    try:
        requests.post(url, json=payload, timeout=10)
    except:
        pass


# ================================
# BITGET DATA
# ================================

def get_tickers():

    url = "https://api.bitget.com/api/v2/mix/market/tickers"

    r = requests.get(url, timeout=10)
    data = r.json()

    return data["data"]


def get_candles(symbol):

    url = f"https://api.bitget.com/api/v2/mix/market/candles"

    params = {
        "symbol": symbol,
        "granularity": "5m",
        "limit": "200"
    }

    r = requests.get(url, params=params, timeout=10)

    return r.json()["data"]


# ================================
# INDICATORS
# ================================

def ema(data, period):

    k = 2 / (period + 1)

    ema_val = data[0]

    for price in data:

        ema_val = price * k + ema_val * (1 - k)

    return ema_val


def rsi(prices, period=14):

    gains = []
    losses = []

    for i in range(1, len(prices)):

        diff = prices[i] - prices[i-1]

        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(abs(diff))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def atr(high, low, close):

    trs = []

    for i in range(1, len(close)):

        tr = max(
            high[i] - low[i],
            abs(high[i] - close[i-1]),
            abs(low[i] - close[i-1])
        )

        trs.append(tr)

    return sum(trs[-14:]) / 14


# ================================
# BTC REGIME FILTER
# ================================

def btc_regime():

    candles = get_candles("BTCUSDT")

    closes = [float(x[4]) for x in candles]

    r = rsi(closes)

    ema50 = ema(closes, 50)

    price = closes[-1]

    if price < ema50 and r < 45:
        return "bear"

    if price > ema50 and r > 55:
        return "bull"

    return "sideways"


# ================================
# DEEP ENTRY CALC
# ================================

def calculate_entry(price, vwap, atr):

    deep_entry = vwap - (atr * 0.5)

    sl = deep_entry - atr

    tp1 = deep_entry + (atr * 1.5)

    tp2 = deep_entry + (atr * 3)

    return deep_entry, sl, tp1, tp2


# ================================
# MANIPULATION FILTER
# ================================

def wick_ratio(high, low, open_p, close):

    body = abs(close - open_p)

    wick = (high - low)

    if body == 0:
        return 0

    return wick / body


# ================================
# SCANNER
# ================================

def scan():

    btc_state = btc_regime()

    if btc_state == "bear":
        print("BTC bearish — skip scan")
        return

    tickers = get_tickers()

    alerts = []

    for t in tickers:

        symbol = t["symbol"]

        if "USDT" not in symbol:
            continue

        vol = float(t["quoteVolume"])

        if vol < CONFIG["min_vol_24h"]:
            continue

        candles = get_candles(symbol)

        closes = [float(x[4]) for x in candles]
        highs  = [float(x[2]) for x in candles]
        lows   = [float(x[3]) for x in candles]
        opens  = [float(x[1]) for x in candles]

        price = closes[-1]

        r = rsi(closes)

        if r > CONFIG["gate_rsi_max"]:
            continue

        a = atr(highs, lows, closes)

        atr_pct = a / price * 100

        if atr_pct < CONFIG["min_atr_pct"]:
            continue

        ema50 = ema(closes, 50)

        if price < ema50:
            continue

        wick = wick_ratio(highs[-1], lows[-1], opens[-1], closes[-1])

        if wick > 2.5:
            continue

        vwap = sum(closes[-20:]) / 20

        entry, sl, tp1, tp2 = calculate_entry(price, vwap, a)

        support = min(lows[-20:])

        msg = f"""
🚀 <b>{symbol}</b>

Entry: {entry:.4f}

SL: {sl:.4f}

TP1: {tp1:.4f}
TP2: {tp2:.4f}

Support: {support:.4f}

BTC Regime: {btc_state}
ATR: {atr_pct:.2f}%
RSI: {r:.1f}
"""

        alerts.append(msg)

        if len(alerts) >= CONFIG["max_alerts_per_run"]:
            break

    for a in alerts:

        send_telegram(a)

        time.sleep(1)


# ================================
# MAIN
# ================================

if __name__ == "__main__":

    scan()
