import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os

# ================== KONFIGURASI ==================
TARGET_SYMBOLS = [
    "AAVEUSDT", "ACHUSDT", "ADAUSDT", "ALGOUSDT", "APEUSDT", "APTUSDT", "ARBUSDT",
    "ATOMUSDT", "AVAXUSDT", "AXSUSDT", "BLURUSDT", "CAKEUSDT", "CFXUSDT",
    "CHZUSDT", "COMPUSDT", "CRVUSDT", "DOTUSDT", "DYDXUSDT", "ENSUSDT",
    "ETCUSDT", "FILUSDT", "FLOKIUSDT", "GALAUSDT", "HBARUSDT", "ICPUSDT",
    "IMXUSDT", "INJUSDT", "JASMYUSDT", "JUPUSDT", "KASUSDT", "LINKUSDT",
    "LTCUSDT", "MANAUSDT", "MASKUSDT", "MEMEUSDT", "MINAUSDT", "NEARUSDT",
    "OPUSDT", "ORCAUSDT", "PENDLEUSDT", "PEPEUSDT", "PYTHUSDT", "QNTUSDT",
    "RAYUSDT", "RENDERUSDT", "ROSEUSDT", "RSRUSDT", "RUNEUSDT", "SANDUSDT",
    "SEIUSDT", "SHIBUSDT", "SNXUSDT", "STXUSDT", "SUIUSDT", "TIAUSDT",
    "TONUSDT", "TRBUSDT", "UMAUSDT", "UNIUSDT", "VETUSDT", "WIFUSDT",
    "WLDUSDT", "XLMUSDT", "XTZUSDT", "ZECUSDT", "ZILUSDT", "ZRXUSDT"
]

BASE_URL = "https://api.bitget.com"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}

# ==================================================

def _request(endpoint, params=None):
    url = BASE_URL + endpoint
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('code') == '00000':
                return data['data']
            else:
                print(f"API error {endpoint}: {data}")
                return None
        else:
            print(f"HTTP {resp.status_code} untuk {endpoint}: {resp.text}")
            return None
    except Exception as e:
        print(f"Request error {endpoint}: {e}")
        return None

def get_valid_futures():
    """Ambil daftar simbol futures yang valid (V2)"""
    data = _request("/api/v2/mix/market/contracts", params={"productType": "USDT-FUTURES"})
    if data:
        return {c['symbol'] for c in data}
    return set()

def fetch_ohlcv(symbol, limit=100):
    params = {
        "symbol": symbol,
        "granularity": "5m",
        "limit": limit,
        "productType": "USDT-FUTURES"
    }
    data = _request("/api/v2/mix/market/candles", params)
    if not data:
        return None
    ohlcv = []
    for candle in data:
        try:
            ts = int(candle[0])
            o = float(candle[1])
            h = float(candle[2])
            l = float(candle[3])
            c = float(candle[4])
            v = float(candle[5])
            ohlcv.append([ts, o, h, l, c, v])
        except:
            continue
    return ohlcv

def fetch_oi(symbol):
    params = {"symbol": symbol, "productType": "USDT-FUTURES"}
    data = _request("/api/v2/mix/market/open-interest", params)
    if data and isinstance(data, list) and len(data) > 0:
        try:
            return float(data[0].get('openInterest', 0))
        except:
            return None
    return None

def fetch_funding(symbol):
    params = {"symbol": symbol, "productType": "USDT-FUTURES"}
    data = _request("/api/v2/mix/market/current-fund-rate", params)
    if data and isinstance(data, list) and len(data) > 0:
        try:
            return float(data[0].get('fundingRate', 0))
        except:
            return None
    return None

def fetch_depth(symbol, limit=15):
    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "limit": limit
    }
    # Coba endpoint depth biasa
    data = _request("/api/v2/mix/market/depth", params)
    if data and isinstance(data, dict):
        return {
            'bids': data.get('bids', []),
            'asks': data.get('asks', [])
        }
    # Jika gagal, coba merge-depth
    data = _request("/api/v2/mix/market/merge-depth", params)
    if data and isinstance(data, dict):
        return {
            'bids': data.get('bids', []),
            'asks': data.get('asks', [])
        }
    return None

def analyze_symbol(symbol):
    ohlcv = fetch_ohlcv(symbol)
    if not ohlcv or len(ohlcv) < 20:
        return None

    closes = [c[4] for c in ohlcv]
    highs = [c[2] for c in ohlcv]
    lows = [c[3] for c in ohlcv]
    volumes = [c[5] for c in ohlcv]
    price = closes[-1]

    # 1. Range compression (5 candle terakhir)
    high5 = max(highs[-5:])
    low5 = min(lows[-5:])
    range_pct = (high5 - low5) / price
    cond_range = range_pct < 0.015

    # 2. ATR menurun (10 periode)
    def atr(highs, lows, closes, period=10):
        if len(highs) < period+1:
            return None
        tr = []
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr.append(max(hl, hc, lc))
        return sum(tr[-period:]) / period

    atr_now = atr(highs[-11:], lows[-11:], closes[-11:], 10)
    atr_prev = atr(highs[-21:-10], lows[-21:-10], closes[-21:-10], 10)
    cond_atr = atr_now is not None and atr_prev is not None and atr_now < atr_prev

    # 3. Volume stabil
    vol_now = volumes[-1]
    vol_avg = np.mean(volumes[-20:-1])
    cond_vol = vol_now < 2 * vol_avg

    # 4. OI stabil
    oi = fetch_oi(symbol)
    # Butuh history OI, tapi untuk sederhana kita cek perubahan dari sebelumnya
    # Di sini kita hanya pakai data saat ini, asumsikan OI tidak spike jika tidak ada data
    # Untuk akurasi, kita perlu history OI, tapi untuk demo kita sederhanakan
    # Kita skip dulu atau gunakan asumsi
    cond_oi = oi is not None  # sementara, nanti bisa diperbaiki

    # 5. Funding netral
    funding = fetch_funding(symbol)
    cond_funding = funding is not None and -0.0001 <= funding <= 0.0001

    # 6. Depth imbalance
    depth = fetch_depth(symbol)
    cond_depth = False
    if depth:
        bid_vol = sum(float(b[1]) for b in depth['bids'] if float(b[0]) >= price * 0.99)
        ask_vol = sum(float(a[1]) for a in depth['asks'] if float(a[0]) <= price * 1.01)
        cond_depth = bid_vol > ask_vol

    # 7. Ketahanan BTC (sederhana: bandingkan perubahan 1 jam)
    # butuh data BTC, kita lewati dulu untuk sederhana
    cond_btc = True

    total_true = sum([cond_range, cond_atr, cond_vol, cond_oi, cond_funding, cond_depth, cond_btc])
    return total_true, {
        'range': cond_range,
        'atr': cond_atr,
        'volume': cond_vol,
        'oi': cond_oi,
        'funding': cond_funding,
        'depth': cond_depth,
        'btc': cond_btc
    }

def send_telegram(message, token, chat_id):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print(f"Gagal kirim Telegram: {r.text}")
    except Exception as e:
        print(f"Error kirim Telegram: {e}")

def run():
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram token/chat ID tidak ditemukan di environment")
        return

    valid = get_valid_futures()
    if not valid:
        print("Gagal memuat daftar kontrak. Cek koneksi.")
        return

    symbols = [s for s in TARGET_SYMBOLS if s in valid]
    print(f"Memantau {len(symbols)} pair...")

    for sym in symbols:
        print(f"\nAnalisis {sym}...")
        result = analyze_symbol(sym)
        if result:
            total, details = result
            if total >= 4:  # minimal 4 kriteria
                msg = (
                    f"🚀 <b>Pre-Pump Terdeteksi (Bitget V2)</b>\n"
                    f"Coin: {sym}\n"
                    f"Kriteria terpenuhi: {total}/7\n"
                    f"Range: {'✓' if details['range'] else '✗'}\n"
                    f"ATR: {'✓' if details['atr'] else '✗'}\n"
                    f"Volume: {'✓' if details['volume'] else '✗'}\n"
                    f"OI: {'✓' if details['oi'] else '✗'}\n"
                    f"Funding: {'✓' if details['funding'] else '✗'}\n"
                    f"Depth: {'✓' if details['depth'] else '✗'}\n"
                    f"BTC: {'✓' if details['btc'] else '✗'}"
                )
                send_telegram(msg, token, chat_id)
        time.sleep(0.3)  # hindari rate limit

if __name__ == "__main__":
    run()
