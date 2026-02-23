"""
╔══════════════════════════════════════════════════════════════════╗
║         PRE-PUMP INTELLIGENCE SCANNER v2.0                      ║
║         BTC-Independent | Whale Detection | Smart Entry         ║
║                                                                  ║
║  SETUP:                                                          ║
║  1. pip install requests python-dotenv                           ║
║  2. Buat file .env di folder yang sama (lihat .env.example)     ║
║  3. python scanner.py                                            ║
╚══════════════════════════════════════════════════════════════════╝
"""

import requests
import time
import os
import math
import logging
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv

# ── Load environment variables (AMAN, tidak expose ke GitHub) ──
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scanner.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  ⚙️  KONFIGURASI UTAMA — SESUAIKAN DI SINI
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # Skor minimum untuk trigger alert (0-100)
    "min_score_alert":        55,

    # Whale score minimum untuk ditampilkan di alert
    "min_whale_score":        30,

    # Volume minimum 24h dalam USD ($)
    "min_volume_usd_24h":     3_000_000,

    # Harga change 4h maksimum — hindari yang sudah pump
    "max_pump_pct_4h":        12.0,

    # Market cap minimum & maksimum USD (sweet spot pump)
    "min_market_cap":         3_000_000,
    "max_market_cap":         500_000_000,

    # Interval scan dalam detik (300 = 5 menit)
    "scan_interval_sec":      300,

    # Cooldown alert per coin dalam detik (3600 = 1 jam)
    "alert_cooldown_sec":     3600,

    # Jumlah candle historis untuk analisis
    "candle_limit_15m":       96,   # 24 jam data 15M
    "candle_limit_1h":        72,   # 72 jam data 1H
}

# ══════════════════════════════════════════════════════════════
#  🗺️  PETA SEKTOR NARASI
#  Tambah atau edit coin sesuai kondisi pasar terkini
# ══════════════════════════════════════════════════════════════
SECTOR_MAP = {
    "SOLANA_ECOSYSTEM": [
        "ORCAUSDT", "RAYUSDT", "JTOUSDT", "PYTHUSDT",
        "BONKUSDT", "WIFUSDT", "JUPUSDT", "MEWUSDT"
    ],
    "DEFI": [
        "SNXUSDT", "ENSOUSDT", "SIRENUSDT", "UNIUSDT",
        "AAVEUSDT", "CRVUSDT", "MKRUSDT", "COMPUSDT"
    ],
    "AI_CRYPTO": [
        "FETUSDT", "AGIXUSDT", "OCEANUSDT", "TAOUSDT",
        "NEARUSDT", "RENDERUSDT", "AKASHUSDT"
    ],
    "ZK_PRIVACY": [
        "AZTECUSDT", "MINAUSDT", "SCRTUSDT", "ZKUSDT",
        "STRKUSDT", "SCROLLUSDT"
    ],
    "DESCI_BIOTECH": [
        "BIOUSDT", "ATHUSDT", "VITAUSDT", "GROWUSDT"
    ],
    "GAMING_METAVERSE": [
        "AGLDUSDT", "IMXUSDT", "GALAUSDT", "AXSUSDT",
        "BEAMUSDT", "PIXELUSDT", "YGGUSDT"
    ],
    "INFRA_LAYER1": [
        "POWERUSDT", "NAORISUSDT", "ALTUSDT", "TIAUSDT",
        "EIGENUSDT", "ENSOUSUSDT"
    ],
    "MEME": [
        "PEPEUSDT", "SHIBUSDT", "DOGEUSDT", "FLOKIUSDT",
        "BRETTUSDT", "MOGUUSDT"
    ],
    "LOW_CAP_MISC": [
        "VVVUSDT", "SIRENUSDT",
    ]
}

# ══════════════════════════════════════════════════════════════
#  🌐  API BASE URLs
# ══════════════════════════════════════════════════════════════
BITGET_BASE   = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Cache sederhana untuk kurangi API call
_cache = {}
_alert_cooldown = {}  # {symbol: last_alert_timestamp}


# ══════════════════════════════════════════════════════════════
#  🔧  UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════

def safe_get(url, params=None, timeout=10):
    """HTTP GET dengan error handling dan logging."""
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.Timeout:
        log.warning(f"Timeout: {url}")
    except requests.exceptions.HTTPError as e:
        log.warning(f"HTTP Error {e.response.status_code}: {url}")
    except Exception as e:
        log.warning(f"Request gagal ({url}): {e}")
    return None


def send_telegram(msg):
    """Kirim pesan ke Telegram dengan error handling."""
    if not BOT_TOKEN or not CHAT_ID:
        log.error("BOT_TOKEN atau CHAT_ID tidak ditemukan di .env!")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }
    try:
        r = requests.post(url, data=data, timeout=10)
        if r.status_code != 200:
            log.warning(f"Telegram gagal: {r.text}")
            return False
        return True
    except Exception as e:
        log.warning(f"Telegram error: {e}")
        return False


def send_error_alert(error_msg):
    """Kirim notifikasi error ke Telegram."""
    msg = f"⚠️ <b>SCANNER ERROR</b>\n\n{error_msg}\n\n<i>{utc_now()}</i>"
    send_telegram(msg)


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def utc_hour():
    return datetime.now(timezone.utc).hour


def is_in_cooldown(symbol):
    """Cek apakah coin masih dalam cooldown alert."""
    last_alert = _alert_cooldown.get(symbol, 0)
    return (time.time() - last_alert) < CONFIG["alert_cooldown_sec"]


def set_cooldown(symbol):
    _alert_cooldown[symbol] = time.time()


def pearson_correlation(x, y):
    """Hitung korelasi Pearson antara dua list."""
    n = len(x)
    if n < 2:
        return 0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den_x = math.sqrt(sum((x[i] - mean_x) ** 2 for i in range(n)))
    den_y = math.sqrt(sum((y[i] - mean_y) ** 2 for i in range(n)))
    if den_x == 0 or den_y == 0:
        return 0
    return num / (den_x * den_y)


def percentile_rank(value, data):
    """Hitung di persentil berapa 'value' berada dalam 'data'."""
    if not data:
        return 50
    below = sum(1 for d in data if d < value)
    return (below / len(data)) * 100


# ══════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS — BITGET API v2
# ══════════════════════════════════════════════════════════════

def get_all_futures_tickers():
    """Ambil semua ticker USDT Futures dari Bitget."""
    url = f"{BITGET_BASE}/api/v2/mix/market/tickers"
    data = safe_get(url, params={"productType": "USDT-FUTURES"})
    if data and data.get("code") == "00000":
        return data.get("data", [])
    log.error("Gagal ambil futures tickers")
    return []


def get_candles(symbol, granularity="15m", limit=96):
    """
    Ambil data candlestick OHLCV.
    granularity: '1m','5m','15m','1h','4h','1d'
    Return: list of dict {open, high, low, close, volume, timestamp}
    """
    cache_key = f"candles_{symbol}_{granularity}_{limit}"
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if time.time() - cached_time < 60:  # Cache 60 detik
            return cached_data

    url = f"{BITGET_BASE}/api/v2/mix/market/candles"
    params = {
        "symbol": symbol,
        "granularity": granularity,
        "limit": str(limit),
        "productType": "USDT-FUTURES"
    }
    data = safe_get(url, params=params)
    if not data or data.get("code") != "00000":
        return []

    candles = []
    for c in data.get("data", []):
        try:
            candles.append({
                "timestamp": int(c[0]),
                "open":      float(c[1]),
                "high":      float(c[2]),
                "low":       float(c[3]),
                "close":     float(c[4]),
                "volume":    float(c[5]),
                "volume_usd": float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            })
        except (IndexError, ValueError):
            continue

    candles.sort(key=lambda x: x["timestamp"])
    _cache[cache_key] = (time.time(), candles)
    return candles


def get_open_interest(symbol):
    """Ambil Open Interest saat ini dalam USD."""
    url = f"{BITGET_BASE}/api/v2/mix/market/open-interest"
    data = safe_get(url, params={"symbol": symbol, "productType": "USDT-FUTURES"})
    if data and data.get("code") == "00000":
        d = data.get("data", {})
        try:
            return float(d.get("openInterestList", [{}])[0].get("size", 0))
        except (IndexError, ValueError, TypeError):
            try:
                return float(d.get("size", 0))
            except:
                return 0
    return 0


def get_funding_rate(symbol):
    """Ambil funding rate saat ini."""
    url = f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate"
    data = safe_get(url, params={"symbol": symbol, "productType": "USDT-FUTURES"})
    if data and data.get("code") == "00000":
        try:
            return float(data["data"][0].get("fundingRate", 0))
        except (IndexError, KeyError, ValueError, TypeError):
            return 0
    return 0


def get_orderbook(symbol, limit=20):
    """Ambil order book (bids dan asks)."""
    url = f"{BITGET_BASE}/api/v2/mix/market/merge-depth"
    data = safe_get(url, params={
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "limit": str(limit)
    })
    if data and data.get("code") == "00000":
        raw = data.get("data", {})
        try:
            bids = [{"price": float(b[0]), "size": float(b[1])} for b in raw.get("bids", [])]
            asks = [{"price": float(a[0]), "size": float(a[1])} for a in raw.get("asks", [])]
            return {"bids": bids, "asks": asks}
        except (ValueError, IndexError):
            pass
    return {"bids": [], "asks": []}


def get_recent_trades(symbol, limit=200):
    """Ambil recent trades (transaksi terakhir)."""
    url = f"{BITGET_BASE}/api/v2/mix/market/fills"
    data = safe_get(url, params={
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "limit": str(limit)
    })
    if data and data.get("code") == "00000":
        trades = []
        for t in data.get("data", []):
            try:
                trades.append({
                    "price": float(t.get("price", 0)),
                    "size":  float(t.get("size", 0)),
                    "side":  t.get("side", "").lower(),  # 'buy' atau 'sell'
                    "ts":    int(t.get("ts", 0))
                })
            except (ValueError, TypeError):
                continue
        return trades
    return []


def get_coingecko_data(coin_symbol):
    """
    Ambil data market dari CoinGecko (market cap, float, dll).
    coin_symbol: e.g. 'ORCA', 'SNX', 'BIO'
    """
    # CoinGecko butuh coin id, bukan symbol — kita cari dulu
    cache_key = f"cg_{coin_symbol}"
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if time.time() - cached_time < 300:  # Cache 5 menit
            return cached_data

    # Search coin
    search_url = f"{COINGECKO_BASE}/search"
    search_data = safe_get(search_url, params={"query": coin_symbol})
    if not search_data:
        return None

    coins = search_data.get("coins", [])
    if not coins:
        return None

    # Ambil coin pertama yang symbolnya cocok
    coin_id = None
    for c in coins[:5]:
        if c.get("symbol", "").upper() == coin_symbol.upper():
            coin_id = c.get("id")
            break

    if not coin_id:
        return None

    # Ambil detail market
    detail_url = f"{COINGECKO_BASE}/coins/{coin_id}"
    detail = safe_get(detail_url, params={
        "localization": "false",
        "tickers": "false",
        "community_data": "false",
        "developer_data": "false"
    })

    if not detail:
        return None

    market_data = detail.get("market_data", {})
    result = {
        "market_cap":          market_data.get("market_cap", {}).get("usd", 0) or 0,
        "circulating_supply":  market_data.get("circulating_supply") or 0,
        "total_supply":        market_data.get("total_supply") or 1,
        "volume_24h":          market_data.get("total_volume", {}).get("usd", 0) or 0,
    }

    _cache[cache_key] = (time.time(), result)
    return result


def get_cg_trending():
    """Ambil daftar coin trending dari CoinGecko."""
    cache_key = "cg_trending"
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if time.time() - cached_time < 300:
            return cached_data

    data = safe_get(f"{COINGECKO_BASE}/search/trending")
    if data:
        trending = [c["item"]["symbol"].upper() for c in data.get("coins", [])]
        _cache[cache_key] = (time.time(), trending)
        return trending
    return []


# ══════════════════════════════════════════════════════════════
#  📊  ANALISIS TEKNIKAL
# ══════════════════════════════════════════════════════════════

def calculate_bollinger_bands(candles, period=20, std_dev=2):
    """Hitung Bollinger Bands dan BBW (Band Width)."""
    if len(candles) < period:
        return None

    closes = [c["close"] for c in candles]
    results = []

    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        mean   = sum(window) / period
        variance = sum((x - mean) ** 2 for x in window) / period
        std    = math.sqrt(variance)
        upper  = mean + std_dev * std
        lower  = mean - std_dev * std
        bbw    = (upper - lower) / mean * 100 if mean != 0 else 0
        results.append({"middle": mean, "upper": upper, "lower": lower, "bbw": bbw})

    return results


def calculate_atr(candles, period=14):
    """Hitung Average True Range."""
    if len(candles) < period + 1:
        return []

    trs = []
    for i in range(1, len(candles)):
        high  = candles[i]["high"]
        low   = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    atrs = []
    window_sum = sum(trs[:period])
    atrs.append(window_sum / period)
    for i in range(period, len(trs)):
        atr = (atrs[-1] * (period - 1) + trs[i]) / period
        atrs.append(atr)

    return atrs


def calculate_vwap(candles):
    """Hitung VWAP dan standar deviasinya."""
    cum_tp_vol = 0
    cum_vol    = 0
    vwap_vals  = []

    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_tp_vol += tp * c["volume"]
        cum_vol    += c["volume"]
        vwap = cum_tp_vol / cum_vol if cum_vol > 0 else tp
        vwap_vals.append(vwap)

    if not vwap_vals:
        return None, None, None

    current_vwap = vwap_vals[-1]

    # Hitung standar deviasi VWAP
    deviations = []
    cum_tp_vol2 = 0
    cum_vol2    = 0
    for i, c in enumerate(candles):
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_tp_vol2 += tp * c["volume"]
        cum_vol2    += c["volume"]
        v_sum = sum((((candles[j]["high"] + candles[j]["low"] + candles[j]["close"]) / 3) - vwap_vals[i]) ** 2 * candles[j]["volume"]
                    for j in range(i + 1))
        v_vol = sum(candles[j]["volume"] for j in range(i + 1))
        dev = math.sqrt(v_sum / v_vol) if v_vol > 0 else 0
        deviations.append(dev)

    current_std = deviations[-1] if deviations else 0

    return current_vwap, current_std, vwap_vals


def calculate_point_of_control(candles, buckets=50):
    """
    Hitung Point of Control — harga dengan volume terbanyak.
    Ini adalah 'fair value' historis menurut pasar.
    """
    if not candles:
        return None

    price_min = min(c["low"] for c in candles)
    price_max = max(c["high"] for c in candles)
    if price_max == price_min:
        return candles[-1]["close"]

    bucket_size = (price_max - price_min) / buckets
    volume_by_bucket = defaultdict(float)

    for c in candles:
        # Distribusi volume secara merata antara high dan low
        low_bucket  = int((c["low"] - price_min) / bucket_size)
        high_bucket = int((c["high"] - price_min) / bucket_size)
        num_buckets = max(high_bucket - low_bucket + 1, 1)
        vol_per_bucket = c["volume"] / num_buckets

        for b in range(low_bucket, high_bucket + 1):
            volume_by_bucket[b] += vol_per_bucket

    if not volume_by_bucket:
        return candles[-1]["close"]

    poc_bucket = max(volume_by_bucket, key=volume_by_bucket.get)
    poc_price  = price_min + (poc_bucket + 0.5) * bucket_size
    return poc_price


# ══════════════════════════════════════════════════════════════
#  🐋  WHALE DETECTION ENGINE
# ══════════════════════════════════════════════════════════════

def detect_order_book_absorption(symbol):
    """
    Deteksi apakah whale sedang menyerap tekanan jual.
    Sell wall berkurang tapi harga tidak turun = ABSORPTION.
    """
    book1 = get_orderbook(symbol, limit=10)
    if not book1["asks"]:
        return False, 0, ""

    price1 = book1["asks"][0]["price"] if book1["asks"] else 0
    sell_wall1 = sum(a["size"] * a["price"] for a in book1["asks"][:5])

    time.sleep(15)  # Tunggu 15 detik

    book2 = get_orderbook(symbol, limit=10)
    if not book2["asks"]:
        return False, 0, ""

    price2 = book2["asks"][0]["price"] if book2["asks"] else 0
    sell_wall2 = sum(a["size"] * a["price"] for a in book2["asks"][:5])

    if sell_wall1 == 0:
        return False, 0, ""

    wall_change   = (sell_wall1 - sell_wall2) / sell_wall1
    price_change  = (price2 - price1) / price1 * 100 if price1 > 0 else 0

    # Sell wall berkurang >20% tapi harga tidak turun > 0.2%
    is_absorbing = wall_change > 0.20 and price_change > -0.2

    detail = (f"Sell wall turun {wall_change:.0%}, harga bergerak {price_change:+.2f}%"
              if is_absorbing else "")

    return is_absorbing, wall_change, detail


def detect_iceberg_orders(trades, current_price, tolerance_pct=0.15):
    """
    Deteksi iceberg order — banyak order kecil di level harga yang sama.
    Tanda whale membeli diam-diam dengan memecah order besar.
    """
    if not trades:
        return False, 0, 0

    # Filter trade beli di sekitar harga sekarang (±0.15%)
    buy_trades_at_level = [
        t for t in trades
        if t["side"] == "buy"
        and abs(t["price"] - current_price) / current_price * 100 < tolerance_pct
    ]

    if len(buy_trades_at_level) < 5:
        return False, 0, 0

    total_absorbed_usd = sum(t["size"] * t["price"] for t in buy_trades_at_level)
    trade_count        = len(buy_trades_at_level)
    avg_size_usd       = total_absorbed_usd / trade_count

    # Banyak transaksi kecil + total besar = iceberg
    is_iceberg = (
        trade_count >= 15
        and avg_size_usd < 5_000     # Rata-rata kecil (< $5K per transaksi)
        and total_absorbed_usd > 50_000  # Tapi total besar (> $50K)
    )

    return is_iceberg, total_absorbed_usd, trade_count


def detect_large_trade_dominance(trades):
    """
    Hitung berapa % volume yang dikuasai oleh order besar (whale).
    Order besar = transaksi > $30,000.
    """
    if not trades:
        return 0, 0

    total_vol_usd  = sum(t["size"] * t["price"] for t in trades)
    large_buy_usd  = sum(
        t["size"] * t["price"] for t in trades
        if t["side"] == "buy" and t["size"] * t["price"] > 30_000
    )

    if total_vol_usd == 0:
        return 0, 0

    dominance   = large_buy_usd / total_vol_usd
    return dominance, large_buy_usd


def get_taker_ratio(trades):
    """
    Hitung rasio Taker Buy vs total.
    > 0.65 saat harga flat = akumulasi agresif tersembunyi.
    """
    if not trades:
        return 0.5

    buy_vol  = sum(t["size"] for t in trades if t["side"] == "buy")
    sell_vol = sum(t["size"] for t in trades if t["side"] == "sell")
    total    = buy_vol + sell_vol

    return buy_vol / total if total > 0 else 0.5


def whale_composite_score(symbol, candles_15m, oi_now, oi_4h_ago, funding):
    """
    Gabungkan semua sinyal whale menjadi skor 0-100.
    Semakin tinggi = semakin besar kemungkinan whale sedang akumulasi.
    """
    whale_score = 0
    evidence    = []
    current_price = candles_15m[-1]["close"] if candles_15m else 0

    # ── 1. Taker Buy Ratio dari recent trades ────────────────
    trades = get_recent_trades(symbol, limit=200)
    if trades:
        taker = get_taker_ratio(trades)
        if taker > 0.70:
            whale_score += 20
            evidence.append(f"✅ Taker Buy {taker:.0%} — pembeli sangat agresif")
        elif taker > 0.62:
            whale_score += 10
            evidence.append(f"🔶 Taker Buy {taker:.0%} — bias beli")

        # ── 2. Large Trade Dominance ─────────────────────────
        dominance, large_buy = detect_large_trade_dominance(trades)
        if dominance > 0.40:
            whale_score += 20
            evidence.append(f"✅ Whale prints {dominance:.0%} dari volume (${large_buy:,.0f})")
        elif dominance > 0.25:
            whale_score += 10
            evidence.append(f"🔶 Smart money {dominance:.0%} volume")

        # ── 3. Iceberg Order Detection ────────────────────────
        is_iceberg, total_abs, trade_count = detect_iceberg_orders(trades, current_price)
        if is_iceberg:
            whale_score += 20
            evidence.append(f"✅ Iceberg order: ${total_abs:,.0f} ({trade_count} transaksi kecil)")

    # ── 4. OI Building Silently ──────────────────────────────
    if oi_4h_ago and oi_4h_ago > 0:
        oi_change_pct = (oi_now - oi_4h_ago) / oi_4h_ago * 100

        # Hitung price change dalam 4h
        price_4h_ago = candles_15m[-16]["close"] if len(candles_15m) >= 16 else current_price
        price_change_4h = abs((current_price - price_4h_ago) / price_4h_ago * 100) if price_4h_ago else 0

        if oi_change_pct > 15 and price_change_4h < 2.0:
            whale_score += 20
            evidence.append(f"✅ OI +{oi_change_pct:.0f}% dalam 4h — harga masih flat (stealth)")
        elif oi_change_pct > 8 and price_change_4h < 3.0:
            whale_score += 10
            evidence.append(f"🔶 OI +{oi_change_pct:.0f}% building")

    # ── 5. Funding Rate Setup ────────────────────────────────
    if funding < -0.05:
        whale_score += 15
        evidence.append(f"✅ Funding {funding:.3f}% — short squeeze setup")
    elif -0.02 < funding < 0.02:
        whale_score += 5
        evidence.append(f"🔷 Funding netral ({funding:.3f}%)")

    # ── Klasifikasi ──────────────────────────────────────────
    if whale_score >= 70:
        classification = "🐋 WHALE ACCUMULATION CONFIRMED"
    elif whale_score >= 45:
        classification = "🦈 SMART MONEY BUILDING"
    elif whale_score >= 25:
        classification = "👀 POSSIBLE INSTITUTIONAL INTEREST"
    else:
        classification = "🔇 NO CLEAR WHALE SIGNAL"

    return min(whale_score, 100), classification, evidence


# ══════════════════════════════════════════════════════════════
#  🔬  INTERNAL PUMP ANATOMY
# ══════════════════════════════════════════════════════════════

def analyze_volatility_compression(candles_15m, candles_1h):
    """
    Deteksi apakah coin sedang dalam fase kompresi (pegas ditekan).
    Makin lama dan makin ketat kompresi = makin besar potensi ledakan.
    """
    score   = 0
    signals = []

    # ── A. Bollinger Band Width Compression ─────────────────
    bb_1h = calculate_bollinger_bands(candles_1h, period=20)
    if bb_1h and len(bb_1h) >= 20:
        recent_bbw = bb_1h[-1]["bbw"]
        hist_bbw   = [b["bbw"] for b in bb_1h[-50:]]
        bbw_pct    = percentile_rank(recent_bbw, hist_bbw)  # Semakin rendah = semakin terkompresi

        if bbw_pct < 10:
            score += 20
            signals.append(f"BBW Squeeze Ekstrem (persentil {bbw_pct:.0f}%)")
        elif bbw_pct < 25:
            score += 12
            signals.append(f"BBW Menyempit (persentil {bbw_pct:.0f}%)")
        elif bbw_pct < 40:
            score += 5
            signals.append(f"BBW Mulai Kompres (persentil {bbw_pct:.0f}%)")

    # ── B. ATR Compression Ratio ─────────────────────────────
    atr_1h = calculate_atr(candles_1h, period=14)
    if len(atr_1h) >= 50:
        atr_short = sum(atr_1h[-7:]) / 7   # ATR 7 candle terakhir
        atr_long  = sum(atr_1h[-50:]) / 50  # ATR 50 candle
        atr_ratio = atr_short / atr_long if atr_long > 0 else 1

        if atr_ratio < 0.40:
            score += 15
            signals.append(f"ATR Compressed Extreme ({atr_ratio:.2f}x normal)")
        elif atr_ratio < 0.60:
            score += 8
            signals.append(f"ATR Compressing ({atr_ratio:.2f}x normal)")

    # ── C. Coiling Duration (berapa lama sudah diam?) ────────
    quiet_candles = 0
    for c in reversed(candles_1h):
        body_pct = abs(c["close"] - c["open"]) / c["open"] * 100
        if body_pct < 0.8:
            quiet_candles += 1
        else:
            break

    if quiet_candles >= 12:
        score += 18
        signals.append(f"Coiling {quiet_candles}h — energi terkumpul lama")
    elif quiet_candles >= 6:
        score += 10
        signals.append(f"Coiling {quiet_candles}h")
    elif quiet_candles >= 3:
        score += 4
        signals.append(f"Mulai diam {quiet_candles}h")

    return score, signals, quiet_candles


def analyze_volume_signature(candles_15m):
    """
    Deteksi pola volume yang muncul sebelum pump.
    Pattern: Volume turun → Naik diam-diam saat harga masih flat.
    """
    score   = 0
    signals = []

    if len(candles_15m) < 24:
        return score, signals

    vols = [c["volume_usd"] for c in candles_15m]

    # Rata-rata volume 20 candle pertama (baseline)
    vol_baseline = sum(vols[:20]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)

    # Rata-rata volume 8 candle terakhir (terkini)
    vol_recent   = sum(vols[-8:]) / 8

    vol_ratio    = vol_recent / vol_baseline if vol_baseline > 0 else 1

    # Perubahan harga selama window yang sama
    price_start  = candles_15m[-24]["close"]
    price_now    = candles_15m[-1]["close"]
    price_change = abs((price_now - price_start) / price_start * 100) if price_start > 0 else 99

    # Volume naik tapi harga flat = akumulasi tersembunyi
    if 1.5 <= vol_ratio <= 4.0 and price_change < 2.0:
        score += 25
        signals.append(f"Volume Creep {vol_ratio:.1f}x — harga flat (akumulasi tersembunyi)")
    elif 1.2 <= vol_ratio <= 1.5 and price_change < 3.0:
        score += 12
        signals.append(f"Volume perlahan naik {vol_ratio:.1f}x")

    # Cek CVD Divergence (approximasi dari candle data)
    cvd = 0
    cvd_start = None
    for c in candles_15m[-20:]:
        rng = c["high"] - c["low"]
        if rng == 0:
            continue
        buy_ratio = (c["close"] - c["low"]) / rng
        delta = c["volume"] * (2 * buy_ratio - 1)
        cvd  += delta
        if cvd_start is None:
            cvd_start = cvd

    price_trend = candles_15m[-1]["close"] - candles_15m[-20]["close"]
    cvd_bullish_divergence = (cvd > 0 and price_trend < 0.01 * candles_15m[-1]["close"])

    if cvd_bullish_divergence:
        score += 15
        signals.append("CVD Bullish Divergence — beli tersembunyi tidak tercermin di harga")

    return score, signals


def analyze_oi_dynamics(symbol, oi_now, candles_1h):
    """
    Analisis dinamika Open Interest vs pergerakan harga.
    OI naik + harga flat = akumulasi. OI turun + harga naik = distribusi.
    """
    score   = 0
    signals = []

    if len(candles_1h) < 4 or oi_now == 0:
        return score, signals

    # Estimasi OI 4 jam lalu (tidak ada API historis gratis, gunakan estimasi dari candle)
    # Kita gunakan perubahan harga sebagai proxy
    price_now    = candles_1h[-1]["close"]
    price_4h_ago = candles_1h[-4]["close"] if len(candles_1h) >= 4 else price_now
    price_chg_4h = (price_now - price_4h_ago) / price_4h_ago * 100 if price_4h_ago else 0

    funding = get_funding_rate(symbol)

    # OI tinggi + harga flat + funding netral/negatif = akumulasi paling bersih
    if abs(price_chg_4h) < 1.5 and funding < 0.03:
        score += 15
        signals.append(f"OI Aktif — harga flat, funding sehat ({funding:.3f}%)")

    # Short squeeze setup: funding negatif + OI besar
    if funding < -0.04:
        score += 18
        signals.append(f"Short Squeeze Setup — funding {funding:.3f}%")
    elif funding < -0.02:
        score += 8
        signals.append(f"Bias short ada ({funding:.3f}%) — potensi squeeze")

    # Funding terlalu positif = sudah overheated
    if funding > 0.08:
        score -= 15
        signals.append(f"⚠️ Funding terlalu positif ({funding:.3f}%) — rawan dump")

    return score, signals, funding


# ══════════════════════════════════════════════════════════════
#  🎯  ENTRY ZONE CALCULATOR
# ══════════════════════════════════════════════════════════════

def calculate_smart_entry_zones(candles, funding_rate):
    """
    Hitung 3 entry zone yang digunakan smart money.
    Return zona entry, stop loss, target 1 & 2, risk/reward.
    """
    if not candles:
        return None

    current_price = candles[-1]["close"]

    # ── Zone 1: VWAP Institutional Level ────────────────────
    vwap, vwap_std, _ = calculate_vwap(candles)
    zone1 = (vwap - 1.5 * vwap_std) if (vwap and vwap_std) else current_price * 0.97

    # ── Zone 2: Point of Control ─────────────────────────────
    poc = calculate_point_of_control(candles)
    zone2 = poc if poc else current_price * 0.98

    # ── Zone 3: Short Liquidation Target (atas) ──────────────
    estimated_leverage = 10
    if funding_rate < -0.08:
        estimated_leverage = 20
    elif funding_rate < -0.04:
        estimated_leverage = 15

    short_liq_zone = current_price * (1 + 1 / estimated_leverage)

    # ── Entry yang direkomendasikan ───────────────────────────
    best_support   = max(zone1, zone2)  # Yang lebih dekat ke harga
    entry_ideal    = best_support * 1.002  # Sedikit di atas zona support
    stop_loss      = best_support * 0.967  # 3.3% di bawah support
    target1        = short_liq_zone
    target2        = short_liq_zone * 1.06

    risk           = current_price - stop_loss
    reward1        = target1 - current_price
    rr_ratio       = round(reward1 / risk, 1) if risk > 0 else 0

    return {
        "current_price":   current_price,
        "vwap":            round(vwap, 6) if vwap else 0,
        "zone1_vwap":      round(zone1, 6),
        "zone2_poc":       round(zone2, 6),
        "entry_ideal":     round(entry_ideal, 6),
        "stop_loss":       round(stop_loss, 6),
        "target1":         round(target1, 6),
        "target2":         round(target2, 6),
        "risk_reward":     rr_ratio,
        "liq_distance_pct": round((short_liq_zone - current_price) / current_price * 100, 1)
    }


# ══════════════════════════════════════════════════════════════
#  🔄  SECTOR ROTATION ENGINE
# ══════════════════════════════════════════════════════════════

def build_sector_lookup():
    """Buat mapping dari symbol ke sektor."""
    lookup = {}
    for sector, coins in SECTOR_MAP.items():
        for coin in coins:
            lookup[coin] = sector
    return lookup


SECTOR_LOOKUP = build_sector_lookup()


def get_coin_sector(symbol):
    return SECTOR_LOOKUP.get(symbol, "UNKNOWN")


def detect_sector_rotation(symbol, all_tickers_dict):
    """
    Cek apakah ada coin lain dalam sektor yang sudah pump.
    Jika ya, coin ini berpotensi kena rotasi berikutnya.
    """
    sector = get_coin_sector(symbol)
    if sector == "UNKNOWN":
        return 0, []

    score   = 0
    signals = []
    peers   = SECTOR_MAP.get(sector, [])

    for peer in peers:
        if peer == symbol:
            continue
        peer_data = all_tickers_dict.get(peer)
        if not peer_data:
            continue

        try:
            peer_change_24h = float(peer_data.get("change24h", 0)) * 100
        except (ValueError, TypeError):
            continue

        if peer_change_24h > 15:
            score += 25
            signals.append(
                f"🔄 {peer} pump +{peer_change_24h:.0f}% — rotasi ke {symbol.replace('USDT','')} mungkin segera"
            )
            break
        elif peer_change_24h > 8:
            score += 12
            signals.append(
                f"Sektor {sector} panas — {peer} +{peer_change_24h:.0f}%"
            )
            break

    return score, signals


def analyze_relative_strength(symbol, sector, all_tickers_dict):
    """
    Bandingkan kekuatan coin ini vs rata-rata sektornya.
    Coin yang paling kuat relatif = pemimpin sektoral = pump pertama.
    """
    peers   = SECTOR_MAP.get(sector, [])
    changes = []

    for peer in peers:
        peer_data = all_tickers_dict.get(peer)
        if not peer_data:
            continue
        try:
            chg = float(peer_data.get("change24h", 0)) * 100
            changes.append(chg)
        except:
            continue

    if len(changes) < 2:
        return 0, ""

    sector_avg = sum(changes) / len(changes)

    try:
        coin_data   = all_tickers_dict.get(symbol)
        coin_change = float(coin_data.get("change24h", 0)) * 100
    except:
        return 0, ""

    rs_score_val = coin_change - sector_avg

    score  = 0
    signal = ""

    if rs_score_val > 2 and coin_change < 6:
        score  = 20
        signal = f"RS Leader #{1} di sektor — kuat tapi belum pump (RS: +{rs_score_val:.1f}%)"
    elif rs_score_val > 0 and coin_change < 4:
        score  = 10
        signal = f"Lebih kuat dari rata-rata sektor (RS: +{rs_score_val:.1f}%)"

    return score, signal


# ══════════════════════════════════════════════════════════════
#  💎  FLOAT & CATALYST ANALYSIS
# ══════════════════════════════════════════════════════════════

def analyze_float_and_cap(symbol):
    """
    Analisis market cap dan float.
    Float rendah + cap kecil = mudah dipump dengan modal kecil.
    """
    coin_name = symbol.replace("USDT", "")
    cg_data   = get_coingecko_data(coin_name)

    score   = 0
    signals = []

    if not cg_data:
        return score, signals

    mcap = cg_data["market_cap"]
    circ = cg_data["circulating_supply"]
    total = cg_data["total_supply"]
    float_ratio = circ / total if total > 0 else 1

    # Sweet spot: market cap $3M - $200M
    if CONFIG["min_market_cap"] < mcap < CONFIG["max_market_cap"]:
        if mcap < 30_000_000:
            score += 15
            signals.append(f"Micro Cap (${mcap/1e6:.1f}M) — mudah bergerak")
        else:
            score += 8
            signals.append(f"Small Cap (${mcap/1e6:.1f}M) — sweet spot pump")

    # Float rendah = lebih mudah dipump
    if float_ratio < 0.25:
        score += 15
        signals.append(f"Low Float ({float_ratio:.0%} beredar) — butuh modal kecil untuk pump")
    elif float_ratio < 0.45:
        score += 7
        signals.append(f"Float moderat ({float_ratio:.0%})")

    return score, signals


def check_social_signals(symbol):
    """
    Cek apakah coin trending di CoinGecko tapi belum pump.
    Trending + belum pump = pre-pump terkuat.
    """
    coin_name = symbol.replace("USDT", "").upper()
    trending  = get_cg_trending()

    score   = 0
    signals = []

    if coin_name in trending:
        score  += 20
        signals.append(f"🔥 {coin_name} TRENDING di CoinGecko — tapi belum pump")

    return score, signals


# ══════════════════════════════════════════════════════════════
#  ⏰  TIME INTELLIGENCE
# ══════════════════════════════════════════════════════════════

def get_time_multiplier():
    """
    Pump altcoin paling sering terjadi di window waktu tertentu.
    Sinyal yang muncul menjelang window = probabilitas lebih tinggi.
    """
    hour = utc_hour()

    # Window tinggi: pre-Asia afternoon, pre-Eropa, pre-US
    if hour in [5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.3, f"High-prob window ({hour}:00 UTC)"

    # Dead hours: tengah malam UTC
    if hour in [1, 2, 3, 4]:
        return 0.7, f"Low-prob window ({hour}:00 UTC — likuiditas rendah)"

    return 1.0, f"Normal window ({hour}:00 UTC)"


# ══════════════════════════════════════════════════════════════
#  🎯  MASTER SCORING ENGINE
# ══════════════════════════════════════════════════════════════

def master_score(symbol, ticker_data, all_tickers_dict):
    """
    Gabungkan semua layer analisis menjadi satu skor final.
    Return: (total_score, whale_score, whale_class, whale_evidence,
             all_signals, entry_zones)
    """
    total  = 0
    sigs   = []

    # ── AMBIL DATA ───────────────────────────────────────────
    candles_15m = get_candles(symbol, "15m", CONFIG["candle_limit_15m"])
    candles_1h  = get_candles(symbol, "1h",  CONFIG["candle_limit_1h"])

    if len(candles_15m) < 24 or len(candles_1h) < 12:
        return None  # Data tidak cukup

    oi_now = get_open_interest(symbol)

    # Estimasi OI 4h lalu (proxy: gunakan 0.9x dari OI sekarang jika OI naik)
    # Ini approximation — tanpa API historis OI gratis
    oi_4h_ago = oi_now * 0.85  # Estimasi konservatif

    funding = get_funding_rate(symbol)

    # ── LAYER 1: VOLATILITY COMPRESSION (max 35) ────────────
    vol_score, vol_sigs, quiet_h = analyze_volatility_compression(candles_15m, candles_1h)
    total += min(vol_score, 35)
    sigs  += vol_sigs

    # ── LAYER 2: VOLUME SIGNATURE (max 30) ──────────────────
    vsig_score, vsig_sigs = analyze_volume_signature(candles_15m)
    total += min(vsig_score, 30)
    sigs  += vsig_sigs

    # ── LAYER 3: OI DYNAMICS (max 20) ───────────────────────
    oi_score, oi_sigs, funding = analyze_oi_dynamics(symbol, oi_now, candles_1h)
    total += min(oi_score, 20)
    sigs  += oi_sigs

    # ── LAYER 4: SECTOR ROTATION (max 25) ───────────────────
    sec_score, sec_sigs = detect_sector_rotation(symbol, all_tickers_dict)
    total += min(sec_score, 25)
    sigs  += sec_sigs

    # ── LAYER 5: RELATIVE STRENGTH (max 20) ─────────────────
    sector = get_coin_sector(symbol)
    rs_score, rs_sig = analyze_relative_strength(symbol, sector, all_tickers_dict)
    total += min(rs_score, 20)
    if rs_sig:
        sigs.append(rs_sig)

    # ── LAYER 6: FLOAT & SOCIAL (max 25) ────────────────────
    float_score, float_sigs = analyze_float_and_cap(symbol)
    social_score, social_sigs = check_social_signals(symbol)
    total += min(float_score + social_score, 25)
    sigs  += float_sigs + social_sigs

    # ── LAYER 7: WHALE COMPOSITE (dipisah, tidak masuk total) ─
    whale_sc, whale_class, whale_ev = whale_composite_score(
        symbol, candles_15m, oi_now, oi_4h_ago, funding
    )

    # Whale score bonus masuk ke total jika cukup kuat
    if whale_sc >= 50:
        total += 15
        sigs.append(f"Whale confirmed bonus")
    elif whale_sc >= 30:
        total += 7

    # ── TIME MULTIPLIER ──────────────────────────────────────
    time_mult, time_sig = get_time_multiplier()
    total = int(total * time_mult)
    if time_mult != 1.0:
        sigs.append(f"⏰ {time_sig}")

    # ── ENTRY ZONES ──────────────────────────────────────────
    entry = calculate_smart_entry_zones(candles_1h, funding)

    return {
        "symbol":        symbol,
        "score":         min(total, 100),
        "signals":       sigs,
        "whale_score":   whale_sc,
        "whale_class":   whale_class,
        "whale_evidence": whale_ev,
        "entry":         entry,
        "sector":        sector,
        "funding":       funding,
        "quiet_hours":   quiet_h,
    }


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM ALERT BUILDER
# ══════════════════════════════════════════════════════════════

def build_alert_message(result, ticker_data):
    """Bangun pesan alert yang informatif dan terstruktur."""

    sym    = result["symbol"]
    sc     = result["score"]
    ws     = result["whale_score"]
    wc     = result["whale_class"]
    we     = result["whale_evidence"]
    sigs   = result["signals"]
    entry  = result["entry"]
    sector = result["sector"]
    fund   = result["funding"]

    try:
        price_now = float(ticker_data.get("lastPr", 0))
        chg_24h   = float(ticker_data.get("change24h", 0)) * 100
        vol_24h   = float(ticker_data.get("quoteVolume", 0))
    except:
        price_now = entry["current_price"] if entry else 0
        chg_24h   = 0
        vol_24h   = 0

    # Score bar visual
    filled = int(sc / 5)
    bar    = "█" * filled + "░" * (20 - filled)

    msg = f"""
🚨 <b>PRE-PUMP INTELLIGENCE REPORT</b>

<b>Symbol :</b> {sym}
<b>Score  :</b> {sc}/100  {bar}
<b>Sektor :</b> {sector}
<b>Harga  :</b> ${price_now:.6f} ({chg_24h:+.1f}% 24h)
<b>Vol 24h:</b> ${vol_24h/1e6:.1f}M

━━━━━━━━━━━━━━━━━━━━━━━━━━
🐋 <b>WHALE INTELLIGENCE: {ws}/100</b>
<i>{wc}</i>
"""

    if we:
        for ev in we:
            msg += f"\n  {ev}"
    else:
        msg += "\n  Tidak ada sinyal whale kuat"

    if entry:
        msg += f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━
📍 <b>ENTRY ZONES (Smart Money Level)</b>

  🟢 VWAP Support : ${entry['zone1_vwap']}
  🟢 Point of Control: ${entry['zone2_poc']}
  📌 Entry Ideal  : ${entry['entry_ideal']}
  🛑 Stop Loss    : ${entry['stop_loss']}

🎯 <b>TARGET</b>
  Target 1 : ${entry['target1']} (+{entry['liq_distance_pct']:.1f}%)
  Target 2 : ${entry['target2']}
  R/R Ratio: 1 : {entry['risk_reward']}
"""

    msg += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>SINYAL DETEKSI</b>
"""
    for s in sigs[:8]:  # Max 8 sinyal
        msg += f"\n  • {s}"

    msg += f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━
⏰ Window  : 1-6 jam ke depan
🔁 Funding : {fund:.4f}%
🕐 Waktu   : {utc_now()}

<i>Bukan financial advice. Selalu manage risk.</i>
"""

    return msg.strip()


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCANNER
# ══════════════════════════════════════════════════════════════

def run_scan():
    """Jalankan satu siklus scan lengkap."""
    log.info("=" * 50)
    log.info(f"SCAN DIMULAI — {utc_now()}")

    # Ambil semua ticker
    tickers = get_all_futures_tickers()
    if not tickers:
        log.error("Gagal ambil data ticker — coba lagi nanti")
        send_error_alert("Gagal ambil data dari Bitget API")
        return

    # Buat dictionary untuk lookup cepat
    all_tickers_dict = {}
    for t in tickers:
        sym = t.get("symbol", "")
        if sym:
            all_tickers_dict[sym] = t

    log.info(f"Total ticker ditemukan: {len(tickers)}")

    # ── Filter awal: hanya coin yang layak discan ────────────
    candidates = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue

        try:
            vol_usd   = float(t.get("quoteVolume", 0))
            chg_4h    = abs(float(t.get("change24h", 0)) * 100)  # Approx dengan 24h
        except (ValueError, TypeError):
            continue

        # Filter volume minimum
        if vol_usd < CONFIG["min_volume_usd_24h"]:
            continue

        # Filter yang sudah pump
        if chg_4h > CONFIG["max_pump_pct_4h"]:
            continue

        # Filter cooldown
        if is_in_cooldown(sym):
            continue

        candidates.append(t)

    log.info(f"Kandidat setelah filter: {len(candidates)}")

    results = []

    for i, ticker in enumerate(candidates):
        sym = ticker.get("symbol", "")
        log.info(f"[{i+1}/{len(candidates)}] Analisis {sym}...")

        try:
            result = master_score(sym, ticker, all_tickers_dict)
            if result and result["score"] >= CONFIG["min_score_alert"]:
                results.append((result, ticker))
        except Exception as e:
            log.warning(f"Error analisis {sym}: {e}")

        # Rate limiting — jangan banjiri API
        time.sleep(0.5)

    # Sort berdasarkan skor tertinggi
    results.sort(key=lambda x: x[0]["score"], reverse=True)

    # Kirim alert untuk top results
    alert_sent = 0
    for result, ticker in results[:5]:  # Max 5 alert per siklus
        sym = result["symbol"]

        if result["whale_score"] < CONFIG["min_whale_score"] and result["score"] < 65:
            continue

        msg = build_alert_message(result, ticker)
        success = send_telegram(msg)

        if success:
            set_cooldown(sym)
            alert_sent += 1
            log.info(f"✅ Alert terkirim: {sym} (Score: {result['score']}, Whale: {result['whale_score']})")
        else:
            log.warning(f"Gagal kirim alert untuk {sym}")

        time.sleep(2)  # Jeda antar pesan Telegram

    if alert_sent == 0 and results:
        log.info(f"Ada {len(results)} kandidat tapi belum memenuhi threshold alert")
    elif alert_sent == 0:
        log.info("Tidak ada sinyal kuat ditemukan dalam siklus ini")
    else:
        log.info(f"Siklus selesai — {alert_sent} alert terkirim")


# ══════════════════════════════════════════════════════════════
#  🔁  MAIN LOOP
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════╗")
    log.info("║   PRE-PUMP SCANNER v2.0 — DIMULAI       ║")
    log.info("╚══════════════════════════════════════════╝")

    # Cek konfigurasi
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN atau CHAT_ID tidak ada di file .env!")
        log.error("Buat file .env dengan isi:")
        log.error("BOT_TOKEN=token_bot_telegram_anda")
        log.error("CHAT_ID=chat_id_anda")
        exit(1)

    # Kirim notifikasi sistem aktif
    send_telegram(
        f"🟢 <b>Pre-Pump Scanner v2.0 AKTIF</b>\n\n"
        f"Interval scan: {CONFIG['scan_interval_sec']//60} menit\n"
        f"Min score alert: {CONFIG['min_score_alert']}\n"
        f"Waktu: {utc_now()}"
    )

    consecutive_errors = 0

    while True:
        try:
            run_scan()
            consecutive_errors = 0
            log.info(f"Menunggu {CONFIG['scan_interval_sec']} detik...")
            time.sleep(CONFIG["scan_interval_sec"])

        except KeyboardInterrupt:
            log.info("Scanner dihentikan oleh user")
            send_telegram("🔴 <b>Scanner dihentikan</b>")
            break

        except Exception as e:
            consecutive_errors += 1
            log.error(f"Error tidak terduga (#{consecutive_errors}): {e}")

            if consecutive_errors >= 3:
                send_error_alert(f"Error berulang #{consecutive_errors}: {str(e)[:200]}")

            # Backoff: semakin banyak error = semakin lama tunggu
            wait = min(60 * consecutive_errors, 600)
            log.info(f"Menunggu {wait} detik sebelum retry...")
            time.sleep(wait)
