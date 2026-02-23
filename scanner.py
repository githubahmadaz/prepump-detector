"""
╔══════════════════════════════════════════════════════════════════╗
║         PRE-PUMP INTELLIGENCE SCANNER v3.1                      ║
║         BTC-Independent | Whale Detection | Smart Entry         ║
║                                                                  ║
║  PERBAIKAN v3.1:                                                 ║
║  ✅ Threshold volume diturunkan berdasarkan data forensik        ║
║  ✅ Layer volume diperkaya dengan awakening ratio & penyesuaian ║
║  ✅ Layer funding diberi penalti untuk funding sangat negatif   ║
║  ✅ Konfigurasi pre-filter disesuaikan                           ║
╚══════════════════════════════════════════════════════════════════╝
"""

import requests
import time
import os
import math
import json
import logging
from datetime import datetime, timezone
from collections import defaultdict

# ── Load .env jika ada (untuk test lokal) ──────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  ⚙️  KONFIGURASI (DIUBAH BERDASARKAN FORENSIK)
# ══════════════════════════════════════════════════════════════
CONFIG = {
    "min_score_alert":        52,
    "min_whale_score":        25,
    "min_volume_usd_24h":     5_000,           # ✅ dari 1jt -> 5rb (menangkap ORCA dkk)
    "max_pump_pct_24h":       15.0,
    "min_market_cap":         50_000_000,
    "max_market_cap":         10_000_000_000,
    "alert_cooldown_sec":     3600,
    "candle_limit_15m":       96,
    "candle_limit_1h":        72,
    "max_alerts_per_run":     10,
    "pre_filter_min_vol":     2_000,           # ✅ dari 500rb -> 2rb (lebih longgar)
    "pre_filter_max_pump":    20.0,
    "sleep_between_coins":    1.0,
    "sleep_after_api_error":  3.0,
    "cooldown_file":          "/tmp/pump_scanner_cooldown.json",
}

# ── Mapping granularity ke format Bitget v2 ────────────────────
GRAN_MAP = {
    "1m":  "1m",
    "3m":  "3m",
    "5m":  "5m",
    "15m": "15m",
    "30m": "30m",
    "1h":  "1H",
    "2h":  "2H",
    "4h":  "4H",
    "6h":  "6H",
    "12h": "12H",
    "1d":  "1D",
}

# ══════════════════════════════════════════════════════════════
#  📋  DAFTAR COIN YANG DISCAN (TIDAK BERUBAH)
# ══════════════════════════════════════════════════════════════
TARGET_COINS = [
    "0GUSDT", "1000BONKUSDT", "1000RATSUSDT", "1000SATSUSDT",
    "1MBABYDOGEUSDT", "AAVEUSDT", "ACHUSDT", "ADAUSDT", "AEROUSDT",
    "AKTUSDT", "ALCHUSDT", "ALGOUSDT", "ANKRUSDT", "APEUSDT",
    "APTUSDT", "ARBUSDT", "ARCUSDT", "ASTERUSDT", "ASTRUSDT",
    "ATUSDT", "ATHUSDT", "ATOMUSDT", "AVAXUSDT", "AVNTUSDT",
    "AWEUSDT", "AXLUSDT", "AXSUSDT", "AZTECUSDT", "BUSDT",
    "B2USDT", "BANUSDT", "BANANAS31USDT", "BARDUSDT", "BATUSDT",
    "BEATUSDT", "BERAUSDT", "BGBUSDT", "BIOUSDT", "BIRBUSDT",
    "BLURUSDT", "BRETTUSDT", "BSVUSDT", "CAKEUSDT", "CELOUSDT",
    "CFXUSDT", "CHZUSDT", "COAIUSDT", "COINUSDT", "COMPUSDT",
    "COWUSDT", "CROUSDT", "CRVUSDT", "CVXUSDT", "CYSUSDT",
    "DASHUSDT", "DEEPUSDT", "DEXEUSDT", "DOTUSDT", "DRIFTUSDT",
    "DYDXUSDT", "EGLDUSDT", "EIGENUSDT", "ENAUSDT", "ENSUSDT",
    "ENSOUSDT", "ETCUSDT", "ETHFIUSDT", "FARTCOINUSDT", "FETUSDT",
    "FFUSDT", "FILUSDT", "FLOKIUSDT", "FLUIDUSDT", "FOGOUSDT",
    "FORMUSDT", "GALAUSDT", "GASUSDT", "GLMUSDT", "GPSUSDT",
    "GRASSUSDT", "GRTUSDT", "GUNUSDT", "GWEIUSDT", "HUSDT",
    "HBARUSDT", "HNTUSDT", "HOMEUSDT", "HYPEUSDT", "ICNTUSDT",
    "ICPUSDT", "IDUSDT", "IMXUSDT", "INJUSDT", "IOTAUSDT",
    "IPUSDT", "IRYSUSDT", "JASMYUSDT", "JSTUSDT", "JTOUSDT",
    "JUPUSDT", "KAIAUSDT", "KAITOUSDT", "KASUSDT", "KITEUSDT",
    "KMNOUSDT", "KSMUSDT", "LDOUSDT", "LINEAUSDT", "LINKUSDT",
    "LITUSDT", "LPTUSDT", "LRCUSDT", "LTCUSDT", "LUNAUSDT",
    "LUNCUSDT", "LYNUSDT", "MUSDT", "MANAUSDT", "MASKUSDT",
    "MEUSDT", "MEMEUSDT", "MERLUSDT", "MINAUSDT", "MOCAUSDT",
    "MONUSDT", "MOODENGUSDT", "MORPHOUSDT", "MOVEUSDT", "MYXUSDT",
    "NEARUSDT", "NEOUSDT", "NIGHTUSDT", "NMRUSDT", "NXPCUSDT",
    "ONDOUSDT", "OPUSDT", "ORCAUSDT", "ORDIUSDT", "PARTIUSDT",
    "PENDLEUSDT", "PENGUUSDT", "PEPEUSDT", "PIEVERSEUSDT",
    "PIPPINUSDT", "PLUMEUSDT", "PNUTUSDT", "POLUSDT", "POLYXUSDT",
    "POPCATUSDT", "POWERUSDT", "PUMPUSDT", "PYTHUSDT", "QUSDT",
    "QNTUSDT", "RAVEUSDT", "RAYUSDT", "RENDERUSDT", "RIVERUSDT",
    "ROSEUSDT", "RPLUSDT", "RSRUSDT", "RUNEUSDT", "SUSDT",
    "SAHARAUSDT", "SANDUSDT", "SEIUSDT", "SENTUSDT", "SHIBUSDT",
    "SIGNUSDT", "SIRENUSDT", "SKRUSDT", "SKYUSDT", "SNXUSDT",
    "SOMIUSDT", "SOONUSDT", "SPKUSDT", "SPXUSDT", "SSVUSDT",
    "STABLEUSDT", "STGUSDT", "STRKUSDT", "STXUSDT", "SUIUSDT",
    "SUPERUSDT", "TUSDT", "TAGUSDT", "TAOUSDT", "THETAUSDT",
    "TIAUSDT", "TONUSDT", "TOSHIUSDT", "TRBUSDT", "TRUMPUSDT",
    "TURBOUSDT", "UAIUSDT", "UBUSDT", "UMAUSDT", "UNIUSDT",
    "VANAUSDT", "VETUSDT", "VIRTUALUSDT", "VTHOUSDT", "VVVUSDT",
    "WUSDT", "WALUSDT", "WIFUSDT", "WLDUSDT", "WLFIUSDT",
    "XDCUSDT", "XLMUSDT", "XMRUSDT", "XPLUSDT",
    "XTZUSDT", "XVGUSDT", "ZAMAUSDT", "ZECUSDT", "ZENUSDT",
    "ZETAUSDT", "ZILUSDT", "ZORAUSDT", "ZROUSDT", "ZRXUSDT",
]

# ══════════════════════════════════════════════════════════════
#  🗺️  PETA SEKTOR (TIDAK BERUBAH)
# ══════════════════════════════════════════════════════════════
SECTOR_MAP = {
    "SOLANA_ECOSYSTEM": [
        "ORCAUSDT", "RAYUSDT", "JTOUSDT", "PYTHUSDT",
        "1000BONKUSDT", "WIFUSDT", "JUPUSDT", "DRIFTUSDT",
    ],
    "DEFI": [
        "SNXUSDT", "ENSOUSDT", "SIRENUSDT", "UNIUSDT", "AAVEUSDT",
        "CRVUSDT", "COMPUSDT", "DYDXUSDT", "COWUSDT", "PENDLEUSDT",
        "MORPHOUSDT", "FLUIDUSDT", "SSVUSDT", "CVXUSDT",
    ],
    "AI_CRYPTO": [
        "FETUSDT", "RENDERUSDT", "TAOUSDT", "NEARUSDT",
        "GRASSUSDT", "AKTUSDT", "VANAUSDT", "COAIUSDT", "UAIUSDT",
    ],
    "ZK_PRIVACY": [
        "AZTECUSDT", "MINAUSDT", "STRKUSDT", "ZORAUSDT",
        "ZRXUSDT", "POLYXUSDT",
    ],
    "DESCI_BIOTECH": [
        "BIOUSDT", "ATHUSDT",
    ],
    "GAMING_METAVERSE": [
        "AXSUSDT", "GALAUSDT", "IMXUSDT", "SANDUSDT",
        "MANAUSDT", "APEUSDT", "SUPERUSDT",
    ],
    "LAYER1_INFRA": [
        "APTUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "TONUSDT",
        "AVAXUSDT", "ADAUSDT", "DOTUSDT", "ATOMUSDT", "NEARUSDT",
        "INJUSDT", "EGLDUSDT", "ALGOUSDT", "KASUSDT", "HBARUSDT",
        "BERAUSDT", "KAIAUSDT", "LINEAUSDT", "MOVEUSDT",
    ],
    "LAYER2_SCALING": [
        "ARBUSDT", "OPUSDT", "POLUSDT", "STRKUSDT", "LDOUSDT",
        "CELOUSDT",
    ],
    "MEME": [
        "PEPEUSDT", "SHIBUSDT", "FLOKIUSDT", "BRETTUSDT",
        "FARTCOINUSDT", "MEMEUSDT", "TURBOUSDT", "PNUTUSDT",
        "POPCATUSDT", "MOODENGUSDT", "TOSHIUSDT", "1000BONKUSDT",
        "1000SATSUSDT", "1000RATSUSDT", "1MBABYDOGEUSDT",
    ],
    "ORACLE_DATA": [
        "LINKUSDT", "PYTHUSDT",
    ],
    "LIQUID_STAKING": [
        "LDOUSDT", "RPLUSDT", "SSVUSDT", "ETHFIUSDT", "ANKRUSDT",
    ],
    "LOW_CAP_MISC": [
        "VVVUSDT", "POWERUSDT", "ARCUSDT", "BGBUSDT", "HYPEUSDT",
        "VIRTUALUSDT", "WLFIUSDT", "SPXUSDT", "ONDOUSDT", "ENAUSDT",
        "EIGENUSDT", "STXUSDT", "RUNEUSDT", "ORDIUSDT",
    ],
}

SECTOR_LOOKUP = {}
for _sec, _coins in SECTOR_MAP.items():
    for _c in _coins:
        SECTOR_LOOKUP[_c] = _sec

# ══════════════════════════════════════════════════════════════
#  🌐  KONSTANTA
# ══════════════════════════════════════════════════════════════
BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

_cache = {}


# ══════════════════════════════════════════════════════════════
#  💾  COOLDOWN PERSISTEN
# ══════════════════════════════════════════════════════════════

def load_cooldown_state():
    try:
        path = CONFIG["cooldown_file"]
        if os.path.exists(path):
            with open(path, "r") as f:
                data = json.load(f)
            now = time.time()
            cleaned = {
                k: v for k, v in data.items()
                if now - v < CONFIG["alert_cooldown_sec"]
            }
            return cleaned
    except Exception as e:
        log.warning(f"Gagal load cooldown state: {e}")
    return {}


def save_cooldown_state(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Gagal simpan cooldown state: {e}")


_alert_cooldown = load_cooldown_state()
log.info(f"Cooldown aktif untuk {len(_alert_cooldown)} coin dari run sebelumnya")


def is_in_cooldown(symbol):
    last = _alert_cooldown.get(symbol, 0)
    return (time.time() - last) < CONFIG["alert_cooldown_sec"]


def set_cooldown(symbol):
    _alert_cooldown[symbol] = time.time()
    save_cooldown_state(_alert_cooldown)


# ══════════════════════════════════════════════════════════════
#  🔧  UTILITIES
# ══════════════════════════════════════════════════════════════

def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            log.warning(f"Timeout (attempt {attempt+1}): {url}")
            if attempt == 0:
                time.sleep(CONFIG["sleep_after_api_error"])
        except requests.exceptions.HTTPError as e:
            try:
                body = e.response.text[:300]
            except:
                body = "(tidak bisa baca body)"
            log.warning(f"HTTP {e.response.status_code}: {url} | Body: {body}")
            if e.response.status_code == 429:
                log.warning("Rate limit! Tunggu 10 detik...")
                time.sleep(10)
            break
        except Exception as e:
            log.warning(f"Request error: {e}")
            break
    return None


def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        log.error("BOT_TOKEN / CHAT_ID tidak ditemukan!")
        return False
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        r = requests.post(url, data=data, timeout=15)
        return r.status_code == 200
    except Exception as e:
        log.warning(f"Telegram error: {e}")
        return False


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def utc_hour():
    return datetime.now(timezone.utc).hour


def percentile_rank(value, data):
    if not data:
        return 50
    below = sum(1 for d in data if d < value)
    return (below / len(data)) * 100


# ══════════════════════════════════════════════════════════════
#  🔍  PRE-FILTER (HEMAT API CALLS)
# ══════════════════════════════════════════════════════════════

def passes_quick_filter(ticker):
    if not ticker:
        return False
    try:
        vol_usd = float(ticker.get("quoteVolume", 0))
        chg_24h = abs(float(ticker.get("change24h", 0)) * 100)
        price   = float(ticker.get("lastPr", 0))
    except:
        return False

    if vol_usd < CONFIG["pre_filter_min_vol"]:
        return False
    if chg_24h > CONFIG["pre_filter_max_pump"]:
        return False
    if price <= 0:
        return False
    return True


# ══════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════

def get_all_futures_tickers():
    url  = f"{BITGET_BASE}/api/v2/mix/market/tickers"
    data = safe_get(url, params={"productType": "usdt-futures"})
    if data and data.get("code") == "00000":
        return data.get("data", [])
    log.error("Gagal ambil tickers Bitget")
    return []


def get_candles(symbol, granularity="15m", limit=96):
    gran = GRAN_MAP.get(granularity, granularity)
    cache_key = f"candle_{symbol}_{gran}_{limit}"
    if cache_key in _cache:
        ts, val = _cache[cache_key]
        if time.time() - ts < 90:
            return val

    url    = f"{BITGET_BASE}/api/v2/mix/market/candles"
    params = {
        "symbol":      symbol,
        "granularity": gran,
        "limit":       str(limit),
        "productType": "usdt-futures",
    }

    data = safe_get(url, params=params)
    if not data or data.get("code") != "00000":
        return []

    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({
                "timestamp":  int(c[0]),
                "open":       float(c[1]),
                "high":       float(c[2]),
                "low":        float(c[3]),
                "close":      float(c[4]),
                "volume":     float(c[5]),
                "volume_usd": vol_usd,
            })
        except (IndexError, ValueError):
            continue

    candles.sort(key=lambda x: x["timestamp"])
    _cache[cache_key] = (time.time(), candles)
    return candles


def get_open_interest(symbol):
    url  = f"{BITGET_BASE}/api/v2/mix/market/open-interest"
    data = safe_get(url, params={"symbol": symbol, "productType": "usdt-futures"})
    if data and data.get("code") == "00000":
        try:
            items = data["data"].get("openInterestList", [])
            if items:
                return float(items[0].get("size", 0))
            return float(data["data"].get("size", 0))
        except:
            return 0
    return 0


def get_funding_rate(symbol):
    url  = f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate"
    data = safe_get(url, params={"symbol": symbol, "productType": "usdt-futures"})
    if data and data.get("code") == "00000":
        try:
            return float(data["data"][0].get("fundingRate", 0))
        except:
            return 0
    return 0


def get_orderbook(symbol, limit=15):
    url  = f"{BITGET_BASE}/api/v2/mix/market/merge-depth"
    data = safe_get(url, params={
        "symbol":      symbol,
        "productType": "usdt-futures",
        "limit":       str(limit),
    })
    if data and data.get("code") == "00000":
        raw = data.get("data", {})
        try:
            bids = [{"price": float(b[0]), "size": float(b[1])} for b in raw.get("bids", [])]
            asks = [{"price": float(a[0]), "size": float(a[1])} for a in raw.get("asks", [])]
            return {"bids": bids, "asks": asks}
        except:
            pass
    return {"bids": [], "asks": []}


def get_recent_trades(symbol, limit=200):
    url  = f"{BITGET_BASE}/api/v2/mix/market/fills"
    data = safe_get(url, params={
        "symbol":      symbol,
        "productType": "usdt-futures",
        "limit":       str(limit),
    })
    if data and data.get("code") == "00000":
        trades = []
        for t in data.get("data", []):
            try:
                trades.append({
                    "price": float(t.get("price", 0)),
                    "size":  float(t.get("size", 0)),
                    "side":  t.get("side", "").lower(),
                    "ts":    int(t.get("ts", 0)),
                })
            except:
                continue
        return trades
    return []


def get_cg_trending():
    cache_key = "cg_trending"
    if cache_key in _cache:
        ts, val = _cache[cache_key]
        if time.time() - ts < 600:
            return val
    data = safe_get(f"{COINGECKO_BASE}/search/trending")
    result = []
    if data:
        result = [c["item"]["symbol"].upper() for c in data.get("coins", [])]
    _cache[cache_key] = (time.time(), result)
    return result


# ══════════════════════════════════════════════════════════════
#  📊  INDIKATOR TEKNIKAL
# ══════════════════════════════════════════════════════════════

def calc_bollinger(candles, period=20):
    if len(candles) < period:
        return []
    closes  = [c["close"] for c in candles]
    results = []
    for i in range(period - 1, len(closes)):
        w    = closes[i - period + 1: i + 1]
        mean = sum(w) / period
        std  = math.sqrt(sum((x - mean) ** 2 for x in w) / period)
        bbw  = (4 * std / mean * 100) if mean != 0 else 0
        results.append({
            "middle": mean,
            "upper":  mean + 2 * std,
            "lower":  mean - 2 * std,
            "bbw":    bbw,
        })
    return results


def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return []
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atrs = [sum(trs[:period]) / period]
    for i in range(period, len(trs)):
        atrs.append((atrs[-1] * (period - 1) + trs[i]) / period)
    return atrs


def calc_vwap(candles):
    cum_tv, cum_v, vals = 0, 0, []
    for c in candles:
        tp     = (c["high"] + c["low"] + c["close"]) / 3
        cum_tv += tp * c["volume"]
        cum_v  += c["volume"]
        vals.append(cum_tv / cum_v if cum_v else tp)
    if not vals:
        return None, None
    vwap = vals[-1]
    devs = [abs(candles[i]["close"] - vals[i]) for i in range(len(candles))]
    std  = math.sqrt(sum(d ** 2 for d in devs) / len(devs)) if devs else 0
    return vwap, std


def calc_poc(candles, buckets=40):
    if not candles:
        return None
    pmin = min(c["low"]  for c in candles)
    pmax = max(c["high"] for c in candles)
    if pmax == pmin:
        return candles[-1]["close"]
    bsize   = (pmax - pmin) / buckets
    vol_bkt = defaultdict(float)
    for c in candles:
        lo = int((c["low"]  - pmin) / bsize)
        hi = int((c["high"] - pmin) / bsize)
        nb = max(hi - lo + 1, 1)
        for b in range(lo, hi + 1):
            vol_bkt[b] += c["volume"] / nb
    if not vol_bkt:
        return candles[-1]["close"]
    poc_b = max(vol_bkt, key=vol_bkt.get)
    return pmin + (poc_b + 0.5) * bsize


def find_resistance_level(candles, current_price):
    if not candles or len(candles) < 5:
        return current_price * 1.08, current_price * 1.15

    swing_highs = []
    for i in range(2, len(candles) - 2):
        h = candles[i]["high"]
        if (h > candles[i-1]["high"] and h > candles[i-2]["high"] and
                h > candles[i+1]["high"] and h > candles[i+2]["high"]):
            swing_highs.append(h)

    resistances = sorted([h for h in swing_highs if h > current_price * 1.01])

    if len(resistances) >= 2:
        r1 = resistances[0]
        r2 = resistances[1]
    elif len(resistances) == 1:
        r1 = resistances[0]
        r2 = r1 * 1.06
    else:
        recent_high = max(c["high"] for c in candles[-48:])
        r1 = max(recent_high, current_price * 1.05)
        r2 = r1 * 1.06

    return round(r1, 8), round(r2, 8)


# ══════════════════════════════════════════════════════════════
#  🐋  WHALE DETECTION
# ══════════════════════════════════════════════════════════════

def get_taker_ratio(trades):
    if not trades:
        return 0.5
    bv    = sum(t["size"] for t in trades if t["side"] == "buy")
    sv    = sum(t["size"] for t in trades if t["side"] == "sell")
    total = bv + sv
    return bv / total if total > 0 else 0.5


def detect_large_trade_dominance(trades):
    if not trades:
        return 0, 0
    total     = sum(t["size"] * t["price"] for t in trades)
    large_buy = sum(
        t["size"] * t["price"] for t in trades
        if t["side"] == "buy" and t["size"] * t["price"] > 20_000
    )
    dom = large_buy / total if total > 0 else 0
    return dom, large_buy


def detect_iceberg(trades, current_price, tol=0.15):
    if not trades:
        return False, 0, 0
    at_level = [
        t for t in trades
        if t["side"] == "buy"
        and abs(t["price"] - current_price) / current_price * 100 < tol
    ]
    if len(at_level) < 5:
        return False, 0, 0
    total_usd  = sum(t["size"] * t["price"] for t in at_level)
    count      = len(at_level)
    avg_size   = total_usd / count
    is_iceberg = count >= 12 and avg_size < 8_000 and total_usd > 30_000
    return is_iceberg, total_usd, count


def calc_whale_score(symbol, candles_15m, funding):
    ws       = 0
    evidence = []
    cur      = candles_15m[-1]["close"] if candles_15m else 0

    trades = get_recent_trades(symbol, limit=200)
    if trades:
        tr = get_taker_ratio(trades)
        if tr > 0.70:
            ws += 22
            evidence.append(f"✅ Taker Buy {tr:.0%} — pembeli sangat agresif")
        elif tr > 0.62:
            ws += 11
            evidence.append(f"🔶 Taker Buy {tr:.0%} — bias beli")

        dom, lbuy = detect_large_trade_dominance(trades)
        if dom > 0.38:
            ws += 22
            evidence.append(f"✅ Whale prints {dom:.0%} volume (${lbuy:,.0f})")
        elif dom > 0.23:
            ws += 11
            evidence.append(f"🔶 Smart money {dom:.0%} volume")

        is_ice, tot, cnt = detect_iceberg(trades, cur)
        if is_ice:
            ws += 18
            evidence.append(f"✅ Iceberg order: ${tot:,.0f} ({cnt} tx kecil)")

    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15
            evidence.append("✅ Harga sangat flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7
            evidence.append("🔶 Harga relatif flat 4h")

    if funding < -0.05:
        ws += 18
        evidence.append(f"✅ Funding {funding:.4f}% — short squeeze setup kuat")
    elif funding < -0.02:
        ws += 9
        evidence.append(f"🔶 Funding {funding:.4f}% — bias short ada")
    elif -0.01 < funding < 0.015:
        ws += 5
        evidence.append(f"🔷 Funding netral ({funding:.4f}%)")

    ws = min(ws, 100)
    if ws >= 70:
        cls = "🐋 WHALE ACCUMULATION CONFIRMED"
    elif ws >= 45:
        cls = "🦈 SMART MONEY BUILDING"
    elif ws >= 25:
        cls = "👀 POSSIBLE INSTITUTIONAL INTEREST"
    else:
        cls = "🔇 NO CLEAR WHALE SIGNAL"

    return ws, cls, evidence


# ══════════════════════════════════════════════════════════════
#  🔬  LAYER ANALISIS
# ══════════════════════════════════════════════════════════════

def layer_volatility(candles_15m, candles_1h):
    """Layer 1 — Kompresi volatilitas."""
    score = 0
    sigs  = []

    bb = calc_bollinger(candles_1h, period=20)
    if bb and len(bb) >= 20:
        cur_bbw  = bb[-1]["bbw"]
        hist_bbw = [b["bbw"] for b in bb[-50:]]
        pct      = percentile_rank(cur_bbw, hist_bbw)
        if pct < 10:
            score += 22
            sigs.append(f"BBW Squeeze Ekstrem (persentil {pct:.0f}% terendah)")
        elif pct < 25:
            score += 13
            sigs.append(f"BBW Menyempit (persentil {pct:.0f}%)")
        elif pct < 40:
            score += 5
            sigs.append("BBW Mulai Kompres")

    atr = calc_atr(candles_1h, period=14)
    if len(atr) >= 50:
        atr_s = sum(atr[-7:])  / 7
        atr_l = sum(atr[-50:]) / 50
        ratio = atr_s / atr_l if atr_l else 1
        if ratio < 0.40:
            score += 16
            sigs.append(f"ATR Compressed Extreme ({ratio:.2f}x normal)")
        elif ratio < 0.60:
            score += 8
            sigs.append(f"ATR Compressing ({ratio:.2f}x)")

    quiet = 0
    for c in reversed(candles_1h):
        body = abs(c["close"] - c["open"]) / c["open"] * 100
        if body < 0.8:
            quiet += 1
        else:
            break
    if quiet >= 12:
        score += 20
        sigs.append(f"Coiling {quiet}h — energi terkumpul sangat lama")
    elif quiet >= 6:
        score += 11
        sigs.append(f"Coiling {quiet}h")
    elif quiet >= 3:
        score += 4
        sigs.append(f"Mulai diam {quiet}h")

    return score, sigs, quiet


def layer_volume(candles_15m):
    """
    Layer 2 — Pola volume sebelum pump.
    Diperbarui dengan awakening ratio (6h vs 24h) dan penyesuaian skor.
    """
    score = 0
    sigs  = []

    if len(candles_15m) < 96:   # butuh minimal 24 jam data
        return score, sigs

    vols = [c["volume_usd"] for c in candles_15m]

    # ── Awakening ratio (6 jam terakhir vs 24 jam) ───────────────
    vol_24h = sum(vols[-96:]) / 96
    vol_6h  = sum(vols[-24:]) / 24
    awakening = vol_6h / vol_24h if vol_24h > 0 else 1

    if awakening > 1.2:
        score += 15
        sigs.append(f"Awakening Volume {awakening:.2f}x (6h vs 24h) — mulai bangun")
    elif awakening > 0.9:
        score += 8
        sigs.append(f"Volume meningkat {awakening:.2f}x dalam 6 jam terakhir")
    elif awakening > 0.6:
        score += 4
        sigs.append(f"Volume stabil {awakening:.2f}x")

    # ── Volume creep (volume naik, harga flat) ───────────────────
    if len(vols) >= 28:
        vol_base = sum(vols[:20]) / 20
        vol_recent = sum(vols[-8:]) / 8
        vol_ratio = vol_recent / vol_base if vol_base > 0 else 1

        p_start = candles_15m[-24]["close"]
        p_now   = candles_15m[-1]["close"]
        p_chg   = abs((p_now - p_start) / p_start * 100) if p_start > 0 else 99

        if 1.5 <= vol_ratio <= 5.0 and p_chg < 2.5:
            score += 15
            sigs.append(f"Volume Creep {vol_ratio:.1f}x — harga masih flat (akumulasi)")
        elif 1.2 <= vol_ratio < 1.5 and p_chg < 3.0:
            score += 6
            sigs.append(f"Volume perlahan naik {vol_ratio:.1f}x")

    # ── CVD Divergence ───────────────────────────────────────────
    cvd = 0
    for c in candles_15m[-20:]:
        rng = c["high"] - c["low"]
        if rng == 0:
            continue
        buy_ratio = (c["close"] - c["low"]) / rng
        cvd += c["volume"] * (2 * buy_ratio - 1)

    price_trend = candles_15m[-1]["close"] - candles_15m[-20]["close"]
    if cvd > 0 and abs(price_trend / candles_15m[-1]["close"]) < 0.02:
        score += 12
        sigs.append("CVD Bullish Divergence — tekanan beli tersembunyi")

    return min(score, 40), sigs   # ✅ maksimum layer volume dinaikkan jadi 40


def layer_oi_funding(symbol, candles_1h):
    """
    Layer 3 — Analisis Open Interest dan Funding Rate.
    Ditambah penalti untuk funding sangat negatif.
    """
    score   = 0
    sigs    = []
    funding = get_funding_rate(symbol)

    # Funding rate scoring (dalam desimal, misal 0.0001 = 0.01%)
    if funding > 0.0001:
        score += 8
        sigs.append(f"✅ Funding positif ({funding:.4f}%) — sentimen bullish")
    elif funding < -0.0002:
        score -= 10
        sigs.append(f"⚠️ Funding sangat negatif ({funding:.4f}%) — potensi tekanan jual")
    elif funding < -0.05:
        score += 20
        sigs.append(f"Short Squeeze Setup kuat — funding {funding:.4f}%")
    elif funding < -0.02:
        score += 10
        sigs.append(f"Bias short ada ({funding:.4f}%) — squeeze potensial")
    elif -0.01 < funding < 0.04:
        score += 8
        sigs.append(f"Funding sehat ({funding:.4f}%) — kondisi ideal entry")

    # Funding terlalu tinggi = bahaya dump
    if funding > 0.10:
        score -= 18
        sigs.append(f"⚠️ Funding terlalu tinggi ({funding:.4f}%) — rawan dump")

    return score, sigs, funding


def layer_sector_rotation(symbol, all_tickers_dict):
    """Layer 4 — Deteksi rotasi sektor."""
    sector = SECTOR_LOOKUP.get(symbol, "UNKNOWN")
    if sector == "UNKNOWN":
        return 0, [], sector

    score = 0
    sigs  = []
    peers = SECTOR_MAP.get(sector, [])

    for peer in peers:
        if peer == symbol:
            continue
        pd = all_tickers_dict.get(peer)
        if not pd:
            continue
        try:
            chg = float(pd.get("change24h", 0)) * 100
        except:
            continue
        if chg > 18:
            score += 28
            sigs.append(
                f"🔄 {peer.replace('USDT','')} pump +{chg:.0f}% — "
                f"rotasi ke {symbol.replace('USDT','')} mungkin segera"
            )
            break
        elif chg > 10:
            score += 15
            sigs.append(
                f"Sektor {sector} aktif — "
                f"{peer.replace('USDT','')} +{chg:.0f}%"
            )
            break

    return score, sigs, sector


def layer_relative_strength(symbol, sector, all_tickers_dict):
    """Layer 5 — Kekuatan relatif vs rata-rata sektor."""
    peers   = SECTOR_MAP.get(sector, [])
    changes = []
    for p in peers:
        pd = all_tickers_dict.get(p)
        if not pd:
            continue
        try:
            changes.append(float(pd.get("change24h", 0)) * 100)
        except:
            continue

    if len(changes) < 2:
        return 0, ""

    avg = sum(changes) / len(changes)
    try:
        coin_chg = float(all_tickers_dict[symbol].get("change24h", 0)) * 100
    except:
        return 0, ""

    rs = coin_chg - avg
    if rs > 3 and coin_chg < 8:
        return 22, f"RS Leader — lebih kuat {rs:.1f}% dari rata-rata sektor, belum pump"
    elif rs > 1 and coin_chg < 5:
        return 11, f"Relatif kuat vs sektor (RS: +{rs:.1f}%)"
    return 0, ""


def layer_social(symbol):
    """Layer 6 — Social signal dari CoinGecko trending."""
    coin_name = symbol.replace("USDT", "").replace("1000", "").replace("1M", "").upper()
    trending  = get_cg_trending()
    if coin_name in trending:
        return 22, f"🔥 {coin_name} TRENDING di CoinGecko — belum pump"
    return 0, ""


def get_time_multiplier():
    h = utc_hour()
    if h in [5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.25, f"High-prob window ({h}:00 UTC)"
    if h in [1, 2, 3, 4]:
        return 0.75, f"Low-prob window ({h}:00 UTC)"
    return 1.0, ""


# ══════════════════════════════════════════════════════════════
#  🎯  ENTRY ZONE CALCULATOR
# ══════════════════════════════════════════════════════════════

def calc_entry_zones(candles, funding):
    if not candles:
        return None

    cur      = candles[-1]["close"]
    vwap, vs = calc_vwap(candles)

    z1_raw = (vwap - 1.5 * vs) if (vwap and vs) else cur * 0.97
    z1     = z1_raw if z1_raw < cur else cur * 0.97

    z2_raw = calc_poc(candles) or cur * 0.98
    z2     = z2_raw if z2_raw < cur else cur * 0.98

    t1, t2 = find_resistance_level(candles, cur)
    if t1 <= cur:
        t1 = cur * 1.07
    if t2 <= t1:
        t2 = t1 * 1.05

    support = max(z1, z2)
    entry   = support * 1.002
    if entry >= cur:
        entry = cur * 0.999

    sl      = support * 0.967
    risk    = cur - sl
    reward  = t1 - cur
    rr      = round(reward / risk, 1) if risk > 0 else 0
    liq_pct = round((t1 - cur) / cur * 100, 1) if cur > 0 else 0

    return {
        "cur":     cur,
        "vwap":    round(vwap, 8) if vwap else 0,
        "z1":      round(z1, 8),
        "z2":      round(z2, 8),
        "entry":   round(entry, 8),
        "sl":      round(sl, 8),
        "t1":      round(t1, 8),
        "t2":      round(t2, 8),
        "rr":      rr,
        "liq_pct": liq_pct,
    }


# ══════════════════════════════════════════════════════════════
#  🎯  MASTER SCORE
# ══════════════════════════════════════════════════════════════

def master_score(symbol, ticker_data, all_tickers_dict):
    total = 0
    sigs  = []

    c15 = get_candles(symbol, "15m", CONFIG["candle_limit_15m"])
    c1h = get_candles(symbol, "1h",  CONFIG["candle_limit_1h"])

    if len(c15) < 20 or len(c1h) < 10:
        return None

    funding = get_funding_rate(symbol)

    # Layer 1: Volatility (max 35 poin)
    vs, vsigs, quiet_h = layer_volatility(c15, c1h)
    total += min(vs, 35)
    sigs  += vsigs

    # Layer 2: Volume (max 40 poin)
    vls, vlsigs = layer_volume(c15)
    total += min(vls, 40)
    sigs  += vlsigs

    # Layer 3: OI & Funding (max 22 poin)
    ois, oisigs, funding = layer_oi_funding(symbol, c1h)
    total += min(ois, 22)
    sigs  += oisigs

    # Layer 4: Sector Rotation (max 28 poin)
    srs, srsigs, sector = layer_sector_rotation(symbol, all_tickers_dict)
    total += min(srs, 28)
    sigs  += srsigs

    # Layer 5: Relative Strength (max 22 poin)
    rss, rssig = layer_relative_strength(symbol, sector, all_tickers_dict)
    total += min(rss, 22)
    if rssig:
        sigs.append(rssig)

    # Layer 6: Social (max 22 poin)
    soc, socsig = layer_social(symbol)
    total += min(soc, 22)
    if socsig:
        sigs.append(socsig)

    # Layer 7: Whale Composite Score (bonus poin)
    ws, wcls, wev = calc_whale_score(symbol, c15, funding)
    if ws >= 55:
        total += 16
        sigs.append("Whale confirmation bonus")
    elif ws >= 30:
        total += 7

    # Time multiplier
    tmult, tsig = get_time_multiplier()
    total = int(total * tmult)
    if tsig:
        sigs.append(f"⏰ {tsig}")

    entry = calc_entry_zones(c1h, funding)

    try:
        price_now = float(ticker_data.get("lastPr", 0))
        chg_24h   = float(ticker_data.get("change24h", 0)) * 100
        vol_24h   = float(ticker_data.get("quoteVolume", 0))
    except:
        price_now = c1h[-1]["close"] if c1h else 0
        chg_24h   = 0
        vol_24h   = 0

    return {
        "symbol":  symbol,
        "score":   min(total, 100),
        "signals": sigs,
        "ws":      ws,
        "wcls":    wcls,
        "wev":     wev,
        "entry":   entry,
        "sector":  sector,
        "funding": funding,
        "quiet_h": quiet_h,
        "price":   price_now,
        "chg_24h": chg_24h,
        "vol_24h": vol_24h,
    }


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM ALERT BUILDER
# ══════════════════════════════════════════════════════════════

def build_alert(r, rank=None):
    sc     = r["score"]
    filled = int(sc / 5)
    bar    = "█" * filled + "░" * (20 - filled)
    e      = r["entry"]

    rank_str = f"#{rank} " if rank else ""

    msg = (
        f"🚨 <b>PRE-PUMP INTELLIGENCE {rank_str}</b>\n\n"
        f"<b>Symbol :</b> {r['symbol']}\n"
        f"<b>Score  :</b> {sc}/100  {bar}\n"
        f"<b>Sektor :</b> {r['sector']}\n"
        f"<b>Harga  :</b> ${r['price']:.6f}  ({r['chg_24h']:+.1f}% 24h)\n"
        f"<b>Vol 24h:</b> ${r['vol_24h'] / 1e6:.1f}M\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐋 <b>WHALE SCORE: {r['ws']}/100</b>\n"
        f"<i>{r['wcls']}</i>\n"
    )
    for ev in r["wev"]:
        msg += f"  {ev}\n"

    if e:
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>SMART ENTRY ZONES</b>\n"
            f"  🟢 VWAP Support  : ${e['z1']}\n"
            f"  🟢 Point of Ctrl : ${e['z2']}\n"
            f"  📌 Entry Ideal   : ${e['entry']}\n"
            f"  🛑 Stop Loss     : ${e['sl']}\n\n"
            f"🎯 <b>TARGET</b>\n"
            f"  Target 1 : ${e['t1']}  (+{e['liq_pct']:.1f}%)\n"
            f"  Target 2 : ${e['t2']}\n"
            f"  R/R      : 1 : {e['rr']}\n"
        )

    msg += f"\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL</b>\n"
    for s in r["signals"][:8]:
        msg += f"  • {s}\n"

    msg += (
        f"\n⏰ Window  : 1–6 jam ke depan\n"
        f"📡 Funding : {r['funding']:.4f}%\n"
        f"🕐 {utc_now()}\n\n"
        f"<i>Bukan financial advice. Selalu manage risk.</i>"
    )
    return msg


def build_summary_alert(results):
    msg = f"📋 <b>RINGKASAN SCAN — {utc_now()}</b>\n"
    msg += f"{'━'*30}\n"
    for i, r in enumerate(results, 1):
        bar   = "█" * int(r['score'] / 10) + "░" * (10 - int(r['score'] / 10))
        msg  += (
            f"{i}. <b>{r['symbol']}</b> "
            f"[{r['score']}/100 {bar}]\n"
            f"   🐋{r['ws']} | {r['sector'][:12]} | "
            f"{r['chg_24h']:+.1f}%\n"
        )
    msg += f"\n<i>Detail dikirim per coin di bawah ini ↓</i>"
    return msg


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════

def run_scan():
    log.info(f"=== SCAN DIMULAI — {utc_now()} ===")
    log.info(f"Total coin target: {len(TARGET_COINS)}")

    all_tickers  = get_all_futures_tickers()
    tickers_dict = {t.get("symbol", ""): t for t in all_tickers}

    if not tickers_dict:
        log.error("Gagal ambil data ticker")
        send_telegram("⚠️ <b>Scanner Error</b>: Gagal ambil data Bitget API")
        return

    pre_candidates = []
    skipped_prefilter = 0
    for symbol in TARGET_COINS:
        ticker = tickers_dict.get(symbol)
        if not ticker:
            continue
        if is_in_cooldown(symbol):
            log.info(f"  {symbol} — cooldown aktif, skip")
            continue
        if passes_quick_filter(ticker):
            pre_candidates.append(symbol)
        else:
            skipped_prefilter += 1

    log.info(f"Pre-filter: {len(pre_candidates)} kandidat, {skipped_prefilter} diskip")

    results = []

    for i, symbol in enumerate(pre_candidates):
        ticker = tickers_dict.get(symbol)

        try:
            vol_usd = float(ticker.get("quoteVolume", 0))
            chg_24h = abs(float(ticker.get("change24h", 0)) * 100)
        except:
            continue

        # Final filter (lebih ketat dari pre-filter)
        if vol_usd < CONFIG["min_volume_usd_24h"]:
            log.info(f"[{i+1}/{len(pre_candidates)}] {symbol} — volume ${vol_usd:,.0f} kurang, skip")
            continue

        if chg_24h > CONFIG["max_pump_pct_24h"]:
            log.info(f"[{i+1}/{len(pre_candidates)}] {symbol} — sudah pump {chg_24h:.1f}%, skip")
            continue

        log.info(f"[{i+1}/{len(pre_candidates)}] Analisis {symbol}...")

        try:
            result = master_score(symbol, ticker, tickers_dict)
            if result:
                log.info(
                    f"  Score={result['score']} | "
                    f"Whale={result['ws']} | "
                    f"Sector={result['sector']}"
                )
                if result["score"] >= CONFIG["min_score_alert"]:
                    results.append(result)
        except Exception as ex:
            log.warning(f"  Error {symbol}: {ex}")

        time.sleep(CONFIG["sleep_between_coins"])

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"\nKandidat kuat: {len(results)} coin")

    if not results:
        log.info("Tidak ada sinyal kuat dalam siklus ini")
        return

    qualified = [
        r for r in results
        if r["ws"] >= CONFIG["min_whale_score"] or r["score"] >= 65
    ]

    if not qualified:
        log.info("Tidak ada yang memenuhi syarat whale score + total score")
        return

    top_results = qualified[:CONFIG["max_alerts_per_run"]]
    if len(top_results) > 3:
        summary = build_summary_alert(top_results)
        send_telegram(summary)
        time.sleep(2)

    sent = 0
    for rank, r in enumerate(top_results, 1):
        msg = build_alert(r, rank=rank)
        ok  = send_telegram(msg)
        if ok:
            set_cooldown(r["symbol"])
            sent += 1
            log.info(f"✅ Alert #{rank}: {r['symbol']} Score={r['score']} Whale={r['ws']}")
        else:
            log.warning(f"Gagal kirim alert {r['symbol']}")
        time.sleep(2)

    log.info(f"=== SCAN SELESAI — {sent} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("╔══════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v3.1 — START       ║")
    log.info("╚══════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        log.error("Pastikan GitHub Secrets sudah diset:")
        log.error("  BOT_TOKEN = token dari @BotFather")
        log.error("  CHAT_ID   = ID dari @userinfobot")
        exit(1)

    run_scan()
