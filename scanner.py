"""
╔══════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v8.1 — SHORT SQUEEZE AWARE                        ║
║                                                                      ║
║  Berdasarkan kasus POWER yang pump >30% setelah overbought          ║
║  Menambahkan deteksi short squeeze:                                  ║
║  ✅ Gate overbought menjadi penalti jika funding tinggi & OI naik   ║
║  ✅ Bonus squeeze untuk funding >0.05% + OI naik >2%                ║
║  ✅ Semua fitur v8.0 dipertahankan                                   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
from datetime import datetime, timezone
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # Alert thresholds
    "min_score_alert":          50,
    "min_whale_score":          15,
    "max_alerts_per_run":        8,

    # Volume 24h TOTAL (USD)
    "min_vol_24h":           5_000,
    "max_vol_24h":        50_000_000,
    "pre_filter_vol":        2_000,

    # Gate price change (dilonggarkan, dengan pengecualian squeeze)
    "gate_chg_24h_max":         30.0,
    "gate_chg_7d_max":          35.0,
    "gate_chg_7d_min":         -35.0,
    "gate_funding_extreme":    -0.002,

    # Candle limits
    "candle_1h":               168,
    "candle_15m":               96,
    "candle_4h":                42,

    # Entry/exit
    "min_target_pct":            8.0,
    "max_sl_pct":               12.0,
    "atr_sl_mult":               1.5,
    "atr_t1_mult":               2.5,

    # Operational
    "alert_cooldown_sec":       3600,
    "sleep_coins":               0.9,
    "sleep_error":               3.0,
    "max_deep_scan":              80,
    "cooldown_file":    "/tmp/v7_cooldown.json",
    "oi_snapshot_file": "/tmp/oi_snapshots.json",

    # Bonus thresholds (stealth & explosive)
    "stealth_max_vol":       50_000,
    "stealth_min_coiling":        20,
    "stealth_max_range":         3.0,
    "explosive_min_vol":     200_000,
    "explosive_min_range":       10.0,
    "explosive_min_bbw":          5.0,

    # Short squeeze thresholds
    "squeeze_funding_min":     0.0005,    # 0.05%
    "squeeze_oi_change_min":    2.0,       # 2% naik dalam 1 jam
}

GRAN_MAP = {
    "15m": "15m",
    "1h":  "1H",
    "4h":  "4H",
    "1d":  "1D",
}


# ==================== SECTOR MAP ====================
SECTOR_MAP = {
    "DEFI": [
        "SNXUSDT","ENSOUSDT","SIRENUSDT","CRVUSDT","CVXUSDT","COMPUSDT",
        "AAVEUSDT","UNIUSDT","DYDXUSDT","COWUSDT","PENDLEUSDT","MORPHOUSDT",
        "FLUIDUSDT","SSVUSDT","LRCUSDT","RSRUSDT","NMRUSDT","UMAUSDT","BALUSDT",
    ],
    "ZK_PRIVACY": [
        "AZTECUSDT","MINAUSDT","STRKUSDT","ZORAUSDT","ZRXUSDT","POLYXUSDT",
    ],
    "DESCI": ["BIOUSDT","ATHUSDT"],
    "AI_CRYPTO": [
        "FETUSDT","RENDERUSDT","TAOUSDT","GRASSUSDT","AKTUSDT","VANAUSDT",
        "COAIUSDT","UAIUSDT","GRTUSDT","OCEANUSDT","AGIXUSDT",
    ],
    "SOLANA_ECO": [
        "ORCAUSDT","RAYUSDT","JTOUSDT","DRIFTUSDT","WIFUSDT","JUPUSDT",
        "1000BONKUSDT","PYTHUSDT","MEWUSDT",
    ],
    "LAYER1": [
        "APTUSDT","SUIUSDT","SEIUSDT","INJUSDT","KASUSDT","BERAUSDT",
        "MOVEUSDT","KAIAUSDT","TIAUSDT","EGLDUSDT","NEARUSDT","TONUSDT",
        "ALGOUSDT","HBARUSDT","STEEMUSDT","XTZUSDT","ZILUSDT","VETUSDT",
    ],
    "LAYER2": [
        "ARBUSDT","OPUSDT","CELOUSDT","STRKUSDT","LDOUSDT","POLUSDT","LINEAUSDT",
    ],
    "GAMING": [
        "AXSUSDT","GALAUSDT","IMXUSDT","SANDUSDT","APEUSDT","SUPERUSDT",
        "CHZUSDT","ENJUSDT","GLMUSDT",
    ],
    "LOW_CAP": [
        "VVVUSDT","POWERUSDT","ARCUSDT","AGLDUSDT","VIRTUALUSDT","SPXUSDT",
        "ONDOUSDT","ENAUSDT","EIGENUSDT","STXUSDT","RUNEUSDT","ORDIUSDT",
        "SKRUSDT","BRETTUSDT","AVNTUSDT","AEROUSDT",
    ],
    "MEME": [
        "PEPEUSDT","SHIBUSDT","FLOKIUSDT","BRETTUSDT","FARTCOINUSDT",
        "MEMEUSDT","TURBOUSDT","PNUTUSDT","POPCATUSDT","MOODENGUSDT",
        "1000BONKUSDT","TRUMPUSDT","WIFUSDT","TOSHIUSDT",
    ],
}

SECTOR_LOOKUP = {coin: sec for sec, coins in SECTOR_MAP.items() for coin in coins}

BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_cache         = {}


# ==================== COOLDOWN & OI ====================
def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except:
        pass
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except:
        pass

def load_oi_snapshots():
    try:
        if os.path.exists(CONFIG["oi_snapshot_file"]):
            with open(CONFIG["oi_snapshot_file"]) as f:
                return json.load(f)
    except:
        pass
    return {}

def save_oi_snapshot(symbol, oi_value):
    snapshots = load_oi_snapshots()
    now = time.time()
    if symbol not in snapshots:
        snapshots[symbol] = []
    snapshots[symbol].append({"ts": now, "oi": oi_value})
    snapshots[symbol] = sorted(snapshots[symbol], key=lambda x: x["ts"])[-100:]
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(snapshots, f)
    except:
        pass

def get_oi_changes(symbol, current_oi):
    snapshots = load_oi_snapshots()
    if symbol not in snapshots:
        return 0, 0
    data = snapshots[symbol]
    now = time.time()
    one_hour_ago = now - 3600
    candidates_1h = [d for d in data if abs(d["ts"] - one_hour_ago) < 600]
    if not candidates_1h:
        change_1h = 0
    else:
        closest = min(candidates_1h, key=lambda d: abs(d["ts"] - one_hour_ago))
        old = closest["oi"]
        change_1h = (current_oi - old) / old * 100 if old else 0
    one_day_ago = now - 86400
    candidates_24h = [d for d in data if abs(d["ts"] - one_day_ago) < 3600]
    if not candidates_24h:
        change_24h = 0
    else:
        closest = min(candidates_24h, key=lambda d: abs(d["ts"] - one_day_ago))
        old = closest["oi"]
        change_24h = (current_oi - old) / old * 100 if old else 0
    return change_1h, change_24h

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)


# ==================== HTTP UTILITIES ====================
def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("Rate limit Bitget — tunggu 15s")
                time.sleep(15)
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=15
        )
        return r.status_code == 200
    except:
        return False

def utc_now():  return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
def utc_hour(): return datetime.now(timezone.utc).hour


# ==================== DATA FETCHERS ====================
def get_all_tickers():
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        params={"productType": "usdt-futures"}
    )
    if data and data.get("code") == "00000":
        return {t["symbol"]: t for t in data.get("data", [])}
    return {}

def get_candles(symbol, gran="1h", limit=168):
    g   = GRAN_MAP.get(gran, "1H")
    key = f"c_{symbol}_{g}_{limit}"
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < 90:
            return val
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        params={"symbol": symbol, "granularity": g,
                "limit": str(limit), "productType": "usdt-futures"}
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({
                "ts":         int(c[0]),
                "open":     float(c[1]),
                "high":     float(c[2]),
                "low":      float(c[3]),
                "close":    float(c[4]),
                "volume":   float(c[5]),
                "volume_usd": vol_usd,
            })
        except:
            continue
    candles.sort(key=lambda x: x["ts"])
    _cache[key] = (time.time(), candles)
    return candles

def get_funding(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "usdt-futures"}
    )
    if data and data.get("code") == "00000":
        try:
            return float(data["data"][0].get("fundingRate", 0))
        except:
            pass
    return 0

def get_open_interest(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"}
    )
    if data and data.get("code") == "00000":
        try:
            oi_data = data.get("data", {})
            if "openInterestList" in oi_data and oi_data["openInterestList"]:
                return float(oi_data["openInterestList"][0].get("size", 0))
            else:
                return float(oi_data.get("size", 0))
        except:
            pass
    return 0

def get_long_short_ratio(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/account-long-short-ratio",
        params={"symbol": symbol, "period": "1H",
                "limit": "4", "productType": "usdt-futures"}
    )
    if data and data.get("code") == "00000" and data.get("data"):
        try:
            return float(data["data"][0].get("longShortRatio", 1.0))
        except:
            pass
    return None

def get_trades(symbol, limit=500):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/fills",
        params={"symbol": symbol, "productType": "usdt-futures", "limit": str(limit)}
    )
    if data and data.get("code") == "00000":
        trades = []
        for t in data.get("data", []):
            try:
                trades.append({
                    "price": float(t["price"]),
                    "size":  float(t["size"]),
                    "side":  t.get("side", "").lower(),
                })
            except:
                pass
        return trades
    return []

def get_orderbook(symbol, levels=50):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/merge-depth",
        params={"symbol": symbol, "productType": "usdt-futures",
                "precision": "scale0", "limit": str(levels)}
    )
    if data and data.get("code") == "00000":
        try:
            book    = data["data"]
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            bid_vol = sum(float(b[1]) for b in bids)
            ask_vol = sum(float(a[1]) for a in asks)
            total = bid_vol + ask_vol
            ratio = bid_vol / total if total > 0 else 0.5
            return ratio, bid_vol, ask_vol
        except:
            pass
    return 0.5, 0, 0

def get_cg_trending():
    key = "cg_trend"
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < 600:
            return val
    data   = safe_get(f"{COINGECKO_BASE}/search/trending")
    result = [c["item"]["symbol"].upper() for c in (data or {}).get("coins", [])]
    _cache[key] = (time.time(), result)
    return result


# ==================== MATH HELPERS ====================
def bbw_percentile(candles, period=20):
    closes = [c["close"] for c in candles]
    if len(closes) < period + 10:
        return 0, 50
    bbws = []
    for i in range(period - 1, len(closes)):
        w    = closes[i - period + 1: i + 1]
        mean = sum(w) / period
        std  = math.sqrt(sum((x - mean) ** 2 for x in w) / period)
        bbws.append((4 * std / mean * 100) if mean else 0)
    if not bbws:
        return 0, 50
    cur = bbws[-1]
    pct = sum(1 for b in bbws[:-1] if b < cur) / max(len(bbws) - 1, 1) * 100
    return cur, pct

def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr

def calc_vwap_zone(candles):
    cum_tv, cum_v = 0, 0
    vals = []
    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_tv += tp * c["volume"]
        cum_v  += c["volume"]
        vals.append(cum_tv / cum_v if cum_v else tp)
    if not vals:
        return None, None
    vwap = vals[-1]
    devs = [abs(candles[i]["close"] - vals[i]) for i in range(len(candles))]
    std  = math.sqrt(sum(d ** 2 for d in devs) / len(devs)) if devs else 0
    z1   = vwap - 1.5 * std
    cur  = candles[-1]["close"]
    if z1 >= cur:
        z1 = cur * 0.97
    return vwap, z1

def calc_poc(candles):
    if not candles:
        return None
    pmin = min(c["low"]  for c in candles)
    pmax = max(c["high"] for c in candles)
    if pmax == pmin:
        return candles[-1]["close"]
    bsize   = (pmax - pmin) / 40
    vol_bkt = defaultdict(float)
    for c in candles:
        lo = int((c["low"]  - pmin) / bsize)
        hi = int((c["high"] - pmin) / bsize)
        nb = max(hi - lo + 1, 1)
        for b in range(lo, hi + 1):
            vol_bkt[b] += c["volume"] / nb
    poc_b = max(vol_bkt, key=vol_bkt.get) if vol_bkt else 20
    return pmin + (poc_b + 0.5) * bsize

def find_swing_targets(candles, cur):
    min_t  = cur * (1 + CONFIG["min_target_pct"] / 100)
    swings = []
    for i in range(2, len(candles) - 2):
        h = candles[i]["high"]
        if (h >= min_t and
                h > candles[i - 1]["high"] and h > candles[i - 2]["high"] and
                h > candles[i + 1]["high"] and h > candles[i + 2]["high"]):
            swings.append(h)
    swings.sort()
    t1 = swings[0]           if swings          else cur * 1.10
    t2 = swings[1]           if len(swings) >= 2 else t1 * 1.08
    return round(t1, 8), round(t2, 8)


# ==================== RVOL, CVD, etc. ====================
def calc_rvol(candles_1h):
    if len(candles_1h) < 25:
        return 1.0
    last_complete = candles_1h[-2]
    last_vol      = last_complete["volume_usd"]
    target_hour   = (last_complete["ts"] // 3_600_000) % 24
    same_hour_vols = [
        c["volume_usd"] for c in candles_1h[:-2]
        if (c["ts"] // 3_600_000) % 24 == target_hour
    ]
    if not same_hour_vols:
        return 1.0
    avg = sum(same_hour_vols) / len(same_hour_vols)
    return last_vol / avg if avg > 0 else 1.0

def calc_cvd_signal(candles_1h):
    if len(candles_1h) < 12:
        return 0, ""
    window = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    cvd      = 0
    cvd_vals = []
    for c in window:
        rng = c["high"] - c["low"]
        if rng > 0:
            buy_ratio = (c["close"] - c["low"]) / rng
        else:
            buy_ratio = 0.5
        delta = (buy_ratio * 2 - 1) * c["volume_usd"]
        cvd  += delta
        cvd_vals.append(cvd)
    if len(cvd_vals) < 8:
        return 0, ""
    mid       = len(cvd_vals) // 2
    cvd_early = sum(cvd_vals[:mid]) / mid
    cvd_late  = sum(cvd_vals[mid:]) / (len(cvd_vals) - mid)
    cvd_rising = cvd_late > cvd_early
    p_start   = window[0]["close"]
    p_end     = window[-1]["close"]
    price_chg = (p_end - p_start) / p_start * 100 if p_start > 0 else 0
    if cvd_rising and price_chg < 1.5:
        if price_chg < -1.5:
            return 15, f"🔍 CVD Divergence KUAT: harga {price_chg:+.1f}% tapi buy pressure dominan"
        elif price_chg < 0:
            return 12, f"🔍 CVD naik saat harga turun — akumulasi tersembunyi"
        else:
            return 8, f"🔍 CVD naik, harga flat — hidden accumulation"
    elif cvd_rising and 1.5 <= price_chg <= 5.0:
        return 5, f"CVD bullish, harga naik sehat ({price_chg:+.1f}%)"
    return 0, ""

def detect_higher_lows(candles):
    if len(candles) < 6:
        return 0, ""
    lows = [c["low"] for c in candles]
    local_lows = []
    for i in range(1, len(lows) - 1):
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
            local_lows.append(lows[i])
    if len(local_lows) < 2:
        return 0, ""
    ascending = sum(
        1 for i in range(1, len(local_lows))
        if local_lows[i] > local_lows[i - 1] * 1.001
    )
    if ascending >= 2:
        return 8, f"📐 {ascending + 1}x Higher Lows — ascending triangle terbentuk"
    elif ascending >= 1:
        return 4, f"📐 Higher Low terdeteksi — struktur bullish mulai"
    return 0, ""

def volume_building_at_resistance(candles_1h, ticker):
    if len(candles_1h) < 6:
        return 0, ""
    last3 = candles_1h[-4:-1]
    if len(last3) < 3:
        return 0, ""
    vols   = [c["volume_usd"] for c in last3]
    prices = [c["close"]      for c in last3]
    vol_building   = vols[-1] > vols[0] * 1.25
    price_holding  = prices[-1] >= prices[0] * 0.995
    all_green      = all(c["close"] >= c["open"] for c in last3)
    try:
        high24h          = float(ticker.get("high24h", 0))
        cur              = candles_1h[-1]["close"]
        near_resistance  = high24h > 0 and (high24h - cur) / cur * 100 < 6
    except:
        near_resistance = False
    if vol_building and price_holding and near_resistance:
        return 8, "🧱 Volume building di resistance — wall buying aktif!"
    elif vol_building and price_holding and all_green:
        return 5, "Volume membangun, harga stabil — akumulasi berlanjut"
    elif vol_building and price_holding:
        return 3, "Volume naik, harga tahan"
    return 0, ""

def candle_quality_score(candles_1h):
    if len(candles_1h) < 3:
        return 0, ""
    last = candles_1h[-2]
    prev = candles_1h[-3]
    body  = abs(last["close"] - last["open"])
    rng   = last["high"] - last["low"]
    if rng <= 0:
        return 0, ""
    body_ratio  = body / rng
    lower_wick  = (min(last["close"], last["open"]) - last["low"]) / rng
    upper_wick  = (last["high"] - max(last["close"], last["open"])) / rng
    is_bullish  = last["close"] > last["open"]
    is_breakout = last["close"] > prev["high"]
    score = 0
    sig   = ""
    if is_bullish and body_ratio > 0.70 and upper_wick < 0.15:
        score = 7
        sig   = f"💚 Marubozu bullish (body {body_ratio:.0%}) — buyer total kontrol"
    elif is_bullish and body_ratio > 0.55 and lower_wick < 0.25:
        score = 5
        sig   = f"💚 Candle bullish kuat (body {body_ratio:.0%})"
    elif is_bullish and is_breakout:
        score = 4
        sig   = f"📈 Candle breakout di atas high sebelumnya"
    elif not is_bullish and body_ratio > 0.6:
        score = -3
    return score, sig

def calc_breakout_proximity(candles_1h, ticker):
    score = 0
    sigs  = []
    cur = candles_1h[-1]["close"] if candles_1h else 0
    if cur <= 0:
        return 0, []
    try:
        high_24h = float(ticker.get("high24h", cur))
        low_24h  = float(ticker.get("low24h",  cur))
    except:
        highs_24 = [c["high"] for c in candles_1h[-24:]] if len(candles_1h) >= 24 else [cur]
        lows_24  = [c["low"]  for c in candles_1h[-24:]] if len(candles_1h) >= 24 else [cur]
        high_24h = max(highs_24)
        low_24h  = min(lows_24)
    high_7d = max(c["high"] for c in candles_1h) if candles_1h else cur
    dist_24h = (high_24h - cur) / cur * 100 if cur > 0 else 99
    if dist_24h <= 0:
        score += 18
        sigs.append(f"⚡ Harga di HIGH 24h — breakout AKTIF!")
    elif dist_24h <= 0.8:
        score += 25
        sigs.append(f"🎯 {dist_24h:.1f}% dari high 24h — IMMINENT BREAKOUT!")
    elif dist_24h <= 2.0:
        score += 20
        sigs.append(f"📈 {dist_24h:.1f}% dari resistance — mendekati breakout")
    elif dist_24h <= 4.0:
        score += 13
        sigs.append(f"Dalam range breakout 24h ({dist_24h:.1f}%)")
    elif dist_24h <= 7.0:
        score += 6
    elif dist_24h > 20:
        score -= 5
    hl_sc, hl_sig = detect_higher_lows(candles_1h[-16:] if len(candles_1h) >= 16 else candles_1h)
    score += hl_sc
    if hl_sig:
        sigs.append(hl_sig)
    vb_sc, vb_sig = volume_building_at_resistance(candles_1h, ticker)
    score += vb_sc
    if vb_sig:
        sigs.append(vb_sig)
    cq_sc, cq_sig = candle_quality_score(candles_1h)
    score += cq_sc
    if cq_sig:
        sigs.append(cq_sig)
    return min(max(score, 0), 28), sigs

def calc_4h_confluence(candles_4h):
    if len(candles_4h) < 6:
        return 0, ""
    closes = [c["close"] for c in candles_4h]
    p_7d  = closes[0]
    p_now = closes[-1]
    trend_7d = (p_now - p_7d) / p_7d * 100 if p_7d > 0 else 0
    p_48h    = closes[-12] if len(closes) >= 12 else closes[0]
    trend_48h = (p_now - p_48h) / p_48h * 100 if p_48h > 0 else 0
    if trend_48h > 2 and -10 <= trend_7d <= 15:
        return 8, f"📊 4H: reversal bullish 48h +{trend_48h:.1f}%, trend 7d sehat"
    elif trend_48h > 0 and trend_7d > -15:
        return 4, f"📊 4H upward bias ({trend_48h:+.1f}% 48h)"
    elif trend_48h < -8:
        return -6, f"⚠️ 4H masih downtrend ({trend_48h:+.1f}% 48h)"
    return 0, ""


# ==================== LAYERS (existing) ====================
def layer_volume_intelligence(candles_1h):
    score = 0
    sigs  = []
    rvol = calc_rvol(candles_1h)
    if rvol >= 4.0:
        score += 20
        sigs.append(f"🔥🔥 RVOL {rvol:.1f}x — volume MASIF vs historis jam ini!")
    elif rvol >= 2.8:
        score += 17
        sigs.append(f"🔥 RVOL {rvol:.1f}x — volume spike signifikan")
    elif rvol >= 2.0:
        score += 13
        sigs.append(f"RVOL {rvol:.1f}x — volume mulai bangun")
    elif rvol >= 1.4:
        score += 8
        sigs.append(f"RVOL {rvol:.1f}x — di atas historis normal")
    elif rvol >= 1.1:
        score += 4
    elif rvol < 0.4:
        score -= 5
    cvd_s, cvd_sig = calc_cvd_signal(candles_1h)
    score += cvd_s
    if cvd_sig:
        sigs.append(cvd_sig)
    return min(score, 35), sigs, rvol

def layer_structure(candles_1h):
    score = 0
    sigs  = []
    bbw_val, bbw_pct = bbw_percentile(candles_1h)
    if bbw_pct < 10:
        score += 12
        sigs.append(f"BBW Squeeze Ekstrem ({bbw_pct:.0f}%ile) — siap meledak")
    elif bbw_pct < 25:
        score += 9
        sigs.append(f"BBW Squeeze Kuat ({bbw_pct:.0f}%ile)")
    elif bbw_pct < 45:
        score += 5
        sigs.append(f"BBW Menyempit ({bbw_pct:.0f}%ile)")
    elif bbw_pct > 85:
        score -= 5
        sigs.append(f"⚠️ BBW Melebar ({bbw_pct:.0f}%ile) — volatilitas sudah terjadi")
    coiling = 0
    for c in reversed(candles_1h[-72:]):
        body = abs(c["close"] - c["open"]) / c["open"] * 100 if c["open"] else 99
        if body < 1.0:
            coiling += 1
        else:
            break
    if coiling >= 18:
        score += 5
        sigs.append(f"Coiling {coiling}h — energi terkumpul sangat lama")
    elif coiling >= 10:
        score += 3
        sigs.append(f"Coiling {coiling}h")
    elif coiling >= 5:
        score += 1
    return min(score, 15), sigs, bbw_val, bbw_pct, coiling

def layer_positioning(symbol, funding, oi_change_1h):
    score = 0
    sigs  = []
    if -0.0004 <= funding <= -0.00002:
        score += 8
        sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup!")
    elif 0 <= funding <= 0.0001:
        score += 5
    elif 0.0001 < funding <= 0.0003:
        score += 2
    elif funding > 0.0003:
        score -= 5
        sigs.append(f"⚠️ Funding {funding:.5f} — long overcrowded")

    # Short squeeze bonus (baru)
    if funding > CONFIG["squeeze_funding_min"] and oi_change_1h > CONFIG["squeeze_oi_change_min"]:
        squeeze_bonus = 15
        sigs.append(f"🔥 SHORT SQUEEZE TERINDIKASI: funding {funding:.5f}%, OI naik {oi_change_1h:.1f}%")
        score += squeeze_bonus

    ls = get_long_short_ratio(symbol)
    ls_score = 0
    ls_ratio = None
    if ls is not None:
        ls_ratio = ls
        if ls < 0.6:
            ls_score = 15
            sigs.append(f"🎯 L/S {ls:.2f} — short dominan, squeeze fuel besar!")
        elif ls < 0.75:
            ls_score = 12
            sigs.append(f"🎯 L/S {ls:.2f} — short dominan")
        elif ls < 0.9:
            ls_score = 7
            sigs.append(f"L/S {ls:.2f} — lebih banyak short")
        elif ls <= 1.15:
            ls_score = 3
        elif ls > 3.0:
            ls_score = -20
            sigs.append(f"⚠️⚠️ L/S {ls:.2f} — long overcrowded ekstrem, risiko tinggi!")
        elif ls > 2.5:
            ls_score = -12
            sigs.append(f"⚠️ L/S {ls:.2f} — long sangat dominan")
        elif ls > 2.0:
            ls_score = -8
            sigs.append(f"L/S {ls:.2f} — long dominan")

    score = min(score + ls_score, 15)
    return score, sigs, ls_ratio

def layer_context(symbol, tickers_dict):
    sector = SECTOR_LOOKUP.get(symbol, "MISC")
    peers  = SECTOR_MAP.get(sector, [])
    pumped = []
    for p in peers:
        if p == symbol or p not in tickers_dict:
            continue
        try:
            chg = float(tickers_dict[p].get("change24h", 0)) * 100
            if chg > 8:
                pumped.append((p.replace("USDT", ""), chg))
        except:
            continue
    pumped.sort(key=lambda x: x[1], reverse=True)
    sec_score = 0
    sec_sig   = ""
    if pumped:
        top = pumped[0]
        sec_score = 6 if top[1] > 20 else 4 if top[1] > 12 else 2
        sec_sig   = f"🔄 {sector}: {top[0]} +{top[1]:.0f}% — rotasi potensial"
    name = symbol.replace("USDT", "").replace("1000", "").upper()
    soc_score = 0
    soc_sig   = ""
    if name in get_cg_trending():
        soc_score = 4
        soc_sig   = f"🔥 {name} trending CoinGecko"
    sigs = [s for s in [sec_sig, soc_sig] if s]
    return min(sec_score + soc_score, 10), sigs, sector

def get_time_mult():
    h = utc_hour()
    if h in [5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.20, f"⏰ High-prob window ({h}:00 UTC)"
    if h in [1, 2, 3, 4]:
        return 0.80, "Low-prob window"
    return 1.0, ""

def calc_whale(symbol, candles_15m, funding):
    ws  = 0
    ev  = []
    cur = candles_15m[-1]["close"] if candles_15m else 0
    trades = get_trades(symbol, 500)
    if trades:
        buy_v = sum(t["size"] for t in trades if t["side"] == "buy")
        tot_v = sum(t["size"] for t in trades)
        tr    = buy_v / tot_v if tot_v > 0 else 0.5
        if tr > 0.70:
            ws += 30
            ev.append(f"✅ Taker Buy {tr:.0%} — pembeli sangat dominan")
        elif tr > 0.62:
            ws += 15
            ev.append(f"🔶 Taker Buy {tr:.0%} — bias beli")
        total_usd = sum(t["size"] * t["price"] for t in trades)
        avg_trade = total_usd / len(trades) if trades else 1
        thr       = max(avg_trade * 5, 3_000)
        lbuy_usd  = sum(
            t["size"] * t["price"] for t in trades
            if t["side"] == "buy" and t["size"] * t["price"] > thr
        )
        if total_usd > 0 and lbuy_usd / total_usd > 0.28:
            ws += 25
            ev.append(f"✅ Smart money {lbuy_usd/total_usd:.0%} vol (>${thr:,.0f}/trade)")
        if cur > 0:
            tol = (0.15 if cur >= 10 else 0.30 if cur >= 1 else 0.50)
            at_level = [
                t for t in trades
                if t["side"] == "buy"
                and abs(t["price"] - cur) / cur * 100 < tol
            ]
            if len(at_level) >= 10:
                tot_ice = sum(t["size"] * t["price"] for t in at_level)
                avg_ice = tot_ice / len(at_level)
                if len(at_level) >= 14 and avg_ice < thr * 0.25 and tot_ice > thr * 2.5:
                    ws += 20
                    ev.append(f"✅ Iceberg: {len(at_level)} tx kecil (${tot_ice:,.0f} total)")
    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15
            ev.append("✅ Harga flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7
            ev.append("🔶 Harga relatif flat 4h")
    if -0.0004 <= funding <= -0.00002:
        ws += 10
        ev.append(f"✅ Funding {funding:.5f} — short squeeze setup")
    ob_ratio, bid_vol, ask_vol = get_orderbook(symbol, 50)
    if ob_ratio > 0.65:
        ws += 15
        ev.append(f"✅ OB Bid {ob_ratio:.0%} — tekanan beli kuat di book")
    elif ob_ratio > 0.55:
        ws += 7
        ev.append(f"🔶 OB Bid {ob_ratio:.0%}")
    elif ob_ratio < 0.35:
        ws -= 10
        ev.append(f"⚠️ OB Ask dominan — tekanan jual lebih besar")
    ws  = min(ws, 100)
    return ws, ws // 5, ev


# ==================== NEW METRICS FOR PUMP PROBABILITY ====================
def calculate_volume_irregularity(candles_1h):
    if len(candles_1h) < 24:
        vols = [c["volume"] for c in candles_1h]
    else:
        vols = [c["volume"] for c in candles_1h[-24:]]
    if not vols:
        return 0
    mean_vol = sum(vols) / len(vols)
    if mean_vol == 0:
        return 0
    std_vol = math.sqrt(sum((v - mean_vol)**2 for v in vols) / len(vols))
    return std_vol / mean_vol

def calculate_range_lock_score(candles_1h):
    if len(candles_1h) < 24:
        candles = candles_1h
    else:
        candles = candles_1h[-24:]
    high = max(c["high"] for c in candles)
    low = min(c["low"] for c in candles)
    total_vol = sum(c["volume"] for c in candles)
    if total_vol == 0:
        return 0
    return (high - low) / total_vol

def calculate_efficiency_ratio(candles_1h):
    if len(candles_1h) < 24:
        candles = candles_1h
    else:
        candles = candles_1h[-24:]
    ratios = []
    for c in candles:
        body = abs(c["close"] - c["open"])
        if c["volume"] > 0:
            ratios.append(body / c["volume"])
    if not ratios:
        return 0
    return sum(ratios) / len(ratios)

def calculate_fake_break_count(candles_1h, lookback=5):
    if len(candles_1h) < lookback + 2:
        return 0
    candles = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    false_breaks = 0
    for i in range(lookback, len(candles)-1):
        window = candles[i-lookback:i]
        high_prev = max(c["high"] for c in window)
        low_prev = min(c["low"] for c in window)
        curr = candles[i]
        next_c = candles[i+1]
        if curr["high"] > high_prev and next_c["close"] < high_prev:
            false_breaks += 1
        if curr["low"] < low_prev and next_c["close"] > low_prev:
            false_breaks += 1
    return false_breaks

def calculate_buy_pressure_proxy(candles_1h):
    if len(candles_1h) < 24:
        candles = candles_1h
    else:
        candles = candles_1h[-24:]
    pressures = []
    for c in candles:
        rng = c["high"] - c["low"]
        if rng > 0:
            pressures.append((c["close"] - c["low"]) / rng)
    if not pressures:
        return 0.5
    return sum(pressures) / len(pressures)

def calculate_volatility_drop_rate(candles_1h, period=14):
    if len(candles_1h) < period + 24:
        return 0
    atr_values = []
    for i in range(len(candles_1h)-24, len(candles_1h)):
        start = max(0, i - period - 1)
        sub = candles_1h[start:i+1]
        if len(sub) < period + 1:
            continue
        atr = calc_atr(sub, period)
        if atr:
            atr_values.append(atr)
    if len(atr_values) < 2:
        return 0
    x = list(range(len(atr_values)))
    y = atr_values
    n = len(x)
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(x[i]*y[i] for i in range(n))
    sum_xx = sum(x[i]**2 for i in range(n))
    denom = n*sum_xx - sum_x**2
    if denom == 0:
        return 0
    slope = (n*sum_xy - sum_x*sum_y) / denom
    drop_rate = -slope
    return max(0, min(1, drop_rate / 0.01))

def calculate_trend_stability(candles_1h):
    if len(candles_1h) < 12:
        return 1.0
    slopes = []
    for i in range(6, len(candles_1h)):
        window = candles_1h[i-6:i]
        closes = [c["close"] for c in window]
        x = list(range(6))
        y = closes
        n = 6
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i]*y[i] for i in range(n))
        sum_xx = sum(x[i]**2 for i in range(n))
        denom = n*sum_xx - sum_x**2
        if denom == 0:
            slope = 0
        else:
            slope = (n*sum_xy - sum_x*sum_y) / denom
        slopes.append(slope)
    if len(slopes) < 2:
        return 1.0
    mean_slope = sum(slopes) / len(slopes)
    variance = sum((s - mean_slope)**2 for s in slopes) / len(slopes)
    std_slope = math.sqrt(variance)
    return 1 / (1 + std_slope)

def calculate_absorption_consistency(candles_1h):
    if len(candles_1h) < 24:
        candles = candles_1h
    else:
        candles = candles_1h[-24:]
    absorptions = []
    for c in candles:
        if c["close"] > c["open"]:
            lower_wick = c["open"] - c["low"]
            total_range = c["high"] - c["low"]
            if total_range > 0:
                absorptions.append(lower_wick / total_range)
        else:
            upper_wick = c["high"] - c["open"]
            total_range = c["high"] - c["low"]
            if total_range > 0:
                absorptions.append(upper_wick / total_range)
    if not absorptions:
        return 0
    mean_abs = sum(absorptions) / len(absorptions)
    if mean_abs == 0:
        return 0
    std_abs = math.sqrt(sum((a - mean_abs)**2 for a in absorptions) / len(absorptions))
    cv = std_abs / mean_abs
    return 1 / (1 + cv)


# ==================== PUMP PROBABILITY SCORE ====================
def normalize(value, min_val, max_val, inverse=False):
    if max_val == min_val:
        return 0.5
    norm = (value - min_val) / (max_val - min_val)
    norm = max(0, min(1, norm))
    if inverse:
        return 1 - norm
    return norm

def compute_pump_probability(candles_1h):
    _, bbw_pct = bbw_percentile(candles_1h)
    compression = bbw_pct / 100.0

    # Wick absorption
    if len(candles_1h) >= 24:
        candles = candles_1h[-24:]
    else:
        candles = candles_1h
    abs_list = []
    for c in candles:
        if c["close"] > c["open"]:
            lower_wick = c["open"] - c["low"]
            total_range = c["high"] - c["low"]
            if total_range > 0:
                abs_list.append(lower_wick / total_range)
        else:
            upper_wick = c["high"] - c["open"]
            total_range = c["high"] - c["low"]
            if total_range > 0:
                abs_list.append(upper_wick / total_range)
    wick_abs = sum(abs_list) / len(abs_list) if abs_list else 0.5

    vol_irr = calculate_volume_irregularity(candles_1h)
    range_lock = calculate_range_lock_score(candles_1h)
    eff = calculate_efficiency_ratio(candles_1h)
    fake_break = calculate_fake_break_count(candles_1h)
    buy_pressure = calculate_buy_pressure_proxy(candles_1h)
    vol_drop = calculate_volatility_drop_rate(candles_1h)
    trend_stab = calculate_trend_stability(candles_1h)
    abs_cons = calculate_absorption_consistency(candles_1h)

    # Normalisasi
    n_comp = 1 - normalize(compression, 0, 1, inverse=False)  # compression kecil bagus
    n_wick = normalize(wick_abs, 0.1, 0.5, inverse=False)
    n_vol_irr = normalize(vol_irr, 0.5, 3.0, inverse=False)
    n_range_lock = normalize(range_lock, 1e-9, 1e-6, inverse=True)
    n_eff = normalize(eff, 1e-7, 1e-4, inverse=True)
    n_fake = normalize(fake_break, 0, 10, inverse=False)
    n_buy = normalize(buy_pressure, 0.4, 0.6, inverse=False)
    n_vol_drop = normalize(vol_drop, 0, 1, inverse=False)
    n_trend_stab = normalize(trend_stab, 0.5, 1.0, inverse=False)

    weights = {
        'comp': 0.15,
        'wick': 0.15,
        'vol_irr': 0.15,
        'range': 0.15,
        'eff': 0.10,
        'fake': 0.10,
        'buy': 0.10,
        'vol_drop': 0.05,
        'trend': 0.05,
    }

    total = (n_comp * weights['comp'] +
             n_wick * weights['wick'] +
             n_vol_irr * weights['vol_irr'] +
             n_range_lock * weights['range'] +
             n_eff * weights['eff'] +
             n_fake * weights['fake'] +
             n_buy * weights['buy'] +
             n_vol_drop * weights['vol_drop'] +
             n_trend_stab * weights['trend'])

    if total < 0.3:
        classification = "Noise"
    elif total < 0.5:
        classification = "Neutral"
    elif total < 0.7:
        classification = "Accumulation"
    elif total < 0.85:
        classification = "Pre-Pump"
    else:
        classification = "Imminent Pump"

    return {
        'probability_score': total,
        'classification': classification,
        'metrics': {
            'compression': compression,
            'wick_absorption': wick_abs,
            'volume_irregularity': vol_irr,
            'range_lock_score': range_lock,
            'efficiency_ratio': eff,
            'fake_break_count': fake_break,
            'buy_pressure': buy_pressure,
            'volatility_drop_rate': vol_drop,
            'trend_stability': trend_stab,
            'absorption_consistency': abs_cons,
        },
        'normalized': {
            'n_comp': n_comp,
            'n_wick': n_wick,
            'n_vol_irr': n_vol_irr,
            'n_range_lock': n_range_lock,
            'n_eff': n_eff,
            'n_fake': n_fake,
            'n_buy': n_buy,
            'n_vol_drop': n_vol_drop,
            'n_trend_stab': n_trend_stab,
        }
    }


# ==================== ENTRY ZONE ====================
def calc_entry(candles_1h):
    cur = candles_1h[-1]["close"]
    atr = calc_atr(candles_1h, 14) or cur * 0.02
    recent   = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    vwap, z1 = calc_vwap_zone(recent)
    poc_src  = candles_1h[-48:] if len(candles_1h) >= 48 else candles_1h
    z2       = calc_poc(poc_src)
    if not z2 or z2 >= cur:
        z2 = cur * 0.97
    support = max(z1 or cur * 0.97, z2)
    if support >= cur:
        support = cur * 0.96
    max_dist = CONFIG["max_sl_pct"] / 100
    if (cur - support) / cur > max_dist:
        support = cur * (1 - max_dist + 0.02)
    entry = min(support * 1.002, cur * 0.998)
    sl    = max(entry - CONFIG["atr_sl_mult"] * atr, entry * 0.88)
    t1_sw, t2_sw = find_swing_targets(candles_1h, cur)
    t1_atr       = entry + CONFIG["atr_t1_mult"] * atr
    t1           = min(t1_sw, t1_atr) if t1_sw > cur * 1.06 else t1_atr
    if t1 <= cur * 1.05:
        t1 = cur * 1.10
    t2 = t2_sw if t2_sw > t1 * 1.02 else t1 * 1.08
    risk   = entry - sl
    reward = t1 - entry
    rr     = round(reward / risk, 1) if risk > 0 else 0
    t1_pct = round((t1 - cur) / cur * 100, 1)
    sl_pct = round((entry - sl) / entry * 100, 1)
    return {
        "cur":     cur,
        "atr":     round(atr, 8),
        "vwap":    round(vwap, 8) if vwap else 0,
        "z1":      round(z1, 8)   if z1   else 0,
        "z2":      round(z2, 8),
        "entry":   round(entry, 8),
        "sl":      round(sl, 8),
        "sl_pct":  sl_pct,
        "t1":      round(t1, 8),
        "t2":      round(t2, 8),
        "rr":      rr,
        "liq_pct": t1_pct,
    }


# ==================== MASTER SCORE ====================
def master_score(symbol, ticker, tickers_dict):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candle_4h"])

    if len(c1h) < 48 or len(c15m) < 20:
        return None

    funding = get_funding(symbol)

    # ── GATES (dimodifikasi untuk squeeze) ─────────────────────────
    try:
        p7d_ago = c1h[-168]["close"] if len(c1h) >= 168 else c1h[0]["close"]
        chg_7d  = (c1h[-1]["close"] - p7d_ago) / p7d_ago * 100 if p7d_ago > 0 else 0
    except:
        chg_7d = 0

    # Gate 7d dengan pengecualian squeeze
    if chg_7d > CONFIG["gate_chg_7d_max"]:
        # Cek apakah ini short squeeze
        oi_value = get_open_interest(symbol)
        oi_change_1h, _ = get_oi_changes(symbol, oi_value) if oi_value > 0 else (0,0)
        if funding > CONFIG["squeeze_funding_min"] and oi_change_1h > CONFIG["squeeze_oi_change_min"]:
            log.info(f"  {symbol}: Overbought {chg_7d:.1f}% tapi funding tinggi & OI naik, diberi penalti -15 (tidak direject)")
            # penalti akan diberikan nanti, kita lanjutkan
        else:
            log.info(f"  {symbol}: GATE 7d overbought ({chg_7d:.1f}%) tanpa squeeze, reject")
            return None

    if chg_7d < CONFIG["gate_chg_7d_min"]:
        log.info(f"  {symbol}: GATE 7d downtrend ({chg_7d:.1f}%)")
        return None
    if funding < CONFIG["gate_funding_extreme"]:
        log.info(f"  {symbol}: GATE funding ekstrem ({funding:.5f})")
        return None

    # Hitung metrik tambahan untuk bonus
    avg_vol_pre = 0
    range_6h = 0
    if len(c1h) >= 6:
        pre_candles = c1h[-6:]
        avg_vol_pre = sum(c["volume_usd"] for c in pre_candles) / 6
        high_6 = max(c["high"] for c in pre_candles)
        low_6  = min(c["low"] for c in pre_candles)
        if low_6 > 0:
            range_6h = (high_6 - low_6) / low_6 * 100

    score = 0
    sigs  = []
    bd    = {}

    # Layer 1: Volume Intelligence
    v_sc, v_sigs, rvol = layer_volume_intelligence(c1h)
    score += v_sc
    sigs  += v_sigs
    bd["vol"] = v_sc

    # Layer 2: Breakout Proximity
    bp_sc, bp_sigs = calc_breakout_proximity(c1h, ticker)
    score += bp_sc
    sigs  += bp_sigs
    bd["bp"] = bp_sc

    # Layer 3: Structure
    st_sc, st_sigs, bbw_val, bbw_pct, coiling = layer_structure(c1h)
    score += st_sc
    sigs  += st_sigs
    bd["struct"] = st_sc

    # Bonus Stealth
    stealth_bonus = 0
    if (avg_vol_pre < CONFIG["stealth_max_vol"] and
        coiling > CONFIG["stealth_min_coiling"] and
        range_6h < CONFIG["stealth_max_range"]):
        stealth_bonus = 30
        sigs.append(f"🕵️ STEALTH PATTERN: vol {avg_vol_pre:.0f}, coiling {coiling}h, range {range_6h:.1f}%")
    score += stealth_bonus
    bd["stealth"] = stealth_bonus

    # Bonus Eksplosif
    explosive_bonus = 0
    if (avg_vol_pre > CONFIG["explosive_min_vol"] and
        range_6h > CONFIG["explosive_min_range"] and
        bbw_val > CONFIG["explosive_min_bbw"]):
        explosive_bonus = 15
        sigs.append(f"💥 EXPLOSIVE PATTERN: vol {avg_vol_pre:.0f}, range {range_6h:.1f}%, BBW {bbw_val:.1f}%")
    score += explosive_bonus
    bd["explosive"] = explosive_bonus

    # OI untuk digunakan di layer positioning
    oi_value = get_open_interest(symbol)
    oi_change_1h, oi_change_24h = 0, 0
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)
        oi_change_1h, oi_change_24h = get_oi_changes(symbol, oi_value)

    # Layer 4: Positioning (sekarang dengan oi_change_1h)
    pos_sc, pos_sigs, ls_ratio = layer_positioning(symbol, funding, oi_change_1h)
    score += pos_sc
    sigs  += pos_sigs
    bd["pos"] = pos_sc

    # Layer 5: Multi-TF 4H
    tf4h_sc = 0
    if c4h:
        tf4h_sc, tf4h_sig = calc_4h_confluence(c4h)
        if tf4h_sig:
            sigs.append(tf4h_sig)
    score += tf4h_sc
    bd["tf4h"] = tf4h_sc

    # Layer 6: Context
    ctx_sc, ctx_sigs, sector = layer_context(symbol, tickers_dict)
    score += ctx_sc
    sigs  += ctx_sigs
    bd["ctx"] = ctx_sc

    # Whale
    ws, whale_bonus, wev = calc_whale(symbol, c15m, funding)
    score += whale_bonus
    bd["whale"] = whale_bonus
    for ev in wev:
        sigs.append(ev)

    # Open Interest Changes (penalti/bonus)
    if oi_value > 0:
        if oi_change_24h < -20:
            score -= 15
            sigs.append(f"⚠️ OI 24h turun {oi_change_24h:.1f}% — distribusi besar")
        elif oi_change_24h < -10:
            score -= 7
            sigs.append(f"OI 24h turun {oi_change_24h:.1f}%")
        if oi_change_1h < -5:
            score -= 5
            sigs.append(f"OI 1h turun {oi_change_1h:.1f}% — tekanan jual jangka pendek")
        elif oi_change_1h > 5:
            score += 5
            sigs.append(f"✅ OI 1h naik {oi_change_1h:.1f}% — posisi baru masuk")
        if rvol > 1.5 and oi_change_24h < -10:
            score -= 8
            sigs.append(f"⚠️ Volume naik tapi OI turun — distribusi terindikasi")
        elif rvol > 1.5 and oi_change_24h > 5:
            score += 8
            sigs.append(f"✅ Volume naik + OI naik — akumulasi kuat")
        bd["oi_change"] = round(oi_change_24h, 1)
    else:
        bd["oi_change"] = 0

    # Penalti overbought jika tidak diselamatkan squeeze
    if chg_7d > CONFIG["gate_chg_7d_max"]:
        # sudah dicek di gate, berarti ini squeeze case, beri penalti ringan
        score -= 15
        sigs.append(f"⚠️ Overbought ekstrem ({chg_7d:+.1f}% 7d) tapi funding tinggi — short squeeze aktif, tetap waspada")

    # Pump Probability Score
    prob = compute_pump_probability(c1h)
    bd["prob_score"] = round(prob['probability_score'] * 100, 1)
    bd["prob_class"] = prob['classification']

    # Time Multiplier
    tmult, tsig = get_time_mult()
    score = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    score = min(score, 100)

    # Entry Zones
    entry = calc_entry(c1h)
    if not entry or entry["liq_pct"] < CONFIG["min_target_pct"]:
        return None

    try:
        price_now = float(ticker.get("lastPr",       0))
        chg_24h   = float(ticker.get("change24h",    0)) * 100
        vol_24h   = float(ticker.get("quoteVolume",  0))
    except:
        price_now = c1h[-1]["close"]
        chg_24h   = 0
        vol_24h   = 0

    # Range 24h penalty
    if len(c1h) >= 24:
        high24 = max(c["high"] for c in c1h[-24:])
        low24  = min(c["low"] for c in c1h[-24:])
        if low24 > 0:
            range24 = (high24 - low24) / low24 * 100
            if range24 > 55:
                score = max(0, score - 10)
                sigs.append(f"⚠️ Range 24h {range24:.0f}% — pump mungkin sudah berjalan")

    return {
        "symbol":   symbol,
        "score":    score,
        "signals":  sigs,
        "ws":       ws,
        "wev":      wev,
        "entry":    entry,
        "sector":   sector,
        "funding":  funding,
        "bd":       bd,
        "price":    price_now,
        "chg_24h":  chg_24h,
        "vol_24h":  vol_24h,
        "rvol":     rvol,
        "ls_ratio": ls_ratio,
        "chg_7d":   chg_7d,
        "avg_vol_pre": avg_vol_pre,
        "range_6h": range_6h,
        "coiling":  coiling,
        "bbw_val":  bbw_val,
        "oi_change_24h": bd.get("oi_change", 0),
        "prob_score": prob['probability_score'],
        "prob_class": prob['classification'],
    }


# ==================== TELEGRAM FORMATTER ====================
def build_alert(r, rank=None):
    sc  = r["score"]
    bar = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    e   = r["entry"]
    rk  = f"#{rank} " if rank else ""
    vol = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
           else f"${r['vol_24h']/1e3:.0f}K")
    ls  = f" | L/S:{r['ls_ratio']:.2f}" if r.get("ls_ratio") else ""

    prob_score = r.get("prob_score", 0) * 100
    prob_class = r.get("prob_class", "Unknown")

    msg = (
        f"🚨 <b>PRE-PUMP INTELLIGENCE {rk}</b>\n\n"
        f"<b>Symbol :</b> {r['symbol']}\n"
        f"<b>Score  :</b> {sc}/100  {bar}\n"
        f"<b>Prob. Pump:</b> {prob_score:.1f}% ({prob_class})\n"
        f"<b>Sektor :</b> {r['sector']}\n"
        f"<b>Harga  :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h | {r['chg_7d']:+.1f}% 7d)\n"
        f"<b>Vol 24h:</b> {vol} | RVOL: {r['rvol']:.1f}x{ls}\n"
        f"<b>6h Vol :</b> ${r['avg_vol_pre']:.0f}  | 6h Range: {r['range_6h']:.1f}%\n"
        f"<b>Coiling:</b> {r['coiling']}h  | BBW: {r['bbw_val']:.1f}%\n"
        f"<b>OI 24h :</b> {r['oi_change_24h']:+.1f}%\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐋 <b>WHALE: {r['ws']}/100</b>\n"
    )
    for ev in r["wev"]:
        msg += f"  {ev}\n"

    if e:
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY ZONES</b>\n"
            f"  🟢 VWAP  : ${e['z1']}\n"
            f"  🟢 POC   : ${e['z2']}\n"
            f"  📌 Entry : ${e['entry']}\n"
            f"  🛑 SL    : ${e['sl']}  (-{e['sl_pct']:.1f}% | ATR×{CONFIG['atr_sl_mult']})\n\n"
            f"🎯 <b>TARGET</b>\n"
            f"  T1 : ${e['t1']}  (+{e['liq_pct']:.1f}%)\n"
            f"  T2 : ${e['t2']}\n"
            f"  R/R: 1:{e['rr']}  |  ATR: ${e['atr']}\n"
        )

    msg += f"\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL</b>\n"
    for s in r["signals"][:9]:
        msg += f"  • {s}\n"

    bd = r.get("bd", {})
    msg += (
        f"\n📐 <b>BREAKDOWN</b>\n"
        f"  Vol:{bd.get('vol',0)} BP:{bd.get('bp',0)} "
        f"Struct:{bd.get('struct',0)} Pos:{bd.get('pos',0)} "
        f"4H:{bd.get('tf4h',0)} Ctx:{bd.get('ctx',0)} "
        f"Whale:{bd.get('whale',0)} Stealth:{bd.get('stealth',0)} Exp:{bd.get('explosive',0)} "
        f"Prob:{bd.get('prob_score',0)}%\n\n"
        f"📡 Funding:{r['funding']:.5f}  🕐 {utc_now()}\n"
        f"<i>Bukan financial advice. Manage risk.</i>"
    )
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        bar  = "█" * int(r["score"] / 10) + "░" * (10 - int(r["score"] / 10))
        vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                else f"${r['vol_24h']/1e3:.0f}K")
        t1p  = r["entry"]["liq_pct"] if r.get("entry") else 0
        prob = r.get("prob_score", 0) * 100
        msg += (
            f"{i}. <b>{r['symbol']}</b> [{r['score']}/100 {bar}]\n"
            f"   🐋{r['ws']} | RVOL:{r['rvol']:.1f}x | {vol} | T1:+{t1p:.0f}% | Prob:{prob:.0f}%\n"
        )
    return msg


# ==================== PRE-FILTER ====================
def pre_score_ticker(ticker):
    try:
        cur     = float(ticker.get("lastPr",       0))
        high24h = float(ticker.get("high24h",      cur))
        low24h  = float(ticker.get("low24h",       cur))
        vol     = float(ticker.get("quoteVolume",  0))
        chg24h  = float(ticker.get("change24h",    0)) * 100
    except:
        return 0
    if cur <= 0:
        return 0
    ps = 0
    dist = (high24h - cur) / cur * 100 if cur > 0 and high24h > cur else 0
    if dist <= 1.0:   ps += 6
    elif dist <= 3.0: ps += 4
    elif dist <= 6.0: ps += 2
    elif dist > 20:   ps -= 2
    if 0.5 <= chg24h <= 8:     ps += 3
    elif 8 < chg24h <= 15:     ps += 1
    elif -5 <= chg24h < 0:     ps += 2
    elif chg24h > 20:          ps -= 3
    if low24h > 0:
        range24 = (high24h - low24h) / low24h * 100
        if 3 <= range24 <= 15: ps += 2
        elif range24 > 40:     ps -= 2
    if   200_000 <= vol <= 3_000_000: ps += 2
    elif 100_000 <= vol <  200_000:   ps += 1
    elif vol >     20_000_000:        ps -= 1
    return ps


# ==================== MAIN SCAN ====================
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v8.1 — SHORT SQUEEZE AWARE — {utc_now()} ===")
    log.info(f"Dynamic discovery: scan semua USDT-futures Bitget")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total coin tersedia: {len(tickers)}")

    excluded_keywords = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST", "LUNC", "LUNA"]
    candidates = []
    for sym, t in tickers.items():
        if not sym.endswith("USDT"):
            continue
        if any(kw in sym for kw in excluded_keywords):
            continue
        if is_cooldown(sym):
            continue
        try:
            vol   = float(t.get("quoteVolume", 0))
            chg   = float(t.get("change24h",   0)) * 100
            price = float(t.get("lastPr",       0))
        except:
            continue
        if vol   < CONFIG["pre_filter_vol"]:          continue
        if vol   > CONFIG["max_vol_24h"]:             continue
        if abs(chg) > CONFIG["gate_chg_24h_max"]:    continue
        if price <= 0:                                 continue
        ps = pre_score_ticker(t)
        candidates.append((sym, ps, vol))

    candidates.sort(key=lambda x: (-x[1], -x[2]))
    candidates = candidates[:CONFIG["max_deep_scan"]]

    log.info(f"Pre-filter lolos: {len(candidates)} coin → deep scan")

    results = []
    for i, (sym, ps, vol) in enumerate(candidates):
        t = tickers.get(sym)
        if not t:
            continue
        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum")
            continue
        try:
            chg = abs(float(t.get("change24h", 0)) * 100)
            if chg > CONFIG["gate_chg_24h_max"]:
                continue
        except:
            pass
        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K, pre={ps})...")
        try:
            res = master_score(sym, t, tickers)
            if res:
                log.info(
                    f"  Score={res['score']} Whale={res['ws']} "
                    f"RVOL={res['rvol']:.1f}x T1=+{res['entry']['liq_pct']:.1f}% Prob={res['prob_score']*100:.1f}%"
                )
                if res["score"] >= CONFIG["min_score_alert"]:
                    results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")
        time.sleep(CONFIG["sleep_coins"])

    results.sort(key=lambda x: (x["score"], x["ws"]), reverse=True)
    log.info(f"Kandidat kuat: {len(results)} coin")

    qualified = [
        r for r in results
        if r["ws"] >= CONFIG["min_whale_score"] or r["score"] >= 65
    ]

    if not qualified:
        log.info("Tidak ada sinyal yang memenuhi syarat saat ini")
        return

    top = qualified[:CONFIG["max_alerts_per_run"]]

    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"✅ Alert #{rank}: {r['symbol']} S={r['score']} W={r['ws']} Prob={r['prob_score']*100:.1f}%")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


if __name__ == "__main__":
    log.info("╔════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v8.1 — SQUEEZE AWARE    ║")
    log.info("╚════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)

    run_scan()
