"""
╔══════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v7.0 — DEEP OVERHAUL                              ║
║                                                                      ║
║  PERUBAHAN FILOSOFI:                                                 ║
║  v6.x → Cari coin yang DIAM (compression detection)                 ║
║  v7.0 → Cari coin yang MULAI BERGERAK (early movement detection)    ║
║                                                                      ║
║  7 BUG v6.x DIPERBAIKI:                                             ║
║  ✅ Vol filter *24 double-count → dihapus                           ║
║  ✅ Gate 7d dilonggarkan ke [-35%, +35%]                            ║
║  ✅ Gate awakening (logic terbalik) → dihapus                       ║
║  ✅ OI snapshot ephemeral → tidak dipakai                           ║
║  ✅ STEEMUSDT + semua coin lain → dynamic discovery                 ║
║  ✅ Pre-filter cerdas berbasis breakout proximity dari ticker        ║
║  ✅ Gate funding extreme dilonggarkan                                ║
║                                                                      ║
║  10 GAPS BARU DIPERBAIKI:                                           ║
║  ✅ Dynamic discovery: scan SEMUA USDT-futures Bitget               ║
║  ✅ RVOL: vol jam ini vs historis jam yang sama (7 hari)            ║
║  ✅ CVD: cumulative volume delta (hidden accumulation)              ║
║  ✅ Breakout Proximity: skor jarak dari resistance 24h              ║
║  ✅ Long/Short Ratio Bitget API                                     ║
║  ✅ Multi-TF: konfirmasi 4H bullish/reversal                        ║
║  ✅ Candle Quality: body ratio + wick analysis                      ║
║  ✅ Smart pre-filter dari ticker (tanpa candle call)                ║
║  ✅ Volume building at resistance pattern                           ║
║  ✅ Higher lows pattern (ascending triangle)                        ║
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

    # Volume 24h TOTAL (USD) — TANPA bug *24
    "min_vol_24h":          150_000,    # $150K/hari minimum
    "max_vol_24h":       50_000_000,    # $50M/hari (exclude BTC/ETH besar)
    "pre_filter_vol":        40_000,    # pre-filter cepat

    # Gate price change
    "gate_chg_24h_max":         30.0,   # sudah pump besar hari ini
    "gate_chg_7d_max":          35.0,   # overbought 7 hari
    "gate_chg_7d_min":         -35.0,   # downtrend parah
    "gate_funding_extreme":    -0.002,  # short overcrowded ekstrem

    # Candle limits
    "candle_1h":               168,     # 7 hari (RVOL butuh ini)
    "candle_15m":               96,     # 24 jam (whale detection)
    "candle_4h":                42,     # 7 hari (multi-TF)

    # Entry/exit parameters
    "min_target_pct":            8.0,
    "max_sl_pct":               12.0,
    "atr_sl_mult":               1.5,
    "atr_t1_mult":               2.5,

    # Operational
    "alert_cooldown_sec":       3600,
    "sleep_coins":               0.9,
    "sleep_error":               3.0,
    "max_deep_scan":              80,    # max coin untuk deep analysis per run
    "cooldown_file":    "/tmp/v7_cooldown.json",
}

GRAN_MAP = {
    "15m": "15m",
    "1h":  "1H",
    "4h":  "4H",
    "1d":  "1D",
}


# ══════════════════════════════════════════════════════════════
#  🗺️  SECTOR MAP
# ══════════════════════════════════════════════════════════════
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

SECTOR_LOOKUP = {
    coin: sector
    for sector, coins in SECTOR_MAP.items()
    for coin in coins
}

BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_cache         = {}


# ══════════════════════════════════════════════════════════════
#  💾  COOLDOWN (best-effort, file ephemeral di GitHub Actions)
# ══════════════════════════════════════════════════════════════
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

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)


# ══════════════════════════════════════════════════════════════
#  🌐  HTTP UTILITIES
# ══════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════
def get_all_tickers():
    """Ambil semua ticker USDT-futures sekaligus (1 API call)."""
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

def get_long_short_ratio(symbol):
    """
    Rasio akun long vs short dari Bitget.
    Nilai < 1.0 = lebih banyak akun short = potensi squeeze lebih tinggi.
    """
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

def get_trades(symbol, limit=300):
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

def get_orderbook(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/merge-depth",
        params={"symbol": symbol, "productType": "usdt-futures",
                "precision": "scale0", "limit": "50"}
    )
    if data and data.get("code") == "00000":
        try:
            book    = data["data"]
            bid_usd = sum(float(b[0]) * float(b[1]) for b in book.get("bids", [])[:20])
            ask_usd = sum(float(a[0]) * float(a[1]) for a in book.get("asks", [])[:20])
            total   = bid_usd + ask_usd
            return (bid_usd / total if total > 0 else 0.5), bid_usd, ask_usd
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


# ══════════════════════════════════════════════════════════════
#  📐  MATH HELPERS
# ══════════════════════════════════════════════════════════════
def bbw_percentile(candles, period=20):
    """Bollinger Band Width percentile — ukuran kompresi volatilitas."""
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


# ══════════════════════════════════════════════════════════════
#  🆕  RVOL — RELATIVE VOLUME (leading indicator terkuat)
# ══════════════════════════════════════════════════════════════
def calc_rvol(candles_1h):
    """
    Bandingkan volume candle jam terakhir vs rata-rata historis
    candle pada JAM YANG SAMA selama 7 hari terakhir.

    Contoh: jam 14:00 sekarang vol 5x rata-rata jam 14:00 = pump dini terdeteksi.
    Ini jauh lebih akurat daripada membandingkan dengan rata-rata 24h.
    """
    if len(candles_1h) < 25:
        return 1.0

    # Candle ke-2 dari belakang = candle jam terakhir yang sudah SELESAI
    # Candle terakhir mungkin masih berjalan (volume parsial)
    last_complete = candles_1h[-2]
    last_vol      = last_complete["volume_usd"]

    # Jam UTC dari candle tersebut (timestamp dalam ms)
    target_hour = (last_complete["ts"] // 3_600_000) % 24

    # Kumpulkan semua candle pada jam yang sama (kecuali 2 candle terakhir)
    same_hour_vols = [
        c["volume_usd"] for c in candles_1h[:-2]
        if (c["ts"] // 3_600_000) % 24 == target_hour
    ]

    if not same_hour_vols:
        return 1.0

    avg = sum(same_hour_vols) / len(same_hour_vols)
    return last_vol / avg if avg > 0 else 1.0


# ══════════════════════════════════════════════════════════════
#  🆕  CVD — CUMULATIVE VOLUME DELTA (hidden accumulation)
# ══════════════════════════════════════════════════════════════
def calc_cvd_signal(candles_1h):
    """
    Deteksi hidden accumulation: CVD naik tapi harga flat.
    Ini artinya buyer diam-diam mengakumulasi TANPA menaikkan harga.
    Ini adalah tanda whale positioning sebelum pump.
    """
    if len(candles_1h) < 12:
        return 0, ""

    window = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h

    # Hitung CVD untuk setiap candle
    cvd      = 0
    cvd_vals = []
    for c in window:
        rng = c["high"] - c["low"]
        if rng > 0:
            buy_ratio = (c["close"] - c["low"]) / rng
        else:
            buy_ratio = 0.5
        # Delta: positif = dominasi beli, negatif = dominasi jual
        delta = (buy_ratio * 2 - 1) * c["volume_usd"]
        cvd  += delta
        cvd_vals.append(cvd)

    if len(cvd_vals) < 8:
        return 0, ""

    # Trend CVD: bandingkan paruh pertama vs paruh kedua
    mid       = len(cvd_vals) // 2
    cvd_early = sum(cvd_vals[:mid]) / mid
    cvd_late  = sum(cvd_vals[mid:]) / (len(cvd_vals) - mid)
    cvd_rising = cvd_late > cvd_early

    # Trend harga di window yang sama
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
    elif not cvd_rising and price_chg > 3.0:
        return 0, ""  # Price naik tapi CVD tidak → tidak sustainable

    return 0, ""


# ══════════════════════════════════════════════════════════════
#  🆕  BREAKOUT PROXIMITY LAYER
# ══════════════════════════════════════════════════════════════
def detect_higher_lows(candles):
    """
    Deteksi pola higher lows — ascending triangle atau bull flag.
    Pola ini menunjukkan buyer semakin agresif di harga yang lebih tinggi.
    """
    if len(candles) < 6:
        return 0, ""

    lows = [c["low"] for c in candles]

    # Temukan swing low lokal
    local_lows = []
    for i in range(1, len(lows) - 1):
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
            local_lows.append(lows[i])

    if len(local_lows) < 2:
        return 0, ""

    # Hitung berapa swing low yang ascending
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
    """
    Pola pre-breakout klasik: volume NAIK + harga TIDAK TURUN + dekat resistance.
    Ini adalah sinyal wall buying — ada yang mengakumulasi besar di resistance.
    """
    if len(candles_1h) < 6:
        return 0, ""

    # 3 candle terakhir yang sudah selesai
    last3 = candles_1h[-4:-1]
    if len(last3) < 3:
        return 0, ""

    vols   = [c["volume_usd"] for c in last3]
    prices = [c["close"]      for c in last3]

    vol_building   = vols[-1] > vols[0] * 1.25   # Volume naik 25%+
    price_holding  = prices[-1] >= prices[0] * 0.995  # Harga tidak turun
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
    """
    Kualitas candle terakhir: apakah bullish kuat?
    Candle dengan body besar + wick bawah kecil = buyer in control.
    """
    if len(candles_1h) < 3:
        return 0, ""

    last = candles_1h[-2]  # Candle selesai terakhir
    prev = candles_1h[-3]

    body  = abs(last["close"] - last["open"])
    rng   = last["high"] - last["low"]
    if rng <= 0:
        return 0, ""

    body_ratio  = body / rng
    lower_wick  = (min(last["close"], last["open"]) - last["low"]) / rng
    upper_wick  = (last["high"] - max(last["close"], last["open"])) / rng
    is_bullish  = last["close"] > last["open"]
    is_breakout = last["close"] > prev["high"]  # Close di atas high candle sebelumnya

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
        score = -3  # Bearish strong candle = penalti

    return score, sig

def calc_breakout_proximity(candles_1h, ticker):
    """
    Layer kunci baru: SEBERAPA DEKAT harga dari resistance?

    Skor tinggi = coin coiling TEPAT di bawah resistance = setup pump ideal.
    v6 tidak punya ini sama sekali — itulah mengapa banyak coin diam terus dipilih.
    """
    score = 0
    sigs  = []

    cur = candles_1h[-1]["close"] if candles_1h else 0
    if cur <= 0:
        return 0, []

    # Resistance utama: high 24h dari ticker API (lebih akurat dari candle)
    try:
        high_24h = float(ticker.get("high24h", cur))
        low_24h  = float(ticker.get("low24h",  cur))
    except:
        highs_24 = [c["high"] for c in candles_1h[-24:]] if len(candles_1h) >= 24 else [cur]
        lows_24  = [c["low"]  for c in candles_1h[-24:]] if len(candles_1h) >= 24 else [cur]
        high_24h = max(highs_24)
        low_24h  = min(lows_24)

    # Resistance 7d (semua candle yang tersedia)
    high_7d = max(c["high"] for c in candles_1h) if candles_1h else cur

    # Jarak ke resistance 24h
    dist_24h = (high_24h - cur) / cur * 100 if cur > 0 else 99

    # Scoring berdasarkan proximity
    if dist_24h <= 0:
        # Sudah di atas atau sama dengan high 24h → breakout aktif!
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
        score -= 5  # Terlalu jauh dari resistance = tidak relevan

    # Higher lows dalam 16 jam terakhir
    hl_sc, hl_sig = detect_higher_lows(
        candles_1h[-16:] if len(candles_1h) >= 16 else candles_1h
    )
    score += hl_sc
    if hl_sig:
        sigs.append(hl_sig)

    # Volume building at resistance
    vb_sc, vb_sig = volume_building_at_resistance(candles_1h, ticker)
    score += vb_sc
    if vb_sig:
        sigs.append(vb_sig)

    # Candle quality
    cq_sc, cq_sig = candle_quality_score(candles_1h)
    score += cq_sc
    if cq_sig:
        sigs.append(cq_sig)

    return min(max(score, 0), 28), sigs


# ══════════════════════════════════════════════════════════════
#  🆕  4H MULTI-TIMEFRAME CONFLUENCE
# ══════════════════════════════════════════════════════════════
def calc_4h_confluence(candles_4h):
    """
    Konfirmasi trend 4H: apakah timeframe yang lebih besar mendukung pump?
    Entry dalam downtrend 4H memiliki probabilitas jauh lebih rendah.
    """
    if len(candles_4h) < 6:
        return 0, ""

    closes = [c["close"] for c in candles_4h]

    # Trend 7 hari (42 candle 4h = 7 hari)
    p_7d  = closes[0]
    p_now = closes[-1]
    trend_7d = (p_now - p_7d) / p_7d * 100 if p_7d > 0 else 0

    # Trend 48h (12 candle 4h)
    p_48h    = closes[-12] if len(closes) >= 12 else closes[0]
    trend_48h = (p_now - p_48h) / p_48h * 100 if p_48h > 0 else 0

    # 4H dalam uptrend dan 48h reversal/accumulation
    if trend_48h > 2 and -10 <= trend_7d <= 15:
        return 8, f"📊 4H: reversal bullish 48h +{trend_48h:.1f}%, trend 7d sehat"
    elif trend_48h > 0 and trend_7d > -15:
        return 4, f"📊 4H upward bias ({trend_48h:+.1f}% 48h)"
    elif trend_48h < -8:
        return -6, f"⚠️ 4H masih downtrend ({trend_48h:+.1f}% 48h)"

    return 0, ""


# ══════════════════════════════════════════════════════════════
#  📊  SCORING LAYERS
# ══════════════════════════════════════════════════════════════
def layer_volume_intelligence(candles_1h):
    """
    Layer 1: RVOL + CVD (max 35 pts)
    Ini adalah layer terpenting — volume adalah nyawa dari pump.
    """
    score = 0
    sigs  = []

    # RVOL — relative volume vs jam yang sama historis
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
        score -= 5  # Volume sangat rendah = coin tidak aktif

    # CVD divergence — hidden accumulation detector
    cvd_s, cvd_sig = calc_cvd_signal(candles_1h)
    score += cvd_s
    if cvd_sig:
        sigs.append(cvd_sig)

    return min(score, 35), sigs, rvol

def layer_structure(candles_1h):
    """
    Layer 2: BBW + Coiling (max 15 pts)
    Dikurangi dari v6 karena kompresi saja tidak cukup.
    """
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

    # Coiling: berapa jam terakhir harga benar-benar flat?
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

    return min(score, 15), sigs

def layer_positioning(symbol, funding):
    """
    Layer 3: Funding rate + Long/Short ratio (max 15 pts)
    Funding negatif + L/S rendah = bahan bakar squeeze terbanyak.
    """
    score = 0
    sigs  = []

    # Funding rate score
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
    # Funding < -0.0004 = short overcrowded, bounce bisa terjadi tapi risky

    # Long/Short ratio dari Bitget
    ls = get_long_short_ratio(symbol)
    ls_score = 0
    if ls is not None:
        if ls < 0.75:
            ls_score = 7
            sigs.append(f"🎯 L/S {ls:.2f} — short dominan, squeeze fuel besar!")
        elif ls < 0.90:
            ls_score = 5
            sigs.append(f"L/S {ls:.2f} — lebih banyak short dari long")
        elif ls <= 1.15:
            ls_score = 3
        elif ls > 2.0:
            ls_score = -4
            sigs.append(f"⚠️ L/S {ls:.2f} — long berlebihan")

    score = min(score + ls_score, 15)
    return score, sigs, ls

def layer_context(symbol, tickers_dict):
    """
    Layer 4: Sector rotation + CoinGecko trending (max 10 pts)
    """
    sector = SECTOR_LOOKUP.get(symbol, "MISC")
    peers  = SECTOR_MAP.get(sector, [])

    # Cari peer yang sudah pump
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

    # CoinGecko trending
    name = symbol.replace("USDT", "").replace("1000", "").upper()
    soc_score = 0
    soc_sig   = ""
    if name in get_cg_trending():
        soc_score = 4
        soc_sig   = f"🔥 {name} trending CoinGecko"

    sigs = [s for s in [sec_sig, soc_sig] if s]
    return min(sec_score + soc_score, 10), sigs, sector

def get_time_mult():
    """Multiplier berdasarkan window waktu pump historis."""
    h = utc_hour()
    if h in [5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.20, f"⏰ High-prob window ({h}:00 UTC)"
    if h in [1, 2, 3, 4]:
        return 0.80, "Low-prob window"
    return 1.0, ""


# ══════════════════════════════════════════════════════════════
#  🐋  WHALE DETECTION (improved dari v6)
# ══════════════════════════════════════════════════════════════
def calc_whale(symbol, candles_15m, funding):
    ws  = 0
    ev  = []
    cur = candles_15m[-1]["close"] if candles_15m else 0

    trades = get_trades(symbol, 300)
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

        # Iceberg detection: banyak order kecil di level harga yang sama
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

    # Harga flat 4h (stealth positioning dari 15m)
    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15
            ev.append("✅ Harga flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7
            ev.append("🔶 Harga relatif flat 4h")

    # Funding negative = smart money biarkan short masuk
    if -0.0004 <= funding <= -0.00002:
        ws += 10
        ev.append(f"✅ Funding {funding:.5f} — short squeeze setup")

    # Order book imbalance
    ob_ratio, bid_usd, ask_usd = get_orderbook(symbol)
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
    cls = (
        "🐋 WHALE ACCUMULATION"    if ws >= 65
        else "🦈 SMART MONEY"      if ws >= 45
        else "👀 POSSIBLE INST."   if ws >= 20
        else "🔇 NO SIGNAL"
    )
    return ws, cls, ev


# ══════════════════════════════════════════════════════════════
#  🎯  ENTRY ZONE CALCULATOR
# ══════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════
def master_score(symbol, ticker, tickers_dict):
    # Ambil semua data candle yang diperlukan
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candle_4h"])

    if len(c1h) < 48 or len(c15m) < 20:
        return None

    funding = get_funding(symbol)

    # ── GATES ──────────────────────────────────────────────────
    # Gate 7d (dilonggarkan dari v6)
    try:
        p7d_ago = c1h[-168]["close"] if len(c1h) >= 168 else c1h[0]["close"]
        chg_7d  = (c1h[-1]["close"] - p7d_ago) / p7d_ago * 100 if p7d_ago > 0 else 0
    except:
        chg_7d = 0

    if chg_7d > CONFIG["gate_chg_7d_max"]:
        log.info(f"  {symbol}: GATE 7d overbought (+{chg_7d:.1f}%)")
        return None
    if chg_7d < CONFIG["gate_chg_7d_min"]:
        log.info(f"  {symbol}: GATE 7d downtrend ({chg_7d:.1f}%)")
        return None
    if funding < CONFIG["gate_funding_extreme"]:
        log.info(f"  {symbol}: GATE funding ekstrem ({funding:.5f})")
        return None

    # Gate range 24h (pump sudah berjalan)
    if len(c1h) >= 24:
        h24 = max(c["high"] for c in c1h[-24:])
        l24 = min(c["low"]  for c in c1h[-24:])
        if l24 > 0 and (h24 - l24) / l24 * 100 > 55:
            log.info(f"  {symbol}: GATE range 24h terlalu lebar (pump sudah jalan)")
            return None

    score = 0
    sigs  = []
    bd    = {}

    # ── Layer 1: Volume Intelligence ──────────────────────────
    v_sc, v_sigs, rvol = layer_volume_intelligence(c1h)
    score += v_sc
    sigs  += v_sigs
    bd["vol"] = v_sc

    # ── Layer 2: Breakout Proximity (NEW) ─────────────────────
    bp_sc, bp_sigs = calc_breakout_proximity(c1h, ticker)
    score += bp_sc
    sigs  += bp_sigs
    bd["bp"] = bp_sc

    # ── Layer 3: Structure ────────────────────────────────────
    st_sc, st_sigs = layer_structure(c1h)
    score += st_sc
    sigs  += st_sigs
    bd["struct"] = st_sc

    # ── Layer 4: Positioning ──────────────────────────────────
    pos_sc, pos_sigs, ls_ratio = layer_positioning(symbol, funding)
    score += pos_sc
    sigs  += pos_sigs
    bd["pos"] = pos_sc

    # ── Layer 5: Multi-TF 4H Confluence (NEW) ─────────────────
    tf4h_sc = 0
    if c4h:
        tf4h_sc, tf4h_sig = calc_4h_confluence(c4h)
        if tf4h_sig:
            sigs.append(tf4h_sig)
    score += tf4h_sc
    bd["tf4h"] = tf4h_sc

    # ── Layer 6: Context ──────────────────────────────────────
    ctx_sc, ctx_sigs, sector = layer_context(symbol, tickers_dict)
    score += ctx_sc
    sigs  += ctx_sigs
    bd["ctx"] = ctx_sc

    # ── Whale Bonus ───────────────────────────────────────────
    ws, wcls, wev = calc_whale(symbol, c15m, funding)
    whale_bonus   = (
        15 if ws >= 65
        else 10 if ws >= 45
        else 5  if ws >= 25
        else 0
    )
    score += whale_bonus
    bd["whale"] = whale_bonus

    # ── Time Multiplier ───────────────────────────────────────
    tmult, tsig = get_time_mult()
    score = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    score = min(score, 100)

    # ── Entry Zones ───────────────────────────────────────────
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

    return {
        "symbol":   symbol,
        "score":    score,
        "signals":  sigs,
        "ws":       ws,
        "wcls":     wcls,
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
    }


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    sc  = r["score"]
    bar = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    e   = r["entry"]
    rk  = f"#{rank} " if rank else ""
    vol = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
           else f"${r['vol_24h']/1e3:.0f}K")
    ls  = f" | L/S:{r['ls_ratio']:.2f}" if r.get("ls_ratio") else ""

    msg = (
        f"🚨 <b>PRE-PUMP INTELLIGENCE {rk}</b>\n\n"
        f"<b>Symbol :</b> {r['symbol']}\n"
        f"<b>Score  :</b> {sc}/100  {bar}\n"
        f"<b>Sektor :</b> {r['sector']}\n"
        f"<b>Harga  :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h | {r['chg_7d']:+.1f}% 7d)\n"
        f"<b>Vol 24h:</b> {vol} | RVOL: {r['rvol']:.1f}x{ls}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐋 <b>WHALE: {r['ws']}/100</b> — {r['wcls']}\n"
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
        f"Whale:{bd.get('whale',0)}\n\n"
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
        msg += (
            f"{i}. <b>{r['symbol']}</b> [{r['score']}/100 {bar}]\n"
            f"   🐋{r['ws']} | RVOL:{r['rvol']:.1f}x | {vol} | T1:+{t1p:.0f}%\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════
#  🎯  SMART PRE-FILTER DARI TICKER (tanpa candle call)
# ══════════════════════════════════════════════════════════════
def pre_score_ticker(ticker):
    """
    Skor cepat dari data ticker SAJA — dipakai untuk prioritasi deep scan.
    Coin dengan pre-score tinggi = lebih dekat ke breakout = dianalisis duluan.
    """
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

    # Breakout proximity: semakin dekat high24h, semakin prioritas
    dist = (high24h - cur) / cur * 100 if cur > 0 and high24h > cur else 0
    if dist <= 1.0:   ps += 6   # Sangat dekat breakout
    elif dist <= 3.0: ps += 4
    elif dist <= 6.0: ps += 2
    elif dist > 20:   ps -= 2   # Jauh dari resistance = prioritas rendah

    # Sweet spot pergerakan harga (mulai bergerak tapi belum pump besar)
    if 0.5 <= chg24h <= 8:     ps += 3  # Ideal: sedang bergerak positif
    elif 8 < chg24h <= 15:     ps += 1  # Sudah naik tapi belum terlambat
    elif -5 <= chg24h < 0:     ps += 2  # Slight dip = potential reversal
    elif chg24h > 20:          ps -= 3  # Sudah pump besar

    # Range 24h yang sehat (tidak terlalu volatile, tidak terlalu flat)
    if low24h > 0:
        range24 = (high24h - low24h) / low24h * 100
        if 3 <= range24 <= 15: ps += 2  # Range sehat
        elif range24 > 40:     ps -= 2  # Sudah sangat volatile

    # Volume range yang baik
    if   200_000 <= vol <= 3_000_000: ps += 2
    elif 100_000 <= vol <  200_000:   ps += 1
    elif vol >     20_000_000:        ps -= 1   # Terlalu besar, susah pump signifikan

    return ps


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN — Dynamic Discovery + Smart Pre-filter
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v7.0 — {utc_now()} ===")
    log.info(f"Dynamic discovery: scan semua USDT-futures Bitget")

    # ── Phase 1: Get semua ticker (1 API call) ─────────────
    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total coin tersedia: {len(tickers)}")

    # ── Phase 2: Pre-filter + pre-score dari ticker saja ───
    candidates = []
    for sym, t in tickers.items():
        if not sym.endswith("USDT"):
            continue
        if is_cooldown(sym):
            continue

        try:
            vol   = float(t.get("quoteVolume", 0))
            chg   = float(t.get("change24h",   0)) * 100
            price = float(t.get("lastPr",       0))
        except:
            continue

        # Filter dasar (cepat, tanpa candle)
        if vol   < CONFIG["pre_filter_vol"]:          continue  # Terlalu sepi
        if vol   > CONFIG["max_vol_24h"]:             continue  # Terlalu besar
        if abs(chg) > CONFIG["gate_chg_24h_max"]:    continue  # Sudah pump/dump
        if price <= 0:                                 continue

        ps = pre_score_ticker(t)
        candidates.append((sym, ps, vol))

    # Sort: prioritaskan coin dekat breakout + volume sehat
    candidates.sort(key=lambda x: (-x[1], -x[2]))
    candidates = candidates[:CONFIG["max_deep_scan"]]

    log.info(f"Pre-filter lolos: {len(candidates)} coin → deep scan")

    # ── Phase 3: Deep analysis ─────────────────────────────
    results = []
    for i, (sym, ps, vol) in enumerate(candidates):
        t = tickers.get(sym)
        if not t:
            continue

        # Volume check lebih ketat (TANPA bug *24)
        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum")
            continue

        # Cek 24h change sekali lagi
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
                    f"RVOL={res['rvol']:.1f}x T1=+{res['entry']['liq_pct']:.1f}%"
                )
                if res["score"] >= CONFIG["min_score_alert"]:
                    results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    # ── Phase 4: Ranking dan alert ─────────────────────────
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

    # Kirim summary dulu jika ada lebih dari 2 kandidat
    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    # Kirim alert detail satu per satu
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"✅ Alert #{rank}: {r['symbol']} S={r['score']} W={r['ws']}")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v7.0 — START           ║")
    log.info("║  Dynamic Discovery | RVOL | CVD | BP     ║")
    log.info("╚══════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)

    run_scan()
