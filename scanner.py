"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v9.5                                                   ║
║                                                                          ║
║  GAME CHANGER BARU:                                                      ║
║                                                                          ║
║  GC-2: LIQUIDATION CASCADE DETECTOR                                      ║
║    Bitget /api/v2/mix/market/liquidation-orders dipakai untuk:           ║
║    a) Cluster short liquidation di atas harga = squeeze fuel (BONUS)     ║
║    b) Long liquidation massal baru saja terjadi = BLOCK (pump aborted)   ║
║                                                                          ║
║  GC-3: "LINEA SIGNATURE" — PRIMARY PRE-PUMP TEMPLATE LAYER              ║
║    Dari forensik: LINEA satu-satunya valid karena kombinasi:             ║
║    OI naik + harga turun/flat + RSI oversold + short dominan +           ║
║    futures inflow. Kini menjadi layer scoring tersendiri (max 25 poin).  ║
║                                                                          ║
║  GC-4: STRATIFIED PRE-FILTER (3 BUCKET — fix "80 coin yang sama")       ║
║    Problem v9.4: top-N flat → coin identik tiap run, 2 pump masif miss.  ║
║    Fix v9.5: 3 bucket independen:                                        ║
║    • SPIKE BUCKET (30): proxy volume-anomaly dari ticker saja            ║
║    • QUALITY BUCKET (35): top pre-score seperti biasa                    ║
║    • WILDCARD BUCKET (25): rotasi semi-random dari pool sisanya          ║
║    → Total 90 kandidat, rotasi tiap run, tidak ada coin yang stuck       ║
║                                                                          ║
║  BUG FIX:                                                                ║
║  BUG-A: BABA, AVGO + 5 stock tickers baru                               ║
║  BUG-B: OI 24h threshold bertingkat (-3/-5/-10/-20%) + hard block       ║
║  BUG-C: OI 1h threshold bertingkat (-2/-5/-8%)                          ║
║  BUG-D: Gate "already pumped" — OI 24h>25% + price>3% = block           ║
║  BUG-E: RVOL>5x + CVD negatif = penalti kuat (bukan reward)             ║
║  BUG-F: find_resistance_targets() — gantikan swing yang selalu +10%     ║
║  BUG-G: Score overflow — raw_score tidak di-cap sebelum composite        ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging, random
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
    # ── Threshold alert ───────────────────────────────────────
    "min_composite_alert":       52,
    "min_prob_alert":          0.50,
    "min_score_alert":           30,
    "min_whale_score":           15,
    "max_alerts_per_run":         8,

    # Bobot composite score
    "composite_w_layer":        0.55,
    "composite_w_prob":         0.45,

    # ── Volume 24h TOTAL (USD) ─────────────────────────────────
    "min_vol_24h":            3_000,
    "max_vol_24h":       50_000_000,
    "pre_filter_vol":         1_000,

    # ── Gate perubahan harga ───────────────────────────────────
    "gate_chg_24h_max":          30.0,
    "gate_chg_7d_max":           35.0,
    "gate_chg_7d_min":          -35.0,
    "gate_funding_extreme":      -0.002,

    # ── Candle limits ─────────────────────────────────────────
    "candle_1h":                168,
    "candle_15m":                96,
    "candle_4h":                 42,

    # ── Entry/exit ────────────────────────────────────────────
    "min_target_pct":             5.0,    # turun dari 8% agar T1 tidak selalu fallback
    "max_sl_pct":                12.0,
    "atr_sl_mult":                1.5,
    "atr_t1_mult":                2.5,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":       3600,
    "sleep_coins":               0.8,
    "sleep_error":               3.0,
    "cooldown_file":    "/tmp/v9_cooldown.json",
    "oi_snapshot_file": "/tmp/v9_oi.json",
    "wildcard_seed_file": "/tmp/v9_wildcard.json",  # GC-4: rotasi wildcard

    # ── GC-4: Stratified Pre-filter Buckets ───────────────────
    "bucket_spike":              30,     # coin volume-anomaly
    "bucket_quality":            35,     # top pre-score
    "bucket_wildcard":           25,     # rotasi random
    # total = 90 kandidat deep scan

    # ── Stealth pattern ───────────────────────────────────────
    "stealth_max_vol":       80_000,
    "stealth_min_coiling":       15,
    "stealth_max_range":          4.0,

    # ── Short squeeze ─────────────────────────────────────────
    "squeeze_funding_max":    -0.0001,
    "squeeze_oi_change_min":     3.0,

    # ── Layer max scores ──────────────────────────────────────
    "max_vol_score":             30,
    "max_flat_score":            20,
    "max_struct_score":          15,
    "max_pos_score":             15,
    "max_tf4h_score":             8,
    "max_ctx_score":             10,
    "max_whale_bonus":           20,
    "max_linea_score":           25,     # GC-3: layer baru

    # ── Pump probability weights ──────────────────────────────
    "prob_mvs_w1":         30,
    "prob_irr_w2":         20,
    "prob_avs_w3":         15,
    "prob_atr_w4":         20,
    "prob_slope_w5":       15,

    # ── GC-2: Liquidation Detector ────────────────────────────
    "liq_window_min":            30,     # window cek liquidasi (menit)
    "liq_long_block_usd":    100_000,    # long liq > $100K dalam 30m → waspada
    "liq_short_bonus_usd":   150_000,   # short liq > $150K = squeeze signal

    # ── GC-3: Linea Signature thresholds ─────────────────────
    "linea_oi_1h_min":            2.0,   # OI 1h naik minimal 2%
    "linea_oi_24h_min":           3.0,   # OI 24h naik minimal 3%
    "linea_rsi_max":             48.0,   # RSI di bawah 48 (oversold/netral rendah)
    "linea_ls_max":               1.1,   # L/S < 1.1 (short atau hampir seimbang)
    "linea_price_max_chg":        5.0,   # Harga belum naik > 5%
}

# ── STOCK_TICKERS — v9.5: +7 token dari audit BUG-A ─────────────────────
STOCK_TICKERS = {
    "CSCOUSDT","PEPUSDT","QQQUSDT","AAPLUSDT","MSFTUSDT","GOOGLUSDT",
    "INTCUSDT","AMDUSDT","NVDAUSDT","TSLAUSDT","AMZNUSDT","METAUSDT",
    "NFLXUSDT","ADBEUSDT","CRMUSDT","ORCLUSDT","IBMUSDT","SAPUSDT",
    "PYPLUSDT","UBERUSDT","LYFTUSDT","SPYUSDT","DIAUSDT","IWMUSDT",
    "MCDUSDT","KOLUSDT","DISUSDT","BRKUSDT","JPMCUSDT","BACHUSDT",
    "SBUXUSDT","NKEUSDT","WMTUSDT","COSTUSDT","HDUSTUSDT",
    "LLYUSDT","PFIZUSDT","JNJUSDT","ABBVUSDT","MRKUSDT","AMGNUSDT",
    "ASMLUSDT","TSMCUSDT",
    "HOODUSDT","COINUSDT",
    "GSUSDT","MSUSDT","BAMUSDT",
    "SNAPUSDT",
    # v9.3
    "FUTUUSDT","TIGRUSDT","MUUSDT","MRVLUSDT","QCOMUSDT","TXNUSDT",
    "SMHUSDT","FOUSDT","GMUSDT","RIVUSDT","LCIDUSDT","NIOOUSDT",
    "RDTUSDT","SPOTUSDT","RBLXUSDT","SHOPUSDT","ETSYUSDT",
    # v9.5 BUG-A FIX — CONFIRMED lolos alert v9.4
    "BABAUSDT",     # Alibaba
    "AVGOUSDT",     # Avago/Broadcom
    "BRKBUSDT",     # Berkshire Hathaway B
    "VISAUSDT",     # Visa Inc
    "MAUSDT",       # Mastercard
    "ABNBUSDT",     # Airbnb
    "AIRBNBUSDT",   # alias
}

MANUAL_EXCLUDE = set()

GRAN_MAP = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}

SECTOR_MAP = {
    "DEFI": [
        "SNXUSDT","ENSOUSDT","SIRENUSDT","CRVUSDT","CVXUSDT","COMPUSDT",
        "AAVEUSDT","UNIUSDT","DYDXUSDT","COWUSDT","PENDLEUSDT","MORPHOUSDT",
        "FLUIDUSDT","SSVUSDT","LRCUSDT","RSRUSDT","NMRUSDT","UMAUSDT","BALUSDT",
        "LDOUSDT","ENSUSDT",
    ],
    "ZK_PRIVACY": ["AZTECUSDT","MINAUSDT","STRKUSDT","ZORAUSDT","ZRXUSDT","POLYXUSDT"],
    "DESCI":      ["BIOUSDT","ATHUSDT"],
    "AI_CRYPTO":  [
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
        "ESPUSDT","TRXUSDT",
    ],
    "LAYER2": ["ARBUSDT","OPUSDT","CELOUSDT","STRKUSDT","LDOUSDT","POLUSDT","LINEAUSDT"],
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

EXCLUDED_KEYWORDS = ["XAU","PAXG","BTC","ETH","USDC","DAI","BUSD","UST","LUNC","LUNA"]


# ══════════════════════════════════════════════════════════════
#  🔒  COOLDOWN & OI SNAPSHOT
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

def load_oi_snapshots():
    try:
        if os.path.exists(CONFIG["oi_snapshot_file"]):
            with open(CONFIG["oi_snapshot_file"]) as f:
                return json.load(f)
    except:
        pass
    return {}

def save_oi_snapshot(symbol, oi_value):
    snaps = load_oi_snapshots()
    now   = time.time()
    if symbol not in snaps:
        snaps[symbol] = []
    snaps[symbol].append({"ts": now, "oi": oi_value})
    snaps[symbol] = sorted(snaps[symbol], key=lambda x: x["ts"])[-100:]
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(snaps, f)
    except:
        pass

def get_oi_changes(symbol, current_oi):
    """Return (chg1h, chg24h, valid) — valid=False jika belum ada 2 snapshot."""
    snaps = load_oi_snapshots()
    hist  = snaps.get(symbol, [])
    if len(hist) < 2:
        return 0, 0, False
    now = time.time()

    def nearest(target_ts):
        cands = [d for d in hist if abs(d["ts"] - target_ts) < 600]
        return min(cands, key=lambda d: abs(d["ts"] - target_ts)) if cands else None

    old1h  = nearest(now - 3600)
    old24h = nearest(now - 86400)
    chg1h  = (current_oi - old1h["oi"])  / old1h["oi"]  * 100 if old1h  and old1h["oi"]  else 0
    chg24h = (current_oi - old24h["oi"]) / old24h["oi"] * 100 if old24h and old24h["oi"] else 0
    return chg1h, chg24h, (old1h is not None)

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
                log.warning("Rate limit — tunggu 15s")
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
            timeout=15,
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
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        params={"productType": "usdt-futures"},
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
                "limit": str(limit), "productType": "usdt-futures"},
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
        params={"symbol": symbol, "productType": "usdt-futures"},
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
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            oi = data.get("data", {})
            if "openInterestList" in oi and oi["openInterestList"]:
                return float(oi["openInterestList"][0].get("size", 0))
            return float(oi.get("size", 0))
        except:
            pass
    return 0

def get_long_short_ratio(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/account-long-short-ratio",
        params={"symbol": symbol, "period": "1H",
                "limit": "4", "productType": "usdt-futures"},
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
        params={"symbol": symbol, "productType": "usdt-futures", "limit": str(limit)},
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
                "precision": "scale0", "limit": str(levels)},
    )
    if data and data.get("code") == "00000":
        try:
            book    = data["data"]
            bid_vol = sum(float(b[1]) for b in book.get("bids", []))
            ask_vol = sum(float(a[1]) for a in book.get("asks", []))
            total   = bid_vol + ask_vol
            ratio   = bid_vol / total if total > 0 else 0.5
            return ratio, bid_vol, ask_vol
        except:
            pass
    return 0.5, 0, 0

def get_liquidations(symbol):
    """
    GC-2: Ambil data liquidation order dari Bitget.
    Return: (long_liq_usd_30m, short_liq_usd_30m)
    long_liq  = orang yang long kena likuidasi (bearish signal jika besar)
    short_liq = orang yang short kena likuidasi (bullish signal — squeeze)
    """
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/liquidation-orders",
        params={"symbol": symbol, "productType": "usdt-futures",
                "pageSize": "100"},
    )
    if not data or data.get("code") != "00000":
        return 0, 0
    try:
        orders = data.get("data", {}).get("liquidationOrderList", [])
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - CONFIG["liq_window_min"] * 60 * 1000
        long_liq  = 0.0
        short_liq = 0.0
        for o in orders:
            ts   = int(o.get("cTime", 0))
            if ts < cutoff:
                continue
            usd  = float(o.get("size", 0)) * float(o.get("fillPrice", 0))
            side = o.get("side", "").lower()
            # 'sell' liquidation = long position kena liq
            # 'buy' liquidation  = short position kena liq
            if "sell" in side:
                long_liq += usd
            else:
                short_liq += usd
        return long_liq, short_liq
    except:
        return 0, 0

def get_rsi(candles, period=14):
    """Hitung RSI dari candles."""
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))

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
        tp     = (c["high"] + c["low"] + c["close"]) / 3
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
    pmin  = min(c["low"]  for c in candles)
    pmax  = max(c["high"] for c in candles)
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

def find_resistance_targets(candles_1h, cur):
    """
    BUG-F FIX: Gantikan find_swing_targets yang selalu fallback ke +10%.
    Gunakan resistance levels berbasis volume (high yang sering disentuh).
    Lebih reliabel untuk menentukan target realistis.
    """
    if len(candles_1h) < 24:
        return cur * 1.10, cur * 1.18

    recent = candles_1h[-168:]
    resistance_levels = []
    min_t = cur * (1 + CONFIG["min_target_pct"] / 100)

    for i in range(2, len(recent) - 2):
        h = recent[i]["high"]
        if h <= min_t:
            continue
        # Hitung berapa kali harga mendekati level ini (±1.5%)
        touches = sum(
            1 for c in recent
            if abs(c["high"] - h) / h < 0.015 or abs(c["low"] - h) / h < 0.015
        )
        if touches >= 2:
            resistance_levels.append((h, touches, recent[i]["volume_usd"]))

    if not resistance_levels:
        # Fallback: ATR-based target
        atr = calc_atr(candles_1h[-24:]) or cur * 0.02
        return round(cur * 1.10, 8), round(cur * 1.18, 8)

    # Sort by proximity ke harga sekarang
    resistance_levels.sort(key=lambda x: x[0])
    t1 = resistance_levels[0][0]
    t2 = resistance_levels[1][0] if len(resistance_levels) > 1 else t1 * 1.08
    return round(t1, 8), round(t2, 8)


# ══════════════════════════════════════════════════════════════
#  📊  INDIKATOR
# ══════════════════════════════════════════════════════════════
def calc_rvol(candles_1h):
    """RVOL dengan cap 30x (v9.4 fix dipertahankan)."""
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
    return min(last_vol / avg, 30.0) if avg > 0 else 1.0

def calc_volume_spike_ratio(candles_1h):
    if len(candles_1h) < 24:
        return 1.0, 1.0
    vols    = [c["volume_usd"] for c in candles_1h]
    baseline = sorted(vols[:-6])[:int(len(vols) * 0.6)]
    base_avg = sum(baseline) / len(baseline) if baseline else 1
    if base_avg <= 0:
        return 1.0, 1.0
    recent_vols = vols[-6:]
    spikes      = [v / base_avg for v in recent_vols]
    return max(spikes) if spikes else 1.0, sum(spikes) / len(spikes) if spikes else 1.0

def calc_volume_irregularity(candles_1h):
    window = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    vols   = [c["volume_usd"] for c in window]
    if not vols:
        return 0.0
    mean = sum(vols) / len(vols)
    if mean <= 0:
        return 0.0
    std  = math.sqrt(sum((v - mean) ** 2 for v in vols) / len(vols))
    return std / mean

def calc_normalized_atr(candles_1h):
    cur = candles_1h[-1]["close"] if candles_1h else 0
    if cur <= 0:
        return 0.0
    atr = calc_atr(candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h)
    return (atr / cur) * 100 if atr else 0.0

def calc_price_slope(candles_1h):
    window = candles_1h[-12:] if len(candles_1h) >= 12 else candles_1h
    if len(window) < 2:
        return 0.0
    p_start = window[0]["close"]
    p_end   = window[-1]["close"]
    return (p_end - p_start) / p_start / len(window) if p_start > 0 else 0.0

def calc_cvd_signal(candles_1h):
    """
    CVD divergence — v9.4: tambah tier negatif untuk distribusi.
    """
    if len(candles_1h) < 12:
        return 0, ""
    window    = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    cvd, cvd_vals = 0, []
    for c in window:
        rng = c["high"] - c["low"]
        buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        cvd += (buy_ratio * 2 - 1) * c["volume_usd"]
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
            return 8,  f"🔍 CVD naik, harga flat — hidden accumulation"
    elif cvd_rising and 1.5 <= price_chg <= 5.0:
        return 5, f"CVD bullish, harga naik sehat ({price_chg:+.1f}%)"

    if not cvd_rising and price_chg > 1.5:
        return -12, f"⚠️ CVD turun saat harga naik {price_chg:+.1f}% — distribusi tersembunyi"
    elif not cvd_rising and -1.5 <= price_chg <= 1.5:
        return -8, f"⚠️ CVD turun, harga flat — tekanan jual tersembunyi"
    elif not cvd_rising and price_chg < -1.5:
        return -5, f"⚠️ CVD turun saat harga {price_chg:+.1f}% — tren jual berlanjut"
    return 0, ""

def calc_short_term_cvd(candles_1h):
    """CVD window 6h — lebih sensitif terhadap distribusi baru."""
    if len(candles_1h) < 12:
        return 0, ""
    recent = candles_1h[-6:]
    prev   = candles_1h[-12:-6]

    def cvd_delta(candles):
        delta = 0.0
        for c in candles:
            rng = c["high"] - c["low"]
            buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
            delta += (buy_ratio * 2 - 1) * c["volume_usd"]
        return delta

    recent_d = cvd_delta(recent)
    prev_d   = cvd_delta(prev)
    cur      = candles_1h[-1]["close"]
    p6h      = candles_1h[-6]["close"] if len(candles_1h) >= 6 else cur
    price_chg_6h = (cur - p6h) / p6h * 100 if p6h > 0 else 0

    if recent_d < 0 and recent_d < prev_d * 0.8:
        if price_chg_6h > -2:
            return -10, f"⚠️ CVD 6h memburuk — tekanan jual meningkat ({price_chg_6h:+.1f}% 6h)"
        return -5, f"⚠️ CVD 6h negatif — distribusi aktif"
    if recent_d > 0 and recent_d > prev_d * 1.2:
        if price_chg_6h < 2:
            return 8, f"✅ CVD 6h membaik — akumulasi baru ({price_chg_6h:+.1f}% 6h)"
        return 4, f"CVD 6h positif — buying momentum"
    return 0, ""

def detect_higher_lows(candles):
    if len(candles) < 6:
        return 0, ""
    lows       = [c["low"] for c in candles]
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
        return 8, f"📐 {ascending + 1}x Higher Lows — ascending triangle"
    elif ascending >= 1:
        return 4, f"📐 Higher Low — struktur bullish mulai"
    return 0, ""

def candle_quality_score(candles_1h):
    if len(candles_1h) < 3:
        return 0, ""
    last = candles_1h[-2]
    prev = candles_1h[-3]
    body       = abs(last["close"] - last["open"])
    rng        = last["high"] - last["low"]
    if rng <= 0:
        return 0, ""
    body_ratio  = body / rng
    upper_wick  = (last["high"] - max(last["close"], last["open"])) / rng
    is_bullish  = last["close"] > last["open"]
    is_breakout = last["close"] > prev["high"]
    if is_bullish and body_ratio > 0.70 and upper_wick < 0.15:
        return 7, f"💚 Marubozu bullish (body {body_ratio:.0%})"
    elif is_bullish and body_ratio > 0.55:
        return 5, f"💚 Candle bullish kuat (body {body_ratio:.0%})"
    elif is_bullish and is_breakout:
        return 4, f"📈 Breakout di atas high sebelumnya"
    elif not is_bullish and body_ratio > 0.6:
        return -3, ""
    return 0, ""


# ══════════════════════════════════════════════════════════════
#  🔬  PUMP PROBABILITY MODEL
# ══════════════════════════════════════════════════════════════
def compute_pump_probability(candles_1h, whale_score=0):
    if len(candles_1h) < 24:
        return {"probability_score": 0.3, "classification": "Data Kurang", "metrics": {}}

    max_spike, avg_spike = calc_volume_spike_ratio(candles_1h)
    irr       = calc_volume_irregularity(candles_1h)
    norm_atr  = calc_normalized_atr(candles_1h)
    slope     = calc_price_slope(candles_1h)

    def clamp(v, lo, hi):
        return max(0.0, min(1.0, (v - lo) / (hi - lo))) if hi > lo else 0.5

    n_mvs   = clamp(max_spike,  1.0, 10.0)
    n_irr   = clamp(irr,        0.5,  3.5)
    n_avs   = clamp(avg_spike,  0.5,  2.0)
    n_atr   = 1.0 - clamp(norm_atr, 0.05, 3.0)
    n_slp   = 1.0 - clamp(slope, -0.001, 0.002)
    n_whale = whale_score / 100.0

    score = max(0.0, min(1.0,
        n_mvs   * 0.28 +
        n_irr   * 0.20 +
        n_avs   * 0.12 +
        n_atr   * 0.22 +
        n_slp   * 0.13 +
        n_whale * 0.05
    ))

    if score < 0.30:   cls = "Noise"
    elif score < 0.45: cls = "Sideways"
    elif score < 0.60: cls = "Accumulation"
    elif score < 0.75: cls = "Pre-Pump"
    else:              cls = "Imminent Pump"

    return {
        "probability_score": score,
        "classification":    cls,
        "metrics": {
            "max_vol_spike":    round(max_spike, 2),
            "avg_vol_spike":    round(avg_spike, 2),
            "vol_irregularity": round(irr, 3),
            "norm_atr_pct":     round(norm_atr, 4),
            "price_slope":      round(slope, 8),
        },
    }


# ══════════════════════════════════════════════════════════════
#  🏗️  LAYER SCORING
# ══════════════════════════════════════════════════════════════

def layer_volume_intelligence(candles_1h):
    score, sigs = 0, []
    rvol = calc_rvol(candles_1h)

    if rvol >= 4.0:
        score += 16; sigs.append(f"🔥🔥 RVOL {rvol:.1f}x — volume MASIF vs historis!")
    elif rvol >= 2.8:
        score += 13; sigs.append(f"🔥 RVOL {rvol:.1f}x — volume spike signifikan")
    elif rvol >= 2.0:
        score += 10; sigs.append(f"RVOL {rvol:.1f}x — volume mulai bangun")
    elif rvol >= 1.4:
        score += 6;  sigs.append(f"RVOL {rvol:.1f}x — di atas normal")
    elif rvol >= 1.1:
        score += 3
    elif rvol < 0.4:
        score -= 4

    irr = calc_volume_irregularity(candles_1h)
    if irr >= 2.5:
        score += 10; sigs.append(f"📈 Vol Irregularity {irr:.2f} — whale masuk tidak merata")
    elif irr >= 1.8:
        score += 6;  sigs.append(f"Vol Irregularity {irr:.2f} — aktivitas whale")
    elif irr >= 1.3:
        score += 3

    cvd_s, cvd_sig = calc_cvd_signal(candles_1h)
    score += cvd_s
    if cvd_sig:
        sigs.append(cvd_sig)

    # BUG-E FIX: RVOL tinggi + CVD short-term negatif = distribusi, bukan akumulasi
    if rvol >= 5.0:
        stcvd_check, _ = calc_short_term_cvd(candles_1h)
        if stcvd_check < 0:
            penalty = int(min(rvol, 30) * 0.6)
            score  -= penalty
            sigs.append(f"⚠️ RVOL {rvol:.1f}x + CVD negatif — volume = distribusi/likuidasi")

    return min(score, CONFIG["max_vol_score"]), sigs, rvol


def layer_flat_accumulation(candles_1h):
    score, sigs = 0, []
    if len(candles_1h) < 12:
        return 0, sigs

    if len(candles_1h) >= 24:
        high24 = max(c["high"] for c in candles_1h[-24:])
        low24  = min(c["low"]  for c in candles_1h[-24:])
        range24_pct = (high24 - low24) / low24 * 100 if low24 > 0 else 99
    else:
        range24_pct = 99

    if range24_pct < 3:
        score += 15; sigs.append(f"🎯 Range 24h sangat sempit ({range24_pct:.1f}%) — zona akumulasi tight")
    elif range24_pct < 6:
        score += 10; sigs.append(f"🎯 Range 24h sempit ({range24_pct:.1f}%) — akumulasi aktif")
    elif range24_pct < 10:
        score += 5;  sigs.append(f"Range 24h terbatas ({range24_pct:.1f}%)")
    elif range24_pct < 15:
        score += 2
    elif range24_pct > 40:
        score -= 5

    slope = calc_price_slope(candles_1h)
    if slope < -0.0003:
        score += 8; sigs.append(f"📉 Harga turun perlahan ({slope*1e4:.2f}‱/h) — bahan bakar squeeze")
    elif abs(slope) <= 0.0003:
        score += 5; sigs.append(f"➡️ Harga sangat flat — akumulasi tersembunyi")
    elif slope < 0.001:
        score += 2

    hl_sc, hl_sig = detect_higher_lows(candles_1h[-16:] if len(candles_1h) >= 16 else candles_1h)
    score += hl_sc
    if hl_sig:
        sigs.append(hl_sig)

    cq_sc, cq_sig = candle_quality_score(candles_1h)
    score += cq_sc
    if cq_sig:
        sigs.append(cq_sig)

    return min(max(score, 0), CONFIG["max_flat_score"]), sigs


def layer_structure(candles_1h):
    score, sigs = 0, []

    bbw_val, bbw_pct = bbw_percentile(candles_1h)
    if bbw_pct < 10:
        score += 10; sigs.append(f"BBW Squeeze Ekstrem ({bbw_pct:.0f}%ile) — siap meledak")
    elif bbw_pct < 25:
        score += 7;  sigs.append(f"BBW Squeeze Kuat ({bbw_pct:.0f}%ile)")
    elif bbw_pct < 45:
        score += 4;  sigs.append(f"BBW Menyempit ({bbw_pct:.0f}%ile)")
    elif bbw_pct > 85:
        score -= 5;  sigs.append(f"⚠️ BBW Melebar ({bbw_pct:.0f}%ile) — volatilitas sudah terjadi")

    coiling = 0
    for c in reversed(candles_1h[-72:]):
        body = abs(c["close"] - c["open"]) / c["open"] * 100 if c["open"] else 99
        if body < 1.0:
            coiling += 1
        else:
            break
    if coiling >= 18:
        score += 5; sigs.append(f"Coiling {coiling}h — energi terkumpul lama")
    elif coiling >= 10:
        score += 3; sigs.append(f"Coiling {coiling}h")
    elif coiling >= 5:
        score += 1

    return min(score, CONFIG["max_struct_score"]), sigs, bbw_val, bbw_pct, coiling


def layer_positioning(symbol, funding, oi_chg1h):
    """
    v9.4 FIX BUG 1+2 dipertahankan:
    - Funding 5 tier presisi (true neutral < ±0.001%)
    - L/S gap 1.15-2.0 diisi penalti bertingkat
    """
    score, sigs = 0, []

    if funding <= -0.0004:
        score += 8;  sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup KUAT!")
    elif -0.0004 < funding <= -0.00001:
        score += 6;  sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup")
    elif abs(funding) < 0.00001:
        score += 4;  sigs.append(f"Funding {funding:.5f} — benar-benar netral")
    elif 0.00001 <= funding <= 0.0001:
        score += 1
    elif 0.0001 < funding <= 0.0003:
        score += 0
    elif funding > 0.0003:
        score -= 5;  sigs.append(f"⚠️ Funding {funding:.5f} — long overcrowded, risiko dump")

    if (funding <= CONFIG["squeeze_funding_max"]
            and oi_chg1h > CONFIG["squeeze_oi_change_min"]):
        score += 10
        sigs.append(f"🔥 SHORT SQUEEZE: funding negatif, OI 1h +{oi_chg1h:.1f}%")

    ls       = get_long_short_ratio(symbol)
    ls_score = 0
    if ls is not None:
        if ls < 0.6:
            ls_score = 10; sigs.append(f"🎯 L/S {ls:.2f} — short dominan, squeeze fuel besar!")
        elif ls < 0.75:
            ls_score = 8;  sigs.append(f"🎯 L/S {ls:.2f} — short dominan")
        elif ls < 0.9:
            ls_score = 5;  sigs.append(f"L/S {ls:.2f} — lebih banyak short")
        elif ls <= 1.15:
            ls_score = 2
        elif 1.15 < ls <= 1.5:
            ls_score = -3; sigs.append(f"L/S {ls:.2f} — longs mulai dominan")
        elif 1.5 < ls <= 2.0:
            ls_score = -6; sigs.append(f"⚠️ L/S {ls:.2f} — longs dominan, squeeze fuel berkurang")
        elif ls > 3.0:
            ls_score = -15; sigs.append(f"⚠️⚠️ L/S {ls:.2f} — long overcrowded ekstrem!")
        elif ls > 2.5:
            ls_score = -9;  sigs.append(f"⚠️ L/S {ls:.2f} — long sangat dominan")
        elif ls > 2.0:
            ls_score = -5;  sigs.append(f"L/S {ls:.2f} — long dominan")

    return min(score + ls_score, CONFIG["max_pos_score"]), sigs, ls


def calc_4h_confluence(candles_4h):
    if len(candles_4h) < 6:
        return 0, ""
    closes    = [c["close"] for c in candles_4h]
    p_now     = closes[-1]
    p_7d      = closes[0]
    p_48h     = closes[-12] if len(closes) >= 12 else closes[0]
    trend_7d  = (p_now - p_7d)  / p_7d  * 100 if p_7d  > 0 else 0
    trend_48h = (p_now - p_48h) / p_48h * 100 if p_48h > 0 else 0
    if trend_48h > 2 and -10 <= trend_7d <= 15:
        return 6, f"📊 4H: reversal bullish 48h +{trend_48h:.1f}%, trend 7d sehat"
    elif trend_48h > 0 and trend_7d > -15:
        return 3, f"📊 4H upward bias ({trend_48h:+.1f}% 48h)"
    elif trend_48h < -8:
        return -5, f"⚠️ 4H masih downtrend ({trend_48h:+.1f}% 48h)"
    return 0, ""


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
    sec_score, sec_sig = 0, ""
    if pumped:
        top       = pumped[0]
        sec_score = 5 if top[1] > 20 else 3 if top[1] > 12 else 1
        sec_sig   = f"🔄 {sector}: {top[0]} +{top[1]:.0f}% — rotasi potensial"
    name       = symbol.replace("USDT", "").replace("1000", "").upper()
    soc_score, soc_sig = 0, ""
    if name in get_cg_trending():
        soc_score = 3
        soc_sig   = f"🔥 {name} trending CoinGecko"
    sigs = [s for s in [sec_sig, soc_sig] if s]
    return min(sec_score + soc_score, CONFIG["max_ctx_score"]), sigs, sector


def calc_whale(symbol, candles_15m, funding):
    ws, ev = 0, []
    cur = candles_15m[-1]["close"] if candles_15m else 0

    trades = get_trades(symbol, 500)
    if trades:
        buy_v  = sum(t["size"] for t in trades if t["side"] == "buy")
        tot_v  = sum(t["size"] for t in trades)
        tr     = buy_v / tot_v if tot_v > 0 else 0.5

        if tr > 0.70:
            ws += 30; ev.append(f"✅ Taker Buy {tr:.0%} — pembeli sangat dominan")
        elif tr > 0.62:
            ws += 15; ev.append(f"🔶 Taker Buy {tr:.0%} — bias beli")

        total_usd = sum(t["size"] * t["price"] for t in trades)
        avg_trade = total_usd / len(trades) if trades else 1
        thr       = max(avg_trade * 5, 3_000)
        lbuy_usd  = sum(
            t["size"] * t["price"] for t in trades
            if t["side"] == "buy" and t["size"] * t["price"] > thr
        )
        if total_usd > 0 and lbuy_usd / total_usd > 0.28:
            ws += 25; ev.append(f"✅ Smart money {lbuy_usd/total_usd:.0%} vol")

        if cur > 0:
            tol    = 0.15 if cur >= 10 else (0.30 if cur >= 1 else 0.50)
            at_lvl = [
                t for t in trades
                if t["side"] == "buy" and abs(t["price"] - cur) / cur * 100 < tol
            ]
            if len(at_lvl) >= 10:
                tot_ice = sum(t["size"] * t["price"] for t in at_lvl)
                avg_ice = tot_ice / len(at_lvl)
                if len(at_lvl) >= 14 and avg_ice < thr * 0.25 and tot_ice > thr * 2.5:
                    ws += 20; ev.append(f"✅ Iceberg: {len(at_lvl)} tx kecil (${tot_ice:,.0f})")

    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15; ev.append("✅ Harga flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7;  ev.append("🔶 Harga relatif flat 4h")

    if -0.0004 <= funding <= -0.00002:
        ws += 10; ev.append(f"✅ Funding {funding:.5f} — short squeeze fuel")

    ob_ratio, bid_vol, ask_vol = get_orderbook(symbol, 50)
    if ob_ratio > 0.65:
        ws += 15; ev.append(f"✅ OB Bid {ob_ratio:.0%} — tekanan beli di book")
    elif ob_ratio > 0.55:
        ws += 7;  ev.append(f"🔶 OB Bid {ob_ratio:.0%}")
    elif ob_ratio < 0.35:
        ws -= 10; ev.append(f"⚠️ OB Ask dominan — tekanan jual lebih besar")

    return min(ws, 100), min(ws, 100) // 5, ev


def get_time_mult():
    h = utc_hour()
    if h in [5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.15, f"⏰ High-prob window ({h}:00 UTC)"
    if h in [1, 2, 3, 4]:
        return 0.85, "Low-prob window"
    return 1.0, ""


# ══════════════════════════════════════════════════════════════
#  🔴 GC-2: LIQUIDATION CASCADE DETECTOR
# ══════════════════════════════════════════════════════════════
def layer_liquidation(symbol, candles_1h):
    """
    GC-2: Analisis liquidation order untuk deteksi:
    a) Long liquidation massal baru → coin baru saja kena long flush
       → kemungkinan bounce, tapi hati-hati dead cat bounce
    b) Short liquidation cluster → short squeeze sedang terjadi = bullish
    
    Return: (score, signals, long_liq_usd, short_liq_usd, should_block)
    """
    long_liq, short_liq = get_liquidations(symbol)
    score, sigs = 0, []
    should_block = False

    # Block jika long liquidation masif terjadi baru-baru ini
    # → ini artinya coin baru saja kena dump, bukan pre-pump
    if long_liq > CONFIG["liq_long_block_usd"] * 3:
        should_block = True
        sigs.append(f"🚨 Long liq masif ${long_liq/1e3:.0f}K dalam 30m — pump aborted!")
    elif long_liq > CONFIG["liq_long_block_usd"]:
        score -= 15
        sigs.append(f"⚠️ Long liq ${long_liq/1e3:.0f}K — posisi longs baru saja dihancurkan")
    elif long_liq > CONFIG["liq_long_block_usd"] * 0.5:
        score -= 7
        sigs.append(f"Long liq ${long_liq/1e3:.0f}K — tekanan jual terindikasi")

    # Bonus jika short liquidation cluster → short squeeze signal
    if short_liq > CONFIG["liq_short_bonus_usd"] * 2:
        score += 20
        sigs.append(f"🔥🔥 Short liq ${short_liq/1e3:.0f}K — SHORT SQUEEZE AKTIF!")
    elif short_liq > CONFIG["liq_short_bonus_usd"]:
        score += 12
        sigs.append(f"🔥 Short liq ${short_liq/1e3:.0f}K — tekanan squeeze meningkat")
    elif short_liq > CONFIG["liq_short_bonus_usd"] * 0.5:
        score += 6
        sigs.append(f"Short liq ${short_liq/1e3:.0f}K — short mulai kena squeeze")

    return score, sigs, long_liq, short_liq, should_block


# ══════════════════════════════════════════════════════════════
#  🔴 GC-3: LINEA SIGNATURE LAYER
# ══════════════════════════════════════════════════════════════
def layer_linea_signature(candles_1h, oi_chg1h, oi_chg24h, oi_valid,
                           ls_ratio, funding, chg_24h):
    """
    GC-3: "Linea Signature" — template pre-pump yang paling valid dari forensik.
    
    LINEA (satu-satunya valid dari 8 alert) memiliki kombinasi:
    - OI 1h naik masif (+9.80%)   → posisi baru masuk cepat
    - OI 24h naik (+14.31%)       → akumulasi berlanjut, bukan distribusi
    - Harga turun (-3.53%)        → DIVERGENCE BULLISH (OI naik, harga turun)
    - RSI oversold (32.47)        → seller exhaustion, siap reversal
    - L/S < 1 (0.708)             → short masih dominan = fuel squeeze ada
    - Futures flow inflow         → smart money baru masuk
    
    Semakin banyak komponen yang terpenuhi, semakin tinggi score.
    Score maksimal (full signature): 25 poin
    """
    score, sigs, components = 0, [], 0

    if not oi_valid:
        return 0, [], 0

    # Komponen 1: OI 1h naik signifikan
    oi_1h_ok = oi_chg1h >= CONFIG["linea_oi_1h_min"]
    if oi_chg1h >= 8.0:
        score += 8; components += 1
        sigs.append(f"✅ [Linea-1] OI 1h +{oi_chg1h:.1f}% — posisi baru masuk MASIF")
    elif oi_chg1h >= 4.0:
        score += 5; components += 1
        sigs.append(f"✅ [Linea-1] OI 1h +{oi_chg1h:.1f}% — posisi baru masuk")
    elif oi_1h_ok:
        score += 3; components += 1

    # Komponen 2: OI 24h naik (bukan distribusi)
    oi_24h_ok = oi_chg24h >= CONFIG["linea_oi_24h_min"]
    if oi_chg24h >= 10.0:
        score += 6; components += 1
        sigs.append(f"✅ [Linea-2] OI 24h +{oi_chg24h:.1f}% — akumulasi berlanjut")
    elif oi_24h_ok:
        score += 3; components += 1

    # Komponen 3: Harga turun/flat saat OI naik = DIVERGENCE BULLISH KUAT
    price_ok = chg_24h <= CONFIG["linea_price_max_chg"]
    if oi_1h_ok and oi_24h_ok and chg_24h < 0:
        score += 8; components += 1
        sigs.append(f"✅ [Linea-3] OI naik + Harga {chg_24h:+.1f}% — DIVERGENCE BULLISH!")
    elif oi_1h_ok and price_ok:
        score += 4; components += 1
        sigs.append(f"[Linea-3] OI naik + Harga flat — akumulasi tersembunyi")

    # Komponen 4: RSI — oversold (butuh candles, dihitung di caller)
    # Note: RSI dihitung dan dipass sebagai parameter terpisah
    # (lihat penggunaan di master_score)

    # Komponen 5: L/S ratio — short masih dominan
    if ls_ratio is not None:
        ls_ok = ls_ratio <= CONFIG["linea_ls_max"]
        if ls_ratio < 0.75:
            score += 6; components += 1
            sigs.append(f"✅ [Linea-5] L/S {ls_ratio:.2f} — short sangat dominan = fuel besar")
        elif ls_ok:
            score += 3; components += 1

    # Bonus: Full Linea Signature (4+ komponen terpenuhi)
    if components >= 4:
        score += 5
        sigs.append(f"⭐ FULL LINEA SIGNATURE ({components}/5 komponen) — pre-pump template!")
    elif components >= 3:
        sigs.append(f"[Linea] {components}/5 komponen — setup berkembang")

    return min(score, CONFIG["max_linea_score"]), sigs, components


# ══════════════════════════════════════════════════════════════
#  💰  ENTRY ZONE CALCULATOR
# ══════════════════════════════════════════════════════════════
def calc_entry(candles_1h):
    cur  = candles_1h[-1]["close"]
    atr  = calc_atr(candles_1h, 14) or cur * 0.02
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
    entry  = min(support * 1.002, cur * 0.998)
    sl     = max(entry - CONFIG["atr_sl_mult"] * atr, entry * 0.88)

    # BUG-F FIX: gunakan find_resistance_targets, bukan find_swing_targets
    t1_res, t2_res = find_resistance_targets(candles_1h, cur)
    t1_atr         = entry + CONFIG["atr_t1_mult"] * atr
    t1             = t1_res if t1_res > cur * 1.05 else t1_atr
    if t1 <= cur * 1.05:
        t1 = cur * 1.10
    t2     = t2_res if t2_res > t1 * 1.02 else t1 * 1.08
    risk   = entry - sl
    reward = t1 - entry
    rr     = round(reward / risk, 1) if risk > 0 else 0
    t1_pct = round((t1 - cur) / cur * 100, 1)
    sl_pct = round((entry - sl) / entry * 100, 1)
    return {
        "cur":    cur,
        "atr":    round(atr, 8),
        "vwap":   round(vwap, 8) if vwap else 0,
        "z1":     round(z1, 8)   if z1   else 0,
        "z2":     round(z2, 8),
        "entry":  round(entry, 8),
        "sl":     round(sl, 8),
        "sl_pct": sl_pct,
        "t1":     round(t1, 8),
        "t2":     round(t2, 8),
        "rr":     rr,
        "liq_pct": t1_pct,
    }


# ══════════════════════════════════════════════════════════════
#  🛡️  GATE: SUDAH PUMP (BUG-D FIX)
# ══════════════════════════════════════════════════════════════
def is_already_pumped(oi_chg24h, vol_chg24h_pct, chg_24h, oi_valid):
    """
    BUG-D FIX: Gate "sudah pump" yang kuat.
    
    BARD di v9.4: OI 24h +38.57% + Volume +214% → BUKAN pre-pump, ini POST-PUMP.
    Sekarang: kombinasi OI besar + volume masif + harga naik → block langsung.
    
    Return: (should_block, reason)
    """
    if not oi_valid:
        # Jika OI tidak valid, pakai hanya price + volume
        if chg_24h > 15 and vol_chg24h_pct > 150:
            return True, f"Harga +{chg_24h:.0f}% + Volume +{vol_chg24h_pct:.0f}% — pump sudah terjadi"
        return False, ""

    # OI naik masif + harga naik = posisi dibuka SETELAH pump, bukan sebelum
    if oi_chg24h > 35 and chg_24h > 3:
        return True, f"OI 24h +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — TERLAMBAT (post-pump)"
    if oi_chg24h > 25 and chg_24h > 5:
        return True, f"OI 24h +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — pump sudah berjalan"
    if oi_chg24h > 15 and chg_24h > 10:
        return True, f"OI +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — momentum sudah habis"
    # Volume masif + harga naik tanpa OI validation
    if vol_chg24h_pct > 200 and chg_24h > 8:
        return True, f"Volume +{vol_chg24h_pct:.0f}% + Harga +{chg_24h:.0f}% — crowd sudah masuk"
    return False, ""


# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════
def master_score(symbol, ticker, tickers_dict):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candle_4h"])

    if len(c1h) < 48 or len(c15m) < 20:
        return None

    funding = get_funding(symbol)

    # ── GATES ─────────────────────────────────────────────────
    try:
        p7d_ago = c1h[-168]["close"] if len(c1h) >= 168 else c1h[0]["close"]
        chg_7d  = (c1h[-1]["close"] - p7d_ago) / p7d_ago * 100 if p7d_ago > 0 else 0
    except:
        chg_7d = 0

    try:
        chg_24h = float(ticker.get("change24h", 0)) * 100
        vol_24h = float(ticker.get("quoteVolume", 0))
    except:
        chg_24h, vol_24h = 0, 0

    if chg_7d > CONFIG["gate_chg_7d_max"]:
        oi_value = get_open_interest(symbol)
        oi_chg_1h, _, oi_valid_gate = get_oi_changes(symbol, oi_value) if oi_value > 0 else (0, 0, False)
        real_squeeze = (funding <= CONFIG["squeeze_funding_max"]
                        and oi_valid_gate
                        and oi_chg_1h > CONFIG["squeeze_oi_change_min"])
        if not real_squeeze:
            log.info(f"  {symbol}: GATE overbought ({chg_7d:.1f}%)")
            return None

    if chg_7d < CONFIG["gate_chg_7d_min"]:
        log.info(f"  {symbol}: GATE downtrend ({chg_7d:.1f}%)")
        return None
    if funding < CONFIG["gate_funding_extreme"]:
        log.info(f"  {symbol}: GATE funding ekstrem ({funding:.5f})")
        return None

    if len(c1h) >= 6:
        pre6       = c1h[-6:]
        avg_vol_6h = sum(c["volume_usd"] for c in pre6) / 6
        high_6h    = max(c["high"] for c in pre6)
        low_6h     = min(c["low"]  for c in pre6)
        range_6h   = (high_6h - low_6h) / low_6h * 100 if low_6h > 0 else 0
    else:
        avg_vol_6h, range_6h = 0, 0

    score, sigs, bd = 0, [], {}

    # Layer 1: Volume Intelligence
    v_sc, v_sigs, rvol = layer_volume_intelligence(c1h)
    score += v_sc;  sigs += v_sigs;  bd["vol"] = v_sc

    # Short-term CVD
    stcvd_sc, stcvd_sig = calc_short_term_cvd(c1h)
    score += stcvd_sc
    if stcvd_sig:
        sigs.append(stcvd_sig)
    bd["stcvd"] = stcvd_sc

    # Layer 2: Flat Accumulation
    fa_sc, fa_sigs = layer_flat_accumulation(c1h)
    score += fa_sc;  sigs += fa_sigs;  bd["flat"] = fa_sc

    # Layer 3: Structure
    st_sc, st_sigs, bbw_val, bbw_pct, coiling = layer_structure(c1h)
    score += st_sc;  sigs += st_sigs;  bd["struct"] = st_sc

    # Stealth bonus
    stealth_bonus = 0
    if (avg_vol_6h < CONFIG["stealth_max_vol"]
            and coiling > CONFIG["stealth_min_coiling"]
            and range_6h < CONFIG["stealth_max_range"]):
        stealth_bonus = 25
        sigs.append(f"🕵️ STEALTH PATTERN: vol ${avg_vol_6h:.0f}/h coiling {coiling}h")
    score += stealth_bonus;  bd["stealth"] = stealth_bonus

    # OI
    oi_value   = get_open_interest(symbol)
    oi_chg1h   = 0
    oi_chg24h  = 0
    oi_valid   = False
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)
        oi_chg1h, oi_chg24h, oi_valid = get_oi_changes(symbol, oi_value)

    # BUG-D FIX: Gate "sudah pump"
    # Estimasi vol_chg dari ticker (tidak punya historical, pakai perubahan OI sebagai proxy)
    vol_chg_proxy = oi_chg24h * 3 if oi_valid else 0  # rough proxy
    pumped, pump_reason = is_already_pumped(oi_chg24h, vol_chg_proxy, chg_24h, oi_valid)
    if pumped:
        log.info(f"  {symbol}: GATE already pumped — {pump_reason}")
        return None

    # Layer 4: Positioning
    pos_sc, pos_sigs, ls_ratio = layer_positioning(symbol, funding, oi_chg1h)
    score += pos_sc;  sigs += pos_sigs;  bd["pos"] = pos_sc

    # Layer 5: Multi-TF 4H
    tf4h_sc = 0
    if c4h:
        tf4h_sc, tf4h_sig = calc_4h_confluence(c4h)
        if tf4h_sig:
            sigs.append(tf4h_sig)
    score += tf4h_sc;  bd["tf4h"] = tf4h_sc

    # Layer 6: Context
    ctx_sc, ctx_sigs, sector = layer_context(symbol, tickers_dict)
    score += ctx_sc;  sigs += ctx_sigs;  bd["ctx"] = ctx_sc

    # Layer 7: Whale
    ws, whale_bonus, wev = calc_whale(symbol, c15m, funding)
    score += whale_bonus;  bd["whale"] = whale_bonus

    # GC-2: Liquidation Layer
    liq_sc, liq_sigs, long_liq, short_liq, liq_block = layer_liquidation(symbol, c1h)
    if liq_block:
        log.info(f"  {symbol}: GATE liquidation — long flush baru saja terjadi")
        return None
    score += liq_sc;  sigs += liq_sigs;  bd["liq"] = liq_sc

    # RSI (untuk GC-3 Linea Signature)
    rsi_1h = get_rsi(c1h[-48:] if len(c1h) >= 48 else c1h)

    # GC-3: Linea Signature Layer
    linea_sc, linea_sigs, linea_components = layer_linea_signature(
        c1h, oi_chg1h, oi_chg24h, oi_valid,
        ls_ratio, funding, chg_24h
    )
    # RSI bonus untuk Linea Signature
    if linea_components >= 2 and rsi_1h < CONFIG["linea_rsi_max"]:
        linea_sc += 5
        linea_sigs.append(f"✅ [Linea-4] RSI {rsi_1h:.1f} — oversold, siap reversal")
    score += linea_sc;  sigs += linea_sigs;  bd["linea"] = linea_sc

    # OI adjustments (v9.5: threshold diperketat BUG-B + BUG-C)
    if oi_value > 0:
        if oi_valid:
            # Gate distribusi ekstrem
            if rvol > 4.0 and oi_chg24h < -20:
                log.info(f"  {symbol}: GATE distribusi ekstrem (RVOL {rvol:.1f}x, OI 24h {oi_chg24h:.1f}%)")
                return None

            # BUG-B FIX: OI 24h threshold lebih agresif & bertingkat
            if oi_chg24h < -20:
                score -= 25; sigs.append(f"⚠️ OI 24h turun {oi_chg24h:.1f}% — distribusi masif")
            elif oi_chg24h < -10:
                score -= 15; sigs.append(f"⚠️ OI 24h turun {oi_chg24h:.1f}% — distribusi signifikan")
            elif oi_chg24h < -5:
                score -= 10; sigs.append(f"OI 24h turun {oi_chg24h:.1f}% — distribusi terindikasi")
            elif oi_chg24h < -3:
                score -= 5;  sigs.append(f"OI 24h {oi_chg24h:.1f}% — sedikit berkurang")

            # BUG-C FIX: OI 1h threshold bertingkat
            if oi_chg1h < -8:
                score -= 20; sigs.append(f"🚨 OI 1h turun {oi_chg1h:.1f}% — distribusi CEPAT!")
            elif oi_chg1h < -5:
                score -= 12; sigs.append(f"⚠️ OI 1h turun {oi_chg1h:.1f}% — tekanan jual 1 jam")
            elif oi_chg1h < -2:
                score -= 6;  sigs.append(f"OI 1h {oi_chg1h:.1f}% — mulai berkurang")
            elif oi_chg1h > 5:
                score += 5;  sigs.append(f"✅ OI 1h naik {oi_chg1h:.1f}% — posisi baru masuk")

            # BUG-B+C: HARD BLOCK — multi-TF OI decline
            if oi_chg24h < -3 and oi_chg1h < -2:
                log.info(f"  {symbol}: GATE multi-TF OI decline ({oi_chg24h:.1f}%/{oi_chg1h:.1f}%)")
                return None

            # Multi-TF OI decline gate (v9.3 dipertahankan)
            if oi_chg24h < -1.0 and oi_chg1h < -0.5:
                score -= 12
                sigs.append(f"⚠️ OI turun semua TF (24h:{oi_chg24h:.1f}% 1h:{oi_chg1h:.1f}%)")

            # Volume spike + OI drop
            if rvol > 1.5 and oi_chg24h < -15:
                score -= 18; sigs.append(f"🚨 Vol spike ({rvol:.1f}x) + OI {oi_chg24h:.1f}% — distribusi kuat")
            elif rvol > 1.5 and oi_chg24h < -10:
                score -= 12; sigs.append(f"⚠️ Vol naik tapi OI {oi_chg24h:.1f}% — distribusi")
            elif rvol > 1.5 and oi_chg24h > 5:
                score += 8;  sigs.append(f"✅ Vol naik + OI naik — akumulasi kuat")

        else:
            sigs.append("ℹ️ OI history belum tersedia (run pertama)")

        if len(c1h) >= 24:
            vol_24h_candles  = [c["volume_usd"] for c in c1h[-24:]]
            avg_vol_24h_base = sum(vol_24h_candles) / len(vol_24h_candles) if vol_24h_candles else 0
            if avg_vol_24h_base > 0 and avg_vol_6h > 0:
                vol_momentum = avg_vol_6h / avg_vol_24h_base
                if vol_momentum < 0.50:
                    score -= 10; sigs.append(f"⚠️ Volume 6h hanya {vol_momentum:.0%} avg 24h")
                elif vol_momentum < 0.70:
                    score -= 5;  sigs.append(f"Volume 6h menurun ({vol_momentum:.0%})")

        bd["oi_change"]    = round(oi_chg24h, 1)
        bd["oi_change_1h"] = round(oi_chg1h, 1)
        bd["oi_valid"]     = oi_valid
    else:
        bd["oi_change"] = bd["oi_change_1h"] = 0
        bd["oi_valid"]  = False

    if chg_7d > CONFIG["gate_chg_7d_max"]:
        score -= 15; sigs.append(f"⚠️ Overbought ({chg_7d:+.1f}% 7d)")

    if len(c1h) >= 24:
        high24 = max(c["high"] for c in c1h[-24:])
        low24  = min(c["low"]  for c in c1h[-24:])
        if low24 > 0:
            range24 = (high24 - low24) / low24 * 100
            if range24 > 55:
                score = max(0, score - 10)
                sigs.append(f"⚠️ Range 24h {range24:.0f}% — pump sudah berjalan?")

    # Time multiplier
    tmult, tsig = get_time_mult()
    # BUG-G FIX: jangan cap score sebelum composite — biarkan raw score
    # mencerminkan semua signal. Cap hanya pada composite akhir.
    raw_score = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    # Pump probability
    prob = compute_pump_probability(c1h, ws)
    bd["prob_score"] = round(prob["probability_score"] * 100, 1)
    bd["prob_class"] = prob["classification"]

    # BUG-G FIX: composite dihitung dari raw_score (tidak di-cap dulu)
    # Ini mencegah overflow artifisial seperti ALGO score=100 di v9.4
    composite = int(
        min(raw_score, 100) * CONFIG["composite_w_layer"]
        + prob["probability_score"] * 100 * CONFIG["composite_w_prob"]
    )
    composite = min(composite, 100)
    bd["composite"] = composite
    bd["rsi_1h"]    = round(rsi_1h, 1)
    bd["linea_comp"] = linea_components

    entry = calc_entry(c1h)
    if not entry or entry["liq_pct"] < CONFIG["min_target_pct"]:
        return None

    price_now = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]

    return {
        "symbol":          symbol,
        "score":           raw_score,
        "composite_score": composite,
        "signals":         sigs,
        "ws":              ws,
        "wev":             wev,
        "entry":           entry,
        "sector":          sector,
        "funding":         funding,
        "bd":              bd,
        "price":           price_now,
        "chg_24h":         chg_24h,
        "vol_24h":         vol_24h,
        "rvol":            rvol,
        "ls_ratio":        ls_ratio,
        "chg_7d":          chg_7d,
        "avg_vol_6h":      avg_vol_6h,
        "range_6h":        range_6h,
        "coiling":         coiling,
        "bbw_val":         bbw_val,
        "oi_change_24h":   bd.get("oi_change", 0),
        "oi_change_1h":    bd.get("oi_change_1h", 0),
        "prob_score":      prob["probability_score"],
        "prob_class":      prob["classification"],
        "prob_metrics":    prob.get("metrics", {}),
        "rsi_1h":          rsi_1h,
        "long_liq":        long_liq,
        "short_liq":       short_liq,
        "linea_components": linea_components,
    }


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    sc   = r["score"]
    comp = r.get("composite_score", sc)
    bar  = "█" * int(comp / 5) + "░" * (20 - int(comp / 5))
    e    = r["entry"]
    rk   = f"#{rank} " if rank else ""
    vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
            else f"${r['vol_24h']/1e3:.0f}K")
    ls   = f" | L/S:{r['ls_ratio']:.2f}" if r.get("ls_ratio") else ""

    prob_pct = r.get("prob_score", 0) * 100
    prob_cls = r.get("prob_class", "?")
    pm       = r.get("prob_metrics", {})
    bd       = r.get("bd", {})

    linea_str = ""
    if r.get("linea_components", 0) >= 3:
        linea_str = f"<b>Linea Sig :</b> ⭐ {r['linea_components']}/5 komponen!\n"

    liq_str = ""
    if r.get("long_liq", 0) > 0 or r.get("short_liq", 0) > 0:
        liq_str = (f"<b>Liquidation:</b> Long ${r.get('long_liq',0)/1e3:.0f}K | "
                   f"Short ${r.get('short_liq',0)/1e3:.0f}K (30m)\n")

    msg = (
        f"🚨 <b>PRE-PUMP SIGNAL {rk}— v9.5</b>\n\n"
        f"<b>Symbol    :</b> {r['symbol']}\n"
        f"<b>Composite :</b> {comp}/100  {bar}\n"
        f"<b>Layer Score:</b> {sc}/100\n"
        f"<b>Prob Model :</b> {prob_pct:.1f}% ({prob_cls})\n"
        f"<b>RSI 1h     :</b> {r.get('rsi_1h', 0):.1f}\n"
        f"{linea_str}"
        f"<b>Sektor     :</b> {r['sector']}\n"
        f"<b>Harga      :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h | {r['chg_7d']:+.1f}% 7d)\n"
        f"<b>Vol 24h    :</b> {vol} | RVOL: {r['rvol']:.1f}x{ls}\n"
        f"<b>6h Vol     :</b> ${r['avg_vol_6h']:.0f}/h  | 6h Range: {r['range_6h']:.1f}%\n"
        f"<b>Coiling    :</b> {r['coiling']}h  | BBW: {r['bbw_val']:.1f}%\n"
        f"<b>OI 24h/1h :</b> {r['oi_change_24h']:+.1f}% / {r.get('oi_change_1h',0):+.1f}%\n"
        f"{liq_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐋 <b>WHALE SCORE: {r['ws']}/100</b>\n"
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
            f"  🛑 SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n\n"
            f"🎯 <b>TARGET</b>\n"
            f"  T1 : ${e['t1']}  (+{e['liq_pct']:.1f}%)\n"
            f"  T2 : ${e['t2']}\n"
            f"  R/R: 1:{e['rr']}  |  ATR: ${e['atr']}\n"
        )

    msg += f"\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL AKTIF</b>\n"
    for s in r["signals"][:10]:
        msg += f"  • {s}\n"

    msg += (
        f"\n📐 <b>BREAKDOWN</b>\n"
        f"  Vol:{bd.get('vol',0)} StCVD:{bd.get('stcvd',0)} Flat:{bd.get('flat',0)} "
        f"Struct:{bd.get('struct',0)} Pos:{bd.get('pos',0)} "
        f"4H:{bd.get('tf4h',0)} Ctx:{bd.get('ctx',0)} "
        f"Whale:{bd.get('whale',0)} Liq:{bd.get('liq',0)} "
        f"Linea:{bd.get('linea',0)} Stealth:{bd.get('stealth',0)}\n"
        f"  OI valid:{bd.get('oi_valid','?')} RSI:{bd.get('rsi_1h','?')} "
        f"[Prob] MVS:{pm.get('max_vol_spike','?')}x "
        f"Irr:{pm.get('vol_irregularity','?')} "
        f"ATR:{pm.get('norm_atr_pct','?')}%\n\n"
        f"📡 Funding:{r['funding']:.5f}  🕐 {utc_now()}\n"
        f"<i>⚠️ Bukan financial advice. Manage risk ketat.</i>"
    )
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v9.5 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        comp     = r.get("composite_score", r["score"])
        bar      = "█" * int(comp / 10) + "░" * (10 - int(comp / 10))
        vol      = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                    else f"${r['vol_24h']/1e3:.0f}K")
        t1p      = r["entry"]["liq_pct"] if r.get("entry") else 0
        prob     = r.get("prob_score", 0) * 100
        prob_cls = r.get("prob_class", "?")
        rsi      = r.get("rsi_1h", 0)
        linea    = f" ⭐L{r.get('linea_components',0)}" if r.get("linea_components", 0) >= 2 else ""
        msg += (
            f"{i}. <b>{r['symbol']}</b> [C:{comp} S:{r['score']} {bar}]{linea}\n"
            f"   🐋{r['ws']} | RVOL:{r['rvol']:.1f}x | {vol} | "
            f"T1:+{t1p:.0f}% | {prob:.0f}% {prob_cls} | RSI:{rsi:.0f}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════
#  🔍  GC-4: STRATIFIED PRE-FILTER (fix "80 coin yang sama")
# ══════════════════════════════════════════════════════════════
def calc_volume_anomaly_score(ticker):
    """
    Proxy volume anomaly dari data ticker saja (tanpa API candle tambahan).
    Deteksi: volume tinggi TAPI harga belum naik = akumulasi tersembunyi.
    
    Prinsip: Jika volume besar tapi harga flat → smart money masuk diam-diam.
    """
    try:
        cur     = float(ticker.get("lastPr",      0))
        high24h = float(ticker.get("high24h",     cur))
        low24h  = float(ticker.get("low24h",      cur))
        vol     = float(ticker.get("quoteVolume", 0))
        chg24h  = float(ticker.get("change24h",   0)) * 100
    except:
        return 0

    if cur <= 0:
        return 0

    vas = 0

    # Volume besar relatif terhadap range = akumulasi diam
    range24 = (high24h - low24h) / low24h * 100 if low24h > 0 else 99
    if range24 > 0:
        vol_per_range = vol / range24   # semakin besar = volume padat per % range
        if vol_per_range > 500_000:
            vas += 8
        elif vol_per_range > 200_000:
            vas += 5
        elif vol_per_range > 50_000:
            vas += 2

    # Harga mendekati low 24h tapi volume besar = beli di bawah
    dist_from_low = (cur - low24h) / low24h * 100 if low24h > 0 else 99
    if dist_from_low < 5 and vol > 100_000:
        vas += 7   # harga di dekat low tapi ada yang beli
    elif dist_from_low < 10 and vol > 50_000:
        vas += 4

    # Volume besar tapi harga tidak naik signifikan = tersembunyi
    if vol > 500_000 and -3 <= chg24h <= 5:
        vas += 5
    elif vol > 200_000 and -5 <= chg24h <= 3:
        vas += 3

    # Range sempit = konsolidasi (potensi breakout)
    if range24 < 5:
        vas += 4
    elif range24 < 10:
        vas += 2

    return vas


def pre_score_ticker(ticker):
    """Quick scoring sebelum deep scan (dipertahankan dari v9.4)."""
    try:
        cur     = float(ticker.get("lastPr",      0))
        high24h = float(ticker.get("high24h",     cur))
        low24h  = float(ticker.get("low24h",      cur))
        vol     = float(ticker.get("quoteVolume", 0))
        chg24h  = float(ticker.get("change24h",   0)) * 100
    except:
        return 0
    if cur <= 0:
        return 0

    ps   = 0
    dist = (high24h - cur) / cur * 100 if cur > 0 and high24h > cur else 0

    if 10 <= dist <= 30:   ps += 5
    elif 5 <= dist < 10:   ps += 3
    elif dist < 5:         ps += 1
    elif dist > 40:        ps -= 2

    if -3 <= chg24h <= 5:   ps += 4
    elif 5 < chg24h <= 12:  ps += 2
    elif -8 <= chg24h < -3: ps += 3
    elif chg24h > 20:       ps -= 3

    if low24h > 0:
        range24 = (high24h - low24h) / low24h * 100
        if range24 <= 8:    ps += 3
        elif range24 <= 15: ps += 1
        elif range24 > 40:  ps -= 2

    if 50_000 <= vol <= 5_000_000:  ps += 3
    elif 10_000 <= vol < 50_000:    ps += 2
    elif vol > 20_000_000:          ps -= 1

    return ps


def load_wildcard_state():
    """Load state rotasi wildcard (GC-4)."""
    try:
        p = CONFIG["wildcard_seed_file"]
        if os.path.exists(p):
            with open(p) as f:
                return json.load(f)
    except:
        pass
    return {"last_seen": [], "run_count": 0}

def save_wildcard_state(state):
    try:
        with open(CONFIG["wildcard_seed_file"], "w") as f:
            json.dump(state, f)
    except:
        pass

def build_candidate_list(tickers):
    """
    GC-4: Stratified 3-Bucket Pre-filter.
    
    Menggantikan sistem top-N flat yang menghasilkan 80 coin identik setiap run.
    
    BUCKET A — SPIKE (30 coin):
      Coin dengan volume anomaly score tinggi dari ticker saja.
      Proxy: volume besar tapi harga belum bergerak = akumulasi tersembunyi.
      Target: menangkap coin yang akan pump tapi belum terdekeksi.
    
    BUCKET B — QUALITY (35 coin):
      Top pre-score seperti biasa (coin paling "siap" dari metrik ticker).
      Ini adalah bucket yang paling reliable dan konsisten.
    
    BUCKET C — WILDCARD (25 coin):
      Rotasi semi-random dari pool coin yang lolos filter dasar.
      Seed berubah setiap run untuk coverage yang berbeda.
      Setiap coin mendapat "giliran" scan dalam beberapa run.
      Target: menangkap coin yang tidak masuk bucket A atau B.
    
    Keunggulan vs v9.4:
    - Tidak ada coin yang "stuck" selalu masuk atau selalu dilewat
    - Wildcard memastikan coin low-vol yang mau pump tetap terpantau
    - Spike bucket menangkap accumulation yang tersembunyi di volume
    - Total coverage lebih luas: 90 coin (vs 80 fixed sebelumnya)
    """
    wc_state = load_wildcard_state()
    wc_state["run_count"] = wc_state.get("run_count", 0) + 1

    # Pool awal: semua coin yang lolos filter dasar
    base_pool = []
    for sym, t in tickers.items():
        if not sym.endswith("USDT"):
            continue
        if sym in STOCK_TICKERS or sym in MANUAL_EXCLUDE:
            continue
        if any(kw in sym for kw in EXCLUDED_KEYWORDS):
            continue
        if is_cooldown(sym):
            continue
        try:
            vol   = float(t.get("quoteVolume", 0))
            chg   = float(t.get("change24h",   0)) * 100
            price = float(t.get("lastPr",       0))
        except:
            continue
        if vol   < CONFIG["pre_filter_vol"]:      continue
        if vol   > CONFIG["max_vol_24h"]:         continue
        if abs(chg) > CONFIG["gate_chg_24h_max"]: continue
        if price <= 0:                             continue
        base_pool.append(sym)

    log.info(f"Base pool: {len(base_pool)} coin lolos filter dasar")

    # ── BUCKET A: Volume Anomaly / Spike ─────────────────────
    spike_scores = []
    for sym in base_pool:
        t   = tickers[sym]
        vas = calc_volume_anomaly_score(t)
        ps  = pre_score_ticker(t)
        spike_scores.append((sym, vas, ps, float(t.get("quoteVolume", 0))))

    # Sort: prioritas vas tinggi, tiebreak ps
    spike_scores.sort(key=lambda x: (-x[1], -x[2]))
    bucket_a = [x[0] for x in spike_scores[:CONFIG["bucket_spike"]]]
    a_set    = set(bucket_a)

    # ── BUCKET B: Quality Pre-Score ───────────────────────────
    quality_scores = []
    for sym in base_pool:
        if sym in a_set:
            continue
        t  = tickers[sym]
        ps = pre_score_ticker(t)
        quality_scores.append((sym, ps, float(t.get("quoteVolume", 0))))

    quality_scores.sort(key=lambda x: (-x[1], -x[2]))
    bucket_b = [x[0] for x in quality_scores[:CONFIG["bucket_quality"]]]
    b_set    = set(bucket_b)

    # ── BUCKET C: Wildcard Rotation ───────────────────────────
    wildcard_pool = [
        sym for sym in base_pool
        if sym not in a_set and sym not in b_set
    ]

    # Seed berbeda setiap run berdasarkan waktu + run_count
    # → coin yang berbeda terpilih setiap run
    seed = int(time.time() / 3600) + wc_state["run_count"]
    rng  = random.Random(seed)
    rng.shuffle(wildcard_pool)
    bucket_c = wildcard_pool[:CONFIG["bucket_wildcard"]]

    # Simpan state
    wc_state["last_seen"] = bucket_c[:10]  # track sebagian
    save_wildcard_state(wc_state)

    # Gabungkan semua bucket (tidak ada duplikasi)
    all_candidates = []
    seen = set()
    for sym in bucket_a + bucket_b + bucket_c:
        if sym not in seen:
            all_candidates.append(sym)
            seen.add(sym)

    log.info(
        f"Stratified bucket: "
        f"A(Spike)={len(bucket_a)} | "
        f"B(Quality)={len(bucket_b)} | "
        f"C(Wildcard)={len(bucket_c)} | "
        f"Total={len(all_candidates)}"
    )

    # Attach volume untuk log
    return [(sym, tickers[sym]) for sym in all_candidates if sym in tickers]


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v9.5 — {utc_now()} ===")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total ticker: {len(tickers)}")

    # GC-4: Stratified Pre-filter
    candidates = build_candidate_list(tickers)

    # ── Deep scan ─────────────────────────────────────────────
    results = []
    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except:
            vol = 0

        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum")
            continue

        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")
        try:
            res = master_score(sym, t, tickers)
            if res:
                comp     = res["composite_score"]
                prob     = res["prob_score"] * 100
                prob_cls = res["prob_class"]
                linea    = res.get("linea_components", 0)
                rsi      = res.get("rsi_1h", 0)
                log.info(
                    f"  Score={res['score']} Comp={comp} W={res['ws']} "
                    f"RVOL={res['rvol']:.1f}x Prob={prob:.0f}% ({prob_cls}) "
                    f"Linea={linea}/5 RSI={rsi:.0f} "
                    f"T1=+{res['entry']['liq_pct']:.1f}%"
                )
                if (comp >= CONFIG["min_composite_alert"]
                        and res["prob_score"] >= CONFIG["min_prob_alert"]):
                    results.append(res)
                else:
                    reason = ""
                    if comp < CONFIG["min_composite_alert"]:
                        reason += f"comp={comp}<{CONFIG['min_composite_alert']}"
                    if res["prob_score"] < CONFIG["min_prob_alert"]:
                        reason += f" prob={prob:.0f}%<50%({prob_cls})"
                    if reason:
                        log.info(f"  SKIP: {reason.strip()}")
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    # Sort by composite + linea bonus
    results.sort(
        key=lambda x: (
            x["composite_score"] + x.get("linea_components", 0) * 2,
            x["ws"]
        ),
        reverse=True
    )
    log.info(f"Lolos threshold: {len(results)} coin")

    qualified = [
        r for r in results
        if r["ws"] >= CONFIG["min_whale_score"] or r["composite_score"] >= 62
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
            log.info(
                f"✅ Alert #{rank}: {r['symbol']} "
                f"S={r['score']} C={r['composite_score']} W={r['ws']} "
                f"Prob={r['prob_score']*100:.0f}% Linea={r.get('linea_components',0)}/5"
            )
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v9.5                            ║")
    log.info("║  GC-2: Liquidation Detector                       ║")
    log.info("║  GC-3: Linea Signature Layer                      ║")
    log.info("║  GC-4: Stratified Pre-Filter (3 Bucket)           ║")
    log.info("╚═══════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
