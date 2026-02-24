"""
╔══════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v9.0 — FORENSIC-CALIBRATED                        ║
║                                                                      ║
║  SEMUA BUG v8.2 DIPERBAIKI:                                          ║
║                                                                      ║
║  FIX 1: STOCK FILTER — token saham kini di-exclude di pre-filter    ║
║          v8.2: lolos pre-filter → dapat skor 100 (AAPLUSDT dll)     ║
║                                                                      ║
║  FIX 2: SHORT SQUEEZE LOGIC TERBALIK                                 ║
║          v8.2: funding POSITIF dapat bonus (salah!)                  ║
║          v9.0: funding NEGATIF = short squeeze setup (benar)         ║
║                                                                      ║
║  FIX 3: compute_pump_probability() — 5 metrik tidak valid dihapus   ║
║          range_lock_score   → unit mismatch (USD/coin)               ║
║          efficiency_ratio   → unit mismatch (USD/coin)               ║
║          compression_phase  → semua nilai sama (0.310), tidak berguna║
║          buy_pressure_proxy → arah TERBALIK dari data forensik       ║
║          wick_absorption    → separability < 9%, noise               ║
║                                                                      ║
║  FIX 4: Model probabilitas BARU dari 35 data forensik nyata          ║
║          Fitur valid (separability > 19%):                           ║
║          · max_volume_spike   (46% diff pump vs non-pump)            ║
║          · volume_irregularity (31% diff, tanda whale masuk)         ║
║          · avg_volume_spike   (19% diff)                             ║
║          · ATR ternormalisasi (93% diff — pump terjadi sblm gerak)   ║
║          · price slope        (83% diff — pump saat harga FLAT)      ║
║          · low buy_pressure   (arah benar: akumulasi tersembunyi)    ║
║                                                                      ║
║  FIX 5: Pre-filter threshold diturunkan                              ║
║          ESP/SKR/STEEM/POWER ter-filter karena vol rendah sebelum    ║
║          pump. Sekarang ambang lebih rendah agar tidak miss.         ║
║                                                                      ║
║  FIX 6: breakout_proximity — poin tinggi untuk FLAT, bukan near high ║
║          v8.2: near high 24h = +18 (sinyal terlambat!)              ║
║          v9.0: flat price + vol naik = sinyal pre-pump sejati        ║
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
    # ── Threshold alert ───────────────────────────────────────
    "min_score_alert":           50,   # skor minimum untuk kirim alert
    "min_whale_score":           15,
    "max_alerts_per_run":         8,

    # ── Volume 24h TOTAL (USD) ─────────────────────────────────
    # FIX 5: diturunkan agar ESP/SKR/STEEM/POWER tidak ter-miss
    "min_vol_24h":            3_000,   # was 5_000
    "max_vol_24h":       50_000_000,
    "pre_filter_vol":         1_000,   # was 2_000

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
    "min_target_pct":             8.0,
    "max_sl_pct":                12.0,
    "atr_sl_mult":                1.5,
    "atr_t1_mult":                2.5,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":       3600,
    "sleep_coins":               0.9,
    "sleep_error":               3.0,
    "max_deep_scan":              80,
    "cooldown_file":    "/tmp/v9_cooldown.json",
    "oi_snapshot_file": "/tmp/v9_oi.json",

    # ── Stealth pattern ───────────────────────────────────────
    # FIX 5: avg_vol lebih rendah agar low-vol pre-pump terdeteksi
    "stealth_max_vol":       80_000,   # was 50_000
    "stealth_min_coiling":       15,   # was 20 (lebih sensitif)
    "stealth_max_range":          4.0,

    # ── Short squeeze (FIX 2: NEGATIF bukan positif) ──────────
    # Squeeze terjadi saat banyak short → funding negatif
    "squeeze_funding_max":    -0.0001,   # funding < -0.01% = short terakumulasi
    "squeeze_oi_change_min":     3.0,    # OI naik 3% = posisi baru masuk

    # ── Layer max scores ──────────────────────────────────────
    "max_vol_score":             30,
    "max_flat_score":            20,   # FIX 6: ganti breakout_proximity
    "max_struct_score":          15,
    "max_pos_score":             15,
    "max_tf4h_score":             8,
    "max_ctx_score":             10,
    "max_whale_bonus":           20,

    # ── Pump probability (model forensik baru) ────────────────
    "prob_mvs_w1":         30,    # max_volume_spike  (46% diff)
    "prob_irr_w2":         20,    # volume_irregularity (31% diff)
    "prob_avs_w3":         15,    # avg_volume_spike  (19% diff)
    "prob_atr_w4":         20,    # ATR ternormalisasi (93% diff)
    "prob_slope_w5":       15,    # price slope       (83% diff)
}

# ── Token saham — di-exclude di PRE-FILTER dan SCAN ───────────
# FIX 1: sebelumnya hanya di run_scan(), sekarang juga di pre-filter
STOCK_TICKERS = {
    "CSCOUSDT","PEPUSDT","QQQUSDT","AAPLUSDT","MSFTUSDT","GOOGLUSDT",
    "INTCUSDT","AMDUSDT","NVDAUSDT","TSLAUSDT","AMZNUSDT","METAUSDT",
    "NFLXUSDT","ADBEUSDT","CRMUSDT","ORCLUSDT","IBMUSDT","SAPUSDT",
    "PYPLUSDT","UBERUSDT","LYFTUSDT","SPYUSDT","DIAUSDT","IWMUSDT",
    "MCDUSDT","KOLUSDT","DISUSDT","BRKUSDT","JPMCUSDT","BACHUSDT",
    "SBUXUSDT","NKEUSDT","WMTUSDT","COSTUSDT","HDUSTUSDT",
}

GRAN_MAP = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}


# ══════════════════════════════════════════════════════════════
#  🗂️  SECTOR MAP
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
        "ESPUSDT",
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

EXCLUDED_KEYWORDS = [
    "XAU","PAXG","BTC","ETH","USDC","DAI","BUSD","UST","LUNC","LUNA",
]


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
    snaps = load_oi_snapshots()
    if symbol not in snaps:
        return 0, 0
    data = snaps[symbol]
    now  = time.time()

    def nearest(target_ts):
        cands = [d for d in data if abs(d["ts"] - target_ts) < 600]
        if not cands:
            return None
        return min(cands, key=lambda d: abs(d["ts"] - target_ts))

    old1h  = nearest(now - 3600)
    old24h = nearest(now - 86400)
    chg1h  = (current_oi - old1h["oi"])  / old1h["oi"]  * 100 if old1h  and old1h["oi"]  else 0
    chg24h = (current_oi - old24h["oi"]) / old24h["oi"] * 100 if old24h and old24h["oi"] else 0
    return chg1h, chg24h

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
        params={"symbol": symbol, "productType": "usdt-futures",
                "limit": str(limit)},
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

def find_swing_targets(candles, cur):
    min_t  = cur * (1 + CONFIG["min_target_pct"] / 100)
    swings = []
    for i in range(2, len(candles) - 2):
        h = candles[i]["high"]
        if (h >= min_t
                and h > candles[i - 1]["high"] and h > candles[i - 2]["high"]
                and h > candles[i + 1]["high"] and h > candles[i + 2]["high"]):
            swings.append(h)
    swings.sort()
    t1 = swings[0]            if swings          else cur * 1.10
    t2 = swings[1]            if len(swings) >= 2 else t1 * 1.08
    return round(t1, 8), round(t2, 8)


# ══════════════════════════════════════════════════════════════
#  📊  INDIKATOR TAMBAHAN
# ══════════════════════════════════════════════════════════════
def calc_rvol(candles_1h):
    """RVOL: volume jam ini vs rata-rata jam yang sama di hari-hari sebelumnya."""
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

def calc_volume_spike_ratio(candles_1h):
    """
    Menghitung max_volume_spike dan avg_volume_spike.
    Dari forensik: pump coins max_spike rata-rata 6.2x vs non-pump 4.3x.
    """
    if len(candles_1h) < 24:
        return 1.0, 1.0
    vols    = [c["volume_usd"] for c in candles_1h]
    baseline = sorted(vols[:-6])[:int(len(vols) * 0.6)]   # 60th percentile bawah
    base_avg = sum(baseline) / len(baseline) if baseline else 1

    if base_avg <= 0:
        return 1.0, 1.0

    recent_vols = vols[-6:]   # 6 jam terakhir
    spikes      = [v / base_avg for v in recent_vols]
    max_spike   = max(spikes) if spikes else 1.0
    avg_spike   = sum(spikes) / len(spikes) if spikes else 1.0
    return max_spike, avg_spike

def calc_volume_irregularity(candles_1h):
    """
    Volume irregularity = std/mean dari 24h terakhir.
    Dari forensik: pump coins irr rata-rata 1.73 vs non-pump 1.32.
    Tinggi = whale masuk secara tidak merata (sinyal akumulasi).
    """
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
    """
    ATR ternormalisasi = ATR / harga_sekarang (%).
    Dari forensik: pump coins ATR/price LEBIH KECIL (belum bergerak).
    NonPump ATR/price 15x lebih besar dari pump.
    """
    cur = candles_1h[-1]["close"] if candles_1h else 0
    if cur <= 0:
        return 0.0
    atr = calc_atr(candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h)
    if not atr:
        return 0.0
    return (atr / cur) * 100   # sebagai % dari harga

def calc_price_slope(candles_1h):
    """
    Slope ternormalisasi: (perubahan harga) / harga_awal per candle.
    Dari forensik: pump coins slope flat atau negatif sebelum pump.
    """
    window = candles_1h[-12:] if len(candles_1h) >= 12 else candles_1h
    if len(window) < 2:
        return 0.0
    p_start = window[0]["close"]
    p_end   = window[-1]["close"]
    if p_start <= 0:
        return 0.0
    n     = len(window)
    slope = (p_end - p_start) / p_start / n   # per candle, normalized
    return slope

def calc_cvd_signal(candles_1h):
    """CVD divergence: beli tersembunyi saat harga flat."""
    if len(candles_1h) < 12:
        return 0, ""
    window    = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    cvd       = 0
    cvd_vals  = []
    for c in window:
        rng = c["high"] - c["low"]
        buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        cvd       += (buy_ratio * 2 - 1) * c["volume_usd"]
        cvd_vals.append(cvd)
    if len(cvd_vals) < 8:
        return 0, ""
    mid        = len(cvd_vals) // 2
    cvd_early  = sum(cvd_vals[:mid]) / mid
    cvd_late   = sum(cvd_vals[mid:]) / (len(cvd_vals) - mid)
    cvd_rising = cvd_late > cvd_early
    p_start    = window[0]["close"]
    p_end      = window[-1]["close"]
    price_chg  = (p_end - p_start) / p_start * 100 if p_start > 0 else 0
    if cvd_rising and price_chg < 1.5:
        if price_chg < -1.5:
            return 15, f"🔍 CVD Divergence KUAT: harga {price_chg:+.1f}% tapi buy pressure dominan"
        elif price_chg < 0:
            return 12, f"🔍 CVD naik saat harga turun — akumulasi tersembunyi"
        else:
            return 8,  f"🔍 CVD naik, harga flat — hidden accumulation"
    elif cvd_rising and 1.5 <= price_chg <= 5.0:
        return 5, f"CVD bullish, harga naik sehat ({price_chg:+.1f}%)"
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
        return 7, f"💚 Marubozu bullish (body {body_ratio:.0%}) — buyer kontrol penuh"
    elif is_bullish and body_ratio > 0.55:
        return 5, f"💚 Candle bullish kuat (body {body_ratio:.0%})"
    elif is_bullish and is_breakout:
        return 4, f"📈 Breakout di atas high sebelumnya"
    elif not is_bullish and body_ratio > 0.6:
        return -3, ""
    return 0, ""


# ══════════════════════════════════════════════════════════════
#  🔬  PUMP PROBABILITY — MODEL FORENSIK v9
# ══════════════════════════════════════════════════════════════
def compute_pump_probability(candles_1h, whale_score=0):
    """
    Model probabilitas berbasis 35 data forensik nyata.
    Hanya menggunakan fitur yang terbukti membedakan pump vs non-pump.

    DIHAPUS dari v8.2 (tidak valid):
    - range_lock_score   → (high-low)/volume: unit mismatch, coin harga
                           tinggi dapat nilai 1 juta kali lebih besar
    - efficiency_ratio   → body/volume: masalah sama
    - compression_phase  → semua coin dapat nilai sama (0.310), noise
    - buy_pressure_proxy → arahnya TERBALIK (pump coins bp lebih rendah)
    - wick_absorption    → separability < 9%, tidak berguna

    DIPAKAI (terbukti valid dari forensik):
    - max_volume_spike   → pump 6.2x vs non-pump 4.3x (diff 46%)
    - volume_irregularity → pump 1.73 vs non-pump 1.32 (diff 31%)
    - avg_volume_spike   → pump 1.15 vs non-pump 0.96 (diff 19%)
    - ATR/price norm     → pump lebih kecil 15x (belum bergerak, diff 93%)
    - price slope        → pump flat/turun, non-pump naik (diff 83%)
    """
    if len(candles_1h) < 24:
        return {"probability_score": 0.3, "classification": "Data Kurang", "metrics": {}}

    max_spike, avg_spike = calc_volume_spike_ratio(candles_1h)
    irr                  = calc_volume_irregularity(candles_1h)
    norm_atr             = calc_normalized_atr(candles_1h)   # % dari harga
    slope                = calc_price_slope(candles_1h)

    # ── Normalisasi setiap fitur ke 0–1 ───────────────────────
    def clamp(v, lo, hi):
        return max(0.0, min(1.0, (v - lo) / (hi - lo))) if hi > lo else 0.5

    # max_volume_spike: pump median 5.85x, non-pump 4.11x
    # target range: 1x (buruk) sampai 10x (sangat baik)
    n_mvs  = clamp(max_spike,  1.0,  10.0)

    # volume_irregularity: pump 1.73, non-pump 1.32
    # target: 0.5 (rendah) sampai 3.5 (tinggi)
    n_irr  = clamp(irr,        0.5,   3.5)

    # avg_volume_spike: pump 1.15, non-pump 0.96
    # target: 0.5 sampai 2.0
    n_avs  = clamp(avg_spike,  0.5,   2.0)

    # ATR/price: pump LEBIH KECIL = lebih baik (inverse)
    # non-pump avg ~1.5%, pump avg ~0.1%
    # Semakin kecil ATR = semakin belum bergerak = pre-pump
    n_atr  = 1.0 - clamp(norm_atr, 0.05, 3.0)   # inverse

    # slope: pump flat/negatif, non-pump positif (inverse)
    # range slope: -0.001 (sangat turun) sampai +0.002 (naik)
    n_slp  = 1.0 - clamp(slope, -0.001, 0.002)   # inverse

    # whale score sebagai konfirmasi (0–100 → 0–1)
    n_whale = whale_score / 100.0

    # ── Weighted average (bobot dari separability forensik) ───
    # Total bobot fitur utama: 30+20+15+20+15 = 100
    score = (
        n_mvs   * 0.28 +   # max volume spike (46% diff)
        n_irr   * 0.20 +   # volume irregularity (31% diff)
        n_avs   * 0.12 +   # avg volume spike (19% diff)
        n_atr   * 0.22 +   # ATR rendah = pre-pump (93% diff)
        n_slp   * 0.13 +   # price flat/turun = pre-pump (83% diff)
        n_whale * 0.05     # whale konfirmasi (bobot kecil, opsional)
    )
    score = max(0.0, min(1.0, score))

    # ── Klasifikasi ───────────────────────────────────────────
    if score < 0.30:
        cls = "Noise"
    elif score < 0.45:
        cls = "Sideways"
    elif score < 0.60:
        cls = "Accumulation"
    elif score < 0.75:
        cls = "Pre-Pump"
    else:
        cls = "Imminent Pump"

    return {
        "probability_score": score,
        "classification":    cls,
        "metrics": {
            "max_vol_spike":       round(max_spike, 2),
            "avg_vol_spike":       round(avg_spike, 2),
            "vol_irregularity":    round(irr, 3),
            "norm_atr_pct":        round(norm_atr, 4),
            "price_slope":         round(slope, 8),
        },
    }


# ══════════════════════════════════════════════════════════════
#  🏗️  LAYER SCORING
# ══════════════════════════════════════════════════════════════

# ── Layer 1: Volume Intelligence ─────────────────────────────
def layer_volume_intelligence(candles_1h):
    score = 0
    sigs  = []
    rvol  = calc_rvol(candles_1h)

    if rvol >= 4.0:
        score += 16
        sigs.append(f"🔥🔥 RVOL {rvol:.1f}x — volume MASIF vs historis!")
    elif rvol >= 2.8:
        score += 13
        sigs.append(f"🔥 RVOL {rvol:.1f}x — volume spike signifikan")
    elif rvol >= 2.0:
        score += 10
        sigs.append(f"RVOL {rvol:.1f}x — volume mulai bangun")
    elif rvol >= 1.4:
        score += 6
        sigs.append(f"RVOL {rvol:.1f}x — di atas normal")
    elif rvol >= 1.1:
        score += 3
    elif rvol < 0.4:
        score -= 4

    # Volume irregularity sebagai sinyal whale masuk
    irr = calc_volume_irregularity(candles_1h)
    if irr >= 2.5:
        score += 10
        sigs.append(f"📈 Vol Irregularity {irr:.2f} — whale masuk tidak merata")
    elif irr >= 1.8:
        score += 6
        sigs.append(f"Vol Irregularity {irr:.2f} — aktivitas whale terdeteksi")
    elif irr >= 1.3:
        score += 3

    # CVD divergence
    cvd_s, cvd_sig = calc_cvd_signal(candles_1h)
    score += cvd_s
    if cvd_sig:
        sigs.append(cvd_sig)

    return min(score, CONFIG["max_vol_score"]), sigs, rvol


# ── Layer 2: Flat Price + Volume Naik (ganti breakout_proximity) ──
# FIX 6: v8.2 memberi poin tertinggi untuk "near high 24h" = sinyal terlambat
# Data forensik: pump terjadi saat harga FLAT, bukan saat sudah dekat high
def layer_flat_accumulation(candles_1h):
    """
    Deteksi pola akumulasi sejati: harga flat + volume mulai naik.
    Dari forensik: ESP, SKR, STEEM, POWER semua punya slope negatif/flat
    sebelum pump. Scanner v8.2 justru menghukum pola ini!
    """
    score = 0
    sigs  = []

    if len(candles_1h) < 12:
        return 0, sigs

    # Range 24h — semakin kecil semakin baik (akumulasi zona)
    if len(candles_1h) >= 24:
        high24 = max(c["high"] for c in candles_1h[-24:])
        low24  = min(c["low"]  for c in candles_1h[-24:])
        range24_pct = (high24 - low24) / low24 * 100 if low24 > 0 else 99
    else:
        range24_pct = 99

    if range24_pct < 3:
        score += 15
        sigs.append(f"🎯 Range 24h sangat sempit ({range24_pct:.1f}%) — zona akumulasi tight")
    elif range24_pct < 6:
        score += 10
        sigs.append(f"🎯 Range 24h sempit ({range24_pct:.1f}%) — akumulasi aktif")
    elif range24_pct < 10:
        score += 5
        sigs.append(f"Range 24h terbatas ({range24_pct:.1f}%)")
    elif range24_pct < 15:
        score += 2
    elif range24_pct > 40:
        score -= 5   # sudah bergerak besar

    # Slope 12 jam: flat atau turun = baik (pre-pump pattern)
    slope = calc_price_slope(candles_1h)
    if slope < -0.0003:
        score += 8
        sigs.append(f"📉 Harga turun perlahan ({slope*1e4:.2f}‱/h) — tekanan shorters = bahan bakar")
    elif abs(slope) <= 0.0003:
        score += 5
        sigs.append(f"➡️ Harga sangat flat — akumulasi tersembunyi")
    elif slope < 0.001:
        score += 2

    # Higher lows — struktur akumulasi ascending
    hl_sc, hl_sig = detect_higher_lows(candles_1h[-16:] if len(candles_1h) >= 16 else candles_1h)
    score += hl_sc
    if hl_sig:
        sigs.append(hl_sig)

    # Candle quality
    cq_sc, cq_sig = candle_quality_score(candles_1h)
    score += cq_sc
    if cq_sig:
        sigs.append(cq_sig)

    return min(max(score, 0), CONFIG["max_flat_score"]), sigs


# ── Layer 3: Structure (BBW + Coiling) ───────────────────────
def layer_structure(candles_1h):
    score = 0
    sigs  = []

    bbw_val, bbw_pct = bbw_percentile(candles_1h)
    if bbw_pct < 10:
        score += 10
        sigs.append(f"BBW Squeeze Ekstrem ({bbw_pct:.0f}%ile) — siap meledak")
    elif bbw_pct < 25:
        score += 7
        sigs.append(f"BBW Squeeze Kuat ({bbw_pct:.0f}%ile)")
    elif bbw_pct < 45:
        score += 4
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
        sigs.append(f"Coiling {coiling}h — energi terkumpul lama")
    elif coiling >= 10:
        score += 3
        sigs.append(f"Coiling {coiling}h")
    elif coiling >= 5:
        score += 1

    return min(score, CONFIG["max_struct_score"]), sigs, bbw_val, bbw_pct, coiling


# ── Layer 4: Positioning (Funding + L/S) ─────────────────────
def layer_positioning(symbol, funding, oi_change_1h):
    """
    FIX 2: Short squeeze terjadi saat funding NEGATIF.
    v8.2 memberi bonus saat funding > +0.05% = SALAH (itu long squeeze).
    """
    score = 0
    sigs  = []

    # Funding scoring (benar)
    if -0.0004 <= funding <= -0.00002:
        score += 8
        sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup!")
    elif 0 <= funding <= 0.0001:
        score += 5
        sigs.append(f"Funding netral ({funding:.5f})")
    elif 0.0001 < funding <= 0.0003:
        score += 2
    elif funding > 0.0003:
        score -= 5
        sigs.append(f"⚠️ Funding {funding:.5f} — long overcrowded, risiko dump")

    # FIX 2: Short squeeze = funding NEGATIF + OI NAIK
    # (short terakumulasi, posisi baru masuk, bahan bakar squeeze)
    if (funding <= CONFIG["squeeze_funding_max"]     # < -0.01%
            and oi_change_1h > CONFIG["squeeze_oi_change_min"]):   # OI naik > 3%
        score += 10
        sigs.append(
            f"🔥 SHORT SQUEEZE TERINDIKASI: "
            f"funding {funding:.5f} negatif, OI 1h +{oi_change_1h:.1f}%"
        )

    # Long/Short Ratio
    ls       = get_long_short_ratio(symbol)
    ls_score = 0
    if ls is not None:
        if ls < 0.6:
            ls_score = 10
            sigs.append(f"🎯 L/S {ls:.2f} — short dominan, squeeze fuel besar!")
        elif ls < 0.75:
            ls_score = 8
            sigs.append(f"🎯 L/S {ls:.2f} — short dominan")
        elif ls < 0.9:
            ls_score = 5
            sigs.append(f"L/S {ls:.2f} — lebih banyak short")
        elif ls <= 1.15:
            ls_score = 2
        elif ls > 3.0:
            ls_score = -15
            sigs.append(f"⚠️⚠️ L/S {ls:.2f} — long overcrowded ekstrem!")
        elif ls > 2.5:
            ls_score = -9
            sigs.append(f"⚠️ L/S {ls:.2f} — long sangat dominan")
        elif ls > 2.0:
            ls_score = -5
            sigs.append(f"L/S {ls:.2f} — long dominan")

    return min(score + ls_score, CONFIG["max_pos_score"]), sigs, ls


# ── Layer 5: Multi-TF 4H ─────────────────────────────────────
def calc_4h_confluence(candles_4h):
    if len(candles_4h) < 6:
        return 0, ""
    closes   = [c["close"] for c in candles_4h]
    p_now    = closes[-1]
    p_7d     = closes[0]
    p_48h    = closes[-12] if len(closes) >= 12 else closes[0]
    trend_7d  = (p_now - p_7d)  / p_7d  * 100 if p_7d  > 0 else 0
    trend_48h = (p_now - p_48h) / p_48h * 100 if p_48h > 0 else 0
    if trend_48h > 2 and -10 <= trend_7d <= 15:
        return 6, f"📊 4H: reversal bullish 48h +{trend_48h:.1f}%, trend 7d sehat"
    elif trend_48h > 0 and trend_7d > -15:
        return 3, f"📊 4H upward bias ({trend_48h:+.1f}% 48h)"
    elif trend_48h < -8:
        return -5, f"⚠️ 4H masih downtrend ({trend_48h:+.1f}% 48h)"
    return 0, ""


# ── Layer 6: Context (Sector + Social) ───────────────────────
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
    name      = symbol.replace("USDT", "").replace("1000", "").upper()
    soc_score, soc_sig = 0, ""
    if name in get_cg_trending():
        soc_score = 3
        soc_sig   = f"🔥 {name} trending CoinGecko"
    sigs = [s for s in [sec_sig, soc_sig] if s]
    return min(sec_score + soc_score, CONFIG["max_ctx_score"]), sigs, sector


# ── Layer 7: Whale Intelligence ───────────────────────────────
def calc_whale(symbol, candles_15m, funding):
    ws  = 0
    ev  = []
    cur = candles_15m[-1]["close"] if candles_15m else 0

    trades = get_trades(symbol, 500)
    if trades:
        buy_v  = sum(t["size"] for t in trades if t["side"] == "buy")
        tot_v  = sum(t["size"] for t in trades)
        tr     = buy_v / tot_v if tot_v > 0 else 0.5

        if tr > 0.70:
            ws += 30
            ev.append(f"✅ Taker Buy {tr:.0%} — pembeli sangat dominan")
        elif tr > 0.62:
            ws += 15
            ev.append(f"🔶 Taker Buy {tr:.0%} — bias beli")

        total_usd  = sum(t["size"] * t["price"] for t in trades)
        avg_trade  = total_usd / len(trades) if trades else 1
        thr        = max(avg_trade * 5, 3_000)
        lbuy_usd   = sum(
            t["size"] * t["price"] for t in trades
            if t["side"] == "buy" and t["size"] * t["price"] > thr
        )
        if total_usd > 0 and lbuy_usd / total_usd > 0.28:
            ws += 25
            ev.append(f"✅ Smart money {lbuy_usd/total_usd:.0%} vol (>${thr:,.0f}/trade)")

        # Iceberg detection
        if cur > 0:
            tol     = 0.15 if cur >= 10 else (0.30 if cur >= 1 else 0.50)
            at_lvl  = [
                t for t in trades
                if t["side"] == "buy"
                and abs(t["price"] - cur) / cur * 100 < tol
            ]
            if len(at_lvl) >= 10:
                tot_ice = sum(t["size"] * t["price"] for t in at_lvl)
                avg_ice = tot_ice / len(at_lvl)
                if len(at_lvl) >= 14 and avg_ice < thr * 0.25 and tot_ice > thr * 2.5:
                    ws += 20
                    ev.append(f"✅ Iceberg: {len(at_lvl)} tx kecil (${tot_ice:,.0f} total)")

    # Harga flat 4 jam = stealth positioning
    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15
            ev.append("✅ Harga flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7
            ev.append("🔶 Harga relatif flat 4h")

    # Funding negatif = short squeeze fuel
    if -0.0004 <= funding <= -0.00002:
        ws += 10
        ev.append(f"✅ Funding {funding:.5f} — short squeeze fuel")

    # Order book
    ob_ratio, bid_vol, ask_vol = get_orderbook(symbol, 50)
    if ob_ratio > 0.65:
        ws += 15
        ev.append(f"✅ OB Bid {ob_ratio:.0%} — tekanan beli di book")
    elif ob_ratio > 0.55:
        ws += 7
        ev.append(f"🔶 OB Bid {ob_ratio:.0%}")
    elif ob_ratio < 0.35:
        ws -= 10
        ev.append(f"⚠️ OB Ask dominan — tekanan jual lebih besar")

    ws = min(ws, 100)
    return ws, ws // 5, ev


# ── Time multiplier ───────────────────────────────────────────
def get_time_mult():
    h = utc_hour()
    if h in [5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.15, f"⏰ High-prob window ({h}:00 UTC)"
    if h in [1, 2, 3, 4]:
        return 0.85, "Low-prob window"
    return 1.0, ""


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
    entry = min(support * 1.002, cur * 0.998)
    sl    = max(entry - CONFIG["atr_sl_mult"] * atr, entry * 0.88)
    t1_sw, t2_sw = find_swing_targets(candles_1h, cur)
    t1_atr       = entry + CONFIG["atr_t1_mult"] * atr
    t1           = min(t1_sw, t1_atr) if t1_sw > cur * 1.06 else t1_atr
    if t1 <= cur * 1.05:
        t1 = cur * 1.10
    t2     = t2_sw if t2_sw > t1 * 1.02 else t1 * 1.08
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

    # Gate overbought: FIX 5 — bypass squeeze hanya untuk funding NEGATIF kuat
    if chg_7d > CONFIG["gate_chg_7d_max"]:
        oi_value     = get_open_interest(symbol)
        oi_chg_1h, _ = get_oi_changes(symbol, oi_value) if oi_value > 0 else (0, 0)
        # Squeeze hanya valid jika funding NEGATIF (bukan positif seperti v8.2)
        real_squeeze = (funding <= CONFIG["squeeze_funding_max"]    # < -0.01%
                        and oi_chg_1h > CONFIG["squeeze_oi_change_min"])
        if real_squeeze:
            log.info(f"  {symbol}: Overbought {chg_7d:.1f}% tapi squeeze negatif terindikasi, lanjut")
        else:
            log.info(f"  {symbol}: GATE overbought ({chg_7d:.1f}%)")
            return None

    if chg_7d < CONFIG["gate_chg_7d_min"]:
        log.info(f"  {symbol}: GATE downtrend ({chg_7d:.1f}%)")
        return None
    if funding < CONFIG["gate_funding_extreme"]:
        log.info(f"  {symbol}: GATE funding ekstrem ({funding:.5f})")
        return None

    # Hitung metrik tambahan
    if len(c1h) >= 6:
        pre6       = c1h[-6:]
        avg_vol_6h = sum(c["volume_usd"] for c in pre6) / 6
        high_6h    = max(c["high"] for c in pre6)
        low_6h     = min(c["low"]  for c in pre6)
        range_6h   = (high_6h - low_6h) / low_6h * 100 if low_6h > 0 else 0
    else:
        avg_vol_6h = 0
        range_6h   = 0

    score = 0
    sigs  = []
    bd    = {}

    # Layer 1: Volume Intelligence
    v_sc, v_sigs, rvol = layer_volume_intelligence(c1h)
    score += v_sc
    sigs  += v_sigs
    bd["vol"] = v_sc

    # Layer 2: Flat Accumulation (FIX 6: ganti breakout_proximity)
    fa_sc, fa_sigs = layer_flat_accumulation(c1h)
    score += fa_sc
    sigs  += fa_sigs
    bd["flat"] = fa_sc

    # Layer 3: Structure (BBW + Coiling)
    st_sc, st_sigs, bbw_val, bbw_pct, coiling = layer_structure(c1h)
    score += st_sc
    sigs  += st_sigs
    bd["struct"] = st_sc

    # Bonus Stealth Pattern (volume rendah + coiling panjang + range sempit)
    # FIX 5: threshold disesuaikan agar ESP/SKR/STEEM terdeteksi
    stealth_bonus = 0
    if (avg_vol_6h < CONFIG["stealth_max_vol"]          # < $80K/jam
            and coiling > CONFIG["stealth_min_coiling"]  # coiling > 15h
            and range_6h < CONFIG["stealth_max_range"]):  # range < 4%
        stealth_bonus = 25   # was 30 — dikurangi agar tidak over-inflate
        sigs.append(
            f"🕵️ STEALTH PATTERN: vol ${avg_vol_6h:.0f}/h "
            f"coiling {coiling}h range {range_6h:.1f}%"
        )
    score += stealth_bonus
    bd["stealth"] = stealth_bonus

    # OI untuk positioning
    oi_value = get_open_interest(symbol)
    oi_chg1h = oi_chg24h = 0
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)
        oi_chg1h, oi_chg24h = get_oi_changes(symbol, oi_value)

    # Layer 4: Positioning
    pos_sc, pos_sigs, ls_ratio = layer_positioning(symbol, funding, oi_chg1h)
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

    # Layer 7: Whale
    ws, whale_bonus, wev = calc_whale(symbol, c15m, funding)
    score += whale_bonus
    bd["whale"] = whale_bonus

    # OI adjustments
    if oi_value > 0:
        if oi_chg24h < -20:
            score -= 15
            sigs.append(f"⚠️ OI 24h turun {oi_chg24h:.1f}% — distribusi besar")
        elif oi_chg24h < -10:
            score -= 7
            sigs.append(f"OI 24h turun {oi_chg24h:.1f}%")
        if oi_chg1h < -5:
            score -= 5
            sigs.append(f"OI 1h turun {oi_chg1h:.1f}% — tekanan jual jangka pendek")
        elif oi_chg1h > 5:
            score += 5
            sigs.append(f"✅ OI 1h naik {oi_chg1h:.1f}% — posisi baru masuk")
        if rvol > 1.5 and oi_chg24h < -10:
            score -= 8
            sigs.append(f"⚠️ Volume naik tapi OI turun — distribusi terindikasi")
        elif rvol > 1.5 and oi_chg24h > 5:
            score += 8
            sigs.append(f"✅ Volume naik + OI naik — akumulasi kuat")
        bd["oi_change"] = round(oi_chg24h, 1)
    else:
        bd["oi_change"] = 0

    # Penalti overbought jika lolos squeeze
    if chg_7d > CONFIG["gate_chg_7d_max"]:
        score -= 15
        sigs.append(f"⚠️ Overbought ({chg_7d:+.1f}% 7d) — hanya short squeeze play")

    # Penalti range 24h sudah lebar (pump sudah berjalan)
    if len(c1h) >= 24:
        high24 = max(c["high"] for c in c1h[-24:])
        low24  = min(c["low"]  for c in c1h[-24:])
        if low24 > 0:
            range24 = (high24 - low24) / low24 * 100
            if range24 > 55:
                score = max(0, score - 10)
                sigs.append(f"⚠️ Range 24h {range24:.0f}% — pump sudah berjalan?")

    # Pump Probability (model forensik baru)
    prob = compute_pump_probability(c1h, ws)
    bd["prob_score"] = round(prob["probability_score"] * 100, 1)
    bd["prob_class"] = prob["classification"]

    # Time multiplier
    tmult, tsig = get_time_mult()
    score = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    score = min(score, 100)

    # Entry zones
    entry = calc_entry(c1h)
    if not entry or entry["liq_pct"] < CONFIG["min_target_pct"]:
        return None

    try:
        price_now = float(ticker.get("lastPr",      0))
        chg_24h   = float(ticker.get("change24h",   0)) * 100
        vol_24h   = float(ticker.get("quoteVolume", 0))
    except:
        price_now = c1h[-1]["close"]
        chg_24h   = 0
        vol_24h   = 0

    return {
        "symbol":       symbol,
        "score":        score,
        "signals":      sigs,
        "ws":           ws,
        "wev":          wev,
        "entry":        entry,
        "sector":       sector,
        "funding":      funding,
        "bd":           bd,
        "price":        price_now,
        "chg_24h":      chg_24h,
        "vol_24h":      vol_24h,
        "rvol":         rvol,
        "ls_ratio":     ls_ratio,
        "chg_7d":       chg_7d,
        "avg_vol_6h":   avg_vol_6h,
        "range_6h":     range_6h,
        "coiling":      coiling,
        "bbw_val":      bbw_val,
        "oi_change_24h":bd.get("oi_change", 0),
        "prob_score":   prob["probability_score"],
        "prob_class":   prob["classification"],
        "prob_metrics": prob.get("metrics", {}),
    }


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    sc   = r["score"]
    bar  = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    e    = r["entry"]
    rk   = f"#{rank} " if rank else ""
    vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
            else f"${r['vol_24h']/1e3:.0f}K")
    ls   = f" | L/S:{r['ls_ratio']:.2f}" if r.get("ls_ratio") else ""

    prob_pct  = r.get("prob_score", 0) * 100
    prob_cls  = r.get("prob_class", "?")
    pm        = r.get("prob_metrics", {})

    msg = (
        f"🚨 <b>PRE-PUMP SIGNAL {rk}— v9.0</b>\n\n"
        f"<b>Symbol :</b> {r['symbol']}\n"
        f"<b>Score  :</b> {sc}/100  {bar}\n"
        f"<b>Prob.  :</b> {prob_pct:.1f}% ({prob_cls})\n"
        f"<b>Sektor :</b> {r['sector']}\n"
        f"<b>Harga  :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h | {r['chg_7d']:+.1f}% 7d)\n"
        f"<b>Vol 24h:</b> {vol} | RVOL: {r['rvol']:.1f}x{ls}\n"
        f"<b>6h Vol :</b> ${r['avg_vol_6h']:.0f}/h  | 6h Range: {r['range_6h']:.1f}%\n"
        f"<b>Coiling:</b> {r['coiling']}h  | BBW: {r['bbw_val']:.1f}%\n"
        f"<b>OI 24h :</b> {r['oi_change_24h']:+.1f}%\n\n"
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
    for s in r["signals"][:9]:
        msg += f"  • {s}\n"

    bd  = r.get("bd", {})
    msg += (
        f"\n📐 <b>BREAKDOWN</b>\n"
        f"  Vol:{bd.get('vol',0)} Flat:{bd.get('flat',0)} "
        f"Struct:{bd.get('struct',0)} Pos:{bd.get('pos',0)} "
        f"4H:{bd.get('tf4h',0)} Ctx:{bd.get('ctx',0)} "
        f"Whale:{bd.get('whale',0)} Stealth:{bd.get('stealth',0)}\n"
        f"  [Prob] MVS:{pm.get('max_vol_spike','?')}x "
        f"Irr:{pm.get('vol_irregularity','?')} "
        f"ATR:{pm.get('norm_atr_pct','?')}%\n\n"
        f"📡 Funding:{r['funding']:.5f}  🕐 {utc_now()}\n"
        f"<i>⚠️ Bukan financial advice. Manage risk ketat.</i>"
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
            f"   🐋{r['ws']} | RVOL:{r['rvol']:.1f}x | {vol} | "
            f"T1:+{t1p:.0f}% | Prob:{prob:.0f}%\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════
#  🔍  PRE-FILTER
# ══════════════════════════════════════════════════════════════
def pre_score_ticker(ticker):
    """
    Quick scoring dari data ticker saja, sebelum ambil candles.
    Dipakai untuk memilih top-80 yang akan di-deep scan.
    FIX 5: diturunkan threshold agar coin low-volume seperti ESP/SKR lolos.
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

    ps   = 0
    dist = (high24h - cur) / cur * 100 if cur > 0 and high24h > cur else 0

    # FIX 6: beri poin lebih untuk JAUH dari high (flat = pre-pump)
    # v8.2: near high dapat +18 (sinyal terlambat)
    # v9.0: flat/jauh dari high = potensi pre-pump sejati
    if 10 <= dist <= 30:
        ps += 5   # flat, belum naik — ideal pre-pump
    elif 5 <= dist < 10:
        ps += 3
    elif dist < 5:
        ps += 1   # sudah dekat high — mungkin sudah pump
    elif dist > 40:
        ps -= 2   # terlalu jauh = coin mati

    # Perubahan 24h — sedikit naik atau flat lebih baik dari sudah naik banyak
    if -3 <= chg24h <= 5:
        ps += 4   # flat atau naik sedikit = ideal
    elif 5 < chg24h <= 12:
        ps += 2
    elif -8 <= chg24h < -3:
        ps += 3   # turun sedikit = bahan bakar short squeeze
    elif chg24h > 20:
        ps -= 3   # sudah pump besar

    # Range 24h sempit = akumulasi
    if low24h > 0:
        range24 = (high24h - low24h) / low24h * 100
        if range24 <= 8:
            ps += 3   # range sempit = akumulasi
        elif range24 <= 15:
            ps += 1
        elif range24 > 40:
            ps -= 2

    # Volume — FIX 5: beri nilai untuk range lebih lebar
    if 50_000 <= vol <= 5_000_000:
        ps += 3
    elif 10_000 <= vol < 50_000:
        ps += 2   # low volume tapi masih aktif (ESP/SKR territory)
    elif vol > 20_000_000:
        ps -= 1

    return ps


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v9.0 — FORENSIC-CALIBRATED — {utc_now()} ===")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total ticker: {len(tickers)}")

    # ── Build candidate list ───────────────────────────────────
    candidates = []
    for sym, t in tickers.items():
        # Harus suffix USDT
        if not sym.endswith("USDT"):
            continue

        # FIX 1: Exclude token saham DI SINI (pre-filter level)
        # v8.2: filter hanya ada di run_scan() tapi setelah pre_score
        if sym in STOCK_TICKERS:
            log.debug(f"  SKIP stock token: {sym}")
            continue

        # Exclude keyword umum
        if any(kw in sym for kw in EXCLUDED_KEYWORDS):
            continue

        # Cooldown
        if is_cooldown(sym):
            continue

        try:
            vol   = float(t.get("quoteVolume", 0))
            chg   = float(t.get("change24h",   0)) * 100
            price = float(t.get("lastPr",       0))
        except:
            continue

        # Volume minimum — FIX 5: diturunkan
        if vol   < CONFIG["pre_filter_vol"]:       continue
        if vol   > CONFIG["max_vol_24h"]:          continue
        if abs(chg) > CONFIG["gate_chg_24h_max"]:  continue
        if price <= 0:                              continue

        ps = pre_score_ticker(t)
        candidates.append((sym, ps, vol))

    # Sort: pre-score descending, vol descending sebagai tiebreak
    candidates.sort(key=lambda x: (-x[1], -x[2]))
    candidates = candidates[:CONFIG["max_deep_scan"]]
    log.info(f"Pre-filter lolos: {len(candidates)} → deep scan")

    # ── Deep scan ─────────────────────────────────────────────
    results = []
    for i, (sym, ps, vol) in enumerate(candidates):
        t = tickers.get(sym)
        if not t:
            continue
        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum")
            continue

        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K, pre={ps})...")
        try:
            res = master_score(sym, t, tickers)
            if res:
                log.info(
                    f"  Score={res['score']} W={res['ws']} "
                    f"RVOL={res['rvol']:.1f}x "
                    f"Prob={res['prob_score']*100:.0f}% ({res['prob_class']}) "
                    f"T1=+{res['entry']['liq_pct']:.1f}%"
                )
                if res["score"] >= CONFIG["min_score_alert"]:
                    results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    results.sort(key=lambda x: (x["score"], x["ws"]), reverse=True)
    log.info(f"Lolos threshold: {len(results)} coin")

    # Qualified: harus punya whale signal ATAU score cukup tinggi
    qualified = [
        r for r in results
        if r["ws"] >= CONFIG["min_whale_score"] or r["score"] >= 60
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
                f"S={r['score']} W={r['ws']} "
                f"Prob={r['prob_score']*100:.0f}%"
            )
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v9.0 — FORENSIC CALIBRATED     ║")
    log.info("╚═══════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
