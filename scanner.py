"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v16.0 — Quantitative Pump Detection System                ║
║                                                                              ║
║  ARSITEKTUR BARU v16 (berdasarkan laporan analisis v15.8):                  ║
║                                                                              ║
║  MASALAH v15.8 yang diselesaikan:                                            ║
║  1. Scoring terlalu konservatif — baru detect fase 3 (breakout).            ║
║     v16 target fase 2 (volume spike) = entry lebih awal.                    ║
║  2. Volume spike tidak diprioritaskan — v16 bobot volume = 30/100.          ║
║  3. Tidak ada momentum detection — v16 pakai price/volume acceleration.     ║
║  4. Tidak ada booster logic — v16 punya 4 kombinasi booster.                ║
║  5. Tidak ada early pump detection — v16 punya watchlist + alert tier.      ║
║                                                                              ║
║  MODEL SCORING v16 — Quant Multi-Factor (total 100 poin):                   ║
║  ┌─────────────────────────────────────────────────────┐                    ║
║  │  Komponen          Bobot  Formula                   │                    ║
║  │  Volume Signal      30    log(vol_ratio) × 30       │                    ║
║  │  Price Momentum     20    price_accel + chg score   │                    ║
║  │  Buy Pressure       20    buy_ratio score           │                    ║
║  │  Momentum Accel     20    vol_accel + price_accel   │                    ║
║  │  Volatility/Struct  10    BBsq + ATRc + HTF         │                    ║
║  └─────────────────────────────────────────────────────┘                    ║
║                                                                              ║
║  BOOSTER LOGIC:                                                              ║
║  vol_ratio > 4 dan buy_ratio > 0.70  → +15 poin                             ║
║  price_chg > 3% dan vol_ratio > 3    → +12 poin                             ║
║  momentum > 2 dan buy_ratio > 0.65   → +10 poin                             ║
║  vol_accel > 1.5 dan buy rising      → +8  poin                             ║
║                                                                              ║
║  ALERT TIER:                                                                 ║
║  WATCHLIST  : score ≥ 35 (early warning — belum pump)                       ║
║  ALERT      : score ≥ 55 (sinyal kuat — pump imminent)                      ║
║  STRONG     : score ≥ 75 (konfirmasi ganda — entry segera)                  ║
║                                                                              ║
║  FAKE PUMP FILTER:                                                           ║
║  - buy_ratio < 0.45 saat vol spike = wash trade                             ║
║  - pump terlalu cepat (>8% dalam 1 candle) tanpa konsistensi                ║
║  - spread terlalu besar = manipulasi whale                                  ║
║                                                                              ║
║  DIPERTAHANKAN dari v15.8:                                                  ║
║  - OI snapshot persisten ke disk                                            ║
║  - Funding snapshot persisten                                               ║
║  - safe_get retry 429                                                       ║
║  - calc_entry dinamis (ATR/Pivot/Fib)                                       ║
║  - HTF accumulation 4H filter                                               ║
║  - Liquidity sweep detection                                                ║
║  - BB Squeeze + ATR contracting                                             ║
║  - VWAP gate                                                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests
import time
import os
import math
import json
import logging
import logging.handlers as _lh
from datetime import datetime, timezone
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging ───────────────────────────────────────────────────────────────────
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v16.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v16.0 — Quantitative Pump Detection System")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Alert threshold (dari 100 poin) ──────────────────────────────────────
    "score_watchlist":         35,   # early warning
    "score_alert":             55,   # sinyal kuat
    "score_strong":            75,   # entry segera
    "max_alerts_per_run":      15,

    # ── Filter awal market ────────────────────────────────────────────────────
    "min_vol_24h_usd":     50_000,
    "max_vol_24h_usd":  50_000_000,
    "min_oi_usd":          50_000,
    "gate_chg_24h_min":     -20.0,   # buang coin yang dump parah
    "gate_chg_24h_max":      20.0,   # buang coin yang sudah pump besar

    # ── Gate overbought ───────────────────────────────────────────────────────
    "gate_rsi_max":           78.0,
    "gate_bb_pos_max":         1.1,
    "gate_price_pos_max":      0.90, # posisi harga max 90% dari range 48h

    # ── VWAP gate ─────────────────────────────────────────────────────────────
    "vwap_gate_tolerance":     0.95,  # price > vwap * 0.95

    # ── Uptrend gate ──────────────────────────────────────────────────────────
    "gate_uptrend_max_hours":  12,

    # ═══════════════════════════════════════════════════════════════════════════
    #  QUANT SCORING MODEL — bobot komponen (total max ~100)
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Komponen 1: Volume Signal (bobot 30) ──────────────────────────────────
    # Formula: score = min(30, log2(vol_ratio) × 15)
    # vol_ratio 2x → ~15 poin, 4x → ~30 poin, 8x → capped 30
    "vol_score_max":           30,
    "vol_score_log_mult":      15.0,
    "vol_ratio_baseline_min":  1.2,   # minimal vol_ratio untuk dapat skor

    # ── Komponen 2: Price Momentum (bobot 20) ─────────────────────────────────
    "price_mom_max":           20,
    "price_chg_1h_strong":     2.0,   # perubahan 1h > 2% → skor penuh
    "price_chg_1h_mild":       0.5,   # perubahan 1h > 0.5% → skor parsial

    # ── Komponen 3: Buy Pressure (bobot 20) ──────────────────────────────────
    # buy_ratio = buy_volume / total_volume (estimasi dari candle data)
    "buy_pressure_max":        20,
    "buy_ratio_strong":        0.65,  # dominasi buyer kuat
    "buy_ratio_mild":          0.55,  # buyer tipis lebih banyak

    # ── Komponen 4: Momentum Acceleration (bobot 20) ─────────────────────────
    # Mengukur percepatan: vol_accel + price_accel
    "momentum_max":            20,
    "vol_accel_strong":        1.0,   # vol meningkat > 100% dari candle sebelumnya
    "vol_accel_mild":          0.4,
    "price_accel_strong":      1.5,   # price_chg_1h > 1.5x price_chg_3h
    "price_accel_mild":        1.1,

    # ── Komponen 5: Struktur & Volatilitas (bobot 10) ─────────────────────────
    "structure_max":           10,
    "bb_squeeze_threshold":    0.04,  # BBW < 4%
    "atr_contract_ratio":      0.75,  # ATR 6c < 75% ATR 24c

    # ═══════════════════════════════════════════════════════════════════════════
    #  BOOSTER LOGIC — bonus poin untuk kombinasi sinyal kuat
    # ═══════════════════════════════════════════════════════════════════════════
    "boost_vol_buypressure":   15,    # vol_ratio > 4 dan buy_ratio > 0.70
    "boost_vol_buypressure_vol_min": 4.0,
    "boost_vol_buypressure_buy_min": 0.70,

    "boost_price_vol":         12,    # price_chg > 3% dan vol_ratio > 3
    "boost_price_vol_chg_min":  3.0,
    "boost_price_vol_ratio_min": 3.0,

    "boost_momentum":          10,    # vol_accel > 1.0 dan buy_ratio > 0.65
    "boost_momentum_accel_min": 1.0,
    "boost_momentum_buy_min":   0.65,

    "boost_vol_accel":          8,    # vol_accel > 1.5 dan buy_rising (3 candle)
    "boost_vol_accel_min":      1.5,

    # ═══════════════════════════════════════════════════════════════════════════
    #  EARLY PUMP DETECTION — watchlist criteria
    # ═══════════════════════════════════════════════════════════════════════════
    "early_vol_ratio_min":     2.0,
    "early_price_chg_min":     0.8,
    "early_buy_ratio_min":     0.58,

    # ═══════════════════════════════════════════════════════════════════════════
    #  FAKE PUMP FILTER
    # ═══════════════════════════════════════════════════════════════════════════
    "fake_buy_ratio_max":      0.45,  # vol spike tapi buy ratio rendah = wash trade
    "fake_spike_1c_max":       8.0,   # pump > 8% dalam 1 candle = suspicious

    # ═══════════════════════════════════════════════════════════════════════════
    #  OI & FUNDING
    # ═══════════════════════════════════════════════════════════════════════════
    "oi_strong_pct":           8.0,
    "oi_mild_pct":             3.0,
    "oi_score_strong":          8,
    "oi_score_mild":            4,
    "funding_penalty_avg":     0.0003,
    "funding_bonus_avg":      -0.0002,
    "funding_streak_min":       4,
    "funding_score_bonus":      5,
    "funding_score_penalty":   -5,

    # ── HTF Accumulation 4H ───────────────────────────────────────────────────
    "htf_atr_contract_ratio":  0.85,
    "htf_vol_ratio_min":       1.3,
    "htf_range_max_pct":       3.0,
    "htf_max_pos_in_range":    0.75,

    # ── Liquidity Sweep ───────────────────────────────────────────────────────
    "liq_sweep_lookback":      20,
    "liq_sweep_wick_min_pct":  0.3,

    # ── Entry / SL ────────────────────────────────────────────────────────────
    "entry_bos_buffer":        0.0005,
    "sl_swing_lookback":       12,
    "sl_swing_buffer":         0.003,
    "sl_atr_mult_min":         1.0,
    "sl_atr_mult_max":         3.0,
    "max_sl_pct":              8.0,
    "min_sl_pct":              0.5,

    # ── Operasional ───────────────────────────────────────────────────────────
    "alert_cooldown_sec":     1800,
    "sleep_coins":             0.8,
    "sleep_error":             3.0,
    "cooldown_file":          "./cooldown.json",
    "funding_snapshot_file":  "./funding.json",
    "oi_snapshot_file":       "./oi_snapshot.json",
    "history_file":           "./history.json",  # cache historical data

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h":               168,
    "candle_4h":                48,
}

MANUAL_EXCLUDE = set()
EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    # ── Tier 1: Large Cap ─────────────────────────────────────────────────────
    "DOGEUSDT", "ADAUSDT", "XMRUSDT", "LINKUSDT", "XLMUSDT", "HBARUSDT",
    "LTCUSDT", "AVAXUSDT", "SHIBUSDT", "SUIUSDT", "TONUSDT",
    "UNIUSDT", "DOTUSDT", "TAOUSDT", "AAVEUSDT", "PEPEUSDT",
    "ETCUSDT", "NEARUSDT", "ONDOUSDT", "POLUSDT", "ICPUSDT", "ATOMUSDT",
    "ENAUSDT", "KASUSDT", "ALGOUSDT", "RENDERUSDT", "FILUSDT", "APTUSDT",
    "ARBUSDT", "JUPUSDT", "SEIUSDT", "STXUSDT", "DYDXUSDT", "VIRTUALUSDT",
    # ── Tier 2: Mid Cap ───────────────────────────────────────────────────────
    "FETUSDT", "INJUSDT", "PYTHUSDT", "GRTUSDT", "TIAUSDT", "LDOUSDT",
    "OPUSDT", "ENSUSDT", "AXSUSDT", "PENDLEUSDT", "WIFUSDT", "SANDUSDT",
    "MANAUSDT", "COMPUSDT", "GALAUSDT", "RAYUSDT", "RUNEUSDT", "EGLDUSDT",
    "SNXUSDT", "ARUSDT", "CRVUSDT", "IMXUSDT", "EIGENUSDT", "JTOUSDT",
    "CELOUSDT", "MASKUSDT", "APEUSDT", "MOVEUSDT", "MINAUSDT", "SONICUSDT",
    "KAIAUSDT", "HYPEUSDT", "WLDUSDT", "STRKUSDT", "CFXUSDT", "BOMEUSDT",
    # ── Tier 3: Active Trading ────────────────────────────────────────────────
    "FLOKIUSDT", "CAKEUSDT", "CHZUSDT", "HNTUSDT", "ROSEUSDT", "IOTXUSDT",
    "ANKRUSDT", "ZILUSDT", "ONTUSDT", "ENJUSDT", "GMTUSDT", "NOTUSDT",
    "PEOPLEUSDT", "METISUSDT", "AIXBTUSDT", "GOATUSDT", "PNUTUSDT",
    "GRASSUSDT", "POPCATUSDT", "ORDIUSDT", "MOODENGUSDT", "BIOUSDT",
    "MAGICUSDT", "REZUSDT", "ARPAUSDT", "ACTUSDT", "USUALUSDT",
    "SLPUSDT", "XAIUSDT", "BLURUSDT", "ARKMUSDT", "API3USDT", "AGLDUSDT",
    "TNSRUSDT", "LAYERUSDT", "ANIMEUSDT", "YGGUSDT", "THEUSDT",
}

GRAN_MAP    = {"1h": "1H", "4h": "4H", "15m": "15m", "1d": "1D"}
BITGET_BASE = "https://api.bitget.com"
_cache      = {}

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
# ══════════════════════════════════════════════════════════════════════════════
def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception:
        pass
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  💾  FUNDING SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots = {}
_btc_candles_cache = {"ts": 0, "data": []}

def load_funding_snapshots():
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                _funding_snapshots = json.load(f)
    except Exception:
        _funding_snapshots = {}

def save_all_funding_snapshots():
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception:
        pass

def add_funding_snapshot(symbol, rate):
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({"ts": time.time(), "funding": rate})
    if len(_funding_snapshots[symbol]) > 48:
        _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

def get_funding_stats(symbol):
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None
    rates  = [s["funding"] for s in snaps]
    last6  = rates[-6:]
    avg6   = sum(last6) / len(last6)
    cumul  = sum(last6)
    streak = 0
    for f in reversed(rates):
        if f < 0: streak += 1
        else: break
    return {
        "avg":     avg6,
        "cumul":   cumul,
        "streak":  streak,
        "current": rates[-1],
        "n":       len(rates),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  💾  OI SNAPSHOTS — persisten ke disk
# ══════════════════════════════════════════════════════════════════════════════
_oi_snapshot = {}

def load_oi_snapshots():
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            _oi_snapshot = {k: v for k, v in data.items()
                            if now - v.get("ts", 0) < 7200}
            log.info(f"OI snapshots loaded: {len(_oi_snapshot)} coins")
        else:
            _oi_snapshot = {}
    except Exception:
        _oi_snapshot = {}

def save_oi_snapshots():
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(_oi_snapshot, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  💾  HISTORICAL DATA CACHE — untuk acceleration detection
# ══════════════════════════════════════════════════════════════════════════════
_history = {}

def load_history():
    global _history
    try:
        p = CONFIG["history_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            # Buang data lebih dari 2 jam
            _history = {
                sym: [e for e in entries if now - e["ts"] < 7200]
                for sym, entries in data.items()
            }
        else:
            _history = {}
    except Exception:
        _history = {}

def save_history():
    try:
        with open(CONFIG["history_file"], "w") as f:
            json.dump(_history, f)
    except Exception:
        pass

def add_history_entry(symbol, price, vol_ratio, buy_ratio):
    """Simpan snapshot singkat untuk hitung acceleration antar-run."""
    if symbol not in _history:
        _history[symbol] = []
    _history[symbol].append({
        "ts":        time.time(),
        "price":     price,
        "vol_ratio": vol_ratio,
        "buy_ratio": buy_ratio,
    })
    if len(_history[symbol]) > 12:
        _history[symbol] = _history[symbol][-12:]

def get_history_prev(symbol):
    """Ambil entry historis terakhir untuk perbandingan acceleration."""
    entries = _history.get(symbol, [])
    if len(entries) < 2:
        return None
    return entries[-2]

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=10):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit — tunggu 15s, lalu retry")
                time.sleep(15)
                continue
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN atau CHAT_ID tidak ada!")
        return False
    if len(msg) > 4000:
        msg = msg[:3900] + "\n<i>...[dipotong]</i>"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=15,
        )
        return r.status_code == 200
    except Exception as e:
        log.warning(f"Telegram error: {e}")
        return False

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════
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
        params={
            "symbol":      symbol,
            "granularity": g,
            "limit":       str(limit),
            "productType": "usdt-futures",
        },
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({
                "ts":         int(c[0]),
                "open":       float(c[1]),
                "high":       float(c[2]),
                "low":        float(c[3]),
                "close":      float(c[4]),
                "volume":     float(c[5]),
                "volume_usd": vol_usd,
            })
        except Exception:
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
            d_list = data.get("data") or []
            if d_list:
                return float(d_list[0].get("fundingRate", 0))
        except Exception:
            pass
    return 0.0

def get_btc_candles_cached(limit=48):
    global _btc_candles_cache
    if time.time() - _btc_candles_cache["ts"] < 300 and _btc_candles_cache["data"]:
        return _btc_candles_cache["data"]
    candles = get_candles("BTCUSDT", "1h", limit)
    if candles:
        _btc_candles_cache = {"ts": time.time(), "data": candles}
    return candles

def get_open_interest(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d = data["data"]
            if isinstance(d, list) and d:
                d = d[0]
            elif isinstance(d, list):
                return 0.0
            if "openInterestList" in d:
                oi_list = d.get("openInterestList") or []
                oi = float(oi_list[0].get("openInterest", 0)) if oi_list else float(d.get("openInterest", 0))
            else:
                oi = float(d.get("openInterest", d.get("holdingAmount", 0)))
            price = float(d.get("indexPrice", d.get("lastPr", 0)) or 0)
            if 0 < oi < 1e9 and price > 0:
                return oi * price
            return oi
        except Exception:
            pass
    return 0.0

def get_oi_change(symbol):
    global _oi_snapshot
    oi_now = get_open_interest(symbol)
    prev   = _oi_snapshot.get(symbol)
    if prev is None or oi_now <= 0:
        if oi_now > 0:
            _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
        return {"oi_now": oi_now, "oi_prev": 0.0, "change_pct": 0.0, "is_new": True}
    oi_prev    = prev["oi"]
    change_pct = ((oi_now - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0.0
    _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
    return {
        "oi_now":     round(oi_now, 2),
        "oi_prev":    round(oi_prev, 2),
        "change_pct": round(change_pct, 2),
        "is_new":     False,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📊  INDIKATOR TEKNIKAL
# ══════════════════════════════════════════════════════════════════════════════

def _atr_n(candles, n):
    trs = []
    for i in range(1, min(n + 1, len(candles))):
        idx = len(candles) - i
        if idx < 1: break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 0.0

def calc_atr_abs(candles, period=14):
    if len(candles) < period + 1:
        return candles[-1]["close"] * 0.01
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1: break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else candles[-1]["close"] * 0.01

def calc_atr_pct(candles, period=14):
    atr = calc_atr_abs(candles, period)
    cur = candles[-1]["close"]
    return (atr / cur * 100) if cur > 0 else 0.0

def calc_bbw(candles, period=20):
    if len(candles) < period:
        return 0.0, 0.5
    closes   = [c["close"] for c in candles[-period:]]
    mean     = sum(closes) / period
    std      = math.sqrt(sum((x - mean) ** 2 for x in closes) / period)
    bb_upper = mean + 2 * std
    bb_lower = mean - 2 * std
    bbw      = (bb_upper - bb_lower) / mean if mean > 0 else 0.0
    bb_pct   = ((candles[-1]["close"] - bb_lower) / (bb_upper - bb_lower)
                if bb_upper != bb_lower else 0.5)
    return bbw, bb_pct

def calc_vwap(candles, lookback=24):
    n = min(lookback, len(candles))
    if n == 0:
        return candles[-1]["close"] if candles else 0.0
    recent = candles[-n:]
    cum_tv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in recent)
    cum_v  = sum(c["volume"] for c in recent)
    return (cum_tv / cum_v) if cum_v > 0 else candles[-1]["close"]

def get_rsi(candles, period=14):
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))

def calc_swing_range_position(candles, lookback=48):
    n = min(lookback, len(candles))
    recent = candles[-n:]
    if not recent:
        return 0.5
    lo = min(c["low"]  for c in recent)
    hi = max(c["high"] for c in recent)
    rng = hi - lo
    if rng <= 0:
        return 0.5
    pos = (candles[-1]["close"] - lo) / rng
    return round(min(max(pos, 0.0), 1.0), 3)

def calc_uptrend_age(candles):
    if len(candles) < 4:
        return {"age_hours": 0, "is_late": False}
    streak = 0
    for i in range(len(candles) - 1, 0, -1):
        if candles[i]["close"] > candles[i-1]["close"]:
            streak += 1
        else:
            break
    return {
        "age_hours": streak,
        "is_late":   streak > CONFIG["gate_uptrend_max_hours"],
    }

def detect_bos_up(candles, lookback=8):
    if len(candles) < lookback + 1:
        return False, 0.0
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level  = max(prev_highs)
    return candles[-1]["close"] > bos_level, bos_level

def calc_support_resistance(candles, lookback=48, n_levels=3):
    if len(candles) < 10:
        return {"resistance": [], "support": [], "nearest_res": None, "nearest_sup": None}
    n      = min(lookback, len(candles))
    recent = candles[-n:]
    price  = candles[-1]["close"]

    pivots_high, pivots_low = [], []
    for i in range(1, len(recent) - 1):
        h, l = recent[i]["high"], recent[i]["low"]
        if h > recent[i-1]["high"] and h > recent[i+1]["high"]:
            pivots_high.append(h)
        if l < recent[i-1]["low"]  and l < recent[i+1]["low"]:
            pivots_low.append(l)

    def cluster(levels):
        if not levels:
            return []
        levels = sorted(levels)
        cls, cur = [], [levels[0]]
        for lv in levels[1:]:
            if (lv - cur[-1]) / cur[-1] < 0.005:
                cur.append(lv)
            else:
                cls.append((sum(cur) / len(cur), len(cur)))
                cur = [lv]
        cls.append((sum(cur) / len(cur), len(cur)))
        cls.sort(key=lambda x: -x[1])
        return [round(lv, 8) for lv, _ in cls[:n_levels]]

    res_all = cluster(pivots_high)
    sup_all = cluster(pivots_low)
    res = sorted([r for r in res_all if r > price * 1.001])[:n_levels]
    sup = sorted([s for s in sup_all if s < price * 0.999], reverse=True)[:n_levels]

    def fmt(lv): return {"level": round(lv, 8), "gap_pct": round((lv - price) / price * 100, 1)}

    return {
        "resistance":  [fmt(r) for r in res],
        "support":     [fmt(s) for s in sup],
        "nearest_res": fmt(res[0]) if res else None,
        "nearest_sup": fmt(sup[0]) if sup else None,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📈  QUANT INDICATORS (NEW in v16)
# ══════════════════════════════════════════════════════════════════════════════

def calc_buy_ratio(candles, lookback=6):
    """
    Estimasi buy_ratio dari candle data.
    Buy volume ≈ volume candle bullish (close > open).
    Lebih akurat jika exchange memberikan taker buy volume, tapi
    estimasi ini cukup untuk deteksi dominasi buyer.
    Lookback pendek (6 candle = 6 jam) untuk sensitifitas tinggi.
    """
    if len(candles) < lookback:
        return 0.5
    recent    = candles[-lookback:]
    total_vol = sum(c["volume_usd"] for c in recent)
    if total_vol <= 0:
        return 0.5
    # Candle bullish = buyer dominan pada periode itu
    # Bobot lebih pada candle dengan volume tinggi
    buy_vol = sum(
        c["volume_usd"] for c in recent
        if c["close"] >= c["open"]
    )
    return round(buy_vol / total_vol, 4)

def calc_buy_ratio_rising(candles, lookback=3):
    """
    Cek apakah buy ratio sedang meningkat (trend buyer makin dominan).
    Bandingkan buy_ratio 3 candle terbaru vs 3 candle sebelumnya.
    """
    if len(candles) < lookback * 2:
        return False
    recent = candles[-lookback:]
    prev   = candles[-(lookback * 2):-lookback]

    def br(c_list):
        tv = sum(c["volume_usd"] for c in c_list)
        if tv <= 0: return 0.5
        bv = sum(c["volume_usd"] for c in c_list if c["close"] >= c["open"])
        return bv / tv

    return br(recent) > br(prev)

def calc_volume_ratio(candles, lookback=24):
    """vol candle terakhir vs rata-rata lookback sebelumnya."""
    if len(candles) < lookback + 1:
        return 1.0
    avg = sum(c["volume_usd"] for c in candles[-(lookback + 1):-1]) / lookback
    if avg <= 0:
        return 1.0
    return round(candles[-1]["volume_usd"] / avg, 3)

def calc_vol_ratio_3h(candles, lookback=24):
    """Rata-rata vol 3 candle terakhir vs baseline — lebih stabil dari 1 candle."""
    if len(candles) < lookback + 3:
        return 1.0
    avg_3h   = sum(c["volume_usd"] for c in candles[-3:]) / 3
    baseline = sum(c["volume_usd"] for c in candles[-(lookback + 3):-3]) / lookback
    if baseline <= 0:
        return 1.0
    return round(avg_3h / baseline, 3)

def calc_volume_acceleration(candles):
    """
    Volume acceleration: vol_1h terbaru vs avg 3h sebelumnya.
    Nilai > 1.0 = volume sedang akselerasi kuat.
    """
    if len(candles) < 4:
        return 0.0
    vol_1h = candles[-1]["volume_usd"]
    vol_3h = sum(c["volume_usd"] for c in candles[-4:-1]) / 3
    if vol_3h <= 0:
        return 0.0
    return round((vol_1h - vol_3h) / vol_3h, 3)

def calc_price_acceleration(candles):
    """
    Price acceleration: bandingkan price change 1h terbaru vs avg 3h sebelumnya.
    Nilai > 1 = harga sedang akselerasi (breakout awal).
    """
    if len(candles) < 5:
        return 0.0
    # price change candle terakhir
    if candles[-2]["close"] <= 0:
        return 0.0
    chg_1h = abs(candles[-1]["close"] - candles[-2]["close"]) / candles[-2]["close"]

    # avg price change 3 candle sebelumnya
    chg_prev = []
    for i in range(2, 5):
        if len(candles) > i and candles[-i-1]["close"] > 0:
            chg_prev.append(
                abs(candles[-i]["close"] - candles[-i-1]["close"]) / candles[-i-1]["close"]
            )
    if not chg_prev:
        return 0.0
    avg_prev = sum(chg_prev) / len(chg_prev)
    if avg_prev <= 0:
        return 0.0
    return round(chg_1h / avg_prev, 3)

def calc_price_chg_1h(candles):
    """Price change candle terakhir (%)."""
    if len(candles) < 2 or candles[-2]["close"] <= 0:
        return 0.0
    return round((candles[-1]["close"] - candles[-2]["close"]) / candles[-2]["close"] * 100, 4)

def calc_price_chg_nh(candles, n=3):
    """Price change N candle terakhir (%)."""
    if len(candles) < n + 1 or candles[-n-1]["close"] <= 0:
        return 0.0
    return round((candles[-1]["close"] - candles[-n-1]["close"]) / candles[-n-1]["close"] * 100, 4)

def detect_fake_pump(candles, vol_ratio, buy_ratio):
    """
    Filter fake pump / wash trade / whale trap.
    Return (is_fake, reason).
    """
    # Wash trade: volume spike tapi buyer tidak dominan
    if vol_ratio > 2.5 and buy_ratio < CONFIG["fake_buy_ratio_max"]:
        return True, f"Wash trade (vol {vol_ratio:.1f}x tapi buy_ratio {buy_ratio:.0%})"

    # Pump sangat cepat dalam 1 candle tanpa konsistensi
    if len(candles) >= 2:
        last_chg = abs(candles[-1]["close"] - candles[-2]["close"]) / candles[-2]["close"] * 100
        if last_chg > CONFIG["fake_spike_1c_max"] and buy_ratio < 0.55:
            return True, f"Spike mencurigakan ({last_chg:.1f}% dalam 1c, buy ratio rendah)"

    return False, ""

def calc_htf_accumulation(candles_4h):
    """HTF accumulation filter — kondisi 4H mendukung akumulasi."""
    if len(candles_4h) < 16:
        return {"is_htf_accum": False, "label": "Data 4H tidak cukup"}

    atr_r = _atr_n(candles_4h, 4)
    atr_a = _atr_n(candles_4h, 12)
    atr_ratio = (atr_r / atr_a) if atr_a > 0 else 1.0

    vr = sum(c["volume_usd"] for c in candles_4h[-2:]) / 2
    va = (sum(c["volume_usd"] for c in candles_4h[-10:-2]) / 8
          if len(candles_4h) >= 10 else vr)
    vol_ratio = (vr / va) if va > 0 else 1.0

    r8 = candles_4h[-8:]
    hi8, lo8 = max(c["high"] for c in r8), min(c["low"] for c in r8)
    mid8 = (hi8 + lo8) / 2
    range_pct = ((hi8 - lo8) / mid8 * 100) if mid8 > 0 else 99.0

    price_pos = calc_swing_range_position(candles_4h, lookback=32)

    ok = (atr_ratio <= CONFIG["htf_atr_contract_ratio"] and
          vol_ratio  >= CONFIG["htf_vol_ratio_min"] and
          range_pct  <= CONFIG["htf_range_max_pct"] and
          price_pos  <  CONFIG["htf_max_pos_in_range"])

    return {
        "is_htf_accum": ok,
        "atr_ratio":    round(atr_ratio, 3),
        "vol_ratio":    round(vol_ratio, 2),
        "range_pct":    round(range_pct, 2),
        "price_pos":    price_pos,
        "label":        (f"4H HTF Accum — ATR{atr_ratio:.2f} vol{vol_ratio:.1f}x"
                         if ok else "—"),
    }

def detect_liquidity_sweep(candles, lookback=None):
    """Liquidity sweep / stop hunt detection."""
    if lookback is None:
        lookback = CONFIG["liq_sweep_lookback"]
    if len(candles) < lookback + 3:
        return {"is_sweep": False, "label": "—"}

    ref = candles[-(lookback + 3):-3]
    if not ref:
        return {"is_sweep": False, "label": "—"}

    support = sum(sorted(c["low"] for c in ref)[:3]) / 3

    for candle in candles[-3:]:
        rng = candle["high"] - candle["low"]
        if rng <= 0: continue
        wick = (candle["open"] - candle["low"]
                if candle["close"] > candle["open"]
                else candle["close"] - candle["low"])
        if (candle["low"] < support and
                candle["close"] > support and
                wick / rng >= CONFIG["liq_sweep_wick_min_pct"]):
            return {
                "is_sweep": True,
                "label":    f"🎯 Liq Sweep — support ${support:.6g}",
            }
    return {"is_sweep": False, "label": "—"}

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def find_swing_low_sl(candles):
    n = min(CONFIG["sl_swing_lookback"], len(candles) - 1)
    if n < 2: return None
    return min(c["low"] for c in candles[-(n + 1):-1]) * (1.0 - CONFIG["sl_swing_buffer"])

def calc_entry(candles, bos_level, alert_level, vwap, price_now, atr_abs_val=None, sr=None):
    if atr_abs_val is None:
        atr_abs_val = calc_atr_abs(candles)

    # Entry
    gap_vwap = (price_now - vwap) / vwap * 100 if vwap > 0 else 0
    if alert_level == "STRONG" and bos_level > 0 and bos_level < price_now * 1.05:
        entry        = bos_level * (1.0 + CONFIG["entry_bos_buffer"])
        entry_reason = "BOS"
    elif gap_vwap <= 2.0:
        entry        = max(vwap, price_now)
        entry_reason = "VWAP"
    else:
        entry        = price_now * 1.001
        entry_reason = "Market"
    if entry < price_now:
        entry = price_now * 1.001

    # SL
    sl_swing = find_swing_low_sl(candles)
    if sl_swing is None or sl_swing >= entry:
        sl_swing = entry - atr_abs_val * 2.0
    sl = max(sl_swing, entry - atr_abs_val * CONFIG["sl_atr_mult_max"])
    sl = min(sl, entry - atr_abs_val * CONFIG["sl_atr_mult_min"])
    sl = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry: sl = entry * 0.98

    # Target — kumpulkan resistance pivot
    res_levels = []
    if sr and sr.get("resistance"):
        for rv in sr["resistance"]:
            if rv["level"] > entry * 1.005:
                res_levels.append(rv["level"])

    lookback_long = min(168, len(candles))
    recent_long   = candles[-lookback_long:]
    pivot_highs   = []
    for i in range(2, len(recent_long) - 2):
        h = recent_long[i]["high"]
        if (h > recent_long[i-1]["high"] and h > recent_long[i-2]["high"] and
                h > recent_long[i+1]["high"] and h > recent_long[i+2]["high"]):
            pivot_highs.append(h)
    if pivot_highs:
        pivot_highs = sorted(set(pivot_highs))
        cls, cur = [], [pivot_highs[0]]
        for ph in pivot_highs[1:]:
            if (ph - cur[-1]) / cur[-1] < 0.015:
                cur.append(ph)
            else:
                cls.append(sum(cur) / len(cur))
                cur = [ph]
        cls.append(sum(cur) / len(cur))
        for lv in cls:
            if lv > entry * 1.005 and lv not in res_levels:
                res_levels.append(lv)
    res_levels = sorted(set(res_levels))

    swing_lo  = min(c["low"]  for c in recent_long)
    swing_hi  = max(c["high"] for c in recent_long)
    swing_rng = swing_hi - swing_lo
    pos_pct   = ((entry - swing_lo) / swing_rng) if swing_rng > 0 else 0.5

    if pos_pct < 0.4:   m1, m2 = 3.5, 6.5
    elif pos_pct < 0.6: m1, m2 = 2.5, 5.0
    else:               m1, m2 = 1.5, 3.0

    af1 = entry + atr_abs_val * m1
    af2 = entry + atr_abs_val * m2

    if res_levels:
        t1, t1_src = res_levels[0], "R1"
        t2, t2_src = (res_levels[1], "R2") if len(res_levels) >= 2 else (t1 * 1.272, "R1×1.27")
        if t1 < af1: t1, t1_src = af1, f"ATR×{m1:.0f}"
        if t2 < af2: t2, t2_src = af2, f"ATR×{m2:.0f}"
    else:
        if swing_rng > atr_abs_val * 2 and swing_lo < entry:
            t1, t1_src = entry + swing_rng * 0.382, "Fib38%"
            t2, t2_src = entry + swing_rng * 0.618, "Fib62%"
        else:
            t1, t1_src = af1, f"ATR×{m1:.0f}"
            t2, t2_src = af2, f"ATR×{m2:.0f}"
        if t1 < af1: t1 = af1
        if t2 < af2: t2 = af2

    if t2 <= t1: t2 = t1 * (1 + (atr_abs_val / entry) * m1)
    t1 = max(t1, entry * 1.005)
    t2 = max(t2, t1   * 1.005)

    risk   = entry - sl
    rr_val = round((t1 - entry) / risk, 1) if risk > 0 else 0.0

    return {
        "entry":       round(entry, 8),
        "sl":          round(sl, 8),
        "sl_pct":      round((entry - sl) / entry * 100, 2),
        "t1":          round(t1, 8),
        "t2":          round(t2, 8),
        "gain_t1_pct": round((t1 - entry) / entry * 100, 1),
        "gain_t2_pct": round((t2 - entry) / entry * 100, 1),
        "rr":          rr_val,
        "rr_str":      f"{rr_val:.1f}",
        "method":      entry_reason,
        "atr_abs":     round(atr_abs_val, 8),
        "t1_src":      t1_src,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  QUANT PUMP SCORING ENGINE (v16 CORE)
# ══════════════════════════════════════════════════════════════════════════════

def quant_pump_score(
    vol_ratio, buy_ratio, price_chg_1h, vol_accel, price_accel,
    bbw, bb_pct, atr_contr_ratio, htf_accum, liq_sweep,
    oi_data, fstats, buy_ratio_rising
):
    """
    Quant Multi-Factor Pump Score (0–100+).

    Komponen:
      1. Volume Signal    (max 30) — log-scaled, volume adalah sinyal paling awal
      2. Price Momentum   (max 20) — price change 1h + acceleration
      3. Buy Pressure     (max 20) — dominasi buyer
      4. Momentum Accel   (max 20) — akselerasi volume + harga
      5. Struktur         (max 10) — BB squeeze, ATR contracting, HTF, sweep

    Bonus OI & Funding: di luar 100 (bisa push skor lebih tinggi)
    Booster: kombinasi sinyal kuat → bonus poin
    """
    score    = 0.0
    breakdown = {}

    # ── Komponen 1: Volume Signal (max 30) ────────────────────────────────────
    # Formula non-linear: log2(vol_ratio) × 15, capped 30
    # vol_ratio 1.2x → ~3 poin, 2x → 15, 4x → 30
    if vol_ratio >= CONFIG["vol_ratio_baseline_min"]:
        vol_s = min(CONFIG["vol_score_max"],
                    math.log2(max(vol_ratio, 1.0)) * CONFIG["vol_score_log_mult"])
    else:
        vol_s = 0.0
    score += vol_s
    breakdown["vol"] = round(vol_s, 1)

    # ── Komponen 2: Price Momentum (max 20) ───────────────────────────────────
    if price_chg_1h >= CONFIG["price_chg_1h_strong"]:
        price_s = CONFIG["price_mom_max"]          # 20
    elif price_chg_1h >= CONFIG["price_chg_1h_mild"]:
        # interpolasi linear
        ratio   = ((price_chg_1h - CONFIG["price_chg_1h_mild"]) /
                   (CONFIG["price_chg_1h_strong"] - CONFIG["price_chg_1h_mild"]))
        price_s = CONFIG["price_mom_max"] * 0.4 + ratio * CONFIG["price_mom_max"] * 0.6
    elif price_chg_1h > 0:
        price_s = CONFIG["price_mom_max"] * 0.2    # gerakan positif kecil
    else:
        price_s = 0.0
    score += price_s
    breakdown["price_mom"] = round(price_s, 1)

    # ── Komponen 3: Buy Pressure (max 20) ─────────────────────────────────────
    if buy_ratio >= CONFIG["buy_ratio_strong"]:
        buy_s = CONFIG["buy_pressure_max"]
    elif buy_ratio >= CONFIG["buy_ratio_mild"]:
        ratio = ((buy_ratio - CONFIG["buy_ratio_mild"]) /
                 (CONFIG["buy_ratio_strong"] - CONFIG["buy_ratio_mild"]))
        buy_s = CONFIG["buy_pressure_max"] * 0.4 + ratio * CONFIG["buy_pressure_max"] * 0.6
    elif buy_ratio > 0.5:
        buy_s = CONFIG["buy_pressure_max"] * 0.15
    else:
        buy_s = 0.0
    # Bonus kecil jika buy ratio sedang naik
    if buy_ratio_rising and buy_ratio >= 0.55:
        buy_s = min(CONFIG["buy_pressure_max"], buy_s + 3.0)
    score += buy_s
    breakdown["buy_pressure"] = round(buy_s, 1)

    # ── Komponen 4: Momentum Acceleration (max 20) ────────────────────────────
    # Volume acceleration
    if vol_accel >= CONFIG["vol_accel_strong"]:
        va_s = 10.0
    elif vol_accel >= CONFIG["vol_accel_mild"]:
        va_s = 10.0 * (vol_accel - CONFIG["vol_accel_mild"]) / (CONFIG["vol_accel_strong"] - CONFIG["vol_accel_mild"])
    else:
        va_s = 0.0

    # Price acceleration
    if price_accel >= CONFIG["price_accel_strong"]:
        pa_s = 10.0
    elif price_accel >= CONFIG["price_accel_mild"]:
        pa_s = 10.0 * (price_accel - CONFIG["price_accel_mild"]) / (CONFIG["price_accel_strong"] - CONFIG["price_accel_mild"])
    else:
        pa_s = 0.0

    mom_s = min(CONFIG["momentum_max"], va_s + pa_s)
    score += mom_s
    breakdown["momentum"] = round(mom_s, 1)

    # ── Komponen 5: Struktur & Volatilitas (max 10) ───────────────────────────
    struct_s = 0.0
    bb_squeeze = bbw < CONFIG["bb_squeeze_threshold"]
    atr_contr  = atr_contr_ratio <= CONFIG["atr_contract_ratio"]

    if bb_squeeze:         struct_s += 3.0
    if atr_contr:          struct_s += 2.0
    if htf_accum:          struct_s += 3.0
    if liq_sweep:          struct_s += 2.0

    struct_s = min(CONFIG["structure_max"], struct_s)
    score    += struct_s
    breakdown["structure"] = round(struct_s, 1)

    # ── OI Bonus (di luar 100) ────────────────────────────────────────────────
    oi_bonus = 0.0
    if not oi_data.get("is_new", True) and oi_data.get("oi_now", 0) > 0:
        chg = oi_data.get("change_pct", 0.0)
        if chg >= CONFIG["oi_strong_pct"]:
            oi_bonus = CONFIG["oi_score_strong"]
        elif chg >= CONFIG["oi_mild_pct"]:
            oi_bonus = CONFIG["oi_score_mild"]
    score += oi_bonus
    breakdown["oi"] = round(oi_bonus, 1)

    # ── Funding Bonus/Penalty ─────────────────────────────────────────────────
    fund_adj = 0.0
    if fstats:
        avg = fstats.get("avg", 0)
        if avg <= CONFIG["funding_bonus_avg"]:
            fund_adj = CONFIG["funding_score_bonus"]
        elif avg >= CONFIG["funding_penalty_avg"]:
            fund_adj = CONFIG["funding_score_penalty"]
        # Extra bonus: streak funding negatif
        if fstats.get("streak", 0) >= CONFIG["funding_streak_min"] and avg < 0:
            fund_adj += 2.0
    score    += fund_adj
    breakdown["funding"] = round(fund_adj, 1)

    # ══════════════════════════════════════════════════════════════════════════
    #  BOOSTER LOGIC — kombinasi sinyal kuat
    # ══════════════════════════════════════════════════════════════════════════
    boosts   = []
    boost_total = 0

    # Booster 1: Volume spike + buy pressure tinggi
    if (vol_ratio >= CONFIG["boost_vol_buypressure_vol_min"] and
            buy_ratio >= CONFIG["boost_vol_buypressure_buy_min"]):
        b = CONFIG["boost_vol_buypressure"]
        boost_total += b
        boosts.append(f"🚀 Vol×{vol_ratio:.1f} + Buy{buy_ratio:.0%} (+{b})")

    # Booster 2: Price momentum + volume
    elif (price_chg_1h >= CONFIG["boost_price_vol_chg_min"] and
            vol_ratio >= CONFIG["boost_price_vol_ratio_min"]):
        b = CONFIG["boost_price_vol"]
        boost_total += b
        boosts.append(f"💥 Price+{price_chg_1h:.1f}% + Vol×{vol_ratio:.1f} (+{b})")

    # Booster 3: Momentum acceleration + buy pressure
    if (vol_accel >= CONFIG["boost_momentum_accel_min"] and
            buy_ratio >= CONFIG["boost_momentum_buy_min"]):
        b = CONFIG["boost_momentum"]
        boost_total += b
        boosts.append(f"⚡ VolAccel{vol_accel:.1f} + Buy{buy_ratio:.0%} (+{b})")

    # Booster 4: Volume acceleration kuat + buy rising
    elif (vol_accel >= CONFIG["boost_vol_accel_min"] and buy_ratio_rising):
        b = CONFIG["boost_vol_accel"]
        boost_total += b
        boosts.append(f"📈 VolAccel{vol_accel:.1f} + BuyRising (+{b})")

    score += boost_total
    breakdown["boost"] = boost_total

    # Clamp negatif
    score = max(0.0, score)

    return {
        "score":     round(score, 1),
        "breakdown": breakdown,
        "boosts":    boosts,
        "bb_squeeze":  bb_squeeze,
        "atr_contr":   atr_contr,
    }

def score_to_prob(score):
    """Konversi skor ke probabilitas pump (0–100%)."""
    # Sigmoid-like mapping: score 35 → ~30%, 55 → ~55%, 75 → ~75%, 90+ → ~90%
    clamped = max(0.0, min(100.0, score))
    prob    = 100 / (1 + math.exp(-0.08 * (clamped - 55)))
    return round(prob, 1)

def get_alert_tier(score):
    if score >= CONFIG["score_strong"]:
        return "STRONG", "🔥"
    elif score >= CONFIG["score_alert"]:
        return "ALERT", "📡"
    elif score >= CONFIG["score_watchlist"]:
        return "WATCHLIST", "👀"
    return None, None

def is_early_pump(vol_ratio, price_chg_1h, buy_ratio):
    """Early pump detection — fase 2 sebelum breakout penuh."""
    return (vol_ratio  >= CONFIG["early_vol_ratio_min"] and
            price_chg_1h >= CONFIG["early_price_chg_min"] and
            buy_ratio  >= CONFIG["early_buy_ratio_min"])

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    c4h = get_candles(symbol, "4h", CONFIG["candle_4h"])

    if len(c1h) < 48:
        log.info(f"  {symbol}: Data tidak cukup ({len(c1h)} candle)")
        return None

    try:
        vol_24h   = float(ticker.get("quoteVolume", 0))
        chg_24h   = float(ticker.get("change24h",  0)) * 100
        price_now = float(ticker.get("lastPr",      0)) or c1h[-1]["close"]
    except Exception:
        return None

    if vol_24h <= 0 or price_now <= 0:
        return None

    # ── GATE 0: OI minimum ────────────────────────────────────────────────────
    oi_data = get_oi_change(symbol)
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]:
        log.info(f"  {symbol}: OI ${oi_data['oi_now']:,.0f} terlalu kecil — skip")
        return None

    # ── GATE 1: Funding snapshot ──────────────────────────────────────────────
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)
    fstats  = get_funding_stats(symbol) or {
        "avg": funding, "cumul": funding, "streak": 0,
        "current": funding, "n": 1,
    }

    # ── GATE 2: VWAP ──────────────────────────────────────────────────────────
    vwap = calc_vwap(c1h, lookback=24)
    if price_now < vwap * CONFIG["vwap_gate_tolerance"]:
        log.info(f"  {symbol}: Harga di bawah VWAP gate — skip")
        return None

    # ── Hitung semua indikator ────────────────────────────────────────────────
    bbw, bb_pct      = calc_bbw(c1h)
    atr_abs_val      = calc_atr_abs(c1h)
    atr_pct_val      = calc_atr_pct(c1h)
    atr_s            = _atr_n(c1h, 6)
    atr_l            = _atr_n(c1h, 24)
    atr_contr_ratio  = (atr_s / atr_l) if atr_l > 0 else 1.0

    rsi              = get_rsi(c1h[-48:])
    bos_up, bos_lvl  = detect_bos_up(c1h)
    price_pos_48     = calc_swing_range_position(c1h, lookback=48)
    uptrend          = calc_uptrend_age(c1h)
    sr               = calc_support_resistance(c1h)
    htf_accum        = calc_htf_accumulation(c4h)
    liq_sweep        = detect_liquidity_sweep(c1h)

    # Quant indicators
    vol_ratio        = calc_volume_ratio(c1h)
    vol_ratio_3h     = calc_vol_ratio_3h(c1h)
    vol_accel        = calc_volume_acceleration(c1h)
    price_chg_1h     = calc_price_chg_1h(c1h)
    price_accel      = calc_price_acceleration(c1h)
    buy_ratio        = calc_buy_ratio(c1h, lookback=6)
    buy_ratio_rising = calc_buy_ratio_rising(c1h, lookback=3)

    # ── GATE 3: Uptrend tidak terlalu tua ─────────────────────────────────────
    if uptrend["is_late"]:
        log.info(f"  {symbol}: Uptrend {uptrend['age_hours']}h — terlalu tua, skip")
        return None

    # ── GATE 4: RSI tidak overbought ──────────────────────────────────────────
    if rsi >= CONFIG["gate_rsi_max"]:
        log.info(f"  {symbol}: RSI {rsi:.1f} overbought — skip")
        return None

    # ── GATE 5: BB tidak di puncak ────────────────────────────────────────────
    if bb_pct >= CONFIG["gate_bb_pos_max"]:
        log.info(f"  {symbol}: BB pos {bb_pct:.0%} — overbought BB, skip")
        return None

    # ── GATE 6: Posisi harga tidak di zona distribusi atas ───────────────────
    if price_pos_48 > CONFIG["gate_price_pos_max"]:
        log.info(f"  {symbol}: Posisi harga {price_pos_48:.0%} — distribusi, skip")
        return None

    # ── GATE 7: Fake pump filter ──────────────────────────────────────────────
    is_fake, fake_reason = detect_fake_pump(c1h, vol_ratio, buy_ratio)
    if is_fake:
        log.info(f"  {symbol}: Fake pump — {fake_reason} — skip")
        return None

    # ── Early pump detection ──────────────────────────────────────────────────
    early_pump = is_early_pump(vol_ratio, price_chg_1h, buy_ratio)

    # ── Quant scoring ─────────────────────────────────────────────────────────
    qs = quant_pump_score(
        vol_ratio      = vol_ratio,
        buy_ratio      = buy_ratio,
        price_chg_1h   = price_chg_1h,
        vol_accel      = vol_accel,
        price_accel    = price_accel,
        bbw            = bbw,
        bb_pct         = bb_pct,
        atr_contr_ratio = atr_contr_ratio,
        htf_accum      = htf_accum["is_htf_accum"],
        liq_sweep      = liq_sweep["is_sweep"],
        oi_data        = oi_data,
        fstats         = fstats,
        buy_ratio_rising = buy_ratio_rising,
    )

    score = qs["score"]
    tier, tier_icon = get_alert_tier(score)

    # Watchlist juga lolos jika early pump terdeteksi
    if tier is None and early_pump:
        tier, tier_icon = "WATCHLIST", "👀"

    if tier is None:
        log.info(f"  {symbol}: Skor {score:.0f} < threshold — skip")
        return None

    prob = score_to_prob(score)

    # ── Entry & target ────────────────────────────────────────────────────────
    entry_data = calc_entry(
        c1h, bos_lvl, tier, vwap, price_now,
        atr_abs_val=atr_abs_val, sr=sr
    )

    # Simpan history
    add_history_entry(symbol, price_now, vol_ratio, buy_ratio)

    return {
        "symbol":          symbol,
        "score":           score,
        "prob":            prob,
        "tier":            tier,
        "tier_icon":       tier_icon,
        "breakdown":       qs["breakdown"],
        "boosts":          qs["boosts"],
        "entry":           entry_data,
        "price":           price_now,
        "chg_24h":         round(chg_24h, 2),
        "vol_24h":         vol_24h,
        "rsi":             round(rsi, 1),
        "bbw":             round(bbw * 100, 2),
        "bb_pct":          round(bb_pct * 100, 1),
        "bb_squeeze":      qs["bb_squeeze"],
        "atr_pct":         round(atr_pct_val, 2),
        "atr_contr":       qs["atr_contr"],
        "atr_contr_ratio": round(atr_contr_ratio, 3),
        "vwap":            round(vwap, 8),
        "bos_up":          bos_up,
        "bos_level":       round(bos_lvl, 8),
        "price_pos_48":    price_pos_48,
        "uptrend_age":     uptrend["age_hours"],
        "vol_ratio":       round(vol_ratio, 2),
        "vol_ratio_3h":    round(vol_ratio_3h, 2),
        "vol_accel":       round(vol_accel, 3),
        "price_chg_1h":    round(price_chg_1h, 2),
        "price_accel":     round(price_accel, 2),
        "buy_ratio":       round(buy_ratio * 100, 1),
        "buy_ratio_rising": buy_ratio_rising,
        "early_pump":      early_pump,
        "sr":              sr,
        "htf_accum":       htf_accum,
        "liq_sweep":       liq_sweep,
        "oi_data":         oi_data,
        "fstats":          fstats,
        "funding":         round(funding * 100, 5),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER — Ringkas, fokus entry/SL/TP
# ══════════════════════════════════════════════════════════════════════════════
def _fmt(p):
    """Format harga ringkas."""
    if p == 0:   return "0"
    if p >= 100: return f"{p:.2f}"
    if p >= 1:   return f"{p:.4f}"
    if p >= 0.01:return f"{p:.5f}"
    return f"{p:.8f}"

def build_alert(r, rank=None):
    """
    Pesan Telegram ringkas — fokus pada data trading penting.
    Format: harga saat ini, entry, SL, TP1, TP2, RR.
    """
    e    = r["entry"]
    tier = r["tier"]
    icon = r["tier_icon"]

    # ── Header ────────────────────────────────────────────────────────────────
    rank_str = f" #{rank}" if rank else ""
    msg  = f"{icon} <b>{r['symbol']}</b>{rank_str} — {tier}\n"
    msg += f"Score: <b>{r['score']:.0f}/100</b>  Prob: <b>{r['prob']}%</b>\n"

    # ── Harga saat ini ────────────────────────────────────────────────────────
    msg += "─────────────────────\n"
    msg += f"💰 Harga : <code>{_fmt(r['price'])}</code>  ({r['chg_24h']:+.1f}% 24h)\n"

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    msg += "─────────────────────\n"
    msg += f"📍 Entry : <code>{_fmt(e['entry'])}</code>  [{e['method']}]\n"
    msg += f"🛑 SL    : <code>{_fmt(e['sl'])}</code>  (-{e['sl_pct']:.1f}%)\n"
    msg += f"🎯 TP1   : <code>{_fmt(e['t1'])}</code>  (+{e['gain_t1_pct']:.1f}%)  [{e['t1_src']}]\n"
    msg += f"🎯 TP2   : <code>{_fmt(e['t2'])}</code>  (+{e['gain_t2_pct']:.1f}%)\n"
    msg += f"⚖️ RR    : {e['rr_str']}x\n"

    # ── Volume & Buy Pressure ─────────────────────────────────────────────────
    msg += "─────────────────────\n"
    vol_emoji = "🔥" if r["vol_ratio"] >= 4 else ("📈" if r["vol_ratio"] >= 2 else "📊")
    buy_emoji = "🟢" if r["buy_ratio"] >= 65 else ("🟡" if r["buy_ratio"] >= 55 else "🔴")
    msg += f"{vol_emoji} Vol    : {r['vol_ratio']:.1f}x  Accel: {r['vol_accel']*100:+.0f}%\n"
    msg += f"{buy_emoji} Buy%  : {r['buy_ratio']:.0f}%"
    if r["buy_ratio_rising"]:
        msg += "  ↑ rising"
    msg += "\n"
    msg += f"📊 RSI   : {r['rsi']}  |  Pos: {r['price_pos_48']:.0%} dari range\n"

    # ── Booster aktif ─────────────────────────────────────────────────────────
    if r["boosts"]:
        msg += "─────────────────────\n"
        for b in r["boosts"][:2]:
            msg += f"• {b}\n"

    # ── Early pump flag ───────────────────────────────────────────────────────
    if r["early_pump"] and tier == "WATCHLIST":
        msg += "⚡ <i>Early pump signal — pantau ketat</i>\n"

    msg += f"\n<i>v16 | {utc_now()} | ⚠️ Bukan financial advice</i>"
    return msg

def build_summary(results):
    """Summary ringkas untuk semua kandidat top."""
    msg = f"📋 <b>PUMP SCANNER v16 — {utc_now()}</b>\n"
    msg += f"{'─'*24}\n"
    for i, r in enumerate(results, 1):
        e      = r["entry"]
        icon   = r["tier_icon"]
        vol_str = f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K"
        msg += (
            f"{i}. {icon} <b>{r['symbol']}</b>  [{r['tier']} | {r['score']:.0f}pt | {r['prob']}%]\n"
            f"   Now:{_fmt(r['price'])}  TP1:+{e['gain_t1_pct']}%  "
            f"Vol:{r['vol_ratio']:.1f}x  Buy:{r['buy_ratio']:.0f}%\n"
        )
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found      = []
    filtered_stats = defaultdict(int)

    for sym in WHITELIST_SYMBOLS:
        if any(kw in sym for kw in EXCLUDED_KEYWORDS):
            filtered_stats["excluded_kw"] += 1
            continue
        if sym in MANUAL_EXCLUDE:
            filtered_stats["manual_exclude"] += 1
            continue
        if is_cooldown(sym):
            filtered_stats["cooldown"] += 1
            continue
        if sym not in tickers:
            not_found.append(sym)
            continue

        t = tickers[sym]
        try:
            vol   = float(t.get("quoteVolume", 0))
            chg   = float(t.get("change24h",   0)) * 100
            price = float(t.get("lastPr",       0))
        except Exception:
            filtered_stats["parse_error"] += 1
            continue

        if vol < CONFIG["min_vol_24h_usd"]:
            filtered_stats["vol_too_low"] += 1
            continue
        if vol > CONFIG["max_vol_24h_usd"]:
            filtered_stats["vol_too_high"] += 1
            continue
        if chg > CONFIG["gate_chg_24h_max"]:
            filtered_stats["chg_too_high"] += 1
            continue
        if chg < CONFIG["gate_chg_24h_min"]:
            filtered_stats["dump_too_deep"] += 1
            continue
        if price <= 0:
            filtered_stats["invalid_price"] += 1
            continue

        all_candidates.append((sym, t))

    total     = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    n_excl    = filtered_stats["excluded_kw"] + filtered_stats["manual_exclude"]
    n_filt    = sum(v for k, v in filtered_stats.items()
                    if k not in ("excluded_kw", "manual_exclude"))

    log.info("=" * 60)
    log.info(f"📊 SCAN SUMMARY v16")
    log.info(f"   Total whitelist  : {total}")
    log.info(f"   ✅ Akan discan   : {will_scan}")
    log.info(f"   🚫 Excluded      : {n_excl}")
    log.info(f"   ❌ Filtered      : {n_filt}")
    log.info(f"   ⚠️  Not in Bitget : {len(not_found)}")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"      {k:20s}: {v}")
    est = will_scan * CONFIG["sleep_coins"]
    log.info(f"   ⏱️  Est. waktu    : {est:.0f}s (~{est/60:.1f} min)")
    log.info("=" * 60)
    return all_candidates

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v16 — {utc_now()} ===")

    load_funding_snapshots()
    load_oi_snapshots()
    load_history()

    log.info(f"Funding loaded: {len(_funding_snapshots)} coins")
    log.info(f"OI snapshot loaded: {len(_oi_snapshot)} coins")
    log.info(f"History loaded: {len(_history)} coins")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner v16: Gagal ambil data Bitget")
        return
    log.info(f"Ticker Bitget: {len(tickers)}")

    candidates = build_candidate_list(tickers)
    results    = []

    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except Exception:
            vol = 0.0
        log.info(f"[{i+1}/{len(candidates)}] {sym} (${vol/1e3:.0f}K)...")

        try:
            res = master_score(sym, t)
            if res:
                log.info(
                    f"  ✅ {res['tier']} | Score={res['score']:.0f} "
                    f"Prob={res['prob']}% | Vol={res['vol_ratio']:.1f}x "
                    f"Buy={res['buy_ratio']:.0f}% | TP1=+{res['entry']['gain_t1_pct']}%"
                )
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    # Simpan semua state ke disk
    save_all_funding_snapshots()
    save_oi_snapshots()
    save_history()

    # Sort: STRONG dulu, lalu by score
    tier_order = {"STRONG": 0, "ALERT": 1, "WATCHLIST": 2}
    results.sort(key=lambda x: (tier_order.get(x["tier"], 9), -x["score"]))

    log.info(f"\nTotal sinyal: {len(results)}")
    if not results:
        log.info("Tidak ada sinyal — selesai.")
        return

    # Pisahkan tier
    strong    = [r for r in results if r["tier"] == "STRONG"]
    alerts    = [r for r in results if r["tier"] == "ALERT"]
    watchlist = [r for r in results if r["tier"] == "WATCHLIST"]

    log.info(f"  STRONG   : {len(strong)}")
    log.info(f"  ALERT    : {len(alerts)}")
    log.info(f"  WATCHLIST: {len(watchlist)}")

    # Kirim summary jika ada ≥ 2 hasil
    top_all = results[:CONFIG["max_alerts_per_run"]]
    if len(top_all) >= 2:
        send_telegram(build_summary(top_all))
        time.sleep(2)

    # Kirim alert detail: STRONG + ALERT dulu, watchlist terakhir
    sent = 0
    for r in top_all:
        if sent >= CONFIG["max_alerts_per_run"]:
            break
        ok = send_telegram(build_alert(r, rank=sent + 1))
        if ok:
            set_cooldown(r["symbol"])
            sent += 1
            log.info(
                f"✅ Alert {sent}: {r['symbol']} "
                f"[{r['tier']} | {r['score']:.0f}pt]"
            )
        time.sleep(2)

    log.info(f"=== SELESAI v16 — {sent} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v16.0                                 ║")
    log.info("║  Quantitative Pump Detection System                     ║")
    log.info("║  Volume → Buy Pressure → Momentum → Probability         ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
