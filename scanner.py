"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v10.0                                                  ║
║                                                                          ║
║  Research-backed improvements dari 3 jurnal ilmiah:                     ║
║                                                                          ║
║  [J1] "The Doge of Wall Street" — arXiv 2105.00733                     ║
║       + StdRushOrder (threshold 12.8)                                   ║
║       + Sell Rush Dominance Gate (sumber FP utama)                     ║
║                                                                          ║
║  [J2] "Microstructure and Manipulation" — arXiv 2504.15790             ║
║       + On-The-Spot Pump Detector (30.7% pump yang terlewat)           ║
║       + On-The-Spot Weak Signal Gate                                    ║
║                                                                          ║
║  [J3] "Detecting P&D with Crypto-Assets" — MDPI 2023                   ║
║       + Bobot probability model dari RF feature importance              ║
║       + Volume 60-minute insider buying signal                          ║
║                                                                          ║
║  Perubahan vs v9.9:                                                     ║
║  - Whitelist diperbarui sesuai pilihan user                             ║
║  - Stratified bucket dinonaktifkan → scan semua whitelist langsung      ║
║  - Gate baru: Sell Rush Dominance, On-The-Spot Weak                    ║
║  - Sinyal baru: StdRushOrder, On-The-Spot, Vol60m                      ║
║  - Bobot prob: irr 0.20→0.26, slope 0.13→0.20, spike 0.28→0.22        ║
║  - min_prob_alert: 0.50 → 0.45 (lebih sensitif, kurangi FN)            ║
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

# ── Logging ───────────────────────────────────────────────────────────────
import logging.handlers as _lh
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v10.log", maxBytes=10*1024*1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)
log = logging.getLogger(__name__)
log.info("Log file aktif: /tmp/scanner_v10.log")


# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────
    "min_composite_alert":       52,
    "min_prob_alert":          0.45,   # v10.0: 0.50→0.45 [J3: lebih sensitif]
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
    "min_target_pct":             5.0,
    "max_sl_pct":                12.0,
    "atr_sl_mult":                1.5,
    "atr_t1_mult":                2.5,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":       3600,
    "sleep_coins":               0.8,
    "sleep_error":               3.0,
    "cooldown_file":    "/tmp/v10_cooldown.json",
    "oi_snapshot_file": "/tmp/v10_oi.json",

    # ── Stealth pattern ───────────────────────────────────────
    "stealth_max_vol":       80_000,
    "stealth_min_coiling":       15,
    "stealth_max_range":          4.0,

    # ── Short squeeze ─────────────────────────────────────────
    "squeeze_funding_max":    -0.0001,
    "squeeze_oi_change_min":     3.0,

    # ── Layer max scores ──────────────────────────────────────
    "max_vol_score":             50,   # v10.0: 30→50 (ada sinyal OTS + Vol60m)
    "max_flat_score":            20,
    "max_struct_score":          15,
    "max_pos_score":             15,
    "max_tf4h_score":             8,
    "max_ctx_score":             10,
    "max_whale_bonus":           20,
    "max_linea_score":           25,

    # ── Pump probability weights v10.0 (dari RF feature importance [J3]) ─
    "prob_mvs_w1":         22,   # max_vol_spike    30→22 [J3]
    "prob_irr_w2":         26,   # vol_irregularity 20→26 [J1: StdRushOrder proxy]
    "prob_avs_w3":         15,   # avg_vol_spike    15=15
    "prob_atr_w4":         17,   # norm_atr         20→17 [J3]
    "prob_slope_w5":       20,   # price_slope      15→20 [J3: avg_price tertinggi]

    # ── GC-2: Liquidation Detector ────────────────────────────
    "liq_window_min":            30,
    "liq_long_block_usd":    100_000,
    "liq_short_bonus_usd":   150_000,

    # ── GC-3: Linea Signature thresholds ─────────────────────
    "linea_oi_1h_min":            2.0,
    "linea_oi_24h_min":           3.0,
    "linea_rsi_max":             48.0,
    "linea_ls_max":               1.1,
    "linea_price_max_chg":        5.0,

    # ── GC-5: Micro-cap OI Acceleration ──────────────────────
    "max_oi_accel_score":        30,
    "oi_accel_micro_thresh": 3_000_000,
    "oi_accel_dormant_vol":    500_000,
    "oi_accel_weak":             15.0,
    "oi_accel_medium":           35.0,
    "oi_accel_strong":           70.0,
    "oi_accel_extreme":         120.0,
    "oi_accel_div_price_max":     5.0,
    "oi_dormant_baseline_mult":   3.0,

    # ── v9.9 BUG #2: Dead Activity Gate ──────────────────────
    "dead_activity_threshold":   0.10,

    # ── v10.0 NEW: Rush Order Config [J1] ────────────────────
    "rush_std_threshold":        12.8,
    "rush_std_micro_threshold":   6.4,
    "rush_micro_vol_usd":    500_000,
    "rush_sell_fp_ratio":        0.30,

    # ── v10.0 NEW: On-The-Spot Pump Config [J2] ──────────────
    "ots_no_accum_vol_ratio":    0.50,
    "ots_spike_vol_ratio":        5.0,
    "ots_price_change_min":      0.05,
    "ots_confirm_vol_ratio":      7.0,

    # ── v10.0 NEW: Volume 60-Minute Window [J3] ──────────────
    "vol60m_ratio_threshold":     3.0,
    "vol60m_accum_ratio":         2.0,

    # ── Log file ──────────────────────────────────────────────
    "log_file": "/tmp/scanner_v10.log",
    "log_max_mb": 10,
}

# ── WHITELIST — pilihan user ──────────────────────────────────────────────
WHITELIST_SYMBOLS = {
    "UAIUSDT", "VTHOUSDT", "RAVEUSDT", "NMRUSDT", "COAIUSDT", "GWEIUSDT", "MEUSDT", "ORCAUSDT",
    "BLURUSDT", "MERLUSDT", "MOODENGUSDT", "BIOUSDT", "SOMIUSDT", "B2USDT", "ORDIUSDT", "SPKUSDT",
    "ZAMAUSDT", "PARTIUSDT", "1000RATSUSDT", "SSVUSDT", "BIRBUSDT", "POPCATUSDT", "GUNUSDT", "BEATUSDT",
    "BANANAS31USDT", "LAUSDT", "LINEAUSDT", "DRIFTUSDT", "AVNTUSDT", "GRASSUSDT", "GPSUSDT", "PNUTUSDT",
    "CELOUSDT", "LUNAUSDT", "VANAUSDT", "TRIAUSDT", "IOTXUSDT", "POLYXUSDT", "ANKRUSDT", "SAHARAUSDT",
    "RPLUSDT", "MASKUSDT", "UMAUSDT", "TAGUSDT", "USELESSUSDT", "MEMEUSDT", "ATUSDT", "KGENUSDT",
    "SKYAIUSDT", "ONTUSDT", "ENJUSDT", "SIGNUSDT", "CTKUSDT", "NOTUSDT", "CYBERUSDT", "GMTUSDT",
    "FIDAUSDT", "CROSSUSDT", "STEEMUSDT", "LABUSDT", "BREVUSDT", "AUCTIONUSDT", "HOLOUSDT", "PEOPLEUSDT",
    "CVCUSDT", "IOUSDT", "BROCCOLIUSDT", "SXTUSDT", "CLANKERUSDT", "BIGTIMEUSDT", "BLASTUSDT", "THEUSDT",
    "XPINUSDT", "MANTAUSDT", "YGGUSDT", "WAXPUSDT", "ONGUSDT", "LAYERUSDT", "ANIMEUSDT", "BOMEUSDT",
    "C98USDT", "API3USDT", "AGLDUSDT", "MMTUSDT", "INXUSDT", "GIGGLEUSDT", "IDOLUSDT", "ARKMUSDT",
    "RESOLVUSDT", "EULUSDT", "METISUSDT", "SONICUSDT", "TNSRUSDT", "PROMUSDT", "SAPIENUSDT", "VELVETUSDT",
    "FLOCKUSDT", "BANKUSDT", "ALLOUSDT", "USUALUSDT", "SLPUSDT", "ARIAUSDT", "MIRAUSDT", "MAGICUSDT",
    "ZKCUSDT", "INUSDT", "NAORISUSDT", "MAGMAUSDT", "REZUSDT", "WCTUSDT", "FUSDT", "ELSAUSDT",
    "SPACEUSDT", "APRUSDT", "AIXBTUSDT", "GOATUSDT", "DENTUSDT", "JCTUSDT", "XAIUSDT", "AIOUSDT",
    "ZKPUSDT", "VINEUSDT", "METAUSDT", "FIGHTUSDT", "INITUSDT", "BASUSDT", "NEWTUSDT", "FUNUSDT",
    "FOLKSUSDT", "ARPAUSDT", "MOVRUSDT", "MUBARAKUSDT", "NOMUSDT", "ACTUSDT", "ZKJUSDT", "VANRYUSDT",
    "AINUSDT", "RECALLUSDT", "MAVUSDT", "CLOUSDT", "LIGHTUSDT", "TOWNSUSDT", "BLESSUSDT", "HAEDALUSDT",
    "4USDT", "USUSDT", "HEIUSDT", "OGUSDT",
    "DOGEUSDT", "BCHUSDT", "ADAUSDT", "HYPEUSDT", "XMRUSDT", "LINKUSDT", "XLMUSDT", "HBARUSDT",
    "LTCUSDT", "ZECUSDT", "AVAXUSDT", "SHIBUSDT", "SUIUSDT", "TONUSDT", "WLFIUSDT", "CROUSDT",
    "UNIUSDT", "DOTUSDT", "TAOUSDT", "MUSDT", "AAVEUSDT", "ASTERUSDT", "PEPEUSDT", "BGBUSDT",
    "SKYUSDT", "ETCUSDT", "NEARUSDT", "ONDOUSDT", "POLUSDT", "ICPUSDT", "WLDUSDT", "ATOMUSDT",
    "XDCUSDT", "COINUSDT", "NIGHTUSDT", "ENAUSDT", "PIPPINUSDT", "KASUSDT", "TRUMPUSDT", "QNTUSDT",
    "ALGOUSDT", "RENDERUSDT", "FILUSDT", "MORPHOUSDT", "APTUSDT", "SUPERUSDT", "VETUSDT", "PUMPUSDT",
    "1000SATSUSDT", "ARBUSDT", "1000BONKUSDT", "STABLEUSDT", "KITEUSDT", "JUPUSDT", "SEIUSDT", "ZROUSDT",
    "STXUSDT", "DYDXUSDT", "VIRTUALUSDT", "DASHUSDT", "PENGUUSDT", "CAKEUSDT", "JSTUSDT", "XTZUSDT",
    "ETHFIUSDT", "1MBABYDOGEUSDT", "IPUSDT", "LITUSDT", "HUSDT", "FETUSDT", "CHZUSDT", "CRVUSDT",
    "KAIAUSDT", "IMXUSDT", "BSVUSDT", "INJUSDT", "AEROUSDT", "PYTHUSDT", "IOTAUSDT", "EIGENUSDT",
    "GRTUSDT", "JASMYUSDT", "DEXEUSDT", "SPXUSDT", "TIAUSDT", "FLOKIUSDT", "HNTUSDT", "SIRENUSDT",
    "LDOUSDT", "CFXUSDT", "OPUSDT", "ENSUSDT", "STRKUSDT", "MONUSDT", "AXSUSDT", "SANDUSDT",
    "PENDLEUSDT", "WIFUSDT", "LUNCUSDT", "FFUSDT", "NEOUSDT", "THETAUSDT", "RIVERUSDT", "BATUSDT",
    "MANAUSDT", "CVXUSDT", "COMPUSDT", "BARDUSDT", "SENTUSDT", "GALAUSDT", "VVVUSDT", "RAYUSDT",
    "XPLUSDT", "FLUIDUSDT", "FARTCOINUSDT", "GLMUSDT", "RUNEUSDT", "0GUSDT", "POWERUSDT", "SKRUSDT",
    "EGLDUSDT", "BUSDT", "BERAUSDT", "SNXUSDT", "BANUSDT", "JTOUSDT", "ARUSDT", "COWUSDT",
    "DEEPUSDT", "SUSDT", "LPTUSDT", "MELANIAUSDT", "UBUSDT", "FOGOUSDT", "ARCUSDT", "WUSDT",
    "PIEVERSEUSDT", "AWEUSDT", "HOMEUSDT", "GASUSDT", "ICNTUSDT", "ZENUSDT", "XVGUSDT", "ROSEUSDT",
    "MYXUSDT", "KSMUSDT", "RSRUSDT", "ATHUSDT", "KMNOUSDT", "AKTUSDT", "ZORAUSDT", "ESPUSDT",
    "TOSHIUSDT", "STGUSDT", "ZILUSDT", "LYNUSDT", "APEUSDT", "KAITOUSDT", "FORMUSDT", "AZTECUSDT",
    "QUSDT", "MOVEUSDT", "MINAUSDT", "SOONUSDT", "TUSDT", "BRETTUSDT", "ACHUSDT", "TURBOUSDT",
    "NXPCUSDT", "ALCHUSDT", "ZETAUSDT", "MOCAUSDT", "CYSUSDT", "ASTRUSDT", "ENSOUSDT", "AXLUSDT",
}

MANUAL_EXCLUDE = set()

GRAN_MAP = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}

SECTOR_MAP = {
    "DEFI": [
        "SNXUSDT","ENSOUSDT","SIRENUSDT","CRVUSDT","CVXUSDT","COMPUSDT",
        "AAVEUSDT","UNIUSDT","DYDXUSDT","COWUSDT","PENDLEUSDT","MORPHOUSDT",
        "FLUIDUSDT","SSVUSDT","RSRUSDT","NMRUSDT","UMAUSDT","LDOUSDT","ENSUSDT",
    ],
    "ZK_PRIVACY": ["AZTECUSDT","MINAUSDT","STRKUSDT","ZORAUSDT","POLYXUSDT","ZKPUSDT","ZKCUSDT","ZKJUSDT"],
    "AI_CRYPTO": [
        "FETUSDT","RENDERUSDT","TAOUSDT","GRASSUSDT","AKTUSDT","VANAUSDT",
        "COAIUSDT","UAIUSDT","GRTUSDT","AIOUSDT","AIXBTUSDT","SKYAIUSDT",
    ],
    "SOLANA_ECO": [
        "ORCAUSDT","RAYUSDT","JTOUSDT","DRIFTUSDT","WIFUSDT","JUPUSDT",
        "1000BONKUSDT","PYTHUSDT",
    ],
    "LAYER1": [
        "APTUSDT","SUIUSDT","SEIUSDT","INJUSDT","KASUSDT","BERAUSDT",
        "MOVEUSDT","KAIAUSDT","TIAUSDT","EGLDUSDT","NEARUSDT","TONUSDT",
        "ALGOUSDT","HBARUSDT","STEEMUSDT","XTZUSDT","ZILUSDT","VETUSDT",
        "ESPUSDT","SONICUSDT","INITUSDT",
    ],
    "LAYER2": ["ARBUSDT","OPUSDT","CELOUSDT","STRKUSDT","LDOUSDT","POLUSDT","LINEAUSDT","MANTAUSDT","BLASTUSDT"],
    "GAMING": [
        "AXSUSDT","GALAUSDT","IMXUSDT","SANDUSDT","APEUSDT","SUPERUSDT",
        "CHZUSDT","ENJUSDT","GLMUSDT","BIGTIMEUSDT","MAGICUSDT","YGGUSDT",
    ],
    "MEME": [
        "PEPEUSDT","SHIBUSDT","FLOKIUSDT","BRETTUSDT","FARTCOINUSDT",
        "MEMEUSDT","TURBOUSDT","PNUTUSDT","POPCATUSDT","MOODENGUSDT",
        "1000BONKUSDT","TRUMPUSDT","WIFUSDT","TOSHIUSDT","BROCCOLIUSDT",
        "BANANAS31USDT","NOTUSDT","GMTUSDT","PEOPLEUSDT",
    ],
    "ORDI_BTC": [
        "ORDIUSDT","1000RATSUSDT","1000SATSUSDT","SPKUSDT",
    ],
    "LOW_CAP": [
        "VVVUSDT","POWERUSDT","ARCUSDT","VIRTUALUSDT","SPXUSDT",
        "ONDOUSDT","ENAUSDT","EIGENUSDT","STXUSDT","RUNEUSDT",
        "SKRUSDT","AVNTUSDT","AEROUSDT","GRASSUSDT","BIOUSDT",
        "VANAUSDT","GOATUSDT","PNUTUSDT","GUNUSDT",
    ],
}
SECTOR_LOOKUP = {coin: sec for sec, coins in SECTOR_MAP.items() for coin in coins}

BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_cache         = {}


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
    hist  = snaps.get(symbol, [])
    if len(hist) < 2:
        return 0, 0, False
    now = time.time()

    def nearest(target_ts, tolerance=1800):
        cands = [d for d in hist if abs(d["ts"] - target_ts) < tolerance]
        return min(cands, key=lambda d: abs(d["ts"] - target_ts)) if cands else None

    old1h  = nearest(now - 3600)
    old24h = nearest(now - 86400, tolerance=7200)

    if not old1h:
        older_snaps = [d for d in hist if d["ts"] < now - 60]
        if older_snaps:
            old1h = min(older_snaps, key=lambda d: d["ts"])

    chg1h  = (current_oi - old1h["oi"])  / old1h["oi"]  * 100 if old1h  and old1h["oi"]  else 0
    chg24h = (current_oi - old24h["oi"]) / old24h["oi"] * 100 if old24h and old24h["oi"] else 0
    oi_valid = (old1h is not None)
    return chg1h, chg24h, oi_valid


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
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/liquidation-orders",
        params={"symbol": symbol, "productType": "usdt-futures", "pageSize": "100"},
    )
    if not data or data.get("code") != "00000":
        return 0, 0
    try:
        orders  = data.get("data", {}).get("liquidationOrderList", [])
        now_ms  = int(time.time() * 1000)
        cutoff  = now_ms - CONFIG["liq_window_min"] * 60 * 1000
        long_liq = short_liq = 0.0
        for o in orders:
            ts  = int(o.get("cTime", 0))
            if ts < cutoff:
                continue
            usd  = float(o.get("size", 0)) * float(o.get("fillPrice", 0))
            side = o.get("side", "").lower()
            if "sell" in side:
                long_liq += usd
            else:
                short_liq += usd
        return long_liq, short_liq
    except:
        return 0, 0

def get_rsi(candles, period=14):
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
    return 100 - (100 / (1 + avg_g / avg_l))

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
    if len(candles_1h) < 24:
        return cur * 1.10, cur * 1.18
    recent = candles_1h[-168:]
    resistance_levels = []
    min_t = cur * (1 + CONFIG["min_target_pct"] / 100)
    for i in range(2, len(recent) - 2):
        h = recent[i]["high"]
        if h <= min_t:
            continue
        touches = sum(
            1 for c in recent
            if abs(c["high"] - h) / h < 0.015 or abs(c["low"] - h) / h < 0.015
        )
        if touches >= 2:
            resistance_levels.append((h, touches, recent[i]["volume_usd"]))
    if not resistance_levels:
        return round(cur * 1.10, 8), round(cur * 1.18, 8)
    resistance_levels.sort(key=lambda x: x[0])
    t1 = resistance_levels[0][0]
    t2 = resistance_levels[1][0] if len(resistance_levels) > 1 else t1 * 1.08
    return round(t1, 8), round(t2, 8)


# ══════════════════════════════════════════════════════════════
#  📊  INDIKATOR
# ══════════════════════════════════════════════════════════════
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
    return min(last_vol / avg, 30.0) if avg > 0 else 1.0

def calc_volume_spike_ratio(candles_1h):
    if len(candles_1h) < 24:
        return 1.0, 1.0
    vols     = [c["volume_usd"] for c in candles_1h]
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
    std = math.sqrt(sum((v - mean) ** 2 for v in vols) / len(vols))
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
        return -12, f"⚠️ CVD turun saat harga naik {price_chg:+.1f}% — distribusi"
    elif not cvd_rising and -1.5 <= price_chg <= 1.5:
        return -8, f"⚠️ CVD turun, harga flat — tekanan jual tersembunyi"
    elif not cvd_rising and price_chg < -1.5:
        return -5, f"⚠️ CVD turun saat harga {price_chg:+.1f}%"
    return 0, ""

def calc_short_term_cvd(candles_1h):
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
            return -10, f"⚠️ CVD 6h memburuk — tekanan jual meningkat ({price_chg_6h:+.1f}%)"
        return -5, f"⚠️ CVD 6h negatif"
    if recent_d > 0 and recent_d > prev_d * 1.2:
        if price_chg_6h < 2:
            return 8, f"✅ CVD 6h membaik — akumulasi baru ({price_chg_6h:+.1f}%)"
        return 4, f"CVD 6h positif"
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
#  🆕 v10.0 — FUNGSI BARU [J1] StdRushOrder
# ══════════════════════════════════════════════════════════════
def calc_rush_orders(trades, vol_24h=0):
    """
    Menghitung StdRushOrder dari recent trades [J1].
    Rush order = cluster market buy orders pada harga yang sama.
    Threshold: > 12.8 (normal), > 6.4 (micro-cap < $500K vol).
    """
    if not trades or len(trades) < 20:
        return 0, 0.5, 0, 0, []

    buy_trades  = [t for t in trades if t["side"] == "buy"]
    sell_trades = [t for t in trades if t["side"] == "sell"]
    if not buy_trades:
        return 0, 0.5, 0, 0, []

    # Cluster harga sebagai proxy rush order
    price_buckets = {}
    for t in buy_trades:
        key = round(t["price"], 4)
        if key not in price_buckets:
            price_buckets[key] = []
        price_buckets[key].append(t["size"])

    cluster_counts = [len(v) for v in price_buckets.values()]
    if len(cluster_counts) < 3:
        return 0, 0.5, 0, 0, []

    std_rush = float(np.std(cluster_counts))

    # Rasio buy vs sell rush
    sell_clusters   = {}
    for t in sell_trades:
        key = round(t["price"], 4)
        sell_clusters[key] = sell_clusters.get(key, 0) + 1
    total_buy_rush  = len(price_buckets)
    total_sell_rush = len(sell_clusters)
    buy_rush_ratio  = total_buy_rush / (total_buy_rush + total_sell_rush + 1e-8)

    # Spike ratio: late vs early trades
    mid          = len(buy_trades) // 2
    early_clust  = len(set(round(t["price"], 4) for t in buy_trades[:mid])) if mid > 0 else 1
    late_clust   = len(set(round(t["price"], 4) for t in buy_trades[mid:])) if mid > 0 else 1
    spike_ratio  = late_clust / (early_clust + 1e-8)

    is_micro    = (vol_24h > 0 and vol_24h < CONFIG["rush_micro_vol_usd"])
    threshold   = CONFIG["rush_std_micro_threshold"] if is_micro else CONFIG["rush_std_threshold"]
    micro_tag   = " [micro]" if is_micro else ""

    rush_score  = 0
    rush_signals = []

    if std_rush >= threshold * 1.5:
        rush_score += 20
        rush_signals.append(f"⚡ StdRushOrder {std_rush:.1f} — koordinasi KUAT{micro_tag} [J1]")
    elif std_rush >= threshold:
        rush_score += 14
        rush_signals.append(f"⚡ StdRushOrder {std_rush:.1f} — buy rush terdeteksi{micro_tag} [J1]")
    elif std_rush >= threshold * 0.6:
        rush_score += 7
        rush_signals.append(f"Rush orders membangun ({std_rush:.1f}){micro_tag}")

    if spike_ratio >= 10:
        rush_score += 10
        rush_signals.append(f"🚀 Rush spike {spike_ratio:.1f}x — aktivitas tiba-tiba [J1]")
    elif spike_ratio >= 5:
        rush_score += 6
        rush_signals.append(f"Rush spike {spike_ratio:.1f}x")

    return std_rush, buy_rush_ratio, spike_ratio, min(rush_score, 20), rush_signals


# ══════════════════════════════════════════════════════════════
#  🆕 v10.0 — FUNGSI BARU [J2] On-The-Spot Pump Detector
# ══════════════════════════════════════════════════════════════
def detect_on_the_spot_pump(candles_1h):
    """
    Mendeteksi On-The-Spot pump — 30.7% pump event tidak punya pre-accumulation [J2].
    Ciri: volume 4h sebelumnya RENDAH, lalu volume 1h terakhir SPIKE tiba-tiba.
    """
    if len(candles_1h) < 24:
        return 0, "unknown", []

    vols    = [c["volume_usd"] for c in candles_1h]
    closes  = [c["close"]      for c in candles_1h]
    avg_vol = sum(vols[-24:]) / 24

    if avg_vol <= 0:
        return 0, "unknown", []

    vol_4h_before   = sum(vols[-5:-1]) / 4 if len(vols) >= 5 else avg_vol
    vol_last_1h     = vols[-1]
    price_change_1h = ((closes[-1] - closes[-2]) / closes[-2]
                       if len(closes) >= 2 and closes[-2] > 0 else 0)

    no_accumulation = vol_4h_before < avg_vol * CONFIG["ots_no_accum_vol_ratio"]
    sudden_spike    = vol_last_1h   > avg_vol * CONFIG["ots_spike_vol_ratio"]
    price_rising    = price_change_1h > CONFIG["ots_price_change_min"]

    ots_score   = 0
    ots_type    = "unknown"
    ots_signals = []

    if no_accumulation and sudden_spike and price_rising:
        ots_type   = "on_the_spot"
        spike_mult = vol_last_1h / (avg_vol + 1e-8)
        ots_score  = 25
        ots_signals.append(
            f"🎯 ON-THE-SPOT PUMP: spike {spike_mult:.1f}x, "
            f"harga +{price_change_1h*100:.1f}% 1h, tanpa pre-accum [J2]"
        )
    elif no_accumulation and sudden_spike:
        ots_type   = "on_the_spot_early"
        spike_mult = vol_last_1h / (avg_vol + 1e-8)
        ots_score  = 12
        ots_signals.append(
            f"👀 Early on-the-spot: vol spike {spike_mult:.1f}x tanpa akumulasi [J2]"
        )
    else:
        vol_spikes      = sum(1 for v in vols[-24:] if v > avg_vol * 3)
        price_range_24h = 0
        if len(candles_1h) >= 24:
            h24 = max(c["high"] for c in candles_1h[-24:])
            l24 = min(c["low"]  for c in candles_1h[-24:])
            price_range_24h = (h24 - l24) / l24 * 100 if l24 > 0 else 99
        if vol_spikes >= 2 and price_range_24h < 5:
            ots_type = "pre_accumulation"
            ots_signals.append(
                f"📦 Pre-Accumulation: {vol_spikes}x vol spike, range {price_range_24h:.1f}% [J2]"
            )

    return ots_score, ots_type, ots_signals


# ══════════════════════════════════════════════════════════════
#  🆕 v10.0 — FUNGSI BARU [J3] Volume 60-Minute Signal
# ══════════════════════════════════════════════════════════════
def calc_vol60m_signal(candles_1h):
    """
    Volume 60m terakhir vs baseline = insider buying signal [J3].
    [J3]: unusual volume bisa muncul 60 menit sebelum public announcement.
    """
    if len(candles_1h) < 12:
        return 0, 1.0, []

    vol_60m          = candles_1h[-1]["volume_usd"]
    baseline_candles = candles_1h[-24:-1] if len(candles_1h) >= 24 else candles_1h[:-1]
    if not baseline_candles:
        return 0, 1.0, []

    baseline_avg = sum(c["volume_usd"] for c in baseline_candles) / len(baseline_candles)
    if baseline_avg <= 0:
        return 0, 1.0, []

    vol60m_ratio    = vol_60m / baseline_avg
    price_change_1h = 0
    if len(candles_1h) >= 2 and candles_1h[-2]["close"] > 0:
        price_change_1h = abs(
            (candles_1h[-1]["close"] - candles_1h[-2]["close"])
            / candles_1h[-2]["close"]
        )

    vol60m_score   = 0
    vol60m_signals = []

    if vol60m_ratio >= CONFIG["vol60m_ratio_threshold"]:
        if price_change_1h < 0.02:
            vol60m_score = 15
            vol60m_signals.append(
                f"🔍 Vol 60m {vol60m_ratio:.1f}x baseline + harga flat — insider buying [J3]"
            )
        else:
            vol60m_score = 10
            vol60m_signals.append(f"📈 Vol 60m {vol60m_ratio:.1f}x baseline [J3]")
    elif vol60m_ratio >= CONFIG["vol60m_accum_ratio"]:
        vol60m_score = 6
        vol60m_signals.append(f"Vol 60m {vol60m_ratio:.1f}x baseline")
    elif vol60m_ratio < 0.3:
        vol60m_score = -5
        vol60m_signals.append(f"⚠️ Vol 60m {vol60m_ratio:.1f}x — aktivitas menurun")

    return vol60m_score, vol60m_ratio, vol60m_signals


# ══════════════════════════════════════════════════════════════
#  🔬  PUMP PROBABILITY MODEL v10.0
# ══════════════════════════════════════════════════════════════
def compute_pump_probability(candles_1h, whale_score=0):
    """
    v10.0: Bobot diperbarui dari RF feature importance [J3].
    irr: 0.20→0.26 | slope: 0.13→0.20 | spike: 0.28→0.22 | atr: 0.22→0.17
    """
    if len(candles_1h) < 24:
        return {"probability_score": 0.3, "classification": "Data Kurang", "metrics": {}}

    max_spike, avg_spike = calc_volume_spike_ratio(candles_1h)
    irr      = calc_volume_irregularity(candles_1h)
    norm_atr = calc_normalized_atr(candles_1h)
    slope    = calc_price_slope(candles_1h)

    def clamp(v, lo, hi):
        return max(0.0, min(1.0, (v - lo) / (hi - lo))) if hi > lo else 0.5

    n_mvs = clamp(max_spike,  1.0, 10.0)
    n_irr = clamp(irr,        0.5,  3.5)
    n_avs = clamp(avg_spike,  0.5,  2.0)
    n_atr = 1.0 - clamp(norm_atr, 0.05, 3.0)
    n_slp = 1.0 - clamp(slope, -0.001, 0.002)

    score = max(0.0, min(1.0,
        n_mvs * 0.22 +   # [J3] 0.28→0.22
        n_irr * 0.26 +   # [J1] 0.20→0.26 StdRushOrder proxy
        n_avs * 0.15 +   # tetap
        n_atr * 0.17 +   # [J3] 0.22→0.17
        n_slp * 0.20     # [J3] 0.13→0.20
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
    """v10.0: Tambah On-The-Spot [J2] + Vol60m [J3]."""
    score, sigs = 0, []
    rvol = calc_rvol(candles_1h)

    if rvol >= 4.0:
        score += 16; sigs.append(f"🔥🔥 RVOL {rvol:.1f}x — volume MASIF!")
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
        score += 6;  sigs.append(f"Vol Irregularity {irr:.2f}")
    elif irr >= 1.3:
        score += 3

    cvd_s, cvd_sig = calc_cvd_signal(candles_1h)
    score += cvd_s
    if cvd_sig:
        sigs.append(cvd_sig)

    if rvol >= 5.0:
        stcvd_check, _ = calc_short_term_cvd(candles_1h)
        if stcvd_check <= -10:
            penalty = min(int(rvol * 0.4), 12)
            score  -= penalty
            sigs.append(f"⚠️ RVOL {rvol:.1f}x + CVD negatif — distribusi/likuidasi")
        elif stcvd_check < 0:
            penalty = min(int(rvol * 0.2), 6)
            score  -= penalty

    # ── v10.0 NEW: On-The-Spot Pump Detector [J2] ────────────────────
    ots_score, ots_type, ots_sigs = detect_on_the_spot_pump(candles_1h)
    score += ots_score
    sigs.extend(ots_sigs)

    # ── v10.0 NEW: Volume 60-Minute Signal [J3] ──────────────────────
    vol60m_sc, vol60m_ratio, vol60m_sigs = calc_vol60m_signal(candles_1h)
    score += vol60m_sc
    sigs.extend(vol60m_sigs)

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
        score += 15; sigs.append(f"🎯 Range 24h sangat sempit ({range24_pct:.1f}%) — akumulasi tight")
    elif range24_pct < 6:
        score += 10; sigs.append(f"🎯 Range 24h sempit ({range24_pct:.1f}%)")
    elif range24_pct < 10:
        score += 5;  sigs.append(f"Range 24h terbatas ({range24_pct:.1f}%)")
    elif range24_pct < 15:
        score += 2
    elif range24_pct > 40:
        score -= 5

    slope = calc_price_slope(candles_1h)
    if slope < -0.0003:
        score += 8; sigs.append(f"📉 Harga turun perlahan — bahan bakar squeeze")
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
        score += 5; sigs.append(f"Coiling {coiling}h — energi terkumpul")
    elif coiling >= 10:
        score += 3; sigs.append(f"Coiling {coiling}h")
    elif coiling >= 5:
        score += 1

    return min(score, CONFIG["max_struct_score"]), sigs, bbw_val, bbw_pct, coiling


def layer_positioning(symbol, funding, oi_chg1h):
    score, sigs = 0, []
    ls_block = False

    if funding <= -0.0004:
        score += 8;  sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup KUAT!")
    elif -0.0004 < funding <= -0.00001:
        score += 6;  sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup")
    elif abs(funding) < 0.00001:
        score += 4;  sigs.append(f"Funding {funding:.5f} — netral")
    elif 0.00001 <= funding <= 0.0001:
        score += 1
    elif funding > 0.0003:
        score -= 5;  sigs.append(f"⚠️ Funding {funding:.5f} — long overcrowded")

    if funding <= CONFIG["squeeze_funding_max"] and oi_chg1h > CONFIG["squeeze_oi_change_min"]:
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
        elif 1.15 < ls <= 1.3:
            ls_score = -5;  sigs.append(f"L/S {ls:.2f} — longs mulai dominan")
        elif 1.3 < ls <= 1.6:
            ls_score = -10; sigs.append(f"⚠️ L/S {ls:.2f} — longs dominan")
        elif 1.6 < ls <= 2.0:
            ls_score = -16; sigs.append(f"⚠️⚠️ L/S {ls:.2f} — longs sangat dominan")
        elif 2.0 < ls <= 2.5:
            ls_score = -20; sigs.append(f"🚨 L/S {ls:.2f} — long overcrowded berat")
        elif 2.5 < ls <= 3.0:
            ls_score = -25; sigs.append(f"🚨 L/S {ls:.2f} — long overcrowded ekstrem")
        elif ls > 3.0:
            ls_score = -30
            ls_block  = True
            sigs.append(f"🚨🚨 L/S {ls:.2f} — long overcrowded KRITIS, hard block")
        if 1.3 < ls <= 2.0 and funding <= -0.0003:
            override = min(abs(ls_score) * 0.4, 8)
            ls_score += int(override)
            sigs.append(f"⚡ Mitigasi: funding {funding:.5f} kurangi dampak L/S")

    return min(score + ls_score, CONFIG["max_pos_score"]), sigs, ls, ls_block


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


def calc_whale(symbol, candles_15m, funding, vol_24h=0):
    """v10.0: Tambah StdRushOrder scoring [J1]."""
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

        # ── v10.0 NEW: StdRushOrder [J1] ─────────────────────────────
        std_rush, buy_rush_ratio, spike_ratio, rush_sc, rush_sigs = calc_rush_orders(
            trades, vol_24h
        )
        ws += rush_sc
        ev.extend(rush_sigs)

        # Sell rush dominance sebagai pengurang score (gate ada di master_score)
        if buy_rush_ratio < 0.50 and std_rush > 5:
            ws -= 10
            ev.append(f"⚠️ Sell rush {1-buy_rush_ratio:.0%} dari total — potensi panic [J1]")

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
#  GC-2: LIQUIDATION CASCADE DETECTOR
# ══════════════════════════════════════════════════════════════
def layer_liquidation(symbol, candles_1h):
    long_liq, short_liq = get_liquidations(symbol)
    score, sigs = 0, []
    should_block = False

    if long_liq > CONFIG["liq_long_block_usd"] * 3:
        should_block = True
        sigs.append(f"🚨 Long liq masif ${long_liq/1e3:.0f}K — pump aborted!")
    elif long_liq > CONFIG["liq_long_block_usd"]:
        score -= 15
        sigs.append(f"⚠️ Long liq ${long_liq/1e3:.0f}K — posisi longs dihancurkan")
    elif long_liq > CONFIG["liq_long_block_usd"] * 0.5:
        score -= 7

    if short_liq > CONFIG["liq_short_bonus_usd"] * 2:
        score += 20
        sigs.append(f"🔥🔥 Short liq ${short_liq/1e3:.0f}K — SHORT SQUEEZE AKTIF!")
    elif short_liq > CONFIG["liq_short_bonus_usd"]:
        score += 12
        sigs.append(f"🔥 Short liq ${short_liq/1e3:.0f}K — squeeze meningkat")
    elif short_liq > CONFIG["liq_short_bonus_usd"] * 0.5:
        score += 6

    return score, sigs, long_liq, short_liq, should_block


# ══════════════════════════════════════════════════════════════
#  GC-3: LINEA SIGNATURE LAYER
# ══════════════════════════════════════════════════════════════
def layer_linea_signature(candles_1h, oi_chg1h, oi_chg24h, oi_valid,
                           ls_ratio, funding, chg_24h):
    score, sigs, components = 0, [], 0

    if ls_ratio is not None:
        if ls_ratio < 0.75:
            score += 6; components += 1
            sigs.append(f"✅ [Linea-LS] L/S {ls_ratio:.2f} — short sangat dominan")
        elif ls_ratio <= CONFIG["linea_ls_max"]:
            score += 3; components += 1

    if chg_24h < -3:
        score += 5; components += 1
        sigs.append(f"✅ [Linea-P] Harga {chg_24h:+.1f}% — siap reversal")
    elif chg_24h <= CONFIG["linea_price_max_chg"]:
        score += 2; components += 1

    stcvd_sc, stcvd_sig = calc_short_term_cvd(candles_1h)
    if stcvd_sc >= 8:
        score += 5; components += 1
        sigs.append(f"✅ [Linea-CVD] {stcvd_sig}")
    elif stcvd_sc > 0:
        score += 2

    if oi_valid:
        oi_1h_ok  = oi_chg1h  >= CONFIG["linea_oi_1h_min"]
        oi_24h_ok = oi_chg24h >= CONFIG["linea_oi_24h_min"]

        if oi_chg1h >= 8.0:
            score += 8; components += 1
            sigs.append(f"✅ [Linea-OI1h] OI 1h +{oi_chg1h:.1f}% — posisi baru masuk MASIF")
        elif oi_chg1h >= 4.0:
            score += 5; components += 1
            sigs.append(f"✅ [Linea-OI1h] OI 1h +{oi_chg1h:.1f}%")
        elif oi_1h_ok:
            score += 3; components += 1

        if oi_chg24h >= 10.0:
            score += 6; components += 1
            sigs.append(f"✅ [Linea-OI24h] OI 24h +{oi_chg24h:.1f}%")
        elif oi_24h_ok:
            score += 3; components += 1

        if oi_1h_ok and oi_24h_ok and chg_24h < 0:
            score += 8; components += 1
            sigs.append(f"⭐ [Linea-DIV] OI naik + Harga {chg_24h:+.1f}% — DIVERGENCE BULLISH!")
        elif oi_1h_ok and chg_24h <= CONFIG["linea_price_max_chg"]:
            score += 4; components += 1

    if components >= 4:
        score += 5
        sigs.append(f"⭐ FULL LINEA SIGNATURE ({components}/5) — pre-pump template!")
    elif components >= 3:
        sigs.append(f"[Linea] {components} komponen aktif")

    return min(score, CONFIG["max_linea_score"]), sigs, components


# ══════════════════════════════════════════════════════════════
#  GC-5: MICRO-CAP OI ACCELERATION DETECTOR
# ══════════════════════════════════════════════════════════════
def layer_oi_acceleration(symbol, oi_value, chg_24h, vol_24h):
    score, sigs = 0, []
    accel = {
        "oi_value":       oi_value,
        "is_micro_cap":   False,
        "is_dormant":     False,
        "growth_rate_1h": 0.0,
        "growth_rate_3h": 0.0,
        "growth_rate_6h": 0.0,
        "acceleration":   0.0,
        "divergence":     False,
    }

    if oi_value <= 0:
        return 0, [], accel

    is_micro  = oi_value < CONFIG["oi_accel_micro_thresh"]
    is_dormant = vol_24h < CONFIG["oi_accel_dormant_vol"]
    accel["is_micro_cap"] = is_micro
    accel["is_dormant"]   = is_dormant

    snaps = load_oi_snapshots()
    hist  = snaps.get(symbol, [])
    if len(hist) < 2:
        return 0, [], accel

    now = time.time()

    def oi_at(target_ts, tolerance=900):
        cands = [d for d in hist if abs(d["ts"] - target_ts) < tolerance]
        return min(cands, key=lambda d: abs(d["ts"] - target_ts)) if cands else None

    def growth_pct(old_snap):
        if not old_snap or old_snap["oi"] <= 0:
            return None
        return (oi_value - old_snap["oi"]) / old_snap["oi"] * 100

    snap_1h = oi_at(now - 3600)
    snap_3h = oi_at(now - 10800)
    snap_6h = oi_at(now - 21600)

    gr_1h = growth_pct(snap_1h)
    gr_3h = growth_pct(snap_3h)
    gr_6h = growth_pct(snap_6h)

    if gr_1h is not None: accel["growth_rate_1h"] = round(gr_1h, 2)
    if gr_3h is not None: accel["growth_rate_3h"] = round(gr_3h, 2)
    if gr_6h is not None: accel["growth_rate_6h"] = round(gr_6h, 2)

    multiplier = 1.5 if is_micro else 1.0
    extra_tag  = " [MICRO]" if is_micro else ""
    primary_gr = gr_1h if gr_1h is not None else gr_3h

    if primary_gr is not None:
        if primary_gr >= CONFIG["oi_accel_extreme"] * multiplier:
            score += 20; sigs.append(f"🚀 OI +{primary_gr:.0f}%/1h{extra_tag} — AKUMULASI EKSTREM!")
        elif primary_gr >= CONFIG["oi_accel_strong"] * multiplier:
            score += 14; sigs.append(f"🔥 OI +{primary_gr:.0f}%/1h{extra_tag} — akumulasi sangat kuat")
        elif primary_gr >= CONFIG["oi_accel_medium"]:
            score += 9;  sigs.append(f"OI +{primary_gr:.0f}%/1h{extra_tag} — akumulasi signifikan")
        elif primary_gr >= CONFIG["oi_accel_weak"]:
            score += 4;  sigs.append(f"OI +{primary_gr:.0f}%/1h — awal akumulasi")
        elif primary_gr < -CONFIG["oi_accel_weak"]:
            score -= 8;  sigs.append(f"⚠️ OI {primary_gr:.0f}%/1h — distribusi cepat")

    if gr_1h is not None and gr_3h is not None and gr_3h != 0:
        rate_1h_per_h = gr_1h
        rate_3h_per_h = gr_3h / 3.0
        if rate_3h_per_h > 0 and rate_1h_per_h > rate_3h_per_h * 1.5:
            accel_ratio = rate_1h_per_h / rate_3h_per_h
            accel["acceleration"] = round(accel_ratio, 2)
            score += 6
            sigs.append(f"⚡ OI akselerasi {accel_ratio:.1f}x lebih cepat dari avg 3h")
        elif rate_3h_per_h > 0 and rate_1h_per_h > rate_3h_per_h * 1.2:
            score += 3

    price_flat = abs(chg_24h) <= CONFIG["oi_accel_div_price_max"]
    oi_growing  = (primary_gr or 0) >= CONFIG["oi_accel_weak"]
    if oi_growing and price_flat:
        accel["divergence"] = True
        if chg_24h < 0 and (primary_gr or 0) >= CONFIG["oi_accel_medium"]:
            score += 10
            sigs.append(f"⭐ OI DIVERGENCE: OI +{primary_gr:.0f}% saat harga {chg_24h:+.1f}%")
        elif price_flat and (primary_gr or 0) >= CONFIG["oi_accel_medium"]:
            score += 6

    if is_dormant and is_micro and gr_6h is not None:
        snap_6h_val = snap_6h["oi"] if snap_6h else None
        if snap_6h_val and snap_6h_val > 0:
            awakening_mult = oi_value / snap_6h_val
            if awakening_mult >= CONFIG["oi_dormant_baseline_mult"] * 2:
                score += 12
                sigs.append(f"🌅 DORMANT AWAKENING: OI {awakening_mult:.1f}x dalam 6h")
            elif awakening_mult >= CONFIG["oi_dormant_baseline_mult"]:
                score += 7
                sigs.append(f"🌅 OI awakening {awakening_mult:.1f}x dari baseline")

    return min(score, CONFIG["max_oi_accel_score"]), sigs, accel


# ══════════════════════════════════════════════════════════════
#  ENTRY CALCULATOR
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
#  GATE: SUDAH PUMP
# ══════════════════════════════════════════════════════════════
def is_already_pumped(oi_chg24h, vol_chg24h_pct, chg_24h, oi_valid):
    if not oi_valid:
        if chg_24h > 15 and vol_chg24h_pct > 150:
            return True, f"Harga +{chg_24h:.0f}% + Volume +{vol_chg24h_pct:.0f}%"
        return False, ""
    if oi_chg24h > 35 and chg_24h > 3:
        return True, f"OI 24h +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — TERLAMBAT"
    if oi_chg24h > 25 and chg_24h > 5:
        return True, f"OI 24h +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}%"
    if oi_chg24h > 15 and chg_24h > 10:
        return True, f"OI +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}%"
    if vol_chg24h_pct > 200 and chg_24h > 8:
        return True, f"Volume +{vol_chg24h_pct:.0f}% + Harga +{chg_24h:.0f}%"
    return False, ""


# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE v10.0
# ══════════════════════════════════════════════════════════════
def master_score(symbol, ticker, tickers_dict):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candle_4h"])

    if len(c1h) < 48 or len(c15m) < 20:
        return None

    # ── Dead Activity Gate (v9.9) ──────────────────────────────────────
    if len(c1h) >= 7:
        last_vol   = c1h[-1]["volume_usd"]
        avg_vol_6h = sum(c["volume_usd"] for c in c1h[-7:-1]) / 6
        if avg_vol_6h > 0:
            if last_vol / avg_vol_6h < CONFIG["dead_activity_threshold"]:
                log.info(f"  {symbol}: GATE dead activity")
                return None

    funding = get_funding(symbol)

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

    try:
        vol_change_24h = float(ticker.get("volChange24h", 0))
    except:
        vol_change_24h = 0
    if vol_change_24h == 0 and len(c1h) >= 48:
        vol_24h_now  = sum(c["volume_usd"] for c in c1h[-24:])
        vol_24h_prev = sum(c["volume_usd"] for c in c1h[-48:-24])
        if vol_24h_prev > 0:
            vol_change_24h = (vol_24h_now - vol_24h_prev) / vol_24h_prev * 100

    if vol_change_24h < -60:
        log.info(f"  {symbol}: GATE volume exhaustion ({vol_change_24h:.0f}%)")
        return None

    # ── v10.0 NEW Gate A: Sell Rush Dominance Gate [J1] ───────────────
    # Block jika sell rush orders > 30% dari total — indikasi panic selling
    _trades_gate = get_trades(symbol, 200)
    if _trades_gate and len(_trades_gate) >= 20:
        buy_rush_vol  = sum(t["size"] * t["price"] for t in _trades_gate if t["side"] == "buy")
        sell_rush_vol = sum(t["size"] * t["price"] for t in _trades_gate if t["side"] == "sell")
        total_rush    = buy_rush_vol + sell_rush_vol
        if total_rush > 0:
            sell_ratio = sell_rush_vol / total_rush
            if sell_ratio > CONFIG["rush_sell_fp_ratio"]:
                log.info(
                    f"  {symbol}: GATE sell rush dominance "
                    f"({sell_ratio:.0%} sell) — panic selling [J1]"
                )
                return None

    # ── v10.0 NEW Gate C: On-The-Spot Weak Signal Gate [J2] ───────────
    # Jika tidak ada akumulasi TAPI spike juga tidak cukup kuat → skip
    if len(c1h) >= 5:
        vols      = [c["volume_usd"] for c in c1h]
        avg_vol_g = sum(vols[-24:]) / min(24, len(vols))
        if avg_vol_g > 0:
            vol_4h_bef = sum(vols[-5:-1]) / 4
            vol_last   = vols[-1]
            no_accum   = vol_4h_bef < avg_vol_g * CONFIG["ots_no_accum_vol_ratio"]
            not_strong = vol_last    < avg_vol_g * CONFIG["ots_confirm_vol_ratio"]
            if no_accum and not_strong:
                log.info(f"  {symbol}: GATE OTS weak — tidak ada akumulasi + spike lemah [J2]")
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

    # Layer 1: Volume Intelligence (includes OTS + Vol60m)
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
    oi_value  = get_open_interest(symbol)
    oi_chg1h  = oi_chg24h = 0
    oi_valid  = False
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)
        oi_chg1h, oi_chg24h, oi_valid = get_oi_changes(symbol, oi_value)

    # Gate already pumped
    vol_chg_proxy = oi_chg24h * 3 if oi_valid else 0
    pumped, pump_reason = is_already_pumped(oi_chg24h, vol_chg_proxy, chg_24h, oi_valid)
    if pumped:
        log.info(f"  {symbol}: GATE already pumped — {pump_reason}")
        return None

    # Layer 4: Positioning
    pos_sc, pos_sigs, ls_ratio, ls_block = layer_positioning(symbol, funding, oi_chg1h)
    if ls_block:
        log.info(f"  {symbol}: GATE L/S overcrowded kritis")
        return None
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

    # Layer 7: Whale (v10.0 includes StdRushOrder)
    ws, whale_bonus, wev = calc_whale(symbol, c15m, funding, vol_24h)
    score += whale_bonus;  bd["whale"] = whale_bonus

    # GC-2: Liquidation
    liq_sc, liq_sigs, long_liq, short_liq, liq_block = layer_liquidation(symbol, c1h)
    if liq_block:
        log.info(f"  {symbol}: GATE liquidation — long flush baru saja terjadi")
        return None
    score += liq_sc;  sigs += liq_sigs;  bd["liq"] = liq_sc

    rsi_1h = get_rsi(c1h[-48:] if len(c1h) >= 48 else c1h)

    # GC-3: Linea Signature
    linea_sc, linea_sigs, linea_components = layer_linea_signature(
        c1h, oi_chg1h, oi_chg24h, oi_valid, ls_ratio, funding, chg_24h
    )
    if linea_components >= 2 and rsi_1h < CONFIG["linea_rsi_max"]:
        linea_sc += 5
        linea_sigs.append(f"✅ [Linea-RSI] {rsi_1h:.1f} — oversold, siap reversal")
    score += linea_sc;  sigs += linea_sigs;  bd["linea"] = linea_sc

    # GC-5: OI Acceleration
    oi_accel_sc, oi_accel_sigs, oi_accel_data = layer_oi_acceleration(
        symbol, oi_value, chg_24h, vol_24h
    )
    score += oi_accel_sc;  sigs += oi_accel_sigs;  bd["oi_accel"] = oi_accel_sc

    # OI adjustments
    if oi_value > 0:
        if oi_valid:
            if rvol > 4.0 and oi_chg24h < -20:
                log.info(f"  {symbol}: GATE distribusi ekstrem")
                return None
            if oi_chg24h < -20:
                score -= 25; sigs.append(f"⚠️ OI 24h {oi_chg24h:.1f}% — distribusi masif")
            elif oi_chg24h < -10:
                score -= 15; sigs.append(f"⚠️ OI 24h {oi_chg24h:.1f}% — distribusi signifikan")
            elif oi_chg24h < -5:
                score -= 10; sigs.append(f"OI 24h {oi_chg24h:.1f}% — distribusi terindikasi")
            elif oi_chg24h < -3:
                score -= 5
            if oi_chg1h < -8:
                score -= 20; sigs.append(f"🚨 OI 1h {oi_chg1h:.1f}% — distribusi CEPAT!")
            elif oi_chg1h < -5:
                score -= 12; sigs.append(f"⚠️ OI 1h {oi_chg1h:.1f}% — tekanan jual 1h")
            elif oi_chg1h < -2:
                score -= 6
            elif oi_chg1h > 5:
                score += 5;  sigs.append(f"✅ OI 1h +{oi_chg1h:.1f}% — posisi baru masuk")
            if oi_chg24h < -3 and oi_chg1h < -2:
                log.info(f"  {symbol}: GATE multi-TF OI decline")
                return None
            if oi_chg24h < -1.0 and oi_chg1h < -0.5:
                score -= 12
                sigs.append(f"⚠️ OI turun semua TF (24h:{oi_chg24h:.1f}% 1h:{oi_chg1h:.1f}%)")
            if rvol > 1.5 and oi_chg24h < -15:
                score -= 18; sigs.append(f"🚨 Vol spike + OI {oi_chg24h:.1f}% — distribusi kuat")
            elif rvol > 1.5 and oi_chg24h < -10:
                score -= 12
            elif rvol > 1.5 and oi_chg24h > 5:
                score += 8;  sigs.append(f"✅ Vol naik + OI naik — akumulasi kuat")
        else:
            sigs.append("ℹ️ OI history belum tersedia (run pertama)")
            if oi_value > 0 and vol_24h > 0:
                oi_to_vol = oi_value / vol_24h
                if oi_to_vol < 0.05:
                    score -= 10
            if vol_change_24h < -40:
                score -= 15
            elif vol_change_24h < -25:
                score -= 8
            if ls_ratio is not None and ls_ratio > 1.3:
                score -= 8
            if funding > 0.0003:
                score -= 5
            score -= 10
            sigs.append("⚠️ Run pertama: -10 poin konservatif")

        if len(c1h) >= 24:
            vol_24h_candles  = [c["volume_usd"] for c in c1h[-24:]]
            avg_vol_24h_base = sum(vol_24h_candles) / len(vol_24h_candles)
            if avg_vol_24h_base > 0 and avg_vol_6h > 0:
                vol_momentum = avg_vol_6h / avg_vol_24h_base
                if vol_momentum < 0.50:
                    score -= 10; sigs.append(f"⚠️ Volume 6h hanya {vol_momentum:.0%} avg 24h")
                elif vol_momentum < 0.70:
                    score -= 5

        bd["oi_change"]    = round(oi_chg24h, 1)
        bd["oi_change_1h"] = round(oi_chg1h, 1)
        bd["oi_valid"]     = oi_valid
    else:
        bd["oi_change"] = bd["oi_change_1h"] = 0
        bd["oi_valid"]  = False

    if chg_7d > CONFIG["gate_chg_7d_max"]:
        score -= 15

    if len(c1h) >= 24:
        high24 = max(c["high"] for c in c1h[-24:])
        low24  = min(c["low"]  for c in c1h[-24:])
        if low24 > 0:
            range24 = (high24 - low24) / low24 * 100
            if range24 > 55:
                score = max(0, score - 10)

    tmult, tsig = get_time_mult()
    raw_score = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    prob = compute_pump_probability(c1h, ws)
    bd["prob_score"] = round(prob["probability_score"] * 100, 1)
    bd["prob_class"] = prob["classification"]

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

    # Ambil pump type dari OTS detector
    _, ots_type, _ = detect_on_the_spot_pump(c1h)

    price_now = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]

    return {
        "symbol":           symbol,
        "score":            raw_score,
        "composite_score":  composite,
        "signals":          sigs,
        "ws":               ws,
        "wev":              wev,
        "entry":            entry,
        "sector":           sector,
        "funding":          funding,
        "bd":               bd,
        "price":            price_now,
        "chg_24h":          chg_24h,
        "vol_24h":          vol_24h,
        "rvol":             rvol,
        "ls_ratio":         ls_ratio,
        "chg_7d":           chg_7d,
        "avg_vol_6h":       avg_vol_6h,
        "range_6h":         range_6h,
        "coiling":          coiling,
        "bbw_val":          bbw_val,
        "oi_change_24h":    bd.get("oi_change", 0),
        "oi_change_1h":     bd.get("oi_change_1h", 0),
        "prob_score":       prob["probability_score"],
        "prob_class":       prob["classification"],
        "prob_metrics":     prob.get("metrics", {}),
        "rsi_1h":           rsi_1h,
        "long_liq":         long_liq,
        "short_liq":        short_liq,
        "linea_components": linea_components,
        "oi_accel_score":   oi_accel_sc,
        "oi_accel_data":    oi_accel_data,
        "pump_type":        ots_type,
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

    # Pump type indicator [J2]
    pump_type = r.get("pump_type", "unknown")
    pump_icons = {
        "on_the_spot":       "🎯 On-The-Spot",
        "on_the_spot_early": "👀 Early OTS",
        "pre_accumulation":  "📦 Pre-Accumulation",
        "unknown":           "❓ Unknown",
    }
    pump_type_str = f"<b>Pump Type  :</b> {pump_icons.get(pump_type, pump_type)}\n"

    linea_str = ""
    if r.get("linea_components", 0) >= 3:
        linea_str = f"<b>Linea Sig  :</b> ⭐ {r['linea_components']}/5 komponen!\n"

    accel_str = ""
    ad = r.get("oi_accel_data", {})
    if r.get("oi_accel_score", 0) > 0:
        div_tag   = " 📈DIV" if ad.get("divergence") else ""
        micro_tag = " [MICRO]" if ad.get("is_micro_cap") else ""
        accel_str = (
            f"<b>OI Accel   :</b> +{r['oi_accel_score']}pt{micro_tag}{div_tag} | "
            f"1h:{ad.get('growth_rate_1h',0):+.1f}% "
            f"3h:{ad.get('growth_rate_3h',0):+.1f}%\n"
        )

    liq_str = ""
    if r.get("long_liq", 0) > 0 or r.get("short_liq", 0) > 0:
        liq_str = (f"<b>Liquidation:</b> Long ${r.get('long_liq',0)/1e3:.0f}K | "
                   f"Short ${r.get('short_liq',0)/1e3:.0f}K (30m)\n")

    oi_warning = ""
    if not bd.get("oi_valid", True):
        oi_warning = "⚠️ <i>OI baseline belum tersedia (run pertama)</i>\n"

    msg = (
        f"🚨 <b>PRE-PUMP SIGNAL {rk}— v10.0</b>\n\n"
        f"<b>Symbol     :</b> {r['symbol']}\n"
        f"<b>Composite  :</b> {comp}/100  {bar}\n"
        f"<b>Layer Score:</b> {sc}/100\n"
        f"<b>Prob Model :</b> {prob_pct:.1f}% ({prob_cls})\n"
        f"<b>RSI 1h     :</b> {r.get('rsi_1h', 0):.1f}\n"
        f"{pump_type_str}"
        f"{linea_str}"
        f"{accel_str}"
        f"{oi_warning}"
        f"<b>Sektor     :</b> {r['sector']}\n"
        f"<b>Harga      :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h | {r['chg_7d']:+.1f}% 7d)\n"
        f"<b>Vol 24h    :</b> {vol} | RVOL: {r['rvol']:.1f}x{ls}\n"
        f"<b>6h Vol     :</b> ${r['avg_vol_6h']:.0f}/h  | 6h Range: {r['range_6h']:.1f}%\n"
        f"<b>Coiling    :</b> {r['coiling']}h  | BBW: {r['bbw_val']:.1f}%\n"
        f"<b>OI 24h/1h  :</b> {r['oi_change_24h']:+.1f}% / {r.get('oi_change_1h',0):+.1f}%"
        f"  [valid:{bd.get('oi_valid','?')}]\n"
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
    for s in r["signals"][:12]:
        msg += f"  • {s}\n"

    msg += (
        f"\n📐 <b>BREAKDOWN</b>\n"
        f"  Vol:{bd.get('vol',0)} StCVD:{bd.get('stcvd',0)} Flat:{bd.get('flat',0)} "
        f"Struct:{bd.get('struct',0)} Pos:{bd.get('pos',0)} "
        f"4H:{bd.get('tf4h',0)} Ctx:{bd.get('ctx',0)} "
        f"Whale:{bd.get('whale',0)} Liq:{bd.get('liq',0)} "
        f"Linea:{bd.get('linea',0)} Accel:{bd.get('oi_accel',0)} "
        f"Stealth:{bd.get('stealth',0)}\n"
        f"  OI:{bd.get('oi_valid','?')} RSI:{bd.get('rsi_1h','?')} "
        f"PumpType:{pump_type} "
        f"[Prob] MVS:{pm.get('max_vol_spike','?')}x "
        f"Irr:{pm.get('vol_irregularity','?')}\n\n"
        f"📡 Funding:{r['funding']:.5f}  🕐 {utc_now()}\n"
        f"<i>⚠️ Bukan financial advice. Manage risk ketat.</i>"
    )
    return msg


def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v10.0 — {utc_now()}</b>\n{'━'*28}\n"
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
        oi_tag   = " ⚠️OI?" if not r.get("bd", {}).get("oi_valid", True) else ""
        ptype    = r.get("pump_type", "")[:3].upper()
        msg += (
            f"{i}. <b>{r['symbol']}</b> [C:{comp} S:{r['score']} {bar}]{linea}{oi_tag}\n"
            f"   🐋{r['ws']} | RVOL:{r['rvol']:.1f}x | {vol} | "
            f"T1:+{t1p:.0f}% | {prob:.0f}% {prob_cls} | RSI:{rsi:.0f} | {ptype}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    """
    v10.0: Scan langsung semua coin di WHITELIST (tanpa stratified bucket).
    Whitelist sudah dipilih manual oleh user → tidak perlu random sampling.
    """
    candidates = []
    not_found  = []

    for sym in sorted(WHITELIST_SYMBOLS):
        if sym in MANUAL_EXCLUDE:
            continue
        if is_cooldown(sym):
            continue
        if sym not in tickers:
            not_found.append(sym)
            continue
        t = tickers[sym]
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
        candidates.append((sym, t))

    if not_found:
        log.debug(f"Tidak ditemukan di Bitget: {len(not_found)} coin")
    log.info(f"Kandidat aktif: {len(candidates)}/{len(WHITELIST_SYMBOLS)} coin")
    return candidates


def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v10.0 — {utc_now()} ===")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total ticker Bitget: {len(tickers)}")
    candidates = build_candidate_list(tickers)

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
                ptype    = res.get("pump_type", "?")
                log.info(
                    f"  Score={res['score']} Comp={comp} W={res['ws']} "
                    f"RVOL={res['rvol']:.1f}x Prob={prob:.0f}% ({prob_cls}) "
                    f"Linea={linea}/5 RSI={rsi:.0f} PumpType={ptype} "
                    f"T1=+{res['entry']['liq_pct']:.1f}%"
                )
                if (comp >= CONFIG["min_composite_alert"]
                        and res["prob_score"] >= CONFIG["min_prob_alert"]):
                    results.append(res)
                else:
                    reason = ""
                    if comp < CONFIG["min_composite_alert"]:
                        reason += f"comp={comp}<{CONFIG['min_composite_alert']} "
                    if res["prob_score"] < CONFIG["min_prob_alert"]:
                        reason += f"prob={prob:.0f}%<{CONFIG['min_prob_alert']*100:.0f}%"
                    if reason:
                        log.info(f"  SKIP: {reason.strip()}")
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

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
        if (r["ws"] >= CONFIG["min_whale_score"]
            or r["composite_score"] >= 62
            or r["prob_score"] >= 0.75
            or r.get("linea_components", 0) >= 3)
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
                f"Prob={r['prob_score']*100:.0f}% Linea={r.get('linea_components',0)}/5 "
                f"PumpType={r.get('pump_type','?')}"
            )
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v10.0                           ║")
    log.info("║  [J1] StdRushOrder + Sell Rush Gate               ║")
    log.info("║  [J2] On-The-Spot Pump Detector                   ║")
    log.info("║  [J3] RF Feature Importance Weights               ║")
    log.info("╚═══════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)

    run_scan()
