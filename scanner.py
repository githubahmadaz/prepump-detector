"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v12.0-ENHANCED                                        ║
║                                                                          ║
║  PERUBAHAN vs v11.0:                                                     ║
║    ✅ Hapus Linea Signature (tidak efektif)                             ║
║    ✅ Extended Dry Volume detection (fase volume sangat rendah)         ║
║    ✅ Volume Creep detection (peningkatan bertahap)                     ║
║    ✅ Breakout Confirmation dengan volume eksplosif                     ║
║    ✅ Penalti kuat untuk net flow negatif jangka pendek                 ║
║    ✅ CVD Divergence memerlukan konfirmasi price slope                  ║
║    ✅ Penalti RVOL rendah (diskon sinyal struktur/posisi)               ║
║    ✅ Stealth pattern menggunakan rasio volume (bukan batas absolut)    ║
║    ✅ Entry support dinamis (low 3h, VWAP, EMA20)                       ║
║    ✅ Threshold composite dinaikkan menjadi 42                          ║
║                                                                          ║
║  EXPECTED RESULT:                                                        ║
║    Lebih akurat mendeteksi pump 1-4 jam sebelum terjadi                 ║
║    Mengurangi false positive (seperti LAYERUSDT)                        ║
║    Menangkap pola akumulasi dari POWER, HOLO, RAVE, DENT                ║
║    Entry lebih aman, SL lebih jarang kena                               ║
╚══════════════════════════════════════════════════════════════════════════╝
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

# ── Logging ───────────────────────────────────────────────────────────────
import logging.handlers as _lh
_log_fmt    = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root   = logging.getLogger()
_log_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v12.log", maxBytes=10*1024*1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)
log = logging.getLogger(__name__)
log.info("Log file aktif: /tmp/scanner_v12.log (rotasi 10MB)")

# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG (diperbarui)
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────
    "min_composite_alert":       42,      # naik dari 40 untuk presisi
    "min_prob_alert":          0.38,
    "min_score_alert":           25,
    "min_whale_score":           10,
    "max_alerts_per_run":         15,

    # Bobot composite score
    "composite_w_layer":        0.40,
    "composite_w_prob":         0.60,

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
    "min_target_pct":             8.0,
    "max_sl_pct":                3.0,
    "atr_sl_mult":                1.0,
    "atr_t1_mult":                2.0,
    "deep_entry_pct":            1.5,
    "entry_support_offset":       0.5,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":       1800,
    "sleep_coins":               0.8,
    "sleep_error":               3.0,
    "cooldown_file":    "/tmp/v12_cooldown.json",
    "oi_snapshot_file": "/tmp/v12_oi.json",
    "ob_snapshot_file": "/tmp/v12_orderbook.json",

    # ── Stealth pattern (baru pakai rasio) ───────────────────
    "stealth_min_coiling":       6,
    "stealth_max_range":         4.0,
    "stealth_vol_ratio":         0.3,    # avg_vol_6h < 30% avg_vol_24h

    # ── Extended dry volume (baru) ───────────────────────────
    "extended_dry_min_hours":     4,
    "extended_dry_vol_ratio":    0.3,
    "extended_dry_max_range":     5.0,
    "extended_dry_bonus":        15,

    # ── Volume creep (baru) ──────────────────────────────────
    "creep_ma3_mult":            1.2,
    "creep_ma6_mult":            1.1,
    "creep_bonus_strong":        10,
    "creep_bonus_weak":           5,

    # ── Breakout (baru) ──────────────────────────────────────
    "breakout_vol_mult":         2.0,
    "breakout_bonus":            15,

    # ── Net flow penalty (baru) ──────────────────────────────
    "netflow_15m_penalty_threshold": -15,
    "netflow_15m_penalty":          -20,
    "netflow_6h_penalty_threshold":  -10,
    "netflow_6h_penalty":            -15,

    # ── CVD divergence confirmation ───────────────────────────
    "cvd_div_require_price_slope": True,

    # ── Short squeeze ─────────────────────────────────────────
    "squeeze_funding_max":    -0.0001,
    "squeeze_oi_change_min":     3.0,

    # ── Layer max scores (disesuaikan) ───────────────────────
    "max_vol_score":             50,
    "max_flat_score":            20,
    "max_struct_score":          15,
    "max_pos_score":             15,
    "max_tf4h_score":             8,
    "max_ctx_score":             10,
    "max_whale_bonus":           20,
    "max_continuation_score":    25,
    "max_breakout_score":        15,
    "max_volume_creep_score":     10,
    "max_extended_dry_score":     15,
    "max_netflow_score":         25,

    # ── Pump probability weights (diperluas) ─────────────────
    "prob_mvs_w1":         20,
    "prob_irr_w2":         15,
    "prob_avs_w3":         10,
    "prob_atr_w4":         15,
    "prob_slope_w5":       10,
    "prob_oi_w6":          15,
    "prob_cvd_w7":         15,

    # ── GC-2: Liquidation Detector ────────────────────────────
    "liq_window_min":            30,
    "liq_long_block_usd":    100_000,
    "liq_short_bonus_usd":   150_000,

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

    # ── GC-6: Multi-TF Net Flow (dengan penalti baru) ────────
    "nf_strong_buy":             12.0,
    "nf_buy":                     5.0,
    "nf_neutral_max":             5.0,
    "nf_sell":                   -5.0,
    "nf_strong_sell":           -15.0,
    "nf_gate_72h":              -12.0,
    "nf_gate_24h":               -8.0,
    "nf_gate_6h":                -5.0,
    "nf_whale_72h_max":           3.0,
    "nf_whale_72h_min":         -15.0,
    "nf_whale_24h_min":           3.0,
    "nf_whale_6h_min":            5.0,

    # ── On-The-Spot (v10.0) ───────────────────────────────────
    "max_ots_score":              30,
    "ots_sepi_ratio":           0.50,
    "ots_spike_ratio":           5.0,
    "ots_price_min":             3.0,
    "ots_price_max":            15.0,

    # ── Build detection (v10.0) ───────────────────────────────
    "build_min_duration":          3,
    "build_max_range":          10.0,
    "build_vol_increase":       10.0,

    # ── Continuation pattern (v11.0) ──────────────────────────
    "cont_min_prev_pump":        15.0,
    "cont_max_range":              8.0,
    "cont_vol_decrease_ratio":    0.7,
    "cont_oi_growth_min_24h":     5.0,
    "cont_oi_growth_min_1h":      2.0,

    # ── Divergence (v11.0) ────────────────────────────────────
    "div_max_price_chg":          -2.0,
    "div_oi_growth_min":           5.0,
}

MANUAL_EXCLUDE = set()

# ══════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin pilihan (sama)
# ══════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
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
}

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
#  🔒  COOLDOWN & SNAPSHOTS (OI + Orderbook)
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

def load_ob_snapshots():
    try:
        if os.path.exists(CONFIG["ob_snapshot_file"]):
            with open(CONFIG["ob_snapshot_file"]) as f:
                return json.load(f)
    except:
        pass
    return {}

def save_ob_snapshot(symbol, bid_vol, ask_vol):
    snaps = load_ob_snapshots()
    now   = time.time()
    if symbol not in snaps:
        snaps[symbol] = []
    snaps[symbol].append({"ts": now, "bid_vol": bid_vol, "ask_vol": ask_vol})
    snaps[symbol] = sorted(snaps[symbol], key=lambda x: x["ts"])[-20:]  # simpan 20 snapshot terakhir
    try:
        with open(CONFIG["ob_snapshot_file"], "w") as f:
            json.dump(snaps, f)
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
#  🌐  HTTP UTILITIES (sama)
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
#  📡  DATA FETCHERS (sama, tambah get_oi_change untuk interval)
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

def get_oi_change(symbol, seconds_ago):
    """Mengambil perubahan OI dalam seconds_ago terakhir."""
    snaps = load_oi_snapshots()
    hist = snaps.get(symbol, [])
    if len(hist) < 2:
        return None
    now = time.time()
    target = now - seconds_ago
    # cari snapshot terdekat dalam toleransi
    best = None
    min_diff = float('inf')
    for snap in hist:
        diff = abs(snap["ts"] - target)
        if diff < min_diff and diff < 1800:  # toleransi 30 menit
            min_diff = diff
            best = snap
    if best and best["oi"] > 0:
        current_oi = get_open_interest(symbol)
        if current_oi > 0:
            return (current_oi - best["oi"]) / best["oi"] * 100
    return None

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
                ts_ms = int(t.get("fillTime", t.get("cTime", t.get("ts", 0))))
                trades.append({
                    "price": float(t["price"]),
                    "size":  float(t["size"]),
                    "side":  t.get("side", "").lower(),
                    "ts_ms": ts_ms,
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
#  📐  MATH HELPERS (sama, tambah fungsi untuk support)
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

def find_resistance_targets(candles_1h, entry):
    if len(candles_1h) < 24:
        return entry * 1.08, entry * 1.15

    recent = candles_1h[-168:]
    resistance_levels = []
    min_t = entry * (1 + CONFIG["min_target_pct"] / 100)

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
        atr = calc_atr(candles_1h[-24:]) or entry * 0.02
        return round(entry * 1.08, 8), round(entry * 1.15, 8)

    resistance_levels.sort(key=lambda x: x[0])
    t1 = resistance_levels[0][0]
    t2 = resistance_levels[1][0] if len(resistance_levels) > 1 else t1 * 1.08
    return round(t1, 8), round(t2, 8)

def get_support_levels(candles_1h):
    """Mengembalikan level support terbaik: low 3h, VWAP, EMA20"""
    cur = candles_1h[-1]["close"]
    supports = []
    
    # low 3 jam terakhir
    low_3h = min(c["low"] for c in candles_1h[-3:])
    supports.append(low_3h)
    
    # VWAP 24h
    if len(candles_1h) >= 24:
        vwap, _ = calc_vwap_zone(candles_1h[-24:])
        if vwap and vwap < cur:
            supports.append(vwap)
    
    # EMA20 sederhana
    if len(candles_1h) >= 20:
        closes = [c["close"] for c in candles_1h[-20:]]
        ema20 = sum(closes) / 20
        if ema20 < cur:
            supports.append(ema20)
    
    # Filter yang valid (< cur)
    valid = [s for s in supports if s < cur]
    if valid:
        return max(valid)  # support tertinggi di bawah harga
    else:
        return cur * (1 - CONFIG["deep_entry_pct"] / 100)

# ══════════════════════════════════════════════════════════════
#  📊  INDIKATOR BARU/MODIFIKASI
# ══════════════════════════════════════════════════════════════
def calc_real_cvd(trades, period_hours=6):
    """Menghitung CVD real dari tick trades."""
    if not trades:
        return 0, 0, 0, 0
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - period_hours * 3600 * 1000
    relevant = [t for t in trades if t.get("ts_ms", 0) > cutoff]
    if not relevant:
        return 0, 0, 0, 0
    buy_vol = sum(t["size"] * t["price"] for t in relevant if "buy" in t.get("side", ""))
    sell_vol = sum(t["size"] * t["price"] for t in relevant if "sell" in t.get("side", ""))
    total = buy_vol + sell_vol
    if total == 0:
        return 0, 0, 0, 0
    net = buy_vol - sell_vol
    net_pct = net / total * 100
    return net, net_pct, buy_vol, sell_vol

def detect_on_the_spot_pump(candles_1h):
    """Sama seperti v10.0"""
    if len(candles_1h) < 6:
        return 0, "unknown", []
    score, sigs = 0, []
    vols_24h = [c["volume_usd"] for c in candles_1h[-24:]] if len(candles_1h) >= 24 else [c["volume_usd"] for c in candles_1h]
    avg_vol_24h = sum(vols_24h) / len(vols_24h) if vols_24h else 1
    if avg_vol_24h <= 0:
        return 0, "unknown", []
    vol_4h_before = sum(c["volume_usd"] for c in candles_1h[-5:-1]) / 4 if len(candles_1h) >= 5 else avg_vol_24h
    vol_current = candles_1h[-1]["volume_usd"]
    if len(candles_1h) >= 2:
        price_change = (candles_1h[-1]["close"] - candles_1h[-2]["close"]) / candles_1h[-2]["close"] * 100
    else:
        price_change = 0
    sepi_sebelumnya = vol_4h_before < avg_vol_24h * CONFIG["ots_sepi_ratio"]
    spike_tiba_tiba = vol_current > avg_vol_24h * CONFIG["ots_spike_ratio"]
    price_mulai_naik = CONFIG["ots_price_min"] <= price_change <= CONFIG["ots_price_max"]
    pump_type = "unknown"
    if sepi_sebelumnya and spike_tiba_tiba:
        score += 20
        pump_type = "on_the_spot"
        spike_ratio = vol_current / avg_vol_24h
        sigs.append(f"🎯 ON-THE-SPOT: Sepi 4h → spike {spike_ratio:.1f}x!")
        if price_mulai_naik:
            score += 10
            sigs.append(f"🚀 Price momentum {price_change:+.1f}%")
    elif not sepi_sebelumnya and vol_4h_before > avg_vol_24h * 0.8:
        pump_type = "pre_accumulation"
    return min(score, CONFIG["max_ots_score"]), pump_type, sigs

def calc_rvol(candles_1h):
    """Sama"""
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

def calc_cvd_signal(candles_1h, trades):
    """Menggunakan CVD real dari tick untuk 6 jam, dengan konfirmasi price slope."""
    net, net_pct, buy, sell = calc_real_cvd(trades, 6)
    if net == 0:
        return 0, ""
    if len(candles_1h) >= 6:
        p_start = candles_1h[-6]["close"]
        p_end = candles_1h[-1]["close"]
        price_chg = (p_end - p_start) / p_start * 100
        # Hitung slope 3 jam terakhir
        slope_3h = 0
        if len(candles_1h) >= 3:
            p3_start = candles_1h[-3]["close"]
            slope_3h = (p_end - p3_start) / p3_start * 100
        if net_pct > 10 and price_chg < 2:
            if CONFIG["cvd_div_require_price_slope"] and slope_3h > 0:
                return 15, f"🔍 CVD Divergence KUAT: CVD +{net_pct:.1f}% saat harga {price_chg:+.1f}%, slope positif"
            else:
                return 5, f"🔍 CVD Divergence ringan (tanpa konfirmasi harga)"
        elif net_pct > 5 and price_chg < 1:
            if slope_3h > 0:
                return 10, f"🔍 CVD positif {net_pct:.1f}%, harga flat - konfirmasi"
            else:
                return 3, f"🔍 CVD positif {net_pct:.1f}% tanpa konfirmasi"
    return 0, ""

def calc_short_term_cvd(candles_1h, trades):
    """CVD 6 jam vs 6 jam sebelumnya (12 jam total)"""
    net_6h, pct_6h, _, _ = calc_real_cvd(trades, 6)
    net_12h, pct_12h, _, _ = calc_real_cvd(trades, 12)
    if net_12h == 0:
        return 0, ""
    # Bandingkan tren
    if pct_6h > pct_12h + 5:
        return 8, f"✅ CVD 6h membaik ({pct_6h:+.1f}%) vs 12h ({pct_12h:+.1f}%)"
    elif pct_6h < pct_12h - 5:
        return -8, f"⚠️ CVD 6memburuk ({pct_6h:+.1f}%) vs 12h ({pct_12h:+.1f}%)"
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

# ==================== LAYER BARU ====================
def layer_extended_dry_volume(candles_1h, avg_vol_24h):
    """Deteksi periode volume sangat rendah berkepanjangan."""
    if len(candles_1h) < CONFIG["extended_dry_min_hours"]:
        return 0, ""
    recent = candles_1h[-CONFIG["extended_dry_min_hours"]:]
    avg_vol_dry = sum(c["volume_usd"] for c in recent) / len(recent)
    high_dry = max(c["high"] for c in recent)
    low_dry = min(c["low"] for c in recent)
    range_dry = (high_dry - low_dry) / low_dry * 100 if low_dry else 100
    if avg_vol_dry < avg_vol_24h * CONFIG["extended_dry_vol_ratio"] and range_dry < CONFIG["extended_dry_max_range"]:
        return CONFIG["extended_dry_bonus"], f"💤 Extended dry: vol {avg_vol_dry/avg_vol_24h:.0%} avg, range {range_dry:.1f}%"
    return 0, ""

def layer_volume_creep(candles_1h):
    """Deteksi peningkatan volume bertahap."""
    if len(candles_1h) < 12:
        return 0, ""
    vols = [c["volume_usd"] for c in candles_1h[-12:]]
    ma3 = sum(vols[-3:]) / 3
    ma6 = sum(vols[-6:]) / 6
    ma12 = sum(vols) / 12
    if ma3 > ma6 * CONFIG["creep_ma3_mult"] and ma6 > ma12 * CONFIG["creep_ma6_mult"]:
        return CONFIG["creep_bonus_strong"], f"📈 Volume creep kuat: MA3={ma3:.0f} > MA6={ma6:.0f} > MA12={ma12:.0f}"
    elif ma3 > ma6 * 1.1:
        return CONFIG["creep_bonus_weak"], f"📈 Volume creep ringan"
    return 0, ""

def layer_breakout(candles_1h, avg_vol_24h):
    """Deteksi breakout harga dengan volume eksplosif."""
    if len(candles_1h) < 6:
        return 0, ""
    last = candles_1h[-1]
    high_6h = max(c["high"] for c in candles_1h[-6:])
    high_24h = max(c["high"] for c in candles_1h[-24:]) if len(candles_1h) >= 24 else high_6h
    if last["close"] > high_6h and last["volume_usd"] > avg_vol_24h * CONFIG["breakout_vol_mult"]:
        return CONFIG["breakout_bonus"], f"🚀 Breakout high 6h dengan volume {last['volume_usd']/avg_vol_24h:.1f}x"
    if last["close"] > high_24h and last["volume_usd"] > avg_vol_24h * 1.5:
        return CONFIG["breakout_bonus"] - 5, f"📊 Breakout high 24h dengan volume {last['volume_usd']/avg_vol_24h:.1f}x"
    return 0, ""

# ══════════════════════════════════════════════════════════════
#  🔬  ENHANCED PUMP PROBABILITY MODEL (sama)
# ══════════════════════════════════════════════════════════════
def compute_pump_probability(candles_1h, oi_chg1h, oi_chg24h, funding, ls_ratio, cvd_pct, whale_score=0):
    """Model probabilitas dengan fitur lebih banyak."""
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
    n_oi1h  = clamp(oi_chg1h, -10, 20) if oi_chg1h is not None else 0.5
    n_oi24h = clamp(oi_chg24h, -20, 40) if oi_chg24h is not None else 0.5
    n_fund  = clamp(funding * 1000, -0.5, 0.5) + 0.5  # funding -0.0005 -> 0, 0.0005 -> 1
    n_ls    = 1.0 - clamp(ls_ratio, 0.5, 2.5) if ls_ratio else 0.5
    n_cvd   = clamp(cvd_pct, -20, 20) * 0.025 + 0.5
    n_whale = whale_score / 100.0

    score = (
        n_mvs   * 0.12 +
        n_irr   * 0.08 +
        n_avs   * 0.06 +
        n_atr   * 0.10 +
        n_slp   * 0.08 +
        n_oi1h  * 0.10 +
        n_oi24h * 0.08 +
        n_fund  * 0.10 +
        n_ls    * 0.08 +
        n_cvd   * 0.10 +
        n_whale * 0.10
    )
    score = max(0.0, min(1.0, score))

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
#  🏗️  LAYER SCORING (modifikasi)
# ══════════════════════════════════════════════════════════════
def layer_volume_intelligence(candles_1h, trades):
    """Menggunakan CVD real dari tick."""
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
    elif rvol < 0.4:
        score -= 4

    irr = calc_volume_irregularity(candles_1h)
    if irr >= 2.5:
        score += 10; sigs.append(f"📈 Vol Irregularity {irr:.2f} — whale masuk tidak merata")
    elif irr >= 1.8:
        score += 6;  sigs.append(f"Vol Irregularity {irr:.2f} — aktivitas whale")
    elif irr >= 1.3:
        score += 3

    # CVD real
    cvd_s, cvd_sig = calc_cvd_signal(candles_1h, trades)
    score += cvd_s
    if cvd_sig:
        sigs.append(cvd_sig)

    if rvol >= 5.0:
        stcvd_check, stcvd_sig = calc_short_term_cvd(candles_1h, trades)
        if stcvd_check <= -8:
            penalty = min(int(rvol * 0.4), 12)
            score  -= penalty
            sigs.append(f"⚠️ RVOL {rvol:.1f}x + CVD negatif kuat — distribusi/likuidasi")
        elif stcvd_check < 0:
            penalty = min(int(rvol * 0.2), 6)
            score  -= penalty
            sigs.append(f"RVOL {rvol:.1f}x + CVD negatif ringan — pantau distribusi")

    ots_score, ots_type, ots_sigs = detect_on_the_spot_pump(candles_1h)
    score += ots_score
    sigs += ots_sigs

    return min(score, CONFIG["max_vol_score"]), sigs, rvol, ots_type

def layer_flat_accumulation(candles_1h):
    """Sama, tapi range dinamis bisa ditambahkan nanti."""
    score, sigs = 0, []
    if len(candles_1h) < CONFIG["build_min_duration"] + 1:
        return 0, sigs

    if len(candles_1h) >= 24:
        high24 = max(c["high"] for c in candles_1h[-24:])
        low24  = min(c["low"]  for c in candles_1h[-24:])
        range24_pct = (high24 - low24) / low24 * 100 if low24 > 0 else 99
    else:
        range24_pct = 99

    if range24_pct < 5:
        score += 15; sigs.append(f"🎯 Range 24h sangat sempit ({range24_pct:.1f}%)")
    elif range24_pct < CONFIG["build_max_range"]:
        score += 10; sigs.append(f"🎯 Range 24h sempit ({range24_pct:.1f}%)")
    elif range24_pct < 15:
        score += 5
    elif range24_pct < 20:
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
    """Sama"""
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
    """Sama, tapi funding ekstrem tidak block."""
    score, sigs = 0, []
    ls_block = False

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
    
    if funding < CONFIG["gate_funding_extreme"]:
        if oi_chg1h > 5:
            score += 10
            sigs.append(f"🔥 Funding sangat negatif {funding:.5f} + OI naik — potensi short squeeze besar!")
        else:
            score -= 15
            sigs.append(f"⚠️ Funding ekstrem {funding:.5f} tanpa OI naik — risiko tinggi")

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
        elif 1.15 < ls <= 1.3:
            ls_score = -5;  sigs.append(f"L/S {ls:.2f} — longs mulai dominan")
        elif 1.3 < ls <= 1.6:
            ls_score = -10; sigs.append(f"⚠️ L/S {ls:.2f} — longs dominan, squeeze fuel habis")
        elif 1.6 < ls <= 2.0:
            ls_score = -16; sigs.append(f"⚠️⚠️ L/S {ls:.2f} — longs sangat dominan, pump sangat sulit")
        elif 2.0 < ls <= 2.5:
            ls_score = -20; sigs.append(f"🚨 L/S {ls:.2f} — long overcrowded berat")
        elif 2.5 < ls <= 3.0:
            ls_score = -25; sigs.append(f"🚨 L/S {ls:.2f} — long overcrowded ekstrem")
        elif ls > 3.0:
            ls_score  = -30
            ls_block  = True
            sigs.append(f"🚨🚨 L/S {ls:.2f} — long overcrowded KRITIS, hard block aktif")

        if 1.3 < ls <= 2.0 and funding <= -0.0003:
            override = min(abs(ls_score) * 0.4, 8)
            ls_score += int(override)
            sigs.append(f"⚡ Mitigasi: funding {funding:.5f} kurangi dampak L/S tinggi")

    return min(score + ls_score, CONFIG["max_pos_score"]), sigs, ls, ls_block

def calc_4h_confluence(candles_4h):
    """Sama"""
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
    """Sama"""
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

def calc_whale(symbol, candles_15m, funding, trades):
    """Ditambahkan orderbook absorption."""
    ws, ev = 0, []
    cur = candles_15m[-1]["close"] if candles_15m else 0

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

    ob_ratio, bid_vol, ask_vol = get_orderbook(symbol, 50)
    save_ob_snapshot(symbol, bid_vol, ask_vol)
    ob_snaps = load_ob_snapshots().get(symbol, [])
    if len(ob_snaps) >= 2:
        prev = ob_snaps[-2]
        bid_increase = bid_vol - prev["bid_vol"]
        ask_decrease = prev["ask_vol"] - ask_vol
        if bid_increase > 50000 and ask_decrease > 20000:
            ws += 15
            ev.append(f"🛡️ Bid wall absorption: bid +${bid_increase/1e3:.0f}K, ask -${ask_decrease/1e3:.0f}K")

    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15; ev.append("✅ Harga flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7;  ev.append("🔶 Harga relatif flat 4h")

    if -0.0004 <= funding <= -0.00002:
        ws += 10; ev.append(f"✅ Funding {funding:.5f} — short squeeze fuel")

    return min(ws, 100), min(ws, 100) // 5, ev

def get_time_mult():
    h = utc_hour()
    if h in [0, 1, 5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.15, f"⏰ High-prob window ({h}:00 UTC)"
    if h in [2, 3, 4]:
        return 0.85, "Low-prob window"
    return 1.0, ""

# LAYER LIQUIDATION (sama)
def layer_liquidation(symbol, candles_1h):
    long_liq, short_liq = get_liquidations(symbol)
    score, sigs = 0, []
    should_block = False

    if long_liq > CONFIG["liq_long_block_usd"] * 3:
        should_block = True
        sigs.append(f"🚨 Long liq masif ${long_liq/1e3:.0f}K dalam 30m — pump aborted!")
    elif long_liq > CONFIG["liq_long_block_usd"]:
        score -= 15
        sigs.append(f"⚠️ Long liq ${long_liq/1e3:.0f}K — posisi longs baru saja dihancurkan")
    elif long_liq > CONFIG["liq_long_block_usd"] * 0.5:
        score -= 7
        sigs.append(f"Long liq ${long_liq/1e3:.0f}K — tekanan jual terindikasi")

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

# LAYER OI ACCELERATION (sama, dengan bonus absolut)
def layer_oi_acceleration(symbol, oi_value, chg_24h, vol_24h):
    score, sigs, accel = 0, [], {}

    if oi_value <= 0:
        return 0, [], accel

    is_micro = oi_value < CONFIG["oi_accel_micro_thresh"]
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
            score += 20
            sigs.append(f"🚀 OI tumbuh +{primary_gr:.0f}%/1h{extra_tag} — AKUMULASI EKSTREM!")
        elif primary_gr >= CONFIG["oi_accel_strong"] * multiplier:
            score += 14
            sigs.append(f"🔥 OI tumbuh +{primary_gr:.0f}%/1h{extra_tag} — akumulasi sangat kuat")
        elif primary_gr >= CONFIG["oi_accel_medium"]:
            score += 9
            sigs.append(f"OI tumbuh +{primary_gr:.0f}%/1h{extra_tag} — akumulasi signifikan")
        elif primary_gr >= CONFIG["oi_accel_weak"]:
            score += 4
            sigs.append(f"OI tumbuh +{primary_gr:.0f}%/1h — awal akumulasi")
        elif primary_gr < -CONFIG["oi_accel_weak"]:
            score -= 8
            sigs.append(f"⚠️ OI turun {primary_gr:.0f}%/1h — distribusi cepat")

    # Bonus OI absolut untuk micro-cap
    if is_micro and oi_value > 1_000_000:
        score += 5
        sigs.append(f"💰 OI absolut ${oi_value/1e6:.1f}M — minat besar pada micro-cap")

    if gr_1h is not None and gr_3h is not None and gr_3h != 0:
        rate_1h_per_h = gr_1h
        rate_3h_per_h = gr_3h / 3.0
        if rate_3h_per_h > 0 and rate_1h_per_h > rate_3h_per_h * 1.5:
            accel_ratio = rate_1h_per_h / rate_3h_per_h
            accel["acceleration"] = round(accel_ratio, 2)
            score += 6
            sigs.append(f"⚡ OI akselerasi {accel_ratio:.1f}x lebih cepat dari rata-rata 3h")
        elif rate_3h_per_h > 0 and rate_1h_per_h > rate_3h_per_h * 1.2:
            score += 3
            sigs.append("OI momentum membangun — pertumbuhan makin cepat")

    price_flat = abs(chg_24h) <= CONFIG["oi_accel_div_price_max"]
    oi_growing  = (primary_gr or 0) >= CONFIG["oi_accel_weak"]

    if oi_growing and price_flat:
        accel["divergence"] = True
        if chg_24h < 0 and (primary_gr or 0) >= CONFIG["oi_accel_medium"]:
            score += 10
            sigs.append(
                f"⭐ OI DIVERGENCE: OI +{primary_gr:.0f}% saat harga {chg_24h:+.1f}% "
                f"— akumulasi tersembunyi KUAT"
            )
        elif price_flat and (primary_gr or 0) >= CONFIG["oi_accel_medium"]:
            score += 6
            sigs.append(
                f"OI divergence: OI +{primary_gr:.0f}% tapi harga flat "
                f"({chg_24h:+.1f}%)"
            )

    if is_dormant and is_micro and gr_6h is not None:
        snap_6h_val = snap_6h["oi"] if snap_6h else None
        if snap_6h_val and snap_6h_val > 0:
            awakening_mult = oi_value / snap_6h_val
            if awakening_mult >= CONFIG["oi_dormant_baseline_mult"] * 2:
                score += 12
                sigs.append(
                    f"🌅 DORMANT AWAKENING: OI {awakening_mult:.1f}x dalam 6h pada "
                    f"coin tidur (vol ${vol_24h/1e3:.0f}K)"
                )
            elif awakening_mult >= CONFIG["oi_dormant_baseline_mult"]:
                score += 7
                sigs.append(f"🌅 OI awakening {awakening_mult:.1f}x dari baseline tidur")

    return min(score, CONFIG["max_oi_accel_score"]), sigs, accel

# LAYER NET FLOW (dengan penalti tambahan)
def _candle_net_flow(candles):
    buy_usd  = 0.0
    sell_usd = 0.0
    for c in candles:
        rng = c["high"] - c["low"]
        buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        b = buy_ratio * c["volume_usd"]
        s = (1.0 - buy_ratio) * c["volume_usd"]
        buy_usd  += b
        sell_usd += s
    total  = buy_usd + sell_usd
    net    = buy_usd - sell_usd
    net_pct = net / total * 100 if total > 0 else 0.0
    return net, net_pct, buy_usd, sell_usd

def _tick_net_flow(trades, window_minutes=15):
    if not trades:
        return 0.0, 0.0, 0.0, 0.0, 0
    now_ms  = int(time.time() * 1000)
    cutoff  = now_ms - window_minutes * 60 * 1000
    has_ts  = any(t.get("ts_ms", 0) > 0 for t in trades)
    recent  = [t for t in trades if t.get("ts_ms", 0) > cutoff] if has_ts else trades
    if not recent:
        recent = trades
    buy_usd  = sum(t["size"] * t["price"] for t in recent if "buy"  in t.get("side", ""))
    sell_usd = sum(t["size"] * t["price"] for t in recent if "sell" in t.get("side", ""))
    total    = buy_usd + sell_usd
    net      = buy_usd - sell_usd
    net_pct  = net / total * 100 if total > 0 else 0.0
    return net, net_pct, buy_usd, sell_usd, len(recent)

def _classify_flow(net_pct):
    if net_pct > CONFIG["nf_strong_buy"]:  return "STRONG_BUY"
    if net_pct > CONFIG["nf_buy"]:         return "BUY"
    if net_pct > -CONFIG["nf_neutral_max"]: return "NEUTRAL"
    if net_pct > CONFIG["nf_strong_sell"]: return "SELL"
    return "STRONG_SELL"

def layer_net_flow(candles_1h, candles_15m, trades):
    """GC-6: Multi-TF Net Flow Layer dengan penalti tambahan."""
    score, sigs = 0, []
    flow_data   = {
        "72h": {"net_pct": 0, "net_usd": 0, "label": "NO_DATA"},
        "24h": {"net_pct": 0, "net_usd": 0, "label": "NO_DATA"},
        "6h":  {"net_pct": 0, "net_usd": 0, "label": "NO_DATA"},
        "3h":  {"net_pct": 0, "net_usd": 0, "label": "NO_DATA"},
        "15m": {"net_pct": 0, "net_usd": 0, "label": "NO_DATA", "count": 0},
        "has_data": False,
    }
    should_block = False

    pct_72h = pct_24h = pct_6h = pct_3h = None

    if len(candles_1h) >= 72:
        net, pct, buy, sell = _candle_net_flow(candles_1h[-72:])
        pct_72h = pct
        flow_data["72h"] = {
            "net_pct": round(pct, 1), "net_usd": round(net),
            "buy_usd": round(buy),   "sell_usd": round(sell),
            "label":   _classify_flow(pct),
        }

    if len(candles_1h) >= 24:
        net, pct, buy, sell = _candle_net_flow(candles_1h[-24:])
        pct_24h = pct
        flow_data["24h"] = {
            "net_pct": round(pct, 1), "net_usd": round(net),
            "buy_usd": round(buy),   "sell_usd": round(sell),
            "label":   _classify_flow(pct),
        }

    if len(candles_1h) >= 6:
        net, pct, buy, sell = _candle_net_flow(candles_1h[-6:])
        pct_6h = pct
        flow_data["6h"] = {
            "net_pct": round(pct, 1), "net_usd": round(net),
            "buy_usd": round(buy),   "sell_usd": round(sell),
            "label":   _classify_flow(pct),
        }

    if candles_15m and len(candles_15m) >= 12:
        net, pct, buy, sell = _candle_net_flow(candles_15m[-12:])
        pct_3h = pct
        flow_data["3h"] = {
            "net_pct": round(pct, 1), "net_usd": round(net),
            "buy_usd": round(buy),   "sell_usd": round(sell),
            "label":   _classify_flow(pct),
        }

    pct_15m = None
    if trades:
        net, pct, buy, sell, cnt = _tick_net_flow(trades, window_minutes=15)
        pct_15m = pct
        flow_data["15m"] = {
            "net_pct": round(pct, 1), "net_usd": round(net),
            "buy_usd": round(buy),    "sell_usd": round(sell),
            "label":   _classify_flow(pct), "count": cnt,
        }

    flow_data["has_data"] = (pct_24h is not None)

    if not flow_data["has_data"]:
        return 0, [], flow_data, False

    # GATE: Distribusi Sistematis (tetap block jika ekstrem)
    if (pct_72h is not None
            and pct_72h < CONFIG["nf_gate_72h"]
            and pct_24h < CONFIG["nf_gate_24h"]
            and pct_6h  < CONFIG["nf_gate_6h"]):
        should_block = True
        sigs.append(
            f"🚨 NET FLOW DISTRIBUSI: 72h={pct_72h:+.1f}% "
            f"24h={pct_24h:+.1f}% 6h={pct_6h:+.1f}% — whale keluar semua TF"
        )
        return score, sigs, flow_data, should_block

    # SCORING: Whale Accumulation Funnel
    if (pct_72h is not None
            and CONFIG["nf_whale_72h_min"] <= pct_72h <= CONFIG["nf_whale_72h_max"]
            and pct_24h is not None and pct_24h >= CONFIG["nf_whale_24h_min"]
            and pct_6h  is not None and pct_6h  >= CONFIG["nf_whale_6h_min"]):
        score += 20
        sigs.append(
            f"🐋 WHALE FUNNEL: 72h={pct_72h:+.1f}% → 24h={pct_24h:+.1f}% → "
            f"6h={pct_6h:+.1f}% — akumulasi 3 hari terkonfirmasi!"
        )

    elif (pct_72h is not None and pct_72h > CONFIG["nf_buy"]
            and pct_24h is not None and pct_24h > CONFIG["nf_buy"]
            and pct_6h  is not None and pct_6h  > CONFIG["nf_buy"]):
        score += 15
        sigs.append(
            f"✅ NET FLOW BULLISH: 72h={pct_72h:+.1f}% "
            f"24h={pct_24h:+.1f}% 6h={pct_6h:+.1f}% — semua TF buy dominan"
        )

    else:
        if (pct_24h is not None and pct_24h > CONFIG["nf_buy"]
                and pct_6h is not None and pct_6h > CONFIG["nf_buy"]):
            score += 10
            sigs.append(
                f"✅ Net Flow 24h={pct_24h:+.1f}% & 6h={pct_6h:+.1f}% positif — "
                f"akumulasi mid-term"
            )

        elif pct_6h is not None and pct_6h > CONFIG["nf_strong_buy"]:
            score += 7
            sigs.append(
                f"Net Flow 6h={pct_6h:+.1f}% — buying pressure naik tiba-tiba"
            )
        elif pct_6h is not None and pct_6h > CONFIG["nf_buy"]:
            score += 4
            sigs.append(f"Net Flow 6h={pct_6h:+.1f}% — sedikit buy dominant")

        if (pct_72h is not None and CONFIG["nf_strong_sell"] < pct_72h < 0
                and pct_24h is not None and pct_24h > CONFIG["nf_buy"]):
            score += 5
            sigs.append(
                f"📈 Flow shifting: 72h={pct_72h:+.1f}% → 24h={pct_24h:+.1f}% "
                f"— whale mulai akumulasi"
            )

    if pct_72h is not None:
        if pct_72h < CONFIG["nf_strong_sell"]:
            score -= 12
            sigs.append(f"⚠️ Net Flow 72h={pct_72h:+.1f}% — distribusi besar 3 hari")
        elif pct_72h < CONFIG["nf_sell"]:
            score -= 6
            sigs.append(f"Net Flow 72h={pct_72h:+.1f}% — tekanan jual 3 hari")

    if pct_24h is not None:
        if pct_24h < CONFIG["nf_strong_sell"]:
            score -= 10
            sigs.append(f"⚠️ Net Flow 24h={pct_24h:+.1f}% — distribusi aktif hari ini")
        elif pct_24h < CONFIG["nf_sell"]:
            score -= 5

    if pct_6h is not None and pct_24h is not None:
        rate_6h_daily  = pct_6h  * 4
        rate_24h_daily = pct_24h
        if rate_6h_daily > rate_24h_daily + 10 and pct_6h > 0:
            score += 3
            sigs.append(
                f"⚡ Flow akselerasi: 6h={pct_6h:+.1f}% >> 24h={pct_24h:+.1f}% "
                f"— tekanan beli makin kencang"
            )

    # Penalti tambahan untuk net flow jangka pendek yang sangat negatif
    if pct_15m is not None and flow_data["15m"]["count"] >= 10:
        if pct_15m > CONFIG["nf_strong_buy"]:
            score += 4
            sigs.append(
                f"✅ Ticks 15m={pct_15m:+.1f}% ({flow_data['15m']['count']} trades) "
                f"— beli dominan real-time"
            )
        elif pct_15m > CONFIG["nf_buy"]:
            score += 2
        elif pct_15m < CONFIG["nf_strong_sell"]:
            score += CONFIG["netflow_15m_penalty"]
            sigs.append(
                f"🚨 Ticks 15m={pct_15m:+.1f}% — jual dominan sekarang, penalti besar"
            )
        elif pct_15m < CONFIG["nf_sell"]:
            score -= 5  # penalti sedang

    if pct_6h is not None and pct_6h < CONFIG["netflow_6h_penalty_threshold"]:
        score += CONFIG["netflow_6h_penalty"]
        sigs.append(f"⚠️ Net flow 6h sangat negatif ({pct_6h:+.1f}%) - penalti")

    return min(score, CONFIG["max_netflow_score"]), sigs, flow_data, should_block

# LAYER CONTINUATION (enhanced)
def layer_continuation_enhanced(candles_1h, oi_chg24h, oi_chg1h, funding, chg_24h, trades, avg_vol_24h):
    """
    Mendeteksi apakah sudah pump dan sedang konsolidasi untuk lanjut.
    Dua skenario: volume drying atau volume sustain.
    """
    if chg_24h < CONFIG["cont_min_prev_pump"]:
        return 0, [], {}
    
    score = 0
    sigs = []
    cont_data = {}
    
    # Ambil 6 jam terakhir (konsolidasi)
    if len(candles_1h) < 6:
        return 0, [], cont_data
    recent = candles_1h[-6:]
    high = max(c["high"] for c in recent)
    low = min(c["low"] for c in recent)
    range_pct = (high - low) / low * 100 if low > 0 else 100
    cont_data["range_pct"] = range_pct
    
    # Retrace dari high 6h
    cur = candles_1h[-1]["close"]
    retrace = (high - cur) / high * 100
    
    vol_recent = sum(c["volume_usd"] for c in recent) / 6
    prev = candles_1h[-12:-6] if len(candles_1h) >= 12 else recent
    vol_prev = sum(c["volume_usd"] for c in prev) / len(prev) if prev else vol_recent
    vol_ratio = vol_recent / vol_prev if vol_prev > 0 else 1
    cont_data["vol_ratio"] = vol_ratio
    
    # Skenario volume sustain
    if retrace < 5 and vol_recent > avg_vol_24h * 0.8:
        score += 12
        sigs.append(f"🔄 Continuation dengan volume tinggi ({vol_recent/avg_vol_24h:.1f}x avg)")
    # Skenario volume drying
    elif range_pct < CONFIG["cont_max_range"] and vol_ratio < CONFIG["cont_vol_decrease_ratio"]:
        score += 8
        sigs.append(f"🔄 Continuation dengan volume menurun (konsolidasi)")
    
    # OI positif
    if oi_chg24h is not None and oi_chg24h > CONFIG["cont_oi_growth_min_24h"]:
        score += 5
        sigs.append(f"💰 OI 24h tumbuh {oi_chg24h:+.1f}%")
    elif oi_chg1h is not None and oi_chg1h > CONFIG["cont_oi_growth_min_1h"]:
        score += 3
        sigs.append(f"💰 OI 1h tumbuh {oi_chg1h:+.1f}%")
    
    # Funding mendukung
    if funding < -0.0001:
        score += 4
        sigs.append(f"🔥 Funding negatif {funding:.5f} — potensi squeeze lanjutan")
    
    # CVD real dari tick
    _, cvd_pct, _, _ = calc_real_cvd(trades, 6)
    if cvd_pct > 5:
        score += 4
        sigs.append(f"✅ CVD positif {cvd_pct:+.1f}% dalam 6 jam")
    
    return min(score, CONFIG["max_continuation_score"]), sigs, cont_data

# LAYER DIVERGENCE (baru)
def layer_divergence(candles_1h, oi_snapshots, trades, symbol):
    """
    Deteksi bullish divergence: harga turun tapi OI atau volume naik.
    """
    if len(candles_1h) < 12:
        return 0, []
    
    # Harga 12 jam terakhir
    price_start = candles_1h[-12]["close"]
    price_end = candles_1h[-1]["close"]
    price_chg = (price_end - price_start) / price_start * 100
    
    if price_chg > CONFIG["div_max_price_chg"]:
        return 0, []  # Harga tidak turun cukup
    
    # Cek OI
    oi_div = False
    oi_chg_12h = get_oi_change(symbol, 12*3600)
    if oi_chg_12h and oi_chg_12h > CONFIG["div_oi_growth_min"]:
        oi_div = True
    
    # Cek CVD
    _, cvd_pct, _, _ = calc_real_cvd(trades, 12)
    cvd_div = cvd_pct > 5
    
    score = 0
    sigs = []
    if oi_div and cvd_div:
        score = 15
        sigs.append(f"⭐ BULLISH DIVERGENCE: harga {price_chg:+.1f}%, OI +{oi_chg_12h:.1f}%, CVD +{cvd_pct:.1f}%")
    elif oi_div:
        score = 10
        sigs.append(f"📈 OI Divergence: harga {price_chg:+.1f}%, OI +{oi_chg_12h:.1f}%")
    elif cvd_div:
        score = 8
        sigs.append(f"📈 CVD Divergence: harga {price_chg:+.1f}%, CVD +{cvd_pct:.1f}%")
    
    return score, sigs

# MODIFIKASI PENALTY ALREADY PUMPED (non-block) - sudah ada di v11, gunakan yang sudah ada
def penalty_already_pumped(oi_chg24h, vol_chg24h_pct, chg_24h, oi_valid, candles_1h, oi_chg1h, trades, avg_vol_24h):
    """
    Mengembalikan penalti (negatif) atau bonus jika continuation.
    """
    # Cek continuation terlebih dahulu
    cont_score, cont_sigs, _ = layer_continuation_enhanced(candles_1h, oi_chg24h, oi_chg1h, 0, chg_24h, trades, avg_vol_24h)
    if cont_score > 0:
        return cont_score, "🔄 Continuation pattern detected", cont_sigs
    
    penalty = 0
    reason = ""
    if oi_valid:
        if oi_chg24h > 35 and chg_24h > 3:
            penalty = -20
            reason = f"OI 24h +{oi_chg24h:.0f}% + harga +{chg_24h:.0f}% — potensi kelelahan"
        elif oi_chg24h > 25 and chg_24h > 5:
            penalty = -15
            reason = f"OI 24h +{oi_chg24h:.0f}% + harga +{chg_24h:.0f}% — risiko kehabisan momentum"
        elif oi_chg24h > 15 and chg_24h > 10:
            penalty = -10
            reason = f"OI +{oi_chg24h:.0f}% + harga +{chg_24h:.0f}% — sudah naik signifikan"
    else:
        if chg_24h > 15 and vol_chg24h_pct > 150:
            penalty = -15
            reason = f"Harga +{chg_24h:.0f}% + Volume +{vol_chg24h_pct:.0f}% — crowd sudah masuk"
    return penalty, reason, []

# MODIFIKASI ENTRY (support-based)
def calc_entry(candles_1h, candles_15m):
    cur = candles_1h[-1]["close"]
    
    # Dapatkan level support
    support = get_support_levels(candles_1h)
    entry = support * (1 - CONFIG["entry_support_offset"] / 100)  # 0.5% di bawah support
    
    # Tentukan stop loss: di bawah support terdekat berikutnya atau 2% di bawah entry
    # Cari low 5 jam terakhir sebagai support lebih rendah
    low_5h = min(c["low"] for c in candles_1h[-5:])
    sl_candidate = min(low_5h * 0.995, entry * 0.98)
    sl = max(sl_candidate, entry * 0.97)  # jangan terlalu jauh
    
    # Target
    t1, t2 = find_resistance_targets(candles_1h, entry)
    
    risk = entry - sl
    reward = t1 - entry
    rr = round(reward / risk, 1) if risk > 0 else 0
    
    return {
        "cur": cur,
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "sl_pct": round((entry - sl) / entry * 100, 1),
        "t1": round(t1, 8),
        "t2": round(t2, 8),
        "rr": rr,
        "liq_pct": round((t1 - cur) / cur * 100, 1),
        "support_used": round(support, 8),
    }

# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE (modifikasi besar)
# ══════════════════════════════════════════════════════════════
def master_score(symbol, ticker, tickers_dict):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candle_4h"])

    if len(c1h) < 48 or len(c15m) < 20:
        return None

    # Dead activity gate (sama)
    if len(c1h) >= 7:
        last_vol     = c1h[-1]["volume_usd"]
        avg_vol_6h   = sum(c["volume_usd"] for c in c1h[-7:-1]) / 6
        if avg_vol_6h > 0:
            activity_ratio = last_vol / avg_vol_6h
            if activity_ratio < CONFIG.get("dead_activity_threshold", 0.10):
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

    # Overbought gate (masih ada, tapi longgar dengan squeeze)
    if chg_7d > CONFIG["gate_chg_7d_max"]:
        oi_value = get_open_interest(symbol)
        oi_chg_1h, _, oi_valid_gate = get_oi_changes(symbol, oi_value) if oi_value > 0 else (0, 0, False)
        real_squeeze = (funding <= CONFIG["squeeze_funding_max"]
                        and oi_valid_gate
                        and oi_chg_1h > CONFIG["squeeze_oi_change_min"])
        if not real_squeeze:
            log.info(f"  {symbol}: GATE overbought ({chg_7d:.1f}%) tanpa squeeze")
            return None

    if chg_7d < CONFIG["gate_chg_7d_min"]:
        log.info(f"  {symbol}: GATE downtrend ({chg_7d:.1f}%)")
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
        log.info(f"  {symbol}: GATE volume exhaustion ({vol_change_24h:.0f}% 24h)")
        return None
    elif vol_change_24h < -45:
        log.info(f"  {symbol}: Volume turun {vol_change_24h:.0f}% — penalti aktif")

    if len(c1h) >= 6:
        pre6       = c1h[-6:]
        avg_vol_6h = sum(c["volume_usd"] for c in pre6) / 6
        high_6h    = max(c["high"] for c in pre6)
        low_6h     = min(c["low"]  for c in pre6)
        range_6h   = (high_6h - low_6h) / low_6h * 100 if low_6h > 0 else 0
    else:
        avg_vol_6h, range_6h = 0, 0

    score, sigs, bd = 0, [], {}
    
    trades = get_trades(symbol, 500)  # ambil trades untuk CVD dan whale

    # Hitung rata-rata volume 24h untuk digunakan di berbagai layer
    avg_vol_24h = sum(c["volume_usd"] for c in c1h[-24:]) / 24 if len(c1h) >= 24 else 1

    # Layer volume intelligence (dengan CVD real)
    v_sc, v_sigs, rvol, ots_type = layer_volume_intelligence(c1h, trades)
    score += v_sc;  sigs += v_sigs;  bd["vol"] = v_sc

    # CVD jangka pendek
    stcvd_sc, stcvd_sig = calc_short_term_cvd(c1h, trades)
    score += stcvd_sc
    if stcvd_sig:
        sigs.append(stcvd_sig)
    bd["stcvd"] = stcvd_sc

    # Extended dry volume (baru)
    dry_sc, dry_sig = layer_extended_dry_volume(c1h, avg_vol_24h)
    score += dry_sc
    if dry_sig:
        sigs.append(dry_sig)
    bd["extended_dry"] = dry_sc

    # Volume creep (baru)
    creep_sc, creep_sig = layer_volume_creep(c1h)
    score += creep_sc
    if creep_sig:
        sigs.append(creep_sig)
    bd["volume_creep"] = creep_sc

    # Breakout (baru)
    breakout_sc, breakout_sig = layer_breakout(c1h, avg_vol_24h)
    score += breakout_sc
    if breakout_sig:
        sigs.append(breakout_sig)
    bd["breakout"] = breakout_sc

    # Flat accumulation
    fa_sc, fa_sigs = layer_flat_accumulation(c1h)
    score += fa_sc;  sigs += fa_sigs;  bd["flat"] = fa_sc

    # Structure
    st_sc, st_sigs, bbw_val, bbw_pct, coiling = layer_structure(c1h)
    score += st_sc;  sigs += st_sigs;  bd["struct"] = st_sc

    # Stealth bonus (dengan rasio)
    stealth_bonus = 0
    if len(c1h) >= 6:
        if avg_vol_6h < avg_vol_24h * CONFIG["stealth_vol_ratio"] and coiling > CONFIG["stealth_min_coiling"] and range_6h < CONFIG["stealth_max_range"]:
            stealth_bonus = 25
            sigs.append(f"🕵️ STEALTH PATTERN: vol {avg_vol_6h/avg_vol_24h:.0%} dari rata-rata")
    score += stealth_bonus;  bd["stealth"] = stealth_bonus

    # OI data
    oi_value   = get_open_interest(symbol)
    oi_chg1h   = None
    oi_chg24h  = None
    oi_valid   = False
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)
        snaps = load_oi_snapshots().get(symbol, [])
        oi_chg1h = get_oi_change(symbol, 3600)
        oi_chg24h = get_oi_change(symbol, 86400)
        oi_valid = (oi_chg1h is not None)

    # Penalty already pumped (bukan block)
    pump_penalty, pump_reason, pump_sigs = penalty_already_pumped(
        oi_chg24h or 0, vol_change_24h, chg_24h, oi_valid, c1h, oi_chg1h or 0, trades, avg_vol_24h
    )
    score += pump_penalty
    if pump_reason:
        sigs.append(pump_reason)
    sigs += pump_sigs

    # Positioning layer
    pos_sc, pos_sigs, ls_ratio, ls_block = layer_positioning(symbol, funding, oi_chg1h or 0)
    if ls_block:
        log.info(f"  {symbol}: GATE L/S overcrowded kritis (L/S={ls_ratio:.2f})")
        return None
    score += pos_sc;  sigs += pos_sigs;  bd["pos"] = pos_sc

    # 4h confluence
    tf4h_sc = 0
    if c4h:
        tf4h_sc, tf4h_sig = calc_4h_confluence(c4h)
        if tf4h_sig:
            sigs.append(tf4h_sig)
    score += tf4h_sc;  bd["tf4h"] = tf4h_sc

    # Context
    ctx_sc, ctx_sigs, sector = layer_context(symbol, tickers_dict)
    score += ctx_sc;  sigs += ctx_sigs;  bd["ctx"] = ctx_sc

    # Whale
    ws, whale_bonus, wev = calc_whale(symbol, c15m, funding, trades)
    score += whale_bonus;  bd["whale"] = whale_bonus

    # Liquidation
    liq_sc, liq_sigs, long_liq, short_liq, liq_block = layer_liquidation(symbol, c1h)
    if liq_block:
        log.info(f"  {symbol}: GATE liquidation — long flush baru saja terjadi")
        return None
    score += liq_sc;  sigs += liq_sigs;  bd["liq"] = liq_sc

    # OI Acceleration
    oi_accel_sc, oi_accel_sigs, oi_accel_data = layer_oi_acceleration(
        symbol, oi_value, chg_24h, vol_24h
    )
    score += oi_accel_sc;  sigs += oi_accel_sigs;  bd["oi_accel"] = oi_accel_sc

    # Net Flow (dengan penalti)
    nf_sc, nf_sigs, nf_data, nf_block = layer_net_flow(c1h, c15m, trades)
    if nf_block:
        log.info(f"  {symbol}: GATE net flow distribusi sistematis")
        return None
    score += nf_sc;  sigs += nf_sigs;  bd["netflow"] = nf_sc

    # Divergence layer
    div_sc, div_sigs = layer_divergence(c1h, None, trades, symbol)
    score += div_sc
    sigs += div_sigs
    bd["divergence"] = div_sc

    # RSI
    rsi_1h = get_rsi(c1h[-48:] if len(c1h) >= 48 else c1h)

    # OI additional scoring (sama, disederhanakan)
    if oi_value > 0:
        if oi_valid:
            if oi_chg24h is not None:
                if oi_chg24h < -20:
                    score -= 25; sigs.append(f"⚠️ OI 24h turun {oi_chg24h:.1f}% — distribusi masif")
                elif oi_chg24h < -10:
                    score -= 15; sigs.append(f"⚠️ OI 24h turun {oi_chg24h:.1f}% — distribusi signifikan")
                elif oi_chg24h < -5:
                    score -= 10; sigs.append(f"OI 24h turun {oi_chg24h:.1f}% — distribusi terindikasi")
                elif oi_chg24h < -3:
                    score -= 5;  sigs.append(f"OI 24h {oi_chg24h:.1f}% — sedikit berkurang")

            if oi_chg1h is not None:
                if oi_chg1h < -8:
                    score -= 20; sigs.append(f"🚨 OI 1h turun {oi_chg1h:.1f}% — distribusi CEPAT!")
                elif oi_chg1h < -5:
                    score -= 12; sigs.append(f"⚠️ OI 1h turun {oi_chg1h:.1f}% — tekanan jual 1 jam")
                elif oi_chg1h < -2:
                    score -= 6;  sigs.append(f"OI 1h {oi_chg1h:.1f}% — mulai berkurang")
                elif oi_chg1h > 5:
                    score += 5;  sigs.append(f"✅ OI 1h naik {oi_chg1h:.1f}% — posisi baru masuk")

            if oi_chg24h and oi_chg1h and oi_chg24h < -3 and oi_chg1h < -2:
                log.info(f"  {symbol}: GATE multi-TF OI decline")
                return None

        else:
            # OI cold start: penalti kecil
            sigs.append("ℹ️ OI history belum tersedia (run pertama) — penalti ringan")
            score -= 3

        # Volume momentum
        if len(c1h) >= 24:
            vol_24h_candles  = [c["volume_usd"] for c in c1h[-24:]]
            avg_vol_24h_base = sum(vol_24h_candles) / len(vol_24h_candles) if vol_24h_candles else 0
            if avg_vol_24h_base > 0 and avg_vol_6h > 0:
                vol_momentum = avg_vol_6h / avg_vol_24h_base
                if vol_momentum < 0.50:
                    score -= 10; sigs.append(f"⚠️ Volume 6h hanya {vol_momentum:.0%} avg 24h")
                elif vol_momentum < 0.70:
                    score -= 5;  sigs.append(f"Volume 6h menurun ({vol_momentum:.0%})")

        bd["oi_change"]    = round(oi_chg24h, 1) if oi_chg24h else 0
        bd["oi_change_1h"] = round(oi_chg1h, 1) if oi_chg1h else 0
        bd["oi_valid"]     = oi_valid
    else:
        bd["oi_change"] = bd["oi_change_1h"] = 0
        bd["oi_valid"]  = False

    # Overbought penalty
    if chg_7d > CONFIG["gate_chg_7d_max"]:
        score -= 15; sigs.append(f"⚠️ Overbought ({chg_7d:+.1f}% 7d)")

    # Range 24h terlalu lebar
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
    raw_score = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    # Penalti RVOL rendah (diskon sinyal struktur/posisi)
    if rvol < 0.5:
        penalty = int((bd.get("struct", 0) + bd.get("pos", 0)) * 0.2)
        raw_score -= penalty
        sigs.append(f"⚠️ RVOL rendah ({rvol:.1f}x) - diskon 20% untuk sinyal struktur/posisi")

    # Hitung CVD untuk prob
    _, cvd_pct, _, _ = calc_real_cvd(trades, 6)

    # Probability model baru
    prob = compute_pump_probability(c1h, oi_chg1h, oi_chg24h, funding, ls_ratio, cvd_pct, ws)
    bd["prob_score"] = round(prob["probability_score"] * 100, 1)
    bd["prob_class"] = prob["classification"]

    composite = int(
        min(raw_score, 100) * CONFIG["composite_w_layer"]
        + prob["probability_score"] * 100 * CONFIG["composite_w_prob"]
    )
    composite = min(composite, 100)
    bd["composite"] = composite
    bd["rsi_1h"]    = round(rsi_1h, 1)

    entry = calc_entry(c1h, c15m)
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
        "oi_accel_score":  oi_accel_sc,
        "oi_accel_data":   oi_accel_data,
        "nf_data":         nf_data,
        "nf_score":        nf_sc,
        "pump_type":       ots_type,
        "continuation_score": 0,  # bisa diisi nanti
    }

# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER (modifikasi untuk menampilkan layer baru)
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

    cont_str = ""
    if r.get("continuation_score", 0) > 0:
        cont_str = f"<b>Continuation:</b> +{r['continuation_score']} poin\n"

    accel_str = ""
    ad = r.get("oi_accel_data", {})
    accel_sc = r.get("oi_accel_score", 0)
    if accel_sc > 0:
        div_tag = " 📈DIV" if ad.get("divergence") else ""
        micro_tag = " [MICRO]" if ad.get("is_micro_cap") else ""
        accel_str = (
            f"<b>OI Accel  :</b> +{accel_sc}poin{micro_tag}{div_tag} | "
            f"1h:{ad.get('growth_rate_1h', 0):+.1f}% "
            f"3h:{ad.get('growth_rate_3h', 0):+.1f}% "
            f"6h:{ad.get('growth_rate_6h', 0):+.1f}%\n"
        )

    liq_str = ""
    if r.get("long_liq", 0) > 0 or r.get("short_liq", 0) > 0:
        liq_str = (f"<b>Liquidation:</b> Long ${r.get('long_liq',0)/1e3:.0f}K | "
                   f"Short ${r.get('short_liq',0)/1e3:.0f}K (30m)\n")

    nf_str = ""
    nfd = r.get("nf_data", {})
    if nfd.get("has_data"):
        def _flow_icon(label):
            return {"STRONG_BUY": "🟢🟢", "BUY": "🟢", "NEUTRAL": "⚪",
                    "SELL": "🔴", "STRONG_SELL": "🔴🔴"}.get(label, "⚪")

        f72 = nfd.get("72h", {}); f24 = nfd.get("24h", {})
        f6  = nfd.get("6h",  {}); f15 = nfd.get("15m", {})
        nf72_icon = _flow_icon(f72.get("label", "NO_DATA"))
        nf24_icon = _flow_icon(f24.get("label", "NO_DATA"))
        nf6_icon  = _flow_icon(f6.get("label",  "NO_DATA"))
        nf15_icon = _flow_icon(f15.get("label", "NO_DATA"))
        nf_score  = r.get("nf_score", 0)
        nf_str = (
            f"<b>Net Flow   :</b> [{nf_score:+d}poin]\n"
            f"  {nf72_icon}72h:{f72.get('net_pct',0):+.1f}%  "
            f"{nf24_icon}24h:{f24.get('net_pct',0):+.1f}%  "
            f"{nf6_icon}6h:{f6.get('net_pct',0):+.1f}%  "
            f"{nf15_icon}15m:{f15.get('net_pct',0):+.1f}%\n"
        )

    oi_warning = ""
    if not bd.get("oi_valid", True):
        oi_warning = "⚠️ <i>OI baseline belum tersedia (run pertama)</i>\n"

    pump_type = r.get("pump_type", "unknown")
    type_icon = "🎯" if pump_type == "on_the_spot" else "📦" if pump_type == "pre_accumulation" else "❓"
    type_name = {"on_the_spot": "On-The-Spot", "pre_accumulation": "Pre-Accumulation", "unknown": "Unknown"}.get(pump_type, "Unknown")

    # Tambahan sinyal baru
    extra_signals = []
    if bd.get("extended_dry", 0) > 0:
        extra_signals.append("💤 Extended dry")
    if bd.get("volume_creep", 0) > 0:
        extra_signals.append("📈 Volume creep")
    if bd.get("breakout", 0) > 0:
        extra_signals.append("🚀 Breakout")
    if bd.get("divergence", 0) > 0:
        extra_signals.append("📊 Divergence")

    msg = (
        f"🚨 <b>PRE-PUMP SIGNAL {rk}— v12.0-ENHANCED</b>\n\n"
        f"<b>Symbol    :</b> {r['symbol']}\n"
        f"<b>Pump Type :</b> {type_icon} {type_name}\n"
        f"<b>Composite :</b> {comp}/100  {bar}\n"
        f"<b>Layer Score:</b> {sc}/100\n"
        f"<b>Prob Model :</b> {prob_pct:.1f}% ({prob_cls})\n"
        f"<b>RSI 1h     :</b> {r.get('rsi_1h', 0):.1f}\n"
        f"{' | '.join(extra_signals)}\n"
        f"{cont_str}"
        f"{accel_str}"
        f"{oi_warning}"
        f"<b>Sektor     :</b> {r['sector']}\n"
        f"<b>Harga      :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h | {r['chg_7d']:+.1f}% 7d)\n"
        f"<b>Vol 24h    :</b> {vol} | RVOL: {r['rvol']:.1f}x{ls}\n"
        f"<b>6h Vol     :</b> ${r['avg_vol_6h']:.0f}/h  | 6h Range: {r['range_6h']:.1f}%\n"
        f"<b>Coiling    :</b> {r['coiling']}h  | BBW: {r['bbw_val']:.1f}%\n"
        f"<b>OI 24h/1h :</b> {r['oi_change_24h']:+.1f}% / {r.get('oi_change_1h',0):+.1f}%"
        f"  [valid:{bd.get('oi_valid','?')}]\n"
        f"{nf_str}"
        f"{liq_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐋 <b>WHALE SCORE: {r['ws']}/100</b>\n"
    )
    for ev in r["wev"]:
        msg += f"  {ev}\n"

    if e:
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY ZONES (SUPPORT-BASED)</b>\n"
            f"  💡 Support: ${e['support_used']}\n"
            f"  📌 Entry : ${e['entry']}  ({CONFIG['entry_support_offset']:.1f}% below support)\n"
            f"  🛑 SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n\n"
            f"🎯 <b>TARGET</b>\n"
            f"  T1 : ${e['t1']}  (+{e['liq_pct']:.1f}%)\n"
            f"  T2 : ${e['t2']}\n"
            f"  R/R: 1:{e['rr']}\n\n"
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
        f"Accel:{bd.get('oi_accel',0)} Flow:{bd.get('netflow',0)} "
        f"Cont:{bd.get('continuation',0)} Div:{bd.get('divergence',0)} "
        f"Stealth:{bd.get('stealth',0)} Dry:{bd.get('extended_dry',0)} "
        f"Creep:{bd.get('volume_creep',0)} Brk:{bd.get('breakout',0)}\n"
        f"  OI valid:{bd.get('oi_valid','?')} RSI:{bd.get('rsi_1h','?')} "
        f"[Prob] MVS:{pm.get('max_vol_spike','?')}x "
        f"Irr:{pm.get('vol_irregularity','?')} "
        f"ATR:{pm.get('norm_atr_pct','?')}%\n\n"
        f"📡 Funding:{r['funding']:.5f}  🕐 {utc_now()}\n"
        f"<i>⚠️ Bukan financial advice. Manage risk ketat.</i>"
    )
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v12.0-ENHANCED — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        comp     = r.get("composite_score", r["score"])
        bar      = "█" * int(comp / 10) + "░" * (10 - int(comp / 10))
        vol      = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                    else f"${r['vol_24h']/1e3:.0f}K")
        t1p      = r["entry"]["liq_pct"] if r.get("entry") else 0
        prob     = r.get("prob_score", 0) * 100
        prob_cls = r.get("prob_class", "?")
        rsi      = r.get("rsi_1h", 0)
        # Tambahan ikon
        extra = ""
        if r.get("bd", {}).get("extended_dry", 0) > 0:
            extra += " 💤"
        if r.get("bd", {}).get("volume_creep", 0) > 0:
            extra += " 📈"
        if r.get("bd", {}).get("breakout", 0) > 0:
            extra += " 🚀"
        if r.get("continuation_score", 0) > 0:
            extra += " 🔄"
        msg += (
            f"{i}. <b>{r['symbol']}</b> [C:{comp} S:{r['score']} {bar}]{extra}\n"
            f"   🐋{r['ws']} | RVOL:{r['rvol']:.1f}x | {vol} | "
            f"T1:+{t1p:.0f}% | {prob:.0f}% {prob_cls} | RSI:{rsi:.0f}\n"
        )
    return msg

# ══════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST (sama)
# ══════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found = []
    filtered_stats = {
        "cooldown": 0,
        "manual_exclude": 0,
        "vol_too_low": 0,
        "vol_too_high": 0,
        "change_extreme": 0,
        "invalid_price": 0,
        "parse_error": 0,
    }
    
    log.info("=" * 70)
    log.info("🔍 SCANNING MODE: FULL WHITELIST (ALL 324 COINS)")
    log.info("=" * 70)
    
    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:
            filtered_stats["manual_exclude"] += 1
            continue
        
        if is_cooldown(sym):
            filtered_stats["cooldown"] += 1
            continue
        
        if sym not in tickers:
            not_found.append(sym)
            continue
        
        ticker = tickers[sym]
        
        try:
            vol   = float(ticker.get("quoteVolume", 0))
            chg   = float(ticker.get("change24h", 0)) * 100
            price = float(ticker.get("lastPr", 0))
        except:
            filtered_stats["parse_error"] += 1
            continue
        
        if vol < CONFIG["pre_filter_vol"]:
            filtered_stats["vol_too_low"] += 1
            continue
        
        if vol > CONFIG["max_vol_24h"]:
            filtered_stats["vol_too_high"] += 1
            continue
        
        if abs(chg) > CONFIG["gate_chg_24h_max"]:
            filtered_stats["change_extreme"] += 1
            continue
        
        if price <= 0:
            filtered_stats["invalid_price"] += 1
            continue
        
        all_candidates.append((sym, ticker))
    
    total = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    filtered = total - will_scan
    
    log.info("")
    log.info("📊 SCAN SUMMARY:")
    log.info(f"   Whitelist total: {total} coins")
    log.info(f"   ✅ Will scan:     {will_scan} coins ({will_scan/total*100:.1f}%)")
    log.info(f"   ❌ Filtered:      {filtered} coins ({filtered/total*100:.1f}%)")
    log.info("")
    log.info("📋 Filter breakdown:")
    log.info(f"   Not in Bitget:  {len(not_found)}")
    log.info(f"   Cooldown:       {filtered_stats['cooldown']}")
    log.info(f"   Manual exclude: {filtered_stats['manual_exclude']}")
    log.info(f"   Vol < $1K:      {filtered_stats['vol_too_low']}")
    log.info(f"   Vol > $50M:     {filtered_stats['vol_too_high']}")
    log.info(f"   Chg > ±30%:     {filtered_stats['change_extreme']}")
    log.info(f"   Invalid price:  {filtered_stats['invalid_price']}")
    log.info(f"   Parse error:    {filtered_stats['parse_error']}")
    
    if not_found and len(not_found) <= 30:
        log.info(f"\n⚠️  Missing from Bitget: {', '.join(not_found)}")
    elif not_found:
        log.info(f"\n⚠️  {len(not_found)} coins missing from Bitget")
        log.info(f"     First 10: {', '.join(not_found[:10])}")
    
    log.info(f"\n⏱️  Est. scan time: {will_scan * CONFIG['sleep_coins']:.0f}s (~{will_scan * CONFIG['sleep_coins']/60:.1f} min)")
    log.info("=" * 70)
    log.info("")
    
    return all_candidates

# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v12.0-ENHANCED — {utc_now()} ===")
    log.info("=" * 70)
    log.info("PERUBAHAN vs v11.0:")
    log.info("  • Extended Dry Volume, Volume Creep, Breakout layers")
    log.info("  • Penalti kuat net flow negatif jangka pendek")
    log.info("  • CVD Divergence butuh konfirmasi price slope")
    log.info("  • Stealth pattern pakai rasio volume")
    log.info("  • Penalti RVOL rendah")
    log.info("  • Entry support dinamis (support terdekat)")
    log.info("=" * 70)

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total ticker: {len(tickers)}")

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
                cont     = res.get("continuation_score", 0)
                rsi      = res.get("rsi_1h", 0)
                oi_v     = res.get("bd", {}).get("oi_valid", "?")
                # Tampilkan ikon layer baru
                extra = ""
                if res.get("bd", {}).get("extended_dry", 0) > 0:
                    extra += " 💤"
                if res.get("bd", {}).get("volume_creep", 0) > 0:
                    extra += " 📈"
                if res.get("bd", {}).get("breakout", 0) > 0:
                    extra += " 🚀"
                log.info(
                    f"  Score={res['score']} Comp={comp} W={res['ws']} "
                    f"RVOL={res['rvol']:.1f}x Prob={prob:.0f}% ({prob_cls}) "
                    f"Cont={cont} RSI={rsi:.0f} OI_valid={oi_v} "
                    f"T1=+{res['entry']['liq_pct']:.1f}% {extra}"
                )
                if (comp >= CONFIG["min_composite_alert"]
                        and res["prob_score"] >= CONFIG["min_prob_alert"]):
                    results.append(res)
                else:
                    reason = ""
                    if comp < CONFIG["min_composite_alert"]:
                        reason += f"comp={comp}<{CONFIG['min_composite_alert']}"
                    if res["prob_score"] < CONFIG["min_prob_alert"]:
                        reason += f" prob={prob:.0f}%<{CONFIG['min_prob_alert']*100:.0f}%({prob_cls})"
                    if reason:
                        log.info(f"  SKIP: {reason.strip()}")
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    results.sort(
        key=lambda x: (
            x["composite_score"] + x.get("continuation_score", 0) * 2,
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
            or r.get("continuation_score", 0) >= 15)
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
                f"Prob={r['prob_score']*100:.0f}% Cont={r.get('continuation_score',0)} "
                f"OI_valid={r.get('bd',{}).get('oi_valid','?')}"
            )
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v12.0-ENHANCED                 ║")
    log.info("║  FOKUS: Deteksi pump 1-4 jam sebelumnya          ║")
    log.info("╚═══════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)

    run_scan()
