"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v15.6 — BUG FIXES & OPTIMIZATION                           ║
║                                                                              ║
║  PERUBAHAN DARI v15.3:                                                       ║
║  #1 FIX: IndexError di get_funding dan get_open_interest                     ║
║  #2 FIX: Division by zero protection di semua perhitungan                    ║
║  #3 FIX: Cache memory leak dengan TTL implementation                         ║
║  #4 FIX: Variable initialization yang tidak lengkap                          ║
║  #5 OPT: Consolidate ATR calculation functions                               ║
║  #6 OPT: Better error handling dan logging                                   ║
║  #7 OPT: String building dengan join untuk performa                          ║
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
from typing import Optional, Dict, List, Any, Tuple

# Version info
VERSION = "15.6-FIXED"

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ── Logging ───────────────────────────────────────────────────────────────────
_log_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v15_6.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info(f"Scanner v{VERSION} — log aktif: /tmp/scanner_v15_6.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_alert": 10,
    "max_alerts_per_run": 15,

    # ── Volume 24h total (USD) ────────────────────────────────────────────────
    "min_vol_24h": 10_000,
    "max_vol_24h": 50_000_000,
    "pre_filter_vol": 5_000,

    # ── Open Interest minimum filter ──────────────────────────────────────────
    "min_oi_usd": 100_000,

    # ── Gate perubahan harga ──────────────────────────────────────────────────
    "gate_chg_24h_max": 12.0,
    "gate_chg_24h_min": -15.0,

    # ── VWAP Gate Tolerance ───────────────────────────────────────────────────
    "vwap_gate_tolerance": 0.97,

    # ── Energy Build-Up Detector ──────────────────────────────────────────────
    "energy_oi_change_min": 5.0,
    "energy_vol_ratio_min": 1.5,
    "energy_range_max_pct": 2.5,
    "score_energy_buildup": 4,

    # ── Gate uptrend usia ─────────────────────────────────────────────────────
    "gate_uptrend_max_hours": 10,

    # ── Gate RSI overbought ───────────────────────────────────────────────────
    "gate_rsi_max": 75.0,

    # ── Gate BB Position ──────────────────────────────────────────────────────
    "gate_bb_pos_max": 1.05,

    # ── Funding scoring ────────────────────────────────────────────────────────
    "funding_penalty_avg": 0.0003,
    "funding_bonus_avg": -0.0002,
    "funding_bonus_cumul": -0.001,

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h": 168,
    "candle_15m": 96,
    "candle_4h": 48,

    # ── Entry / SL ────────────────────────────────────────────────────────────
    "deep_entry_vwap_atr_mult": 0.5,
    "deep_entry_sl_atr_mult": 1.0,
    "deep_entry_tp_atr_mult": 2.0,
    "use_deep_entry": True,
    "entry_bos_buffer": 0.001,
    "entry_vwap_buffer": 0.001,
    "sl_swing_lookback": 12,
    "sl_swing_buffer": 0.003,
    "sl_atr_multiplier_min": 0.5,
    "sl_atr_multiplier_max": 2.5,
    "max_sl_pct": 8.0,
    "min_sl_pct": 0.5,

    # ── Operasional ───────────────────────────────────────────────────────────
    "alert_cooldown_sec": 1800,
    "sleep_coins": 0.8,
    "sleep_error": 3.0,
    "cooldown_file": "./cooldown.json",
    "funding_snapshot_file": "./funding.json",

    # ── Bobot skor ────────────────────────────────────────────────────────────
    "score_ema_gap": 2,
    "score_atr_15": 4,
    "score_bbw_10": 4,
    "score_above_vwap_bos": 4,
    "score_bos_up": 3,
    "score_atr_10": 3,
    "score_bbw_6": 3,
    "score_funding_neg_pct": 3,
    "score_funding_streak": 3,
    "score_higher_low": 2,
    "score_bb_squeeze": 2,
    "score_rsi_65": 2,
    "score_funding_cumul": 2,
    "score_vol_ratio": 2,
    "score_vol_accel": 2,
    "score_rsi_55": 1,
    "score_price_chg": 1,
    "score_accumulation": 4,
    "score_vol_compression": 4,

    # ── HTF Accumulation Filter 4H ────────────────────────────────────────────
    "htf_atr_contract_ratio": 0.85,
    "htf_vol_ratio_min": 1.3,
    "htf_range_max_pct": 3.0,
    "score_htf_accumulation": 3,

    # ── Liquidity Sweep Detection ─────────────────────────────────────────────
    "liq_sweep_lookback": 20,
    "liq_sweep_wick_min_pct": 0.3,
    "score_liquidity_sweep": 3,

    # ── OI Expansion ───────────────────────────────────────────────────────────
    "oi_change_min_pct": 3.0,
    "oi_strong_pct": 10.0,
    "score_oi_expansion": 3,
    "score_oi_strong": 5,

    # ── BTC Regime + Outperformance ───────────────────────────────────────────
    "btc_bearish_threshold": -3.0,
    "btc_bullish_threshold": 3.0,
    "outperform_min_delta": 2.0,
    "score_outperform": 3,

    # ── Threshold indikator ───────────────────────────────────────────────────
    "above_vwap_rate_min": 0.6,
    "ema_gap_threshold": 1.0,
    "bbw_threshold_high": 0.10,
    "bbw_threshold_mid": 0.06,
    "bb_squeeze_threshold": 0.04,
    "vol_ratio_threshold": 1.5,
    "vol_accel_threshold": 0.5,
    "funding_streak_min": 5,

    # ── Smart Money Accumulation ──────────────────────────────────────────────
    "accum_vol_ratio": 1.5,
    "accum_price_range_max": 2.0,
    "accum_atr_lookback_long": 24,
    "accum_atr_lookback_short": 6,
    "accum_atr_contract_ratio": 0.75,
}

MANUAL_EXCLUDE = set()
EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

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
}

GRAN_MAP = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
BITGET_BASE = "https://api.bitget.com"

# ══════════════════════════════════════════════════════════════════════════════
#  🗂️  TTL CACHE — Fix memory leak
# ══════════════════════════════════════════════════════════════════════════════
class TTLCache:
    """Cache dengan TTL dan auto-cleanup untuk mencegah memory leak"""
    def __init__(self, ttl_seconds: int = 90, max_size: int = 1000):
        self.ttl = ttl_seconds
        self.max_size = max_size
        self._cache: Dict[str, Tuple[float, Any]] = {}
    
    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            ts, val = self._cache[key]
            if time.time() - ts < self.ttl:
                return val
            else:
                del self._cache[key]
        return None
    
    def set(self, key: str, value: Any) -> None:
        # Cleanup jika melebihi max_size (hapus entry tertua)
        if len(self._cache) >= self.max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][0])
            del self._cache[oldest_key]
        self._cache[key] = (time.time(), value)
    
    def clear(self) -> None:
        self._cache.clear()

_cache = TTLCache(ttl_seconds=90, max_size=1000)

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
# ══════════════════════════════════════════════════════════════════════════════
def load_cooldown() -> Dict[str, float]:
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p, 'r') as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception as e:
        log.warning(f"Error loading cooldown: {e}")
    return {}

def save_cooldown(state: Dict[str, float]) -> None:
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Error saving cooldown: {e}")

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym: str) -> bool:
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym: str) -> None:
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  💾  FUNDING SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots: Dict[str, List[Dict]] = {}
_btc_candles_cache: Dict[str, Any] = {"ts": 0, "data": []}

def load_funding_snapshots() -> None:
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p, 'r') as f:
                _funding_snapshots = json.load(f)
    except Exception as e:
        log.warning(f"Error loading funding snapshots: {e}")
        _funding_snapshots = {}

def save_all_funding_snapshots() -> None:
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception as e:
        log.warning(f"Gagal simpan funding snapshot: {e}")

def add_funding_snapshot(symbol: str, funding_rate: float) -> None:
    now = time.time()
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({"ts": now, "funding": funding_rate})
    # Keep only last 20 entries, sorted by timestamp
    _funding_snapshots[symbol] = sorted(
        _funding_snapshots[symbol], key=lambda x: x["ts"]
    )[-20:]

# ══════════════════════════════════════════════════════════════════════════════
#  🛡️  SAFE OPERATIONS — Division by zero protection
# ══════════════════════════════════════════════════════════════════════════════
def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division dengan protection terhadap division by zero"""
    if denominator == 0 or denominator is None or not isinstance(denominator, (int, float)):
        return default
    try:
        result = numerator / denominator
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ZeroDivisionError, TypeError, ValueError):
        return default

def safe_float(value: Any, default: float = 0.0) -> float:
    """Safe float conversion"""
    if value is None:
        return default
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url: str, params: Optional[Dict] = None, timeout: int = 12, max_retries: int = 2) -> Optional[Dict]:
    """HTTP GET dengan exponential backoff dan better error handling"""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                sleep_time = min(15 * (2 ** attempt), 60)  # Exponential backoff, max 60s
                log.warning(f"Rate limit — tunggu {sleep_time}s")
                time.sleep(sleep_time)
                continue
            log.warning(f"HTTP Error {e.response.status_code if e.response else 'unknown'}: {e}")
            break
        except requests.exceptions.Timeout:
            log.warning(f"Timeout (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(CONFIG["sleep_error"])
        except Exception as e:
            log.warning(f"Request error: {e}")
            if attempt < max_retries - 1:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg: str) -> bool:
    """Kirim pesan ke Telegram dengan error logging"""
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN atau CHAT_ID tidak ada!")
        return False
    
    # Telegram max 4096 chars
    if len(msg) > 4000:
        msg = msg[:3900] + "\n\n<i>...[dipotong, terlalu panjang]</i>"
    
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=15,
        )
        if r.status_code != 200:
            log.warning(f"Telegram gagal: HTTP {r.status_code} — {r.text[:200]}")
            return False
        return True
    except Exception as e:
        log.warning(f"Telegram exception: {e}")
        return False

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════
def get_all_tickers() -> Dict[str, Dict]:
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        params={"productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        return {t["symbol"]: t for t in data.get("data", []) if "symbol" in t}
    return {}

def get_candles(symbol: str, gran: str = "1h", limit: int = 168) -> List[Dict]:
    g = GRAN_MAP.get(gran, "1H")
    key = f"c_{symbol}_{g}_{limit}"
    
    cached = _cache.get(key)
    if cached is not None:
        return cached
    
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        params={
            "symbol": symbol,
            "granularity": g,
            "limit": str(limit),
            "productType": "usdt-futures",
        },
    )
    if not data or data.get("code") != "00000":
        return []
    
    candles = []
    for c in data.get("data", []):
        try:
            if len(c) < 6:
                continue
            
            open_p = safe_float(c[1])
            high_p = safe_float(c[2])
            low_p = safe_float(c[3])
            close_p = safe_float(c[4])
            volume = safe_float(c[5])
            
            # FIX: Safe volume_usd calculation
            if len(c) > 6:
                vol_usd = safe_float(c[6])
            else:
                vol_usd = volume * close_p if close_p > 0 else 0.0
            
            candles.append({
                "ts": int(c[0]),
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close_p,
                "volume": volume,
                "volume_usd": vol_usd,
            })
        except Exception as e:
            log.debug(f"Error parsing candle for {symbol}: {e}")
            continue
    
    candles.sort(key=lambda x: x["ts"])
    _cache.set(key, candles)
    return candles

def get_funding(symbol: str) -> float:
    """FIX: IndexError protection"""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            data_list = data.get("data", [])
            if not data_list:  # FIX: Check for empty list
                return 0.0
            return safe_float(data_list[0].get("fundingRate", 0))
        except Exception as e:
            log.debug(f"Error parsing funding for {symbol}: {e}")
    return 0.0

def get_btc_candles_cached(limit: int = 48) -> List[Dict]:
    """Cache candle BTCUSDT 1h selama 5 menit"""
    global _btc_candles_cache
    if time.time() - _btc_candles_cache["ts"] < 300 and _btc_candles_cache["data"]:
        return _btc_candles_cache["data"]
    candles = get_candles("BTCUSDT", "1h", limit)
    if candles:
        _btc_candles_cache = {"ts": time.time(), "data": candles}
    return candles

def get_funding_stats(symbol: str) -> Optional[Dict]:
    """Hitung statistik funding dari snapshot in-memory"""
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None
    
    all_rates = [s["funding"] for s in snaps]
    last6 = all_rates[-6:]
    avg6 = sum(last6) / len(last6) if last6 else 0.0
    cumul = sum(last6)
    neg_pct = sum(1 for f in last6 if f < 0) / len(last6) * 100 if last6 else 0.0
    
    streak = 0
    for f in reversed(all_rates):
        if f < 0:
            streak += 1
        else:
            break
    
    basis = all_rates[-1] * 100 if all_rates else 0.0
    
    return {
        "avg": avg6,
        "cumulative": cumul,
        "neg_pct": neg_pct,
        "streak": streak,
        "basis": basis,
        "current": all_rates[-1] if all_rates else 0.0,
        "sample_count": len(all_rates),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📊  INDIKATOR TEKNIKAL — Consolidated ATR
# ══════════════════════════════════════════════════════════════════════════════
def calc_atr(candles: List[Dict], period: int = 14, as_pct: bool = False) -> float:
    """
    Consolidated ATR calculation — menggantikan 4 fungsi terpisah.
    
    Args:
        candles: List candle data
        period: ATR period
        as_pct: Jika True, return sebagai % dari close price
    
    Returns:
        ATR value (absolute atau percentage)
    """
    if len(candles) < period + 1:
        if as_pct and candles:
            return 1.0  # Default 1% ATR
        return candles[-1]["close"] * 0.01 if candles else 0.0
    
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h = candles[idx]["high"]
        l = candles[idx]["low"]
        pc = candles[idx - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    
    if not trs:
        return 0.0
    
    atr = sum(trs) / len(trs)
    
    if as_pct:
        cur = candles[-1]["close"]
        return safe_div(atr, cur, 0.0) * 100
    
    return atr

def calc_atr_pct(candles: List[Dict], period: int = 14) -> float:
    """ATR sebagai % dari harga close terakhir"""
    return calc_atr(candles, period, as_pct=True)

def calc_atr_abs(candles: List[Dict], period: int = 14) -> float:
    """ATR dalam nilai absolut"""
    return calc_atr(candles, period, as_pct=False)

def _calc_ema_series(values: List[float], period: int) -> Optional[float]:
    """Calculate EMA dari series of values"""
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1)
    ema_val = sum(values[:period]) / period
    for v in values[period:]:
        ema_val = alpha * v + (1.0 - alpha) * ema_val
    return ema_val

def calc_ema_gap(candles: List[Dict], period: int = 20) -> float:
    """EMA gap = close_terakhir / EMA(period)"""
    if len(candles) < period + 1:
        return 0.0
    closes = [c["close"] for c in candles]
    ema_val = _calc_ema_series(closes, period)
    if ema_val is None or ema_val == 0:
        return 0.0
    return safe_div(candles[-1]["close"], ema_val, 0.0)

def calc_bbw(candles: List[Dict], period: int = 20) -> Tuple[float, float]:
    """BB Width dalam format desimal dan posisi dalam band"""
    if len(candles) < period:
        return 0.0, 0.5
    
    closes = [c["close"] for c in candles[-period:]]
    mean = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std = math.sqrt(variance)
    
    bb_upper = mean + 2 * std
    bb_lower = mean - 2 * std
    bbw = safe_div(bb_upper - bb_lower, mean, 0.0)
    
    last = candles[-1]["close"]
    bb_pct = 0.5
    if bb_upper > bb_lower:
        bb_pct = safe_div(last - bb_lower, bb_upper - bb_lower, 0.5)
    
    return bbw, bb_pct

def calc_bb_squeeze(candles: List[Dict], period: int = 20) -> bool:
    """BB Squeeze: band sangat sempit (< 4%)"""
    bbw, _ = calc_bbw(candles, period)
    return bbw < CONFIG["bb_squeeze_threshold"]

def calc_vwap(candles: List[Dict], lookback: int = 24) -> float:
    """VWAP rolling: rata-rata harga tertimbang volume"""
    n = min(lookback, len(candles))
    if n == 0:
        return candles[-1]["close"] if candles else 0.0
    
    recent = candles[-n:]
    cum_tv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in recent)
    cum_v = sum(c["volume"] for c in recent)
    
    return safe_div(cum_tv, cum_v, candles[-1]["close"] if candles else 0.0)

def get_rsi(candles: List[Dict], period: int = 14) -> float:
    """RSI Wilder (smoothed moving average)"""
    if len(candles) < period + 1:
        return 50.0
    
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    
    if len(gains) < period:
        return 50.0
    
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    
    if avg_l == 0:
        return 100.0
    
    rs = safe_div(avg_g, avg_l, 0.0)
    return 100 - (100 / (1 + rs))

def detect_bos_up(candles: List[Dict], lookback: int = 3) -> Tuple[bool, float]:
    """Break of Structure ke atas"""
    if len(candles) < lookback + 1:
        return False, 0.0
    
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level = max(prev_highs) if prev_highs else 0.0
    is_bos = candles[-1]["close"] > bos_level
    return is_bos, bos_level

def higher_low_detected(candles: List[Dict]) -> bool:
    """Higher Low: low candle terakhir > semua low dalam 5 candle sebelumnya"""
    if len(candles) < 6:
        return False
    lows = [c["low"] for c in candles[-6:]]
    return lows[-1] > min(lows[:-1])

def calc_accumulation_phase(candles: List[Dict]) -> Dict:
    """Deteksi fase akumulasi smart money sebelum pump"""
    if len(candles) < 36:
        return {
            "is_accumulating": False, "is_vol_compress": False,
            "vol_ratio_4h": 0.0, "price_range_pct": 0.0,
            "atr_contract": 1.0, "bbw_contracting": False,
            "phase_label": "Data kurang", "phase_score": 0
        }
    
    # Volume ratio 4h vs 24h
    vol_4h = sum(c["volume_usd"] for c in candles[-4:]) / 4
    vol_24h = sum(c["volume_usd"] for c in candles[-28:-4]) / 24 if len(candles) >= 28 else vol_4h
    vol_ratio_4h = safe_div(vol_4h, vol_24h, 1.0)
    
    # Price range 12h
    r12 = candles[-12:]
    hi12 = max(c["high"] for c in r12)
    lo12 = min(c["low"] for c in r12)
    mid12 = (hi12 + lo12) / 2
    price_range_pct = safe_div(hi12 - lo12, mid12, 99.0) * 100
    
    # ATR contraction
    atr_s = calc_atr_abs(candles[-CONFIG["accum_atr_lookback_short"]:])
    atr_l = calc_atr_abs(candles[-CONFIG["accum_atr_lookback_long"]:])
    atr_contract = safe_div(atr_s, atr_l, 1.0)
    
    # BBW contraction
    bbw_now, _ = calc_bbw(candles)
    bbw_12h, _ = calc_bbw(candles[:-12]) if len(candles) > 32 else (bbw_now, 0.0)
    bbw_contracting = bbw_now < bbw_12h * 0.85 if bbw_12h > 0 else False
    
    vol_rising = vol_ratio_4h >= CONFIG["accum_vol_ratio"]
    price_sideways = price_range_pct <= CONFIG["accum_price_range_max"]
    atr_shrinking = atr_contract <= CONFIG["accum_atr_contract_ratio"]
    
    is_vol_compress = atr_shrinking and bbw_contracting
    is_accumulating = vol_rising and price_sideways
    
    if is_accumulating and is_vol_compress:
        ps, pl = 2, "🏦 AKUMULASI + COMPRESSION — setup pra-pump kuat"
    elif is_accumulating:
        ps, pl = 1, "📦 AKUMULASI — volume naik, harga sideways"
    elif is_vol_compress:
        ps, pl = 1, "🗜️ VOLATILITY COMPRESSION — energi menumpuk"
    else:
        ps, pl = 0, "—"
    
    return {
        "is_accumulating": is_accumulating,
        "is_vol_compress": is_vol_compress,
        "vol_ratio_4h": round(vol_ratio_4h, 2),
        "price_range_pct": round(price_range_pct, 2),
        "atr_contract": round(atr_contract, 3),
        "bbw_contracting": bbw_contracting,
        "phase_label": pl,
        "phase_score": ps,
    }

def calc_htf_accumulation(candles_4h: List[Dict]) -> Dict:
    """HTF Accumulation Filter — deteksi akumulasi di timeframe 4H"""
    if len(candles_4h) < 16:
        return {
            "is_htf_accum": False,
            "atr_ratio": 1.0,
            "vol_ratio": 1.0,
            "range_pct": 99.0,
            "label": "Data 4H tidak cukup",
        }
    
    # ATR ratio
    atr_recent = calc_atr_abs(candles_4h[-4:])
    atr_avg = calc_atr_abs(candles_4h[-12:])
    atr_ratio = safe_div(atr_recent, atr_avg, 1.0)
    
    # Volume ratio
    vol_recent = sum(c["volume_usd"] for c in candles_4h[-2:]) / 2
    vol_avg = sum(c["volume_usd"] for c in candles_4h[-10:-2]) / 8 if len(candles_4h) >= 10 else vol_recent
    vol_ratio = safe_div(vol_recent, vol_avg, 1.0)
    
    # Price range 8 candle
    r8 = candles_4h[-8:]
    hi8 = max(c["high"] for c in r8)
    lo8 = min(c["low"] for c in r8)
    mid8 = (hi8 + lo8) / 2
    range_pct = safe_div(hi8 - lo8, mid8, 99.0) * 100
    
    atr_compressed = atr_ratio <= CONFIG["htf_atr_contract_ratio"]
    vol_building = vol_ratio >= CONFIG["htf_vol_ratio_min"]
    price_sideways = range_pct <= CONFIG["htf_range_max_pct"]
    
    is_htf_accum = atr_compressed and vol_building and price_sideways
    
    if is_htf_accum:
        label = f"🕯️ 4H HTF Akumulasi — ATR ratio {atr_ratio:.2f}, vol {vol_ratio:.1f}x, range {range_pct:.1f}%"
    elif atr_compressed and price_sideways:
        label = f"🕯️ 4H Konsolidasi (vol belum naik) — range {range_pct:.1f}%"
    else:
        label = "—"
    
    return {
        "is_htf_accum": is_htf_accum,
        "atr_ratio": round(atr_ratio, 3),
        "vol_ratio": round(vol_ratio, 2),
        "range_pct": round(range_pct, 2),
        "label": label,
    }

def detect_liquidity_sweep(candles: List[Dict], lookback: Optional[int] = None) -> Dict:
    """Liquidity Sweep Detection — identifikasi stop hunt sebelum reversal/pump"""
    if lookback is None:
        lookback = CONFIG["liq_sweep_lookback"]
    
    if len(candles) < lookback + 3:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "Data kurang"}
    
    reference_candles = candles[-(lookback + 3):-3]
    if not reference_candles:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "—"}
    
    # FIX: Better support calculation dengan clustering
    lows_sorted = sorted(c["low"] for c in reference_candles)
    # Gunakan median dari 3 low terendah, bukan average
    support_level = lows_sorted[2] if len(lows_sorted) >= 3 else (lows_sorted[0] if lows_sorted else 0.0)
    
    recent = candles[-3:]
    sweep_detected = False
    sweep_candle = None
    sweep_low_val = 0.0
    
    for candle in recent:
        candle_range = candle["high"] - candle["low"]
        if candle_range <= 0:
            continue
        
        wick_bottom = candle["open"] - candle["low"] if candle["close"] > candle["open"] else candle["close"] - candle["low"]
        wick_pct = safe_div(wick_bottom, candle_range, 0.0)
        
        went_below = candle["low"] < support_level
        closed_above = candle["close"] > support_level
        has_wick = wick_pct >= CONFIG["liq_sweep_wick_min_pct"]
        
        if went_below and closed_above and has_wick:
            sweep_detected = True
            sweep_candle = candle
            sweep_low_val = candle["low"]
            break
    
    if sweep_detected and sweep_candle is not None:
        depth_pct = safe_div(support_level - sweep_low_val, support_level, 0.0) * 100
        label = f"🎯 Liquidity Sweep — low ${sweep_low_val:.6g} menembus support ${support_level:.6g} ({depth_pct:.2f}%), close kembali di atas"
    else:
        label = "—"
    
    return {
        "is_sweep": sweep_detected,
        "sweep_low": round(sweep_low_val, 8),
        "support": round(support_level, 8),
        "label": label,
    }

def get_open_interest(symbol: str) -> float:
    """FIX: IndexError protection untuk openInterestList"""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d = data["data"]
            if isinstance(d, list) and d:
                d = d[0]
            
            if not isinstance(d, dict):
                return 0.0
            
            # FIX: Safe access ke openInterestList
            oi_list = d.get("openInterestList", [])
            if oi_list and isinstance(oi_list, list):
                oi = safe_float(oi_list[0].get("openInterest", 0))
            else:
                oi = safe_float(d.get("openInterest", d.get("holdingAmount", 0)))
            
            price = safe_float(d.get("indexPrice", d.get("lastPr", 0)))
            
            if 0 < oi < 1e9 and price > 0:
                return oi * price
            return oi
        except Exception as e:
            log.debug(f"Error parsing OI for {symbol}: {e}")
    return 0.0

_oi_snapshot: Dict[str, Dict] = {}

def get_oi_change(symbol: str) -> Dict:
    """Hitung % perubahan OI sejak snapshot terakhir"""
    global _oi_snapshot
    oi_now = get_open_interest(symbol)
    prev = _oi_snapshot.get(symbol)
    
    if prev is None or oi_now <= 0:
        if oi_now > 0:
            _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
        return {"oi_now": oi_now, "oi_prev": 0.0, "change_pct": 0.0, "is_new": True}
    
    oi_prev = prev["oi"]
    change_pct = safe_div(oi_now - oi_prev, oi_prev, 0.0) * 100
    _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
    
    return {
        "oi_now": round(oi_now, 2),
        "oi_prev": round(oi_prev, 2),
        "change_pct": round(change_pct, 2),
        "is_new": False,
    }

def calc_btc_correlation(coin_candles: List[Dict], btc_candles: List[Dict], lookback: int = 24) -> Dict:
    """Pearson correlation antara pct_change coin dan pct_change BTC"""
    if not coin_candles or not btc_candles or len(coin_candles) < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": 0, "risk_note": "Data tidak cukup"}
    
    n = min(lookback, len(coin_candles), len(btc_candles))
    c_c = coin_candles[-n:]
    c_b = btc_candles[-n:]
    
    def pct_changes(candles):
        changes = []
        for i in range(1, len(candles)):
            prev = candles[i-1]["close"]
            if prev > 0:
                changes.append(safe_div(candles[i]["close"] - prev, prev, 0.0))
        return changes
    
    cc = pct_changes(c_c)
    cb = pct_changes(c_b)
    
    mn = min(len(cc), len(cb))
    if mn < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": mn, "risk_note": "Data tidak cukup"}
    
    cc, cb = cc[-mn:], cb[-mn:]
    
    mc = sum(cc) / mn
    mb = sum(cb) / mn
    num = sum((x - mc) * (y - mb) for x, y in zip(cc, cb))
    sd_c = (sum((x - mc)**2 for x in cc)) ** 0.5
    sd_b = (sum((y - mb)**2 for y in cb)) ** 0.5
    
    if sd_c < 1e-10 or sd_b < 1e-10:
        corr = 0.0
    else:
        corr = safe_div(num, sd_c * sd_b, 0.0)
        corr = max(-1.0, min(1.0, corr))
    
    if corr >= 0.75:
        label, emoji, risk_note = "CORRELATED", "🔗", "⚠️ Ikuti BTC! Jika BTC dump → exit cepat"
    elif corr >= 0.40:
        label, emoji, risk_note = "MODERATE", "〰️", "🔶 Sebagian ikuti BTC — pantau jika BTC turun"
    else:
        label, emoji, risk_note = "INDEPENDENT", "🚀", "✅ Pergerakan independen — lebih tahan dump BTC"
    
    btc_chg = 0.0
    if len(c_b) >= 2 and c_b[0]["close"] > 0:
        btc_chg = safe_div(c_b[-1]["close"] - c_b[0]["close"], c_b[0]["close"], 0.0) * 100
    
    if btc_chg <= CONFIG["btc_bearish_threshold"]:
        btc_regime, btc_re, btc_rn = "BEARISH", "🔻", f"⚠️ BTC bearish ({btc_chg:+.1f}%/{mn}h) — risiko tinggi"
    elif btc_chg >= CONFIG["btc_bullish_threshold"]:
        btc_regime, btc_re, btc_rn = "BULLISH", "🟢", f"✅ BTC bullish ({btc_chg:+.1f}%/{mn}h) — kondisi favorable"
    else:
        btc_regime, btc_re, btc_rn = "SIDEWAYS", "⬜", f"BTC sideways ({btc_chg:+.1f}%/{mn}h) — altcoin bisa gerak independen"
    
    coin_chg = 0.0
    if len(c_c) >= 2 and c_c[0]["close"] > 0:
        coin_chg = safe_div(c_c[-1]["close"] - c_c[0]["close"], c_c[0]["close"], 0.0) * 100
    
    delta = coin_chg - btc_chg
    if delta >= CONFIG["outperform_min_delta"] and coin_chg > 0:
        op_label, op_emoji = "OUTPERFORM", "🚀"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% (+{delta:.1f}% lebih kuat)"
    elif delta <= -CONFIG["outperform_min_delta"]:
        op_label, op_emoji = "UNDERPERFORM", "📉"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% ({delta:.1f}% lebih lemah)"
    else:
        op_label, op_emoji = "IN-LINE", "〰️"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% — pergerakan sejalan"
    
    return {
        "correlation": round(corr, 3),
        "label": label,
        "emoji": emoji,
        "lookback": mn,
        "risk_note": risk_note,
        "btc_regime": btc_regime,
        "btc_regime_emoji": btc_re,
        "btc_regime_note": btc_rn,
        "btc_period_chg": round(btc_chg, 2),
        "coin_period_chg": round(coin_chg, 2),
        "outperform_label": op_label,
        "outperform_emoji": op_emoji,
        "outperform_note": op_note,
        "delta_vs_btc": round(delta, 2),
    }

def calc_uptrend_age(candles: List[Dict]) -> Dict:
    """Ukur sudah berapa candle (jam) harga berada dalam tren naik berturut-turut"""
    if len(candles) < 4:
        return {"age_hours": 0, "is_fresh": False, "is_late": False}
    
    streak = 0
    for i in range(len(candles) - 1, 0, -1):
        if candles[i]["close"] > candles[i - 1]["close"]:
            streak += 1
        else:
            break
    
    return {
        "age_hours": streak,
        "is_fresh": 1 <= streak <= 8,
        "is_late": streak > 12,
    }

def calc_support_resistance(candles: List[Dict], lookback: int = 48, n_levels: int = 3) -> Dict:
    """Level support & resistance dari pivot point"""
    if len(candles) < 10:
        return {"resistance": [], "support": [], "nearest_res": None, "nearest_sup": None}
    
    n = min(lookback, len(candles))
    recent = candles[-n:]
    price = candles[-1]["close"]
    
    pivots_high = []
    pivots_low = []
    for i in range(1, len(recent) - 1):
        h = recent[i]["high"]
        l = recent[i]["low"]
        if h > recent[i-1]["high"] and h > recent[i+1]["high"]:
            pivots_high.append(h)
        if l < recent[i-1]["low"] and l < recent[i+1]["low"]:
            pivots_low.append(l)
    
    def cluster_levels(levels: List[float], cluster_pct: float = 0.005) -> List[float]:
        if not levels:
            return []
        levels = sorted(levels)
        clusters = []
        current = [levels[0]]
        for lv in levels[1:]:
            if safe_div(lv - current[-1], current[-1], 1.0) < cluster_pct:
                current.append(lv)
            else:
                clusters.append((sum(current) / len(current), len(current)))
                current = [lv]
        clusters.append((sum(current) / len(current), len(current)))
        clusters.sort(key=lambda x: -x[1])
        return [round(lv, 8) for lv, _ in clusters[:n_levels]]
    
    resistance_all = cluster_levels(pivots_high)
    support_all = cluster_levels(pivots_low)
    
    resistance = sorted([r for r in resistance_all if r > price * 1.001])[:n_levels]
    support = sorted([s for s in support_all if s < price * 0.999], reverse=True)[:n_levels]
    
    def fmt_level(lv: float, ref_price: float) -> Dict:
        gap = safe_div(lv - ref_price, ref_price, 0.0) * 100
        return {"level": round(lv, 8), "gap_pct": round(gap, 1)}
    
    return {
        "resistance": [fmt_level(r, price) for r in resistance],
        "support": [fmt_level(s, price) for s in support],
        "nearest_res": fmt_level(resistance[0], price) if resistance else None,
        "nearest_sup": fmt_level(support[0], price) if support else None,
    }

def calc_volume_ratio(candles: List[Dict], lookback: int = 24) -> float:
    """Rasio volume candle terakhir vs rata-rata lookback candle sebelumnya"""
    if len(candles) < lookback + 1:
        return 0.0
    
    avg_vol = sum(c["volume_usd"] for c in candles[-(lookback + 1):-1]) / lookback
    if avg_vol <= 0:
        return 0.0
    
    return safe_div(candles[-1]["volume_usd"], avg_vol, 0.0)

def calc_volume_acceleration(candles: List[Dict]) -> float:
    """Volume acceleration: volume 1h terakhir vs rata-rata 3h sebelumnya"""
    if len(candles) < 4:
        return 0.0
    
    vol_1h = candles[-1]["volume_usd"]
    vol_3h = sum(c["volume_usd"] for c in candles[-4:-1]) / 3
    
    if vol_3h <= 0:
        return 0.0
    
    return safe_div(vol_1h - vol_3h, vol_3h, 0.0)

def check_volume_consistent(candles: List[Dict], lookback: int = 3, min_ratio: float = 1.5) -> bool:
    """Anti-manipulasi: volume tinggi harus konsisten di >= 2 candle terakhir"""
    if len(candles) < 24:
        return False
    
    avg_vol = sum(c["volume_usd"] for c in candles[-24:]) / 24
    if avg_vol <= 0:
        return False
    
    recent = candles[-lookback:]
    above_avg = sum(1 for c in recent if c["volume_usd"] > avg_vol * min_ratio)
    return above_avg >= max(1, lookback // 2)

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATION
# ══════════════════════════════════════════════════════════════════════════════
def calc_fib_targets(entry: float, candles: List[Dict]) -> Tuple[float, float]:
    """Target Fibonacci extension 1.272 dan 1.618"""
    lookback = min(48, len(candles))
    recent = candles[-lookback:]
    
    lows = [(i, c["low"]) for i, c in enumerate(recent)]
    highs = [(i, c["high"]) for i, c in enumerate(recent)]
    
    low_idx, swing_low = min(lows, key=lambda x: x[1]) if lows else (0, entry * 0.9)
    high_idx, swing_high = max(highs, key=lambda x: x[1]) if highs else (0, entry * 1.1)
    
    if low_idx >= high_idx or (swing_high - swing_low) <= 0:
        return round(entry * 1.08, 8), round(entry * 1.15, 8)
    
    fib_range = swing_high - swing_low
    t1 = swing_low + fib_range * 1.272
    t2 = swing_low + fib_range * 1.618
    
    if t1 <= entry:
        t1 = entry * 1.08
    if t2 <= t1:
        t2 = t1 * 1.08
    
    return round(t1, 8), round(t2, 8)

def find_swing_low_sl(candles: List[Dict], lookback: int = 12) -> Optional[float]:
    """Cari swing low terbaru dalam lookback candle sebagai dasar SL"""
    n = min(lookback, len(candles) - 1)
    if n < 2:
        return None
    
    recent_lows = [c["low"] for c in candles[-(n + 1):-1]]
    swing_low = min(recent_lows) if recent_lows else None
    return swing_low * (1.0 - CONFIG["sl_swing_buffer"]) if swing_low else None

def calc_entry(candles: List[Dict], bos_level: float, alert_level: str, vwap: float, 
               price_now: float, atr_abs_val: Optional[float] = None, 
               sr: Optional[Dict] = None) -> Dict:
    """Entry & SL & Target — dengan fix untuk variable initialization"""
    
    # FIX: Initialize ALL variables first
    if atr_abs_val is None:
        atr_abs_val = calc_atr_abs(candles)
    
    entry = price_now
    entry_reason = "market price"
    t1 = price_now * 1.08  # Default fallback
    t2 = price_now * 1.15
    t1_source = "default 8%"
    t2_source = "default 15%"
    
    # Calculate entry
    gap_to_vwap_pct = safe_div(price_now - vwap, vwap, 0.0) * 100
    
    if alert_level == "HIGH" and bos_level > 0 and bos_level < price_now * 1.05:
        entry = bos_level * 1.0005
        entry_reason = "BOS breakout"
    elif gap_to_vwap_pct <= 2.0:
        entry = max(vwap, price_now)
        entry_reason = "VWAP pullback"
    else:
        entry = price_now * 1.001
        entry_reason = "market price"
    
    # Pastikan entry tidak di bawah harga sekarang
    if entry < price_now:
        entry = price_now * 1.001
    
    # SL calculation
    sl_swing = find_swing_low_sl(candles, lookback=12)
    if sl_swing is None or sl_swing >= entry:
        sl_swing = entry - atr_abs_val * 2.0
    
    sl_floor = entry - atr_abs_val * 3.0
    sl_ceil = entry - atr_abs_val * 1.0
    
    sl = sl_swing
    sl = max(sl, sl_floor)
    sl = min(sl, sl_ceil)
    sl = max(sl, entry * 0.92)
    sl = min(sl, entry * 0.995)
    
    if sl >= entry:
        sl = entry * 0.98
    
    # Target calculation dengan proper initialization
    res_levels = []
    
    if sr and sr.get("resistance"):
        for rv in sr["resistance"]:
            if rv["level"] > entry * 1.005:
                res_levels.append(rv["level"])
    
    # Scan pivot dari 168 candle
    lookback_long = min(168, len(candles))
    recent_long = candles[-lookback_long:]
    pivot_highs = []
    
    for i in range(2, len(recent_long) - 2):
        h = recent_long[i]["high"]
        if (h > recent_long[i-1]["high"] and h > recent_long[i-2]["high"] and
                h > recent_long[i+1]["high"] and h > recent_long[i+2]["high"]):
            pivot_highs.append(h)
    
    if pivot_highs:
        pivot_highs = sorted(set(pivot_highs))
        clusters = []
        cur = [pivot_highs[0]]
        for ph in pivot_highs[1:]:
            if safe_div(ph - cur[-1], cur[-1], 1.0) < 0.015:
                cur.append(ph)
            else:
                clusters.append(sum(cur) / len(cur))
                cur = [ph]
        clusters.append(sum(cur) / len(cur))
        for c_lv in clusters:
            if c_lv > entry * 1.005 and c_lv not in res_levels:
                res_levels.append(c_lv)
    
    res_levels = sorted(set(res_levels))
    
    # Swing range untuk fallback
    swing_low_val = min(c["low"] for c in recent_long) if recent_long else entry * 0.9
    swing_high_val = max(c["high"] for c in recent_long) if recent_long else entry * 1.1
    swing_range = swing_high_val - swing_low_val
    price_pos_pct = safe_div(entry - swing_low_val, swing_range, 0.5)
    
    # ATR floor
    if price_pos_pct < 0.4:
        atr_mult_t1, atr_mult_t2 = 3.5, 6.5
    elif price_pos_pct < 0.6:
        atr_mult_t1, atr_mult_t2 = 2.5, 5.0
    else:
        atr_mult_t1, atr_mult_t2 = 1.5, 3.0
    
    atr_floor_t1 = entry + atr_abs_val * atr_mult_t1
    atr_floor_t2 = entry + atr_abs_val * atr_mult_t2
    
    # Tier 1: gunakan resistance pivot
    if res_levels:
        t1 = res_levels[0]
        t1_source = "R1 pivot"
        
        if len(res_levels) >= 2:
            t2 = res_levels[1]
            t2_source = "R2 pivot"
        else:
            t2 = t1 * 1.272
            t2_source = "R1 × 1.272"
        
        if t1 < atr_floor_t1:
            t1 = atr_floor_t1
            t1_source = f"ATR×{atr_mult_t1:.1f} (R1 terlalu dekat)"
        if t2 < atr_floor_t2:
            t2 = atr_floor_t2
            t2_source = f"ATR×{atr_mult_t2:.1f}"
    else:
        swing_valid = swing_range > atr_abs_val * 2 and swing_low_val < entry
        if swing_valid:
            t1 = entry + swing_range * 0.382
            t2 = entry + swing_range * 0.618
            t1_source = "Fib 38.2% swing"
            t2_source = "Fib 61.8% swing"
        else:
            t1 = atr_floor_t1
            t2 = atr_floor_t2
            t1_source = f"ATR×{atr_mult_t1:.1f}"
            t2_source = f"ATR×{atr_mult_t2:.1f}"
        
        if t1 < atr_floor_t1:
            t1 = atr_floor_t1
        if t2 < atr_floor_t2:
            t2 = atr_floor_t2
    
    if t2 <= t1:
        t2 = t1 * (1 + safe_div(atr_abs_val, entry, 0.0) * atr_mult_t1)
        t2_source = "T1 + ATR ext"
    
    t1 = max(t1, entry * 1.005)
    t2 = max(t2, t1 * 1.005)
    
    risk = entry - sl
    reward = t1 - entry
    rr_val = round(safe_div(reward, risk, 0.0), 1)
    sl_pct = round(safe_div(entry - sl, entry, 0.0) * 100, 2)
    
    return {
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "sl_pct": sl_pct,
        "t1": round(t1, 8),
        "t2": round(t2, 8),
        "rr": rr_val,
        "rr_str": f"{rr_val:.1f}",
        "vwap": round(vwap, 8),
        "bos_level": round(bos_level, 8),
        "alert_level": alert_level,
        "gain_t1_pct": round(safe_div(t1 - entry, entry, 0.0) * 100, 1),
        "gain_t2_pct": round(safe_div(t2 - entry, entry, 0.0) * 100, 1),
        "atr_abs": round(atr_abs_val, 8),
        "sl_method": entry_reason,
        "used_resistance": len(res_levels) > 0,
        "n_res_levels": len(res_levels),
        "t1_source": t1_source,
        "t2_source": t2_source,
        "atr_pct_abs": round(safe_div(atr_abs_val, entry, 0.0) * 100, 2),
        "swing_range_pct": round(safe_div(swing_range, entry, 0.0) * 100, 1),
    }

def detect_energy_buildup(candles_1h: List[Dict], oi_data: Dict) -> Dict:
    """Energy Build-Up Detector — OI Build + Volume Build + Price Stuck"""
    if len(candles_1h) < 24:
        return {
            "is_buildup": False, "is_strong": False,
            "oi_change": 0.0, "vol_ratio": 0.0, "range_pct": 0.0,
            "label": "Data tidak cukup",
        }
    
    # Kondisi 1: OI naik
    oi_change = oi_data.get("change_pct", 0.0)
    oi_rising = (not oi_data.get("is_new", True)) and oi_change >= CONFIG["energy_oi_change_min"]
    
    # Kondisi 2: Volume naik
    vol_1h = candles_1h[-1]["volume_usd"]
    avg_vol = sum(c["volume_usd"] for c in candles_1h[-24:-1]) / 23 if len(candles_1h) >= 24 else vol_1h
    vol_ratio = safe_div(vol_1h, avg_vol, 1.0)
    vol_rising = vol_ratio >= CONFIG["energy_vol_ratio_min"]
    
    # Kondisi 3: Harga tidak bergerak
    recent_3h = candles_1h[-3:]
    hi3 = max(c["high"] for c in recent_3h)
    lo3 = min(c["low"] for c in recent_3h)
    mid3 = (hi3 + lo3) / 2
    range_pct = safe_div(hi3 - lo3, mid3, 99.0) * 100
    price_stuck = range_pct <= CONFIG["energy_range_max_pct"]
    
    is_buildup = oi_rising and vol_rising and price_stuck
    is_strong = False
    
    if is_buildup:
        label = f"⚡ ENERGY BUILD-UP — OI +{oi_change:.1f}%, vol {vol_ratio:.1f}x, range {range_pct:.1f}%"
    else:
        conditions_met = sum([oi_rising, vol_rising, price_stuck])
        label = f"— ({conditions_met}/3 kondisi: OI={oi_rising}, vol={vol_rising}, stuck={price_stuck})"
    
    return {
        "is_buildup": is_buildup,
        "is_strong": is_strong,
        "oi_change": round(oi_change, 2),
        "vol_ratio": round(vol_ratio, 2),
        "range_pct": round(range_pct, 2),
        "oi_rising": oi_rising,
        "vol_rising": vol_rising,
        "price_stuck": price_stuck,
        "label": label,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol: str, ticker: Dict) -> Optional[Dict]:
    """Main scoring function dengan semua fix"""
    
    # Fetch candles
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h = get_candles(symbol, "4h", CONFIG["candle_4h"])
    
    if len(c1h) < 48:
        log.info(f"  {symbol}: Candle 1h tidak cukup ({len(c1h)} < 48)")
        return None
    
    # Parse ticker data
    try:
        vol_24h = safe_float(ticker.get("quoteVolume", 0))
        chg_24h = safe_float(ticker.get("change24h", 0)) * 100
        price_now = safe_float(ticker.get("lastPr", 0)) or c1h[-1]["close"]
    except Exception as e:
        log.debug(f"{symbol}: Ticker parse error — {e}")
        return None
    
    if vol_24h <= 0 or price_now <= 0:
        return None
    
    # Gate 0: Open Interest minimum
    oi_data = get_oi_change(symbol)
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]:
        log.info(f"  {symbol}: OI terlalu kecil ${oi_data['oi_now']:,.0f} < ${CONFIG['min_oi_usd']:,}")
        return None
    
    # Gate 1: Funding
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)
    fstats = get_funding_stats(symbol)
    
    if fstats is None:
        fstats = {
            "avg": funding, "cumulative": funding, "neg_pct": 0.0,
            "streak": 0, "basis": funding * 100, "current": funding,
            "sample_count": 1,
        }
        log.info(f"  {symbol}: Funding snapshot baru (1 data)")
    
    # Gate 2: VWAP dengan toleransi
    vwap = calc_vwap(c1h, lookback=24)
    vwap_gate_level = vwap * CONFIG["vwap_gate_tolerance"]
    if price_now < vwap_gate_level:
        log.info(f"  {symbol}: Harga terlalu jauh di bawah VWAP")
        return None
    
    # Indikator teknikal
    ema_gap = calc_ema_gap(c1h, period=20)
    bbw, bb_pct = calc_bbw(c1h)
    bb_squeeze = calc_bb_squeeze(c1h)
    atr_pct = calc_atr_pct(c1h)
    rsi = get_rsi(c1h[-48:])
    bos_up, bos_level = detect_bos_up(c1h)
    higher_low = higher_low_detected(c1h)
    vol_ratio = calc_volume_ratio(c1h)
    vol_accel = calc_volume_acceleration(c1h)
    vol_consistent = check_volume_consistent(c1h)
    uptrend = calc_uptrend_age(c1h)
    sr = calc_support_resistance(c1h)
    btc_candles = get_btc_candles_cached(48)
    btc_corr = calc_btc_correlation(c1h, btc_candles, lookback=24)
    accum = calc_accumulation_phase(c1h)
    htf_accum = calc_htf_accumulation(c4h)
    liq_sweep = detect_liquidity_sweep(c1h)
    energy = detect_energy_buildup(c1h, oi_data)
    
    if energy["is_buildup"] and fstats and fstats.get("current", 1) <= 0:
        energy["is_strong"] = True
        energy["label"] = energy["label"] + " 🔥 + funding negatif"
    
    atr_abs_val = calc_atr_abs(c1h)
    
    # Gate 3: Uptrend tidak terlalu tua
    if uptrend["is_late"]:
        log.info(f"  {symbol}: Uptrend sudah {uptrend['age_hours']}h — terlalu tua")
        return None
    
    # Gate 4: RSI tidak overbought
    if rsi >= CONFIG["gate_rsi_max"]:
        log.info(f"  {symbol}: RSI {rsi:.1f} ≥ {CONFIG['gate_rsi_max']} — overbought")
        return None
    
    # Gate 5: BB Position tidak di puncak
    if bb_pct >= CONFIG["gate_bb_pos_max"]:
        log.info(f"  {symbol}: BB Pos {bb_pct*100:.0f}% ≥ {CONFIG['gate_bb_pos_max']*100:.0f}%")
        return None
    
    # Rate above VWAP
    above_vwap_rate = 0.0
    if len(c1h) >= 6:
        recent_6 = c1h[-6:]
        above = sum(1 for c in recent_6 if c["close"] > vwap)
        above_vwap_rate = safe_div(above, len(recent_6), 0.0)
    
    # Price change 1h
    price_chg = 0.0
    if len(c1h) >= 2 and c1h[-2]["close"] > 0:
        price_chg = safe_div(c1h[-1]["close"] - c1h[-2]["close"], c1h[-2]["close"], 0.0) * 100
    
    # Scoring
    score = 0
    signals = []
    
    # 1. EMA Gap
    if ema_gap >= CONFIG["ema_gap_threshold"]:
        score += CONFIG["score_ema_gap"]
        signals.append(f"EMA Gap {ema_gap:.3f} ≥ 1.0")
    
    # 2. ATR
    if atr_pct >= 1.5:
        score += CONFIG["score_atr_15"]
        signals.append(f"ATR {atr_pct:.2f}% ≥ 1.5%")
    elif atr_pct >= 1.0:
        score += CONFIG["score_atr_10"]
        signals.append(f"ATR {atr_pct:.2f}% ≥ 1.0%")
    
    # 3. BB Width
    if bbw >= CONFIG["bbw_threshold_high"]:
        score += CONFIG["score_bbw_10"]
        signals.append(f"BB Width {bbw*100:.2f}% ≥ 10%")
    elif bbw >= CONFIG["bbw_threshold_mid"]:
        score += CONFIG["score_bbw_6"]
        signals.append(f"BB Width {bbw*100:.2f}% ≥ 6%")
    
    # 4. BOS Up + Above VWAP
    if bos_up and above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        score += CONFIG["score_above_vwap_bos"]
        signals.append(f"BOS Up + Above VWAP {above_vwap_rate*100:.0f}%")
    elif bos_up:
        score += CONFIG["score_bos_up"]
        signals.append("Break of Structure ke atas")
    
    # 5. Higher Low
    if higher_low:
        score += CONFIG["score_higher_low"]
        signals.append("Higher Low terdeteksi")
    
    # 6. BB Squeeze
    if bb_squeeze:
        score += CONFIG["score_bb_squeeze"]
        signals.append("BB Squeeze aktif")
    
    # 7. RSI
    if rsi >= 65:
        score += CONFIG["score_rsi_65"]
        signals.append(f"RSI {rsi:.1f} ≥ 65")
    elif rsi >= 55:
        score += CONFIG["score_rsi_55"]
        signals.append(f"RSI {rsi:.1f} ≥ 55")
    
    # 8. Funding scoring
    f_avg = fstats["avg"]
    f_cur = fstats["current"]
    
    if f_avg <= CONFIG["funding_bonus_avg"]:
        score += CONFIG["score_funding_cumul"]
        signals.append(f"⭐ Funding avg {f_avg:.6f} — sangat negatif")
    elif fstats["cumulative"] <= CONFIG["funding_bonus_cumul"]:
        score += 1
        signals.append(f"Funding kumulatif {fstats['cumulative']:.5f}")
    elif f_avg < 0:
        signals.append(f"Funding avg {f_avg:.6f} — negatif")
    elif f_avg >= CONFIG["funding_penalty_avg"]:
        score -= 2
        signals.append(f"⚠️ Funding avg {f_avg:.6f} — sangat positif")
    else:
        signals.append(f"Funding avg {f_avg:.6f} — netral")
    
    if fstats["neg_pct"] >= 70 and fstats["sample_count"] >= 3:
        score += CONFIG["score_funding_neg_pct"]
        signals.append(f"Funding negatif {fstats['neg_pct']:.0f}%")
    
    if fstats["streak"] >= CONFIG["funding_streak_min"]:
        score += CONFIG["score_funding_streak"]
        signals.append(f"Funding streak negatif {fstats['streak']}x")
    
    # 9. Volume
    if vol_ratio > CONFIG["vol_ratio_threshold"]:
        if vol_consistent:
            score += CONFIG["score_vol_ratio"]
            signals.append(f"Volume {vol_ratio:.1f}x rata-rata (konsisten)")
        else:
            signals.append(f"⚠️ Volume spike {vol_ratio:.1f}x tapi tidak konsisten")
    
    if vol_accel > CONFIG["vol_accel_threshold"] and vol_consistent:
        score += CONFIG["score_vol_accel"]
        signals.append(f"Volume acceleration {vol_accel*100:.0f}%")
    
    # 10. Price change
    if price_chg >= 0.5:
        score += CONFIG["score_price_chg"]
        signals.append(f"Price +{price_chg:.2f}% dalam 1h")
    
    # 11. Accumulation + Compression
    if accum["is_accumulating"] and accum["is_vol_compress"]:
        score += CONFIG["score_accumulation"] + CONFIG["score_vol_compression"]
        signals.append(f"🏦 AKUMULASI + COMPRESSION")
    elif accum["is_accumulating"]:
        score += CONFIG["score_accumulation"]
        signals.append(f"📦 Smart Money Accumulation")
    elif accum["is_vol_compress"]:
        score += CONFIG["score_vol_compression"]
        signals.append("🗜️ Volatility Compression")
    
    # 12. HTF Accumulation
    if htf_accum["is_htf_accum"]:
        score += CONFIG["score_htf_accumulation"]
        signals.append(htf_accum["label"])
    
    # 13. Liquidity Sweep
    if liq_sweep["is_sweep"]:
        score += CONFIG["score_liquidity_sweep"]
        signals.append(liq_sweep["label"])
    
    # 14. Energy Build-Up
    if energy["is_buildup"]:
        score += CONFIG["score_energy_buildup"]
        signals.append(energy["label"])
        if energy["is_strong"]:
            score += 2
            signals.append("⭐ Energy Build-Up + Funding Negatif")
    
    # 15. OI Expansion
    if not oi_data["is_new"] and oi_data["oi_now"] > 0:
        chg = oi_data["change_pct"]
        if chg >= CONFIG["oi_strong_pct"]:
            score += CONFIG["score_oi_strong"]
            signals.append(f"📈 OI Expansion KUAT +{chg:.1f}%")
        elif chg >= CONFIG["oi_change_min_pct"]:
            score += CONFIG["score_oi_expansion"]
            signals.append(f"📊 OI Expansion +{chg:.1f}%")
    elif oi_data["is_new"] and oi_data["oi_now"] > 0:
        signals.append(f"📊 OI baseline: ${oi_data['oi_now']/1e6:.2f}M")
    
    # 16. BTC Outperformance
    if btc_corr.get("outperform_label") == "OUTPERFORM":
        score += CONFIG["score_outperform"]
        signals.append(f"🚀 OUTPERFORM BTC")
    
    # Alert Level
    alert_level = "MEDIUM"
    pump_type = "VWAP Momentum"
    
    # FIX: Simplified alert level logic (kondisi 1 & 2 overlap)
    if bos_up and ema_gap >= CONFIG["ema_gap_threshold"]:
        alert_level = "HIGH"
        pump_type = "Momentum Breakout"
    elif liq_sweep["is_sweep"] and htf_accum["is_htf_accum"]:
        alert_level = "HIGH"
        pump_type = "Liquidity Sweep + HTF Accumulation"
    elif energy["is_buildup"] and energy["is_strong"]:
        alert_level = "HIGH"
        pump_type = "Energy Build-Up + Short Squeeze"
    elif energy["is_buildup"]:
        alert_level = "MEDIUM"
        pump_type = "Energy Build-Up"
    elif liq_sweep["is_sweep"]:
        alert_level = "MEDIUM"
        pump_type = "Liquidity Sweep Reversal"
    elif htf_accum["is_htf_accum"]:
        alert_level = "MEDIUM"
        pump_type = "HTF Accumulation Build-Up"
    elif above_vwap_rate >= CONFIG["above_vwap_rate_min"] and fstats["cumulative"] <= -0.05 and higher_low:
        alert_level = "MEDIUM"
        pump_type = "Short Squeeze Setup"
    elif above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        alert_level = "MEDIUM"
        pump_type = "VWAP Momentum"
    
    # Entry & Target
    entry_data = calc_entry(c1h, bos_level, alert_level, vwap, price_now, atr_abs_val=atr_abs_val, sr=sr)
    
    if score >= CONFIG["min_score_alert"]:
        return {
            "symbol": symbol,
            "score": score,
            "signals": signals,
            "entry": entry_data,
            "price": price_now,
            "chg_24h": chg_24h,
            "vol_24h": vol_24h,
            "rsi": round(rsi, 1),
            "ema_gap": round(ema_gap, 3),
            "bbw": round(bbw * 100, 2),
            "bb_pct": round(bb_pct, 2),
            "bb_squeeze": bb_squeeze,
            "atr_pct": round(atr_pct, 2),
            "above_vwap_rate": round(above_vwap_rate * 100, 1),
            "vwap": round(vwap, 8),
            "bos_up": bos_up,
            "bos_level": round(bos_level, 8),
            "higher_low": higher_low,
            "funding_stats": fstats,
            "pump_type": pump_type,
            "alert_level": alert_level,
            "vol_ratio": round(vol_ratio, 2),
            "vol_accel": round(vol_accel * 100, 1),
            "vol_consistent": vol_consistent,
            "uptrend_age": uptrend["age_hours"],
            "sr": sr,
            "btc_corr": btc_corr,
            "accum": accum,
            "htf_accum": htf_accum,
            "liq_sweep": liq_sweep,
            "energy": energy,
            "oi_data": oi_data,
        }
    else:
        log.info(f"  {symbol}: Skor {score} < {CONFIG['min_score_alert']}")
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER — Optimized dengan join
# ══════════════════════════════════════════════════════════════════════════════
def _fmt_price(p: float) -> str:
    """Format harga coin secara otomatis sesuai magnitudo"""
    if p == 0:
        return "0"
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.5f}"
    return f"{p:.8f}"

def build_alert(r: Dict, rank: Optional[int] = None) -> str:
    """Pesan Telegram dengan optimized string building"""
    level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
    e = r["entry"]
    bc = r.get("btc_corr", {})
    sr = r.get("sr", {})
    en = r.get("energy", {})
    htf = r.get("htf_accum", {})
    ls = r.get("liq_sweep", {})
    oi = r.get("oi_data", {})
    
    p = r["price"]
    entry = e["entry"]
    sl = e["sl"]
    t1 = e["t1"]
    t2 = e["t2"]
    
    # OPT: Gunakan list untuk string building
    msg_parts = []
    
    # Header
    msg_parts.append(f"{level_icon} <b>{r['symbol']} — {r['alert_level']}</b>  #{rank}")
    msg_parts.append(f"<b>Score:</b> {r['score']}  |  {r['pump_type']}")
    msg_parts.append(f"<b>Waktu scan:</b> {utc_now()}")
    msg_parts.append("━━━━━━━━━━━━━━━━━━━━")
    
    # Harga & VWAP
    msg_parts.append(f"<b>Harga :</b> <code>{_fmt_price(p)}</code>  ({r['chg_24h']:+.1f}% 24h)")
    gap_vwap = safe_div(p - r['vwap'], r['vwap'], 0.0) * 100
    msg_parts.append(f"<b>VWAP  :</b> <code>{_fmt_price(r['vwap'])}</code>  ({gap_vwap:+.1f}% vs harga)")
    
    # Entry / SL / TP
    msg_parts.append("━━━━━━━━━━━━━━━━━━━━")
    msg_parts.append(f"📍 <b>Entry :</b> <code>{_fmt_price(entry)}</code>  [{e['sl_method']}]")
    msg_parts.append(f"🛑 <b>SL    :</b> <code>{_fmt_price(sl)}</code>  (-{e['sl_pct']:.2f}%)")
    
    t1_tag = e.get("t1_source", "resistance" if e.get("used_resistance") else "proj")
    msg_parts.append(f"🎯 <b>T1    :</b> <code>{_fmt_price(t1)}</code>  (+{e['gain_t1_pct']:.1f}%)  [{t1_tag}]")
    msg_parts.append(f"🎯 <b>T2    :</b> <code>{_fmt_price(t2)}</code>  (+{e['gain_t2_pct']:.1f}%)")
    msg_parts.append(f"⚖️ <b>RR    :</b> {e['rr_str']}x  |  ATR: {r['atr_pct']:.2f}%")
    
    # BTC
    msg_parts.append("━━━━━━━━━━━━━━━━━━━━")
    if bc.get("correlation") is not None:
        msg_parts.append(f"{bc.get('btc_regime_emoji','❓')} <b>BTC:</b> {bc.get('btc_regime','?')} ({bc.get('btc_period_chg',0):+.1f}%/{bc['lookback']}h)")
        op_emoji = bc.get("outperform_emoji", "〰️")
        coin_chg = bc.get("coin_period_chg", 0)
        btc_chg = bc.get("btc_period_chg", 0)
        msg_parts.append(f"{op_emoji} <b>vs BTC:</b> {bc.get('outperform_label','?')} | Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}%")
    else:
        msg_parts.append("📊 <b>vs BTC:</b> data tidak tersedia")
    
    # Support & Resistance
    msg_parts.append("━━━━━━━━━━━━━━━━━━━━")
    res_list = sr.get("resistance", [])
    sup_list = sr.get("support", [])
    if res_list:
        for rv in res_list[:2]:
            msg_parts.append(f"🔴 R <code>{_fmt_price(rv['level'])}</code>  ({rv['gap_pct']:+.1f}%)")
    msg_parts.append(f"▶ NOW <code>{_fmt_price(p)}</code>")
    if sup_list:
        for sv in sup_list[:2]:
            msg_parts.append(f"🟢 S <code>{_fmt_price(sv['level'])}</code>  ({sv['gap_pct']:+.1f}%)")
    
    # OI
    if oi.get("oi_now", 0) > 0:
        ov = oi["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs = f"({oi['change_pct']:+.1f}%)" if not oi.get("is_new") else "(baseline)"
        msg_parts.append(f"📈 <b>OI:</b> {os_str} {cs}")
    
    # Sinyal
    msg_parts.append("━━━━━━━━━━━━━━━━━━━━")
    msg_parts.append("<b>Sinyal:</b>")
    
    priority_signals = []
    for s in r["signals"]:
        if any(kw in s for kw in ["AKUMULASI", "BUILD-UP", "Sweep", "BOS", "VWAP",
                                     "Funding", "squeeze", "HTF", "Higher Low", "Compression",
                                     "OI", "Volume", "ATR", "BB Width", "EMA Gap"]):
            priority_signals.append(s)
        if len(priority_signals) >= 6:
            break
    
    for s in priority_signals:
        s_short = s[:80] + "…" if len(s) > 80 else s
        msg_parts.append(f"• {s_short}")
    
    msg_parts.append(f"\n<i>v{VERSION} | ⚠️ Bukan financial advice</i>")
    
    return "\n".join(msg_parts)

def build_summary(results: List[Dict]) -> str:
    """Summary message dengan list join"""
    msg_parts = [f"📋 <b>TOP CANDIDATES v{VERSION} — {utc_now()}</b>", "━" * 28]
    
    for i, r in enumerate(results, 1):
        vol_str = f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K"
        level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
        htf_tag = " 🕯️" if r.get("htf_accum", {}).get("is_htf_accum") else ""
        sweep_tag = " 🎯" if r.get("liq_sweep", {}).get("is_sweep") else ""
        energy_tag = " ⚡" if r.get("energy", {}).get("is_buildup") else ""
        
        msg_parts.append(f"{i}. {level_icon} <b>{r['symbol']}</b> [Score:{r['score']} | {r['alert_level']}{htf_tag}{sweep_tag}{energy_tag}]")
        msg_parts.append(f"   {vol_str} | RSI:{r['rsi']} | EMAGap:{r['ema_gap']} | T1:+{r['entry']['gain_t1_pct']}%")
    
    return "\n".join(msg_parts)

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers: Dict[str, Dict]) -> List[Tuple[str, Dict]]:
    all_candidates = []
    not_found = []
    filtered_stats = defaultdict(int)
    
    log.info("=" * 70)
    log.info("🔍 SCANNING MODE: WHITELIST (top OI & volume pairs)")
    log.info("=" * 70)
    
    for sym in WHITELIST_SYMBOLS:
        if any(kw in sym for kw in EXCLUDED_KEYWORDS):
            filtered_stats["excluded_keyword"] += 1
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
        
        ticker = tickers[sym]
        try:
            vol = safe_float(ticker.get("quoteVolume", 0))
            chg = safe_float(ticker.get("change24h", 0)) * 100
            price = safe_float(ticker.get("lastPr", 0))
        except Exception:
            filtered_stats["parse_error"] += 1
            continue
        
        if vol < CONFIG["pre_filter_vol"]:
            filtered_stats["vol_too_low"] += 1
            continue
        
        if vol > CONFIG["max_vol_24h"]:
            filtered_stats["vol_too_high"] += 1
            continue
        
        if chg > CONFIG["gate_chg_24h_max"]:
            filtered_stats["change_too_high"] += 1
            continue
        
        if chg < CONFIG["gate_chg_24h_min"]:
            filtered_stats["dump_too_deep"] += 1
            continue
        
        if price <= 0:
            filtered_stats["invalid_price"] += 1
            continue
        
        all_candidates.append((sym, ticker))
    
    total = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    filtered = total - will_scan - len(not_found)
    
    log.info(f"\n📊 SCAN SUMMARY:")
    log.info(f"   Whitelist total  : {total} coins")
    log.info(f"   ✅ Will scan     : {will_scan} ({safe_div(will_scan, total, 0.0)*100:.1f}%)")
    log.info(f"   ❌ Filtered      : {filtered}")
    log.info(f"   ⚠️  Not in Bitget : {len(not_found)}")
    log.info(f"\n📋 Filter breakdown:")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"   {k:20s}: {v}")
    if not_found:
        sample = ", ".join(not_found[:10])
        log.info(f"\n   Missing sample   : {sample}{' ...' if len(not_found) > 10 else ''}")
    
    est_secs = will_scan * CONFIG["sleep_coins"]
    log.info(f"\n⏱️  Est. scan time: {est_secs:.0f}s (~{est_secs/60:.1f} min)")
    log.info("=" * 70 + "\n")
    
    return all_candidates

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    log.info(f"=== PRE-PUMP SCANNER v{VERSION} — {utc_now()} ===")
    
    load_funding_snapshots()
    log.info(f"Funding snapshots loaded: {len(_funding_snapshots)} coins")
    
    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    
    log.info(f"Total ticker dari Bitget: {len(tickers)}")
    
    candidates = build_candidate_list(tickers)
    results = []
    
    for i, (sym, t) in enumerate(candidates):
        try:
            vol = safe_float(t.get("quoteVolume", 0))
        except Exception:
            vol = 0.0
        
        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum, skip")
            continue
        
        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")
        
        try:
            res = master_score(sym, t)
            if res:
                log.info(f"  ✅ Score={res['score']} | {res['alert_level']} | {res['pump_type']} | T1:+{res['entry']['gain_t1_pct']}%")
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}")
        
        time.sleep(CONFIG["sleep_coins"])
    
    save_all_funding_snapshots()
    log.info("Funding snapshots disimpan ke disk.")
    
    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"\nLolos threshold: {len(results)} coin")
    
    if not results:
        log.info("Tidak ada sinyal yang memenuhi syarat saat ini.")
        return
    
    top = results[:CONFIG["max_alerts_per_run"]]
    
    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)
    
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"✅ Alert #{rank}: {r['symbol']} Score={r['score']} Level={r['alert_level']}")
        time.sleep(2)
    
    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info(f"║  PRE-PUMP SCANNER v{VERSION:<15s}               ║")
    log.info("║  Focus: Bug Fixes, Robustness, Performance           ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)
    
    run_scan()
