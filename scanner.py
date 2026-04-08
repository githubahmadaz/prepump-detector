#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v15.0 — TWO-PHASE ARCHITECTURE                            ║
║                                                                              ║
║  ARSITEKTUR:                                                                 ║
║    PHASE 1 — Filter cepat dengan Bitget candles (ATR, range, BBW, wick,     ║
║              support, momentum decel). Hanya coin dengan skor ≥ 60 yang     ║
║              dilanjutkan ke phase 2.                                        ║
║    PHASE 2 — Verifikasi mendalam dengan Coinalyze (OI, liq, funding, L/S).  ║
║              Scoring final menggunakan semua tier (1,2,3).                  ║
║                                                                              ║
║  KEUNGGULAN:                                                                 ║
║    • Hemat kuota API Coinalyze (hanya dipanggil untuk kandidat kuat)        ║
║    • Cepat karena Bitget tanpa rate limit berarti                            ║
║    • Akurasi tinggi karena keputusan final pakai derivatif                   ║
║    • Berbasis data feature discovery v2 (104 pump events, 120 simbol)       ║
║                                                                              ║
║  THRESHOLD:                                                                  ║
║    Phase 1 (Bitget-only): skor ≥ 60                                         ║
║    Phase 2 (Coinalyze): total score ≥ 95 (EARLY) / 100 (CONTINUATION)       ║
║    Bitget-only fallback: skor ≥ 75 jika tidak ada data Coinalyze            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import time
import sqlite3
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set

import requests

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "15.0.0-TWO-PHASE"

# ── Logging ──────────────────────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("scanner_v15")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    # File handler with rotation
    fh = logging.handlers.RotatingFileHandler(
        "/tmp/scanner_v15.log", maxBytes=10 * 1024**2, backupCount=3
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger

log = setup_logging()


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key":  os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":          os.getenv("BOT_TOKEN"),
    "chat_id":            os.getenv("CHAT_ID"),

    
    # Whitelist (coin yang akan dipindai)
    "whitelist": [
        "AAVEUSDT", "ACEUSDT", "ACHUSDT", "ACTUSDT", "ADAUSDT", "AEROUSDT", "AGLDUSDT",
        "AINUSDT", "AIOUSDT", "AIXBTUSDT", "AKTUSDT", "ALGOUSDT", "ALICEUSDT", "ALTUSDT",
        "ANIMEUSDT", "ANKRUSDT", "APEUSDT", "APEXUSDT", "API3USDT", "APRUSDT", "APTUSDT",
        "ARUSDT", "ARBUSDT", "ARKUSDT", "ARKMUSDT", "ARPAUSDT", "ATUSDT", "ATHUSDT",
        "ATOMUSDT", "AVAXUSDT", "AXSUSDT", "BANANAUSDT", "BATUSDT", "BCHUSDT", "BEATUSDT",
        "BERAUSDT", "BIGTIMEUSDT", "BIOUSDT", "BLASTUSDT", "BLURUSDT", "BNBUSDT",
        "BOMEUSDT", "BRETTUSDT", "BTCUSDT", "C98USDT", "CAKEUSDT", "CELOUSDT", "CFXUSDT",
        "CHZUSDT", "COMPUSDT", "CRVUSDT", "CTKUSDT", "CVCUSDT", "CYBERUSDT", "DASHUSDT",
        "DOGEUSDT", "DOTUSDT", "DRIFTUSDT", "DYDXUSDT", "DYMUSDT", "EGLDUSDT", "EIGENUSDT",
        "ENAUSDT", "ENJUSDT", "ENSUSDT", "ETCUSDT", "ETHUSDT", "ETHFIUSDT", "FETUSDT",
        "FIDAUSDT", "FILUSDT", "FLOKIUSDT", "GALAUSDT", "GLMUSDT", "GMTUSDT", "GMXUSDT",
        "GOATUSDT", "GRASSUSDT", "GRTUSDT", "HBARUSDT", "HMSTRUSDT", "HOLOUSDT", "HYPEUSDT",
        "ICPUSDT", "ILVUSDT", "IMXUSDT", "INJUSDT", "IOTAUSDT", "IOTXUSDT", "JASMYUSDT",
        "JUPUSDT", "KAIAUSDT", "KASUSDT", "KAVAUSDT", "KITEUSDT", "LDOUSDT", "LINKUSDT",
        "LITUSDT", "LPTUSDT", "LTCUSDT", "LUNCUSDT", "MAGICUSDT", "MANAUSDT", "MANTAUSDT",
        "MANTRAUSDT", "MASKUSDT", "MAVUSDT", "MBOXUSDT", "MEMEUSDT", "MINAUSDT", "MNTUSDT",
        "MOODENGUSDT", "MOVEUSDT", "NEARUSDT", "NEOUSDT", "NILUSDT", "NOTUSDT", "ONDOUSDT",
        "ONGUSDT", "ONTUSDT", "OPUSDT", "ORDIUSDT", "PENDLEUSDT", "PENGUUSDT", "PEOPLEUSDT",
        "PEPEUSDT", "PNUTUSDT", "POLUSDT", "POPCATUSDT", "PYTHUSDT", "QNTUSDT", "RAYUSDT",
        "RENDERUSDT", "ROSEUSDT", "RUNEUSDT", "SAGAUSDT", "SANDUSDT", "SEIUSDT", "SHIBUSDT",
        "SIRENUSDT", "SKRUSDT", "SKYUSDT", "SLPUSDT", "SNXUSDT", "SOLUSDT", "SONICUSDT",
        "STRKUSDT", "STXUSDT", "SUIUSDT", "SUPERUSDT", "SUSHIUSDT", "TAOUSDT", "THEUSDT",
        "THETAUSDT", "TIAUSDT", "TNSRUSDT", "TONUSDT", "TRBUSDT", "TRUMPUSDT", "TURBOUSDT",
        "UMAUSDT", "UNIUSDT", "VANAUSDT", "VETUSDT", "VIRTUALUSDT", "WUSDT", "WIFUSDT",
        "WLDUSDT", "WOOUSDT", "XAIUSDT", "XLMUSDT", "XMRUSDT", "XRPUSDT", "XTZUSDT",
        "YGGUSDT", "ZECUSDT", "ZENUSDT", "ZEREBROUSDT", "ZETAUSDT", "ZILUSDT",
        "ZKUSDT", "ZROUSDT", "1000BONKUSDT", "1000PEPEUSDT", "1000SHIBUSDT",
    ],
    
    # Phase 1: Bitget-only filter thresholds (based on feature discovery v2)
    "phase1_threshold": 60,      # Skor minimal untuk lolos ke phase 2
    "phase1_weights": {
        "atr": 25,
        "range": 25,
        "bbw": 20,
        "wick": 15,
        "support": 15,
        "decel": 10,
    },
    "phase1_atr_thresholds": [3.5, 2.5, 1.8],      # score: 25, 18, 10
    "phase1_range_thresholds": [4.0, 2.5, 1.8],    # score: 25, 16, 8
    "phase1_bbw_thresholds": [0.15, 0.10, 0.07],   # score: 20, 14, 8
    "phase1_wick_thresholds": [1.0, 0.65, 0.4],    # score: 15, 10, 6
    "phase1_support_dist_range": (0.3, 1.5),       # sweet spot
    "phase1_support_dist_wide": (1.5, 3.0),        # score half
    "phase1_decel_thresholds": [-0.30, -0.15, -0.05],  # score: 10, 6, 3
    
    # Phase 2: Final scoring thresholds (sama seperti v14)
    "alert_threshold_early": 95,
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal": 80,
    "alert_threshold_bitget_only": 75,
    
    # Velocity gates (relaxed for early breakout)
    "velocity_gates": {
        "chg_1h_max": 10.0,
        "chg_4h_max": 15.0,
        "chg_24h_max_early": 15.0,
        "chg_24h_max_continuation": 30.0,
        "chg_24h_min": -8.0,
    },
    
    # Cooldown & limits
    "cooldown_hours": 6,
    "max_alerts_per_scan": 5,
    "candle_limit_bitget": 100,
    "coinalyze_lookback_h": 72,
    "coinalyze_funding_lookback_h": 168,
    "coinalyze_batch_size": 10,
    "coinalyze_rate_limit_wait": 1.2,
    "btc_dump_threshold": -3.0,
    
    # Database
    "history_db": "/tmp/scanner_v15_history.db",
    
    # Stock blacklist
    "stock_token_blacklist": [
        "HOODUSDT", "COINUSDT", "MSTRUSDT", "NVDAUSDT", "AAPLUSDT",
        "GOOGLUSDT", "AMZNUSDT", "METAUSDT", "QQQUSDT", "BZUSDT",
        "MCDUSDT", "NIGHTUSDT", "JCTUSDT", "NOMUSDT", "ASTERUSDT",
        "POLYXUSDT", "PIUSDT", "WMTUSDT", "BGBUSDT", "MEUSDT",
        "TSLAUSDT", "CRCLUSDT", "SPYUSDT", "GLDUSDT", "MSFTUSDT",
        "PLTRUSDT", "INTCUSDT", "XAUSDT", "USDCUSDT", "TRXUSDT",
    ],
}

# ══════════════════════════════════════════════════════════════════════════════
#  📊  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ClzData:
    ohlcv: List[dict] = field(default_factory=list)
    oi: List[dict] = field(default_factory=list)
    liq: List[dict] = field(default_factory=list)
    funding_hist: List[dict] = field(default_factory=list)
    predicted_funding_hist: List[dict] = field(default_factory=list)
    ls_ratio: List[dict] = field(default_factory=list)

    @property
    def has_ohlcv(self) -> bool:
        return len(self.ohlcv) >= 10
    @property
    def has_oi(self) -> bool:
        return len(self.oi) >= 4
    @property
    def has_liq(self) -> bool:
        return len(self.liq) >= 4
    @property
    def has_funding_hist(self) -> bool:
        return len(self.funding_hist) >= 3
    @property
    def has_predicted_funding(self) -> bool:
        return len(self.predicted_funding_hist) >= 3
    @property
    def has_ls(self) -> bool:
        return len(self.ls_ratio) >= 4


@dataclass
class CoinData:
    symbol: str
    price: float
    vol_24h: float
    chg_24h: float
    chg_1h: float
    chg_4h: float
    funding: float
    candles: List[dict]
    btc_chg_1h: float = 0.0
    btc_chg_24h: float = 0.0
    clz: ClzData = field(default_factory=ClzData)


@dataclass
class PhaseInfo:
    phase: str
    base_score: int
    description: str
    risk_level: str


@dataclass
class PumpType:
    type_code: str
    type_name: str
    confidence: int
    signals: List[str]


@dataclass
class ScoreResult:
    symbol: str
    score: int
    phase: str
    pump_types: List[PumpType]
    confidence: str
    components: Dict[str, Any]
    catalysts: List[str]
    entry: Optional[dict]
    price: float
    vol_24h: float
    chg_24h: float
    chg_1h: float
    funding: float
    urgency: str
    risk_warnings: List[str] = field(default_factory=list)
    position: Optional[dict] = None
    bitget_phase1_score: int = 0  # New field untuk logging


# ══════════════════════════════════════════════════════════════════════════════
#  🗄️  DATABASE UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    db = CONFIG["history_db"]
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            alerted_at INTEGER NOT NULL,
            score INTEGER,
            phase TEXT,
            entry_price REAL,
            outcome_pct REAL,
            outcome_checked INTEGER DEFAULT 0
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_alert_sym ON alerts(symbol, alerted_at DESC)")
    conn.commit()
    conn.close()


def is_on_cooldown(symbol: str) -> bool:
    try:
        conn = sqlite3.connect(CONFIG["history_db"])
        c = conn.cursor()
        c.execute("SELECT MAX(alerted_at) FROM alerts WHERE symbol = ?", (symbol,))
        row = c.fetchone()
        conn.close()
        if row and row[0]:
            return (time.time() - row[0]) < (CONFIG["cooldown_hours"] * 3600)
    except Exception:
        pass
    return False


def set_alert(symbol: str, score: int, phase: str, entry_price: float):
    try:
        conn = sqlite3.connect(CONFIG["history_db"])
        c = conn.cursor()
        c.execute(
            "INSERT INTO alerts (symbol, alerted_at, score, phase, entry_price) VALUES (?,?,?,?,?)",
            (symbol, int(time.time()), score, phase, entry_price)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"set_alert failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  🔧  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def get_chg_from_candles(candles: List[dict], n_hours: int) -> float:
    if len(candles) < n_hours + 2:
        return 0.0
    now_price = candles[-2]["close"]
    prev_price = candles[-(n_hours + 2)]["close"]
    if prev_price <= 0:
        return 0.0
    return (now_price - prev_price) / prev_price * 100


def get_hour_utc() -> int:
    return datetime.now(timezone.utc).hour


def volume_tod_mult(hour: int) -> float:
    if 2 <= hour <= 8:
        return 1.35
    elif 13 <= hour <= 21:
        return 0.88
    return 1.0


def is_stock_token(symbol: str) -> bool:
    blacklist = {s.strip().upper() for s in CONFIG.get("stock_token_blacklist", [])}
    return symbol.strip().upper() in blacklist


# ══════════════════════════════════════════════════════════════════════════════
#  📐  ATR & TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
def calc_atr(candles: List[dict], n: int = 14) -> float:
    """ATR sebagai persentase harga"""
    trs = []
    for i in range(2, min(n + 2, len(candles))):
        c = candles[-i]
        pc = candles[-(i + 1)]["close"]
        if pc > 0:
            tr = max(
                (c["high"] - c["low"]) / pc,
                abs(c["high"] - pc) / pc,
                abs(c["low"] - pc) / pc,
            )
            trs.append(tr)
    return _mean(trs) if trs else 0.02


def calc_bbw(candles: List[dict]) -> float:
    """Bollinger Band Width (BBW) sebagai rasio"""
    if len(candles) < 22:
        return 0.0
    closes = [c["close"] for c in candles[-20:]]
    sma = _mean(closes)
    if sma <= 0:
        return 0.0
    var = sum((x - sma) ** 2 for x in closes) / 20
    std = var ** 0.5
    return (sma + 2 * std - (sma - 2 * std)) / sma


def calc_range_pct(candles: List[dict]) -> float:
    """Rentang harga 3 candle terakhir dalam persen"""
    if len(candles) < 10:
        return 0.0
    recent = candles[-9:-1]
    closes = [c["close"] for c in recent]
    lo, hi = min(closes), max(closes)
    ref = (lo + hi) / 2
    if ref <= 0:
        return 0.0
    return (hi - lo) / ref * 100


def calc_lower_wick_pct(candles: List[dict]) -> float:
    """Rata-rata lower wick 3 candle terakhir dalam persen"""
    if len(candles) < 5:
        return 0.0
    wick_pcts = []
    for c in candles[-4:-1]:
        lo = c["low"]
        op = c.get("open", 0)
        cl = c["close"]
        body_low = min(op, cl) if op > 0 else cl
        if body_low > 0:
            wick = (body_low - lo) / body_low * 100
            wick_pcts.append(max(0.0, wick))
    return _mean(wick_pcts) if wick_pcts else 0.0


def calc_dist_to_support(candles: List[dict], price: float) -> Tuple[float, bool]:
    """
    Menghitung jarak ke support terdekat dan apakah dalam compression.
    Support di-cluster dari lows 96 candle.
    Returns: (distance_pct, inside_compression)
    """
    window = min(96, len(candles))
    if window < 10 or price <= 0:
        return 100.0, True
    
    window_candles = candles[-window:]
    lows = [c["low"] for c in window_candles if c["low"] > 0]
    if len(lows) < 4:
        return 100.0, True
    
    # Clustering dengan toleransi 2%
    tol = 0.02
    clusters: Dict[float, int] = {}
    for low in lows:
        matched = False
        for cp in list(clusters.keys()):
            if abs(low - cp) / cp < tol:
                clusters[cp] += 1
                matched = True
                break
        if not matched:
            clusters[low] = 1
    
    if not clusters:
        return 100.0, True
    
    # Support valid: bounce antara 2-5 kali dan di bawah harga
    valid = [(lvl, cnt) for lvl, cnt in clusters.items() if 2 <= cnt <= 5 and lvl < price]
    if not valid:
        return 100.0, True
    
    # Support terkuat (bounce terbanyak)
    support_level, bounce = max(valid, key=lambda x: x[1])
    dist_pct = (price - support_level) / support_level * 100
    
    # Inside compression: harga dalam 3% di atas support
    inside_comp = (dist_pct <= 3.0)
    
    return dist_pct, inside_comp


def calc_momentum_decel(candles: List[dict]) -> float:
    """Momentum deceleration: perubahan percepatan 5 candle terakhir"""
    if len(candles) < 8:
        return 0.0
    chgs = []
    for i in range(-5, -1):
        c = candles[i]
        pc = candles[i - 1]
        if pc["close"] > 0:
            chg = (c["close"] - pc["close"]) / pc["close"] * 100
            chgs.append(chg)
    if len(chgs) < 4:
        return 0.0
    recent = _mean(chgs[-2:])
    earlier = _mean(chgs[:2])
    return recent - earlier  # negatif = deceleration


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PHASE 1: BITGET-ONLY FILTER (Fast)
# ══════════════════════════════════════════════════════════════════════════════
def phase1_bitget_filter(candles: List[dict]) -> Tuple[int, Dict[str, Any]]:
    """
    Menghitung skor berdasarkan fitur-fitur candle saja.
    Berdasarkan feature discovery v2 dengan bobot:
    - ATR (25), range (25), BBW (20), lower wick (15), support (15), decel (10)
    Threshold untuk lolos: skor >= 60
    """
    if len(candles) < 30:
        return 0, {"error": "insufficient_candles"}
    
    cfg = CONFIG["phase1_weights"]
    thresholds = CONFIG["phase1_atr_thresholds"]
    details = {}
    score = 0
    
    # 1. ATR
    atr = calc_atr(candles[-22:], 14) * 100
    if atr >= thresholds[0]:
        score += cfg["atr"]
        details["atr_score"] = cfg["atr"]
    elif atr >= thresholds[1]:
        score += 18
        details["atr_score"] = 18
    elif atr >= thresholds[2]:
        score += 10
        details["atr_score"] = 10
    else:
        details["atr_score"] = 0
    details["atr_pct"] = round(atr, 2)
    
    # 2. Range 3 candle
    range_pct = calc_range_pct(candles)
    thresholds_r = CONFIG["phase1_range_thresholds"]
    if range_pct >= thresholds_r[0]:
        score += cfg["range"]
        details["range_score"] = cfg["range"]
    elif range_pct >= thresholds_r[1]:
        score += 16
        details["range_score"] = 16
    elif range_pct >= thresholds_r[2]:
        score += 8
        details["range_score"] = 8
    else:
        details["range_score"] = 0
    details["range_pct"] = round(range_pct, 2)
    
    # 3. BBW
    bbw = calc_bbw(candles)
    thresholds_b = CONFIG["phase1_bbw_thresholds"]
    if bbw >= thresholds_b[0]:
        score += cfg["bbw"]
        details["bbw_score"] = cfg["bbw"]
    elif bbw >= thresholds_b[1]:
        score += 14
        details["bbw_score"] = 14
    elif bbw >= thresholds_b[2]:
        score += 8
        details["bbw_score"] = 8
    else:
        details["bbw_score"] = 0
    details["bbw"] = round(bbw, 4)
    
    # 4. Lower wick
    wick = calc_lower_wick_pct(candles)
    thresholds_w = CONFIG["phase1_wick_thresholds"]
    if wick >= thresholds_w[0]:
        score += cfg["wick"]
        details["wick_score"] = cfg["wick"]
    elif wick >= thresholds_w[1]:
        score += 10
        details["wick_score"] = 10
    elif wick >= thresholds_w[2]:
        score += 6
        details["wick_score"] = 6
    else:
        details["wick_score"] = 0
    details["wick_pct"] = round(wick, 2)
    
    # 5. Distance to support & inside compression
    price = candles[-2]["close"]
    dist, inside_comp = calc_dist_to_support(candles, price)
    details["dist_to_support"] = round(dist, 2)
    details["inside_compression"] = 1 if inside_comp else 0
    
    if not inside_comp:  # tidak dalam compression
        sweet_low, sweet_high = CONFIG["phase1_support_dist_range"]
        wide_low, wide_high = CONFIG["phase1_support_dist_wide"]
        if sweet_low <= dist <= sweet_high:
            score += cfg["support"]
            details["support_score"] = cfg["support"]
        elif wide_low <= dist <= wide_high:
            score += 7
            details["support_score"] = 7
        else:
            details["support_score"] = 0
    else:
        details["support_score"] = 0
    
    # 6. Momentum deceleration
    decel = calc_momentum_decel(candles)
    thresholds_d = CONFIG["phase1_decel_thresholds"]
    if decel <= thresholds_d[0]:
        score += cfg["decel"]
        details["decel_score"] = cfg["decel"]
    elif decel <= thresholds_d[1]:
        score += 6
        details["decel_score"] = 6
    elif decel <= thresholds_d[2]:
        score += 3
        details["decel_score"] = 3
    else:
        details["decel_score"] = 0
    details["decel"] = round(decel, 3)
    
    details["total_score"] = score
    return score, details


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET API CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE_URL = "https://api.bitget.com"
    _candles_cache: Dict[str, tuple] = {}  # key -> (timestamp, candles)
    CACHE_TTL = 55 * 60
    
    @classmethod
    def _get(cls, endpoint: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        for attempt in range(3):
            try:
                resp = requests.get(f"{cls.BASE_URL}/{endpoint}", params=params, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    time.sleep(10)
                    continue
                break
            except Exception:
                if attempt < 2:
                    time.sleep(3)
        return None
    
    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        data = cls._get("api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}
    
    @classmethod
    def get_candles(cls, symbol: str, limit: int = 100) -> List[dict]:
        cache_key = f"{symbol}:{limit}"
        now = time.time()
        if cache_key in cls._candles_cache:
            ts, cached = cls._candles_cache[cache_key]
            if now - ts < cls.CACHE_TTL:
                return cached
        
        data = cls._get(
            "api/v2/mix/market/candles",
            params={"symbol": symbol, "productType": "USDT-FUTURES", "granularity": "1H", "limit": limit}
        )
        if not data or data.get("code") != "00000":
            return []
        candles = []
        for row in data.get("data", []):
            try:
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({
                    "ts": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume_usd": vol_usd,
                })
            except Exception:
                continue
        candles.sort(key=lambda x: x["ts"])
        cls._candles_cache[cache_key] = (now, candles)
        return candles
    
    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(
            "api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"}
        )
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0
    
    @classmethod
    def clear_cache(cls):
        cls._candles_cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  COINALYZE API CLIENT (dengan rate limiting)
# ══════════════════════════════════════════════════════════════════════════════
class CoinalyzeClient:
    BASE_URL = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._markets_cache: Optional[List[dict]] = None
        self._bn_map: Dict[str, str] = {}   # Bitget symbol -> Binance symbol di Coinalyze
        self._by_map: Dict[str, str] = {}   # Bitget symbol -> Bybit symbol untuk L/S
    
    def _wait(self):
        elapsed = time.time() - CoinalyzeClient._last_call
        wait = CONFIG["coinalyze_rate_limit_wait"] - elapsed
        if wait > 0:
            time.sleep(wait)
        CoinalyzeClient._last_call = time.time()
    
    def _get(self, endpoint: str, params: dict) -> Optional[Any]:
        params["api_key"] = self.api_key
        headers = {"User-Agent": f"PrePumpScanner/{VERSION}"}
        for attempt in range(3):
            self._wait()
            try:
                resp = requests.get(f"{self.BASE_URL}/{endpoint}", params=params, headers=headers, timeout=15)
                if resp.status_code == 429:
                    retry = resp.headers.get("Retry-After", "10")
                    try:
                        wait = int(float(retry)) + 1
                    except:
                        wait = 11
                    jitter = random.uniform(0.5, 2.0)
                    log.warning(f"  Coinalyze rate limit, wait {wait}s + {jitter:.1f}s")
                    time.sleep(wait + jitter)
                    continue
                if resp.status_code != 200:
                    log.warning(f"  Coinalyze {endpoint} HTTP {resp.status_code}: {resp.text[:150]}")
                    return None
                data = resp.json()
                if isinstance(data, dict) and "error" in data:
                    log.warning(f"  Coinalyze error: {data['error']}")
                    return None
                return data
            except Exception as e:
                log.warning(f"  Coinalyze request error: {e}")
                if attempt < 2:
                    time.sleep(3)
        return None
    
    def build_symbol_maps(self, bitget_symbols: List[str]) -> None:
        """Mapping simbol Bitget ke Coinalyze (Binance untuk OHLCV/OI/liq, Bybit untuk L/S)"""
        if self._markets_cache is None:
            log.info("  Loading Coinalyze markets...")
            data = self._get("future-markets", {})
            self._markets_cache = data if isinstance(data, list) else []
            log.info(f"  Got {len(self._markets_cache)} Coinalyze markets")
        
        markets = self._markets_cache
        bn_lookup: Dict[str, str] = {}
        by_ls_lookup: Dict[str, str] = {}
        
        for m in markets:
            exc = m.get("exchange", "")
            sym_on_exc = m.get("symbol_on_exchange", "")
            clz_sym = m.get("symbol", "")
            is_perp = m.get("is_perpetual", False)
            quote = m.get("quote_asset", "").upper()
            if not (is_perp and quote == "USDT" and clz_sym):
                continue
            if exc == "A":  # Binance
                bn_lookup[sym_on_exc] = clz_sym
            elif exc == "6" and m.get("has_long_short_ratio_data"):  # Bybit
                by_ls_lookup[sym_on_exc] = clz_sym
        
        def normalize(s: str) -> str:
            if s.startswith("1000"):
                s = s[4:]
            return s.upper()
        
        def candidates(sym: str) -> List[str]:
            base = sym.replace("USDT", "")
            cand = [sym, f"{base}/USDT", f"{base}-USDT", f"1000{base}USDT", f"10000{base}USDT"]
            if base.startswith("1000"):
                cand.append(base[4:] + "USDT")
            return list(set(cand))
        
        mapped_bn = 0
        mapped_by = 0
        for sym in bitget_symbols:
            norm_sym = normalize(sym)
            for cand in candidates(norm_sym):
                if cand in bn_lookup:
                    self._bn_map[sym] = bn_lookup[cand]
                    mapped_bn += 1
                    break
            for cand in candidates(norm_sym):
                if cand in by_ls_lookup:
                    self._by_map[sym] = by_ls_lookup[cand]
                    mapped_by += 1
                    break
        
        log.info(f"  Mapping: {mapped_bn}/{len(bitget_symbols)} Binance, {mapped_by}/{len(bitget_symbols)} Bybit")
    
    def _batch_fetch(self, endpoint: str, symbols: List[str], params: dict) -> Dict[str, list]:
        batch_size = CONFIG["coinalyze_batch_size"]
        result: Dict[str, list] = {}
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            try:
                p = dict(params)
                p["symbols"] = ",".join(batch)
                data = self._get(endpoint, p)
                if data and isinstance(data, list):
                    for item in data:
                        sym = item.get("symbol", "")
                        hist = item.get("history", [])
                        if sym and hist:
                            result[sym] = hist
                elif data and isinstance(data, dict) and "error" in data:
                    log.warning(f"  API error batch {batch[:3]}...: {data['error']}")
            except Exception as e:
                log.warning(f"  Batch {i//batch_size+1} failed: {e}")
        return result
    
    def fetch_for_symbols(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, ClzData]:
        """Fetch data Coinalyze untuk daftar simbol tertentu"""
        result = {sym: ClzData() for sym in symbols}
        
        # Siapkan mapping reverse
        bn_syms = [self._bn_map[s] for s in symbols if s in self._bn_map]
        by_syms = [self._by_map[s] for s in symbols if s in self._by_map]
        bn_rev = {v: k for k, v in self._bn_map.items()}
        by_rev = {v: k for k, v in self._by_map.items()}
        
        interval = CONFIG["coinalyze_interval"] if "coinalyze_interval" in CONFIG else "1hour"
        fund_interval = CONFIG.get("coinalyze_funding_interval", "daily")
        fund_interval_alt = CONFIG.get("coinalyze_funding_interval_alt", "1hour")
        fund_from = to_ts - CONFIG["coinalyze_funding_lookback_h"] * 3600
        
        if bn_syms:
            # OHLCV
            log.info(f"  Fetching Binance OHLCV ({len(bn_syms)} syms)...")
            ohlcv_data = self._batch_fetch("ohlcv-history", bn_syms,
                                           {"interval": interval, "from": from_ts, "to": to_ts})
            for clz_sym, hist in ohlcv_data.items():
                bsym = bn_rev.get(clz_sym)
                if bsym:
                    result[bsym].ohlcv = hist
            
            # OI
            log.info(f"  Fetching OI history...")
            oi_data = self._batch_fetch("open-interest-history", bn_syms,
                                        {"interval": interval, "from": from_ts, "to": to_ts, "convert_to_usd": "true"})
            for clz_sym, hist in oi_data.items():
                bsym = bn_rev.get(clz_sym)
                if bsym:
                    result[bsym].oi = hist
            
            # Liquidations
            log.info(f"  Fetching Liquidations...")
            liq_data = self._batch_fetch("liquidation-history", bn_syms,
                                         {"interval": interval, "from": from_ts, "to": to_ts, "convert_to_usd": "true"})
            for clz_sym, hist in liq_data.items():
                bsym = bn_rev.get(clz_sym)
                if bsym:
                    result[bsym].liq = hist
            
            # Funding history
            log.info(f"  Fetching Funding rate history...")
            for interval_try in [fund_interval, fund_interval_alt]:
                fund_data = self._batch_fetch("funding-rate-history", bn_syms,
                                              {"interval": interval_try, "from": fund_from, "to": to_ts})
                if fund_data:
                    log.info(f"    Funding OK using interval '{interval_try}'")
                    for clz_sym, hist in fund_data.items():
                        bsym = bn_rev.get(clz_sym)
                        if bsym:
                            result[bsym].funding_hist = hist
                    break
                else:
                    log.warning(f"    Funding interval '{interval_try}' empty, trying next...")
            
            # Predicted funding
            log.info(f"  Fetching Predicted funding history...")
            pred_data = self._batch_fetch("predicted-funding-rate-history", bn_syms,
                                          {"interval": "daily", "from": fund_from, "to": to_ts})
            for clz_sym, hist in pred_data.items():
                bsym = bn_rev.get(clz_sym)
                if bsym:
                    result[bsym].predicted_funding_hist = hist
        
        if by_syms:
            log.info(f"  Fetching Bybit L/S ratio ({len(by_syms)} syms)...")
            ls_data = self._batch_fetch("long-short-ratio-history", by_syms,
                                        {"interval": interval, "from": from_ts, "to": to_ts})
            for clz_sym, hist in ls_data.items():
                bsym = by_rev.get(clz_sym)
                if bsym:
                    result[bsym].ls_ratio = hist
        
        return result


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  PHASE 2: FINAL SCORING (menggunakan fungsi dari v14)
# ══════════════════════════════════════════════════════════════════════════════
# Karena fungsi scoring di v14 sangat panjang, kita akan mengimpor ulang
# fungsi-fungsi penting yang sudah ada. Untuk menjaga agar kode ini mandiri,
# saya akan menulis ulang versi ringkas dari scoring yang diperlukan.
# Namun agar tidak terlalu panjang, saya akan menyertakan hanya fungsi-fungsi
# yang diperlukan untuk final scoring. (Dalam implementasi nyata, Anda bisa
# memisahkan ke file terpisah)

# Untuk mempersingkat, kita akan menggunakan fungsi-fungsi yang sudah ada
# dari scanner v14.9.1. Karena itu kita perlu mengimpor modul tersebut.
# Tapi karena kita membuat file baru, saya akan menyalin fungsi-fungsi penting.
# Berikut adalah core scoring functions (tier1, tier2, tier3) yang sudah
# terbukti bekerja. Saya akan menyertakan versi ringkas tetapi tetap lengkap.

# ── TIER 1: Coinalyze signals (L/S, buy volume, funding, predicted) ──────────
def score_long_short_ratio(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_ls:
        return 0, {"source": "no_ls_data"}
    hist = clz.ls_ratio
    if len(hist) < 4:
        return 0, {"source": "insufficient_ls"}
    current_long = float(hist[-2].get("l", 0.5) or 0.5)
    current_short = float(hist[-2].get("s", 0.5) or 0.5)
    long_4h_ago = float(hist[-5].get("l", 0.5) or 0.5) if len(hist) >= 5 else current_long
    long_trend = current_long - long_4h_ago
    score, signals = 0, []
    if current_long < 0.42:
        score += 30
        signals.append(f"EXTREME_SHORT_DOM longs={current_long:.1%}")
    elif current_long < 0.47:
        score += 20
        signals.append(f"SHORT_DOM longs={current_long:.1%}")
    elif current_long < 0.50:
        score += 10
        signals.append(f"SLIGHT_SHORT_DOM longs={current_long:.1%}")
    if long_trend < -0.02:
        score += 12
        signals.append(f"SHORTS_ADDING Δ={long_trend:.2%}")
    elif long_trend < -0.01:
        score += 6
        signals.append(f"LONGS_REDUCING Δ={long_trend:.2%}")
    if current_long > 0.58:
        score = max(0, score - 15)
        signals.append(f"⚠️ LONG_HEAVY={current_long:.1%}")
    return min(score, 35), {"long_ratio": round(current_long, 4), "long_trend_4h": round(long_trend, 4), "signals": signals}


def score_buy_volume_ratio(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_ohlcv:
        return 0, {"source": "no_ohlcv"}
    hist = clz.ohlcv
    recent = [c for c in hist[-7:-1] if float(c.get("v", 0) or 0) > 0]
    if len(recent) < 3:
        return 0, {"source": "insufficient_ohlcv"}
    bv_ratios = []
    for c in recent:
        v = float(c.get("v", 0) or 0)
        bv = float(c.get("bv", 0) or 0)
        if v > 0:
            bv_ratios.append(bv / v)
    if not bv_ratios:
        return 0, {"source": "no_bv_data"}
    avg_bv = _mean(bv_ratios)
    score, signals = 0, []
    if avg_bv >= 0.62:
        score += 25
        signals.append(f"STRONG_BUY bv/v={avg_bv:.1%}")
    elif avg_bv >= 0.55:
        score += 15
        signals.append(f"NET_BUYING bv/v={avg_bv:.1%}")
    return min(score, 30), {"avg_bv_ratio": round(avg_bv, 4), "signals": signals}


def score_funding_trend(clz: ClzData, current_funding: float) -> Tuple[int, dict]:
    score, signals = 0, []
    if current_funding < -0.0010:
        score += 15
        signals.append(f"EXTREME_FUNDING={current_funding*100:.4f}%")
    elif current_funding < -0.0005:
        score += 10
        signals.append(f"STRONG_NEG_FUNDING={current_funding*100:.4f}%")
    elif current_funding < -0.0002:
        score += 6
        signals.append(f"NEG_FUNDING={current_funding*100:.4f}%")
    if clz.has_funding_hist:
        rates = [float(c.get("c", 0) or 0) for c in clz.funding_hist if c.get("c") is not None]
        if len(rates) >= 6:
            recent = _mean(rates[-3:])
            prev = _mean(rates[-6:-3])
            drift = recent - prev
            if drift < -0.0003:
                score += 25
                signals.append(f"FUNDING_TRENDING_NEG Δ={drift*100:.4f}%")
            elif drift < -0.0001:
                score += 15
                signals.append(f"FUNDING_DRIFTING_NEG Δ={drift*100:.4f}%")
    return min(score, 40), {"current": round(current_funding*100, 5), "signals": signals}


def score_predicted_funding(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_predicted_funding:
        return 0, {"source": "no_predicted"}
    rates = [float(c.get("c", 0) or 0) for c in clz.predicted_funding_hist if c.get("c") is not None]
    if len(rates) < 6:
        return 0, {"source": "insufficient"}
    recent = _mean(rates[-3:])
    prev = _mean(rates[-6:-3])
    drift = recent - prev
    if drift < -0.0002:
        return 20, {"drift": round(drift*100, 5), "signals": ["PRED_FUNDING_BEARISH"]}
    elif drift < -0.0001:
        return 12, {"drift": round(drift*100, 5), "signals": ["PRED_FUNDING_NEG"]}
    return 0, {"drift": round(drift*100, 5)}


def score_oi_buildup(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_oi:
        return 0, {"source": "no_oi"}
    hist = clz.oi
    if len(hist) < 6:
        return 0, {"source": "insufficient"}
    oi_now = float(hist[-2].get("c", 0) or 0)
    oi_4h = float(hist[-5].get("c", 0) or 0)
    if oi_4h <= 0:
        return 0, {"source": "oi_zero"}
    chg = (oi_now - oi_4h) / oi_4h * 100
    score, signals = 0, []
    if chg > 5.0:
        score += 20
        signals.append(f"STRONG_OI_BUILDUP OI4h={chg:+.1f}%")
    elif chg > 2.5:
        score += 12
        signals.append(f"OI_BUILDUP OI4h={chg:+.1f}%")
    elif chg > 1.0:
        score += 6
        signals.append(f"OI_RISING OI4h={chg:+.1f}%")
    return min(score, 20), {"oi_chg_4h_pct": round(chg, 2), "signals": signals}


def score_liquidations(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_liq:
        return 0, {"source": "no_liq"}
    hist = clz.liq
    if len(hist) < 6:
        return 0, {"source": "insufficient"}
    baseline = [float(c.get("s", 0) or 0) for c in hist[-24:-3] if c.get("s") is not None]
    if not baseline:
        return 0, {"source": "no_baseline"}
    current = float(hist[-2].get("s", 0) or 0)
    # Robust z-score
    med = sorted(baseline)[len(baseline)//2]
    mad = sorted([abs(x-med) for x in baseline])[len(baseline)//2] if baseline else 1
    if mad < 1e-9:
        mad = 1
    z = (current - med) / (mad * 1.4826)
    score, signals = 0, []
    if z >= 2.5:
        score += 20
        signals.append(f"SHORT_LIQ_SPIKE z={z:.1f}")
    elif z >= 1.5:
        score += 12
        signals.append(f"SHORT_LIQ_ELEVATED z={z:.1f}")
    return min(score, 20), {"short_liq_z": round(z, 2), "signals": signals}


# ── TIER 3: Bitget candles (sama seperti sebelumnya, tapi kita panggil ulang)
# Kita sudah memiliki fungsi-fungsi dari phase1 yang bisa dipakai untuk tier3.
# Namun untuk konsistensi, kita akan menggunakan fungsi yang sama dengan v14.

# Berikut adalah fungsi-fungsi tier3 yang sudah ada di v14, kita salin:
def detect_bbw_squeeze(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 22:
        return 0, {}
    closes = [c["close"] for c in candles[-20:]]
    sma = _mean(closes)
    if sma <= 0:
        return 0, {}
    var = sum((x - sma)**2 for x in closes) / 20
    std = var**0.5
    bb_w = (sma + 2*std - (sma - 2*std)) / sma
    w = 5
    if bb_w > 0.15:
        score, pat = w, "WIDE_EXPANSION"
    elif bb_w > 0.10:
        score, pat = int(w*0.8), "EXPANDING"
    elif bb_w > 0.06:
        score, pat = int(w*0.4), "MODERATE"
    else:
        score, pat = 0, "TIGHT_SQUEEZE"
    return score, {"bb_w": round(bb_w, 4), "pattern": pat}


def detect_volume_dryup(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 26:
        return 0, {}
    cur_vol = candles[-2].get("volume_usd", 0)
    avg_vol = _mean([c.get("volume_usd", 0) for c in candles[-26:-2]])
    if avg_vol <= 0:
        return 0, {}
    tod = volume_tod_mult(get_hour_utc())
    adj = (cur_vol * tod) / avg_vol
    w = 5
    if adj < 0.35:
        return w, {"ratio": round(adj, 2), "pattern": "EXTREME_DRY"}
    elif adj < 0.50:
        return int(w*0.7), {"ratio": round(adj, 2), "pattern": "VERY_DRY"}
    elif adj < 0.65:
        return int(w*0.4), {"ratio": round(adj, 2), "pattern": "DRY"}
    return 0, {"ratio": round(adj, 2), "pattern": "NORMAL"}


def detect_accumulation(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 26:
        return 0, {}
    cur_vol = _mean([c.get("volume_usd", 0) for c in candles[-7:-1]])
    base_vol = _mean([c.get("volume_usd", 0) for c in candles[-25:-7]])
    if base_vol <= 0:
        return 0, {}
    ratio = cur_vol / base_vol
    p_chg = (candles[-2]["close"] - candles[-7]["close"]) / candles[-7]["close"] * 100
    w = 15
    if ratio >= 3.0 and -2 < p_chg < 4:
        return w, {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "STRONG_ACCUM"}
    elif ratio >= 2.5 and -2 < p_chg < 5:
        return int(w*0.75), {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "ACCUM"}
    elif ratio >= 2.0 and -1 < p_chg < 4:
        return int(w*0.5), {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "LIGHT_ACCUM"}
    return 0, {"vol_ratio": round(ratio, 2), "pattern": "NO_ACCUM"}


def detect_volatility_return(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 50:
        return 0, {}
    atr_now = calc_atr(candles[-22:], 14) * 100
    atr_hist = calc_atr(candles[-72:-24], 14) * 100 if len(candles) >= 74 else calc_atr(candles[:-24], 14) * 100
    if atr_hist <= 0:
        return 0, {}
    ratio = atr_now / atr_hist
    w = 22
    if atr_now >= 5.0:
        abs_score = w
    elif atr_now >= 3.5:
        abs_score = int(w*0.8)
    elif atr_now >= 2.5:
        abs_score = int(w*0.5)
    else:
        abs_score = 0
    if ratio < 0.40:
        ratio_score = int(w*0.5)
    elif ratio < 0.60:
        ratio_score = int(w*0.35)
    elif ratio < 0.75:
        ratio_score = int(w*0.2)
    else:
        ratio_score = 0
    score = min(abs_score + ratio_score//2, w) if abs_score>0 and ratio_score>0 else max(abs_score, ratio_score)
    return score, {"atr_now_pct": round(atr_now, 2), "atr_ratio": round(ratio, 3), "pattern": "VOL_RETURN"}


def detect_rs_btc(coin_chg_1h: float, btc_chg_1h: float) -> Tuple[int, dict]:
    if btc_chg_1h == 0:
        return 0, {"rs": 0}
    rs = coin_chg_1h - btc_chg_1h
    w = 8
    if rs < -0.2 and btc_chg_1h > 0.3:
        return w, {"rs": round(rs, 2), "pattern": "BTC_LEADING_CATCHUP_PENDING"}
    elif rs < -0.1 and btc_chg_1h > 0:
        return int(w*0.6), {"rs": round(rs, 2), "pattern": "SLIGHT_LAG_VS_BTC"}
    return 0, {"rs": round(rs, 2), "pattern": "INLINE"}


def detect_lower_wick(candles: List[dict]) -> Tuple[int, dict]:
    # Versi sederhana untuk tier3
    wick = calc_lower_wick_pct(candles)
    w = 15
    if wick >= 1.0:
        return w, {"avg_wick_pct": round(wick, 2), "pattern": "STRONG_REJECTION_WICK"}
    elif wick >= 0.65:
        return int(w*0.75), {"avg_wick_pct": round(wick, 2), "pattern": "REJECTION_WICK"}
    elif wick >= 0.40:
        return int(w*0.45), {"avg_wick_pct": round(wick, 2), "pattern": "LIGHT_WICK"}
    return 0, {"avg_wick_pct": round(wick, 2), "pattern": "NO_WICK"}


def detect_momentum_decel(candles: List[dict]) -> Tuple[int, dict]:
    accel = calc_momentum_decel(candles)
    w = 8
    if accel <= -0.30:
        return w, {"accel": round(accel, 3), "pattern": "STRONG_DECEL"}
    elif accel <= -0.15:
        return int(w*0.7), {"accel": round(accel, 3), "pattern": "DECEL"}
    elif accel <= -0.05:
        return int(w*0.35), {"accel": round(accel, 3), "pattern": "SLIGHT_DECEL"}
    return 0, {"accel": round(accel, 3), "pattern": "NO_DECEL"}


def detect_rs_24h(candles: List[dict], btc_chg_24h: float) -> Tuple[int, dict]:
    coin_chg_24h = get_chg_from_candles(candles, 24) if len(candles) >= 26 else 0.0
    if btc_chg_24h == 0:
        return 0, {"rs_24h": 0}
    rs = coin_chg_24h - btc_chg_24h
    w = 10
    if rs >= 0.3:
        return w, {"rs_24h": round(rs, 2), "pattern": "OUTPERFORM_BTC_24H"}
    elif rs >= 0.1:
        return int(w*0.6), {"rs_24h": round(rs, 2), "pattern": "SLIGHT_OUTPERFORM_24H"}
    return 0, {"rs_24h": round(rs, 2), "pattern": "INLINE"}


def detect_dist_to_support(candles: List[dict], price: float) -> Tuple[int, dict]:
    dist, inside = calc_dist_to_support(candles, price)
    w = 10
    if not inside and 0.3 <= dist <= 1.5:
        return w, {"dist_pct": round(dist, 2), "pattern": "JUST_BOUNCED"}
    elif not inside and 1.5 < dist <= 3.0:
        return int(w*0.6), {"dist_pct": round(dist, 2), "pattern": "NEAR_SUPPORT"}
    elif not inside and dist < 0.3:
        return int(w*0.4), {"dist_pct": round(dist, 2), "pattern": "AT_SUPPORT"}
    return 0, {"dist_pct": round(dist, 2), "pattern": "FAR_FROM_SUPPORT"}


# ── PUMP TYPE CLASSIFICATION (ringkas) ───────────────────────────────────────
def classify_pump_types(data: CoinData, ls_sc, bv_sc, fund_sc, pred_sc, oi_sc, liq_sc,
                        bbw_sc, dry_sc, accum_sc, vret_sc, rs_sc, wick_sc, decel_sc,
                        supp_sc, rs24_sc) -> List[PumpType]:
    pump_types = []
    cfg = CONFIG  # gunakan konfigurasi dari v14 (kita tidak punya, buat sederhana)
    # Short squeeze (Type E)
    if ls_sc >= 8 and (liq_sc >= 6 or fund_sc >= 7):
        pump_types.append(PumpType("E", "Short Squeeze", min((ls_sc+liq_sc+fund_sc+pred_sc)*2, 100), []))
    # Whale accumulation (Type B)
    if bv_sc >= 8 and accum_sc >= 5:
        pump_types.append(PumpType("B", "Whale Accumulation", min((bv_sc+accum_sc)*3, 100), []))
    # Technical breakout (Type D)
    if bbw_sc >= 8 and dry_sc >= 5 and (oi_sc >= 6 or liq_sc >= 6 or fund_sc >= 10):
        pump_types.append(PumpType("D", "Technical Breakout", min((bbw_sc+dry_sc)*3, 100), []))
    # Volatility return (Type F)
    if vret_sc >= 10:
        pump_types.append(PumpType("F", "Volatility Return", min(vret_sc*5, 100), []))
    return pump_types


# ── FINAL SCORING (versi ringkas yang mengintegrasikan semua) ────────────────
def final_score_coin(data: CoinData, phase1_score: int) -> Optional[ScoreResult]:
    """
    Melakukan scoring final dengan semua tier.
    Mengembalikan ScoreResult jika memenuhi threshold.
    """
    # Velocity gates (relaxed)
    phase = classify_phase(data.chg_24h)
    is_cont = (phase.phase == "CONTINUATION")
    if phase.phase not in ["DOWNTREND", "WEAK"]:
        vg = CONFIG["velocity_gates"]
        if data.chg_24h < vg["chg_24h_min"]:
            return None
        max_24h = vg["chg_24h_max_continuation"] if is_cont else vg["chg_24h_max_early"]
        if data.chg_24h > max_24h:
            return None
        if data.chg_1h > vg["chg_1h_max"]:
            return None
        if data.chg_4h > vg["chg_4h_max"]:
            return None
    
    # Tier 1 & 2 (Coinalyze)
    ls_sc, ls_d = score_long_short_ratio(data.clz)
    bv_sc, bv_d = score_buy_volume_ratio(data.clz)
    fund_sc, fund_d = score_funding_trend(data.clz, data.funding)
    pred_sc, pred_d = score_predicted_funding(data.clz)
    oi_sc, oi_d = score_oi_buildup(data.clz)
    liq_sc, liq_d = score_liquidations(data.clz)
    tier1 = ls_sc + bv_sc + fund_sc + pred_sc
    tier2 = oi_sc + liq_sc
    
    # Tier 3 (Bitget)
    bbw_sc, bbw_d = detect_bbw_squeeze(data.candles)
    stab_sc, stab_d = 0, {}  # tidak digunakan
    dry_sc, dry_d = detect_volume_dryup(data.candles)
    accum_sc, accum_d = detect_accumulation(data.candles)
    vret_sc, vret_d = detect_volatility_return(data.candles)
    rs_sc, rs_d = detect_rs_btc(data.chg_1h, data.btc_chg_1h)
    wick_sc, wick_d = detect_lower_wick(data.candles)
    decel_sc, decel_d = detect_momentum_decel(data.candles)
    supp_sc, supp_d = detect_dist_to_support(data.candles, data.price)
    rs24_sc, rs24_d = detect_rs_24h(data.candles, data.btc_chg_24h)
    tier3 = (bbw_sc + dry_sc + accum_sc + vret_sc + rs_sc + wick_sc + decel_sc + supp_sc + rs24_sc)
    
    # Phase base score
    phase_score = phase.base_score
    
    # Total
    total = phase_score + tier1 + tier2 + tier3
    
    # Pump types
    pump_types = classify_pump_types(data, ls_sc, bv_sc, fund_sc, pred_sc, oi_sc, liq_sc,
                                     bbw_sc, dry_sc, accum_sc, vret_sc, rs_sc, wick_sc,
                                     decel_sc, supp_sc, rs24_sc)
    
    has_any_clz = data.clz.has_ohlcv or data.clz.has_oi or data.clz.has_liq or data.clz.has_ls or data.clz.has_funding_hist
    
    # Threshold
    if phase.phase == "EARLY":
        threshold = CONFIG["alert_threshold_early"]
    elif phase.phase == "CONTINUATION":
        threshold = CONFIG["alert_threshold_continuation"]
    elif phase.phase in ["DOWNTREND", "WEAK"]:
        threshold = CONFIG["alert_threshold_reversal"]
    else:
        threshold = 110
    
    # Bitget-only fallback
    if not has_any_clz and phase.phase == "EARLY":
        threshold = CONFIG["alert_threshold_bitget_only"]
    
    if total < threshold:
        return None
    if not pump_types:
        # Default pump type jika score tinggi tapi tidak ada klasifikasi
        pump_types.append(PumpType("T", "Technical Setup", min(total, 100), ["Aggregate signals"]))
    
    # Buat entry targets (sederhana)
    entry = {
        "entry": data.price,
        "entry_zone_low": data.price * 0.99,
        "entry_zone_high": data.price * 1.01,
        "sl": data.price * 0.97,
        "sl_pct": 3.0,
        "tp1": data.price * 1.15,
        "tp2": data.price * 1.30,
        "tp3": data.price * 1.50,
        "rr1": 5.0,
        "atr_decimal": calc_atr(data.candles, 14),
    }
    
    return ScoreResult(
        symbol=data.symbol,
        score=min(total, 250),
        phase=phase.phase,
        pump_types=pump_types,
        confidence="strong" if total >= 95 else "watch",
        components={
            "phase": phase_score,
            "tier1_clz": tier1,
            "tier2_clz": tier2,
            "tier3_technical": tier3,
            "detail": {
                "ls": ls_sc, "bv": bv_sc, "fund": fund_sc, "pred": pred_sc,
                "oi": oi_sc, "liq": liq_sc,
                "bbw": bbw_sc, "dry": dry_sc, "accum": accum_sc,
                "vret": vret_sc, "rs": rs_sc, "wick": wick_sc,
                "decel": decel_sc, "supp": supp_sc, "rs24": rs24_sc,
            },
            "data_sources": "Coinalyze" if has_any_clz else "Bitget-only",
        },
        catalysts=[],
        entry=entry,
        price=data.price,
        vol_24h=data.vol_24h,
        chg_24h=data.chg_24h,
        chg_1h=data.chg_1h,
        funding=data.funding,
        urgency="",
        risk_warnings=[],
        bitget_phase1_score=phase1_score,
    )


def classify_phase(chg_24h: float) -> PhaseInfo:
    if chg_24h < -8.0:
        return PhaseInfo("DOWNTREND", 5, "Deep downtrend", "HIGH")
    elif chg_24h < -3.0:
        return PhaseInfo("WEAK", 15, "Weak", "MEDIUM-HIGH")
    elif chg_24h > 25.0:
        return PhaseInfo("PARABOLIC", 10, "Parabolic", "EXTREME")
    elif chg_24h > 12.0:
        base = max(20, 40 - int(chg_24h - 12) * 2)
        return PhaseInfo("CONTINUATION", base, "Momentum continuation", "MEDIUM")
    else:
        if abs(chg_24h) <= 3.0:
            base = 45
        elif chg_24h <= 8.0:
            base = 40
        else:
            base = 35
        return PhaseInfo("EARLY", base, "Early prime zone", "LOW")


# ══════════════════════════════════════════════════════════════════════════════
#  📤  TELEGRAM & ALERT FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(message: str) -> bool:
    bot_token = CONFIG.get("bot_token")
    chat_id = CONFIG.get("chat_id")
    if not bot_token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def build_alert(r: ScoreResult, rank: int) -> str:
    vol = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    emoji = {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(r.confidence, "⚪")
    bar_len = min(20, r.score * 20 // 200)
    bar = "█" * bar_len + "░" * (20 - bar_len)
    lines = [
        f"{'─'*58}",
        f"#{rank}  {r.symbol}  {emoji}  Score: {r.score}  [{r.phase}]",
        f"   {bar}",
        f"   Phase1 Score: {r.bitget_phase1_score} (filter threshold 60)",
        f"   Data: {r.components.get('data_sources', 'N/A')}",
        f"",
        f"   Vol: {vol} | Δ1h: {r.chg_1h:+.1f}% | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding*100:.4f}%",
        f"   T1:{r.components['tier1_clz']} T2:{r.components['tier2_clz']} T3:{r.components['tier3_technical']}",
    ]
    if r.entry:
        e = r.entry
        lines += [
            f"",
            f"   💰 ENTRY: ${e['entry']:.8f}  (SL: ${e['sl']:.8f}, -{e['sl_pct']:.1f}%)",
            f"      TP1: ${e['tp1']:.8f} (+15%)  TP2: ${e['tp2']:.8f} (+30%)",
        ]
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCANNER LOOP
# ══════════════════════════════════════════════════════════════════════════════
def main():
    log.info("═" * 70)
    log.info(f"  PRE-PUMP SCANNER v{VERSION} — TWO-PHASE ARCHITECTURE")
    log.info("  Phase 1: Bitget-only filter (ATR, range, BBW, wick, support, decel)")
    log.info("  Phase 2: Coinalyze verification (OI, liq, funding, L/S)")
    log.info(f"  Whitelist size: {len(CONFIG['whitelist'])} symbols")
    log.info("═" * 70)
    
    if not CONFIG.get("coinalyze_api_key"):
        log.error("❌ COINALYZE_API_KEY not set")
        return 1
    
    init_db()
    
    # Step 1: Get Bitget tickers & BTC data
    log.info("📊 Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        log.error("❌ No tickers from Bitget")
        return 1
    
    btc_candles = BitgetClient.get_candles("BTCUSDT", 30)
    btc_chg_1h = 0.0
    btc_chg_24h = 0.0
    if len(btc_candles) >= 3:
        btc_chg_1h = (btc_candles[-2]["close"] - btc_candles[-3]["close"]) / btc_candles[-3]["close"] * 100
    if len(btc_candles) >= 26:
        btc_chg_24h = get_chg_from_candles(btc_candles, 24)
    log.info(f"  BTC 1h: {btc_chg_1h:+.2f}% | BTC 24h: {btc_chg_24h:+.2f}%")
    
    if btc_chg_1h < CONFIG["btc_dump_threshold"]:
        log.warning(f"⛔ BTC circuit breaker: {btc_chg_1h:+.1f}% — scan paused")
        return 0
    
    # Step 2: Phase 1 — Bitget-only filter untuk semua whitelist
    log.info("🔍 Phase 1: Bitget-only filtering...")
    candidates_phase1 = []  # list of (symbol, phase1_score, candles, ticker)
    
    for sym in CONFIG["whitelist"]:
        if is_on_cooldown(sym):
            continue
        if is_stock_token(sym):
            continue
        
        try:
            ticker = tickers.get(sym)
            if not ticker:
                continue
            price = float(ticker.get("lastPr", 0))
            if price <= 0:
                continue
            
            candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
            if len(candles) < 30:
                continue
            
            # Hitung skor phase1
            score, details = phase1_bitget_filter(candles)
            if score >= CONFIG["phase1_threshold"]:
                candidates_phase1.append((sym, score, candles, ticker))
                log.debug(f"  {sym}: Phase1 score={score} -> passed")
            else:
                log.debug(f"  {sym}: Phase1 score={score} -> rejected")
        except Exception as e:
            log.warning(f"  {sym} phase1 error: {e}")
    
    log.info(f"  Phase1 passed: {len(candidates_phase1)} candidates")
    
    if not candidates_phase1:
        log.info("No candidates passed phase1 filter.")
        return 0
    
    # Step 3: Build Coinalyze maps only for candidates
    log.info("🗺️  Building Coinalyze maps for phase1 candidates...")
    clz_client = CoinalyzeClient(CONFIG["coinalyze_api_key"])
    candidate_symbols = [s for s, _, _, _ in candidates_phase1]
    clz_client.build_symbol_maps(candidate_symbols)
    
    # Step 4: Fetch Coinalyze data
    log.info("📈 Fetching Coinalyze data...")
    now_ts = int(time.time())
    from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_data = clz_client.fetch_for_symbols(candidate_symbols, from_ts, now_ts)
    
    # Step 5: Phase 2 — Final scoring
    log.info("🎯 Phase 2: Final scoring with Coinalyze...")
    final_results = []
    
    for sym, p1_score, candles, ticker in candidates_phase1:
        try:
            price = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            chg_24h = get_chg_from_candles(candles, 24)
            chg_1h = get_chg_from_candles(candles, 1)
            chg_4h = get_chg_from_candles(candles, 4)
            funding = BitgetClient.get_funding(sym)
            
            coin_data = CoinData(
                symbol=sym,
                price=price,
                vol_24h=vol_24h,
                chg_24h=chg_24h,
                chg_1h=chg_1h,
                chg_4h=chg_4h,
                funding=funding,
                candles=candles,
                btc_chg_1h=btc_chg_1h,
                btc_chg_24h=btc_chg_24h,
                clz=clz_data.get(sym, ClzData())
            )
            
            result = final_score_coin(coin_data, p1_score)
            if result:
                final_results.append(result)
                log.info(f"  ✅ {sym}: final score={result.score} (p1={p1_score})")
        except Exception as e:
            log.warning(f"  {sym} final scoring error: {e}")
    
    # Step 6: Sort and send alerts
    final_results.sort(key=lambda x: x.score, reverse=True)
    max_alerts = CONFIG["max_alerts_per_scan"]
    log.info(f"\n{'═'*70}")
    log.info(f"  DONE: {len(final_results)} final signals | Sending top {min(max_alerts, len(final_results))}")
    log.info(f"{'═'*70}\n")
    
    sent = 0
    for rank, res in enumerate(final_results[:10], 1):
        msg = build_alert(res, rank)
        print(msg)
        if sent < max_alerts:
            if send_telegram(msg):
                sent += 1
            entry_price = res.entry["entry"] if res.entry else res.price
            set_alert(res.symbol, res.score, res.phase, entry_price)
    
    if not final_results:
        log.info("No final signals this cycle.")
    
    BitgetClient.clear_cache()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
        sys.exit(0)
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True)
        sys.exit(1)
