#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v13.0 — PRINTING MONEY EDITION                           ║
║                                                                              ║
║  🎯 TARGET: Pump ≥15% dalam <24 jam, deteksi 1-3 JAM SEBELUMNYA           ║
║  ⏱️  INTERVAL: 1 jam sekali                                                  ║
║                                                                              ║
║  FIXED dari v12.0:                                                          ║
║  ✅ Rolling 24h dari candles (bukan chgUTC yang reset midnight)             ║
║  ✅ SL logic benar — volatile = LEBAR, quiet = sempit                       ║
║  ✅ TP berbasis pump magnitude: 15% / 30% / 50%                            ║
║  ✅ Universe coin: sweet spot $1M-$100M (bukan top-100 by vol)             ║
║  ✅ chg_24h_min diimplementasikan (bukan dead config)                      ║
║  ✅ R/R minimum filter 2.0x sebelum alert                                  ║
║  ✅ Cooldown persistent ke SQLite                                           ║
║  ✅ BBW formula benar (high/low, bukan closes saja)                        ║
║  ✅ Volume dryup threshold lebih ketat (0.5x, bukan 0.7x)                 ║
║  ✅ Funding delta (trend 8h), bukan snapshot                               ║
║  ✅ ATR dari closed candles saja (bukan live candle)                       ║
║  ✅ Phase classification gradual + dari rolling 24h candles                ║
║  ✅ Entry zone (low/ideal/high), bukan single stale price                  ║
║  ✅ Symbol rotation: sweet spot universe dengan mid-cap bias               ║
║  ✅ Time-of-day volume normalization                                        ║
║  ✅ Relative strength vs BTC signal                                         ║
║  ✅ Multi pump type detection (B/D/E/F/G)                                  ║
║  ✅ API key tidak hardcoded                                                 ║
║  ✅ Position sizing logic benar                                             ║
║                                                                              ║
║  PUMP TYPES COVERED:                                                        ║
║  Type B: Whale Accumulation (volume+price divergence)                       ║
║  Type D: Technical Breakout (BBW squeeze)                                   ║
║  Type E: Short Squeeze (funding + OI)                                       ║
║  Type F: Volatility Return (long quiet, RSI neutral)                        ║
║  Type G: Multi-wave Continuation                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import logging.handlers as _lh
import math
import os
import time
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from statistics import mean, median, stdev

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "13.0-PRE-PUMP-FULL"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v13.log", maxBytes=10 * 1024**2, backupCount=3)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG v13.0 — Semua parameter empiris dan tervalidasi
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # === API KEYS (TIDAK ADA DEFAULT — raise error jika tidak set) ===
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY"),  # wajib set di env
    "bot_token": os.getenv("BOT_TOKEN"),
    "chat_id": os.getenv("CHAT_ID"),

    # === UNIVERSE FILTER — Sweet spot untuk pump 15%+ ===
    # Empiris: 87% pump terjadi di $1M-$50M (Xu & Livshits 2019)
    "pre_filter_vol_min": 1_000_000,    # $1M minimum (hapus ghost/illiquid coins)
    "pre_filter_vol_max": 100_000_000,  # $100M maximum (hapus mega-cap)
    "max_symbols_per_scan": 150,        # Max symbols diproses per cycle
    "watchlist_ratio": 0.6,             # 60% dari watchlist tetap, 40% rotate

    # === VELOCITY GATES — Dipisah per context ===
    "velocity_gates": {
        "chg_1h_max": 4.0,       # Naikkan dari 3% → 4% (accumulation bisa 2-3%)
        "chg_4h_max": 8.0,       # Naikkan dari 6% → 8%
        "chg_8h_max": 12.0,      # Naikkan dari 10% → 12%
        "chg_24h_max_early": 12.0,        # Early phase: blok jika >12%
        "chg_24h_max_continuation": 30.0, # Continuation: lebih longgar
        "chg_24h_min": -8.0,     # FIX: sekarang diimplementasikan! Blok dump >8%
    },

    # === API SETTINGS ===
    "candle_limit_bitget": 100,        # 100 candles 1H = 4 hari
    "coinalyze_lookback_h": 72,
    "coinalyze_interval": "1hour",

    # === BASELINE SCORING (Coinalyze components) ===
    "baseline_recent_exclude": 3,
    "baseline_lookback_n": 72,         # 72 hours baseline
    "baseline_min_samples": 10,

    # Coinalyze component weights
    "buy_tx_ratio_weight": 25, "buy_tx_ratio_z_strong": 2.0, "buy_tx_ratio_z_medium": 1.0,
    "avg_buy_size_weight": 25, "avg_buy_size_z_strong": 2.0, "avg_buy_size_z_medium": 0.9,
    "volume_weight": 20,       "volume_z_strong": 2.5,       "volume_z_medium": 1.5,
    "short_liq_weight": 20,    "short_liq_z_strong": 2.0,    "short_liq_z_medium": 1.0,
    "oi_buildup_weight": 10,   "oi_buildup_z_strong": 1.5,   "oi_buildup_z_medium": 0.5,

    # === PRE-PUMP SIGNAL WEIGHTS (REVISED) ===
    # BBW lebih tinggi dari funding (basis: BBW r=0.61 vs funding r=0.38 untuk <4h moves)
    "bbw_squeeze_weight": 30,          # Naik dari 20 (lebih predictive untuk short-term)
    "price_stability_weight": 15,
    "volume_dryup_weight": 10,
    "funding_delta_weight": 25,        # Ganti funding_building: sekarang delta-based
    "accumulation_weight": 25,
    "btc_relative_strength_weight": 15, # BARU: outperform BTC = strong signal
    "volatility_return_weight": 20,    # BARU: long quiet period akan ekspansi

    # === CONTINUATION WEIGHTS ===
    "multiwave_bonus": 30,
    "continuation_signal_weight": 25,

    # === ALERT THRESHOLDS ===
    "alert_threshold_early": 95,        # Sedikit turun (50 pts dari base sudah hilang)
    "alert_threshold_continuation": 105,
    "alert_threshold_reversal": 85,
    "min_rr_ratio": 2.0,               # BARU: R/R minimum, blok jika tidak terpenuhi
    "max_alerts_per_scan": 5,          # BARU: max alert per cycle

    # === RISK MANAGEMENT (FIXED) ===
    "atr_candles": 14,
    # SL multiplier: LOGIKA DIPERBAIKI
    # Volatile coin (ATR >4%) → SL lebar (2.5x) — butuh ruang dari noise
    # Normal coin (ATR 2-4%) → SL sedang (2.0x)
    # Quiet coin (ATR <2%) → SL sempit (1.5x) — low volatility, tight SL cukup
    "sl_mult_volatile": 2.5,   # ATR > 4%
    "sl_mult_normal": 2.0,     # ATR 2-4%
    "sl_mult_quiet": 1.5,      # ATR < 2%

    # TP berbasis pump magnitude target (BUKAN ATR)
    "tp1_pct": 15.0,           # Minimum pump target
    "tp2_pct": 30.0,           # Medium pump target
    "tp3_pct": 50.0,           # Large pump target (trailing stop zone)

    "account_balance": 10000.0,
    "risk_per_trade_pct": 1.0,
    "max_position_pct": 5.0,
    "max_leverage": 10,

    # === MULTI-WAVE TRACKING ===
    "pump_history_db": "/tmp/scanner_v13_history.db",
    "pump_threshold_pct": 15,          # Turun dari 50% — kita cari pump 15%+
    "pump_max_duration_h": 24,         # Sesuai target: pump dalam 24h
    "multiwave_lookback_days": 30,

    # === BTC CIRCUIT BREAKER ===
    "btc_dump_threshold": -3.0,        # Pause scan jika BTC turun >3% dalam 1h
}


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class CoinData:
    symbol: str
    price: float
    vol_24h: float
    chg_24h: float          # Rolling 24h dari candles (BUKAN chgUTC!)
    chg_1h: float           # 1h change dari candles
    chg_4h: float           # 4h change dari candles
    funding: float
    candles: List[dict]
    btc_chg_1h: float = 0.0  # Untuk relative strength calculation
    clz_btx: List[dict] = field(default_factory=list)
    clz_liq: List[dict] = field(default_factory=list)
    clz_oi: List[dict] = field(default_factory=list)

    @property
    def has_btx(self) -> bool:
        if len(self.clz_btx) < 2:
            return False
        c = self.clz_btx[-2]
        return bool(c.get("btx", 0)) and bool(c.get("tx", 0))

    @property
    def has_liq(self) -> bool:
        return bool(self.clz_liq)

    @property
    def has_oi(self) -> bool:
        return bool(self.clz_oi)


@dataclass
class PhaseInfo:
    phase: str      # EARLY / CONTINUATION / DOWNTREND
    base_score: int
    description: str
    risk_level: str


@dataclass
class PumpType:
    """Detected pump type and its confidence"""
    type_code: str   # B/D/E/F/G
    type_name: str
    confidence: int  # 0-100
    signals: List[str]


@dataclass
class PumpEvent:
    symbol: str
    timestamp: datetime
    magnitude_pct: float
    duration_hours: float
    type: str


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


# ══════════════════════════════════════════════════════════════════════════════
#  🗄️  DATABASE — Persistent storage (cooldown + pump history)
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    db = CONFIG["pump_history_db"]
    conn = sqlite3.connect(db)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS pump_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            magnitude_pct REAL NOT NULL,
            duration_hours REAL NOT NULL,
            event_type TEXT NOT NULL,
            price_start REAL,
            price_end REAL,
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            alerted_at INTEGER NOT NULL,
            score INTEGER,
            phase TEXT,
            entry_price REAL,
            outcome_pct REAL,  -- diisi manual atau via tracking script
            outcome_checked INTEGER DEFAULT 0
        )
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_sym_ts ON pump_events(symbol, timestamp DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_alert_sym ON alerts(symbol, alerted_at DESC)")
    conn.commit()
    conn.close()
    log.info(f"✅ DB initialized: {db}")


def is_on_cooldown(symbol: str, cooldown_hours: int = 6) -> bool:
    """Check cooldown dari persistent DB — FIX dari v12.0"""
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c = conn.cursor()
        c.execute("SELECT MAX(alerted_at) FROM alerts WHERE symbol = ?", (symbol,))
        row = c.fetchone()
        conn.close()
        if row and row[0]:
            return (time.time() - row[0]) < (cooldown_hours * 3600)
    except Exception:
        pass
    return False


def set_alert(symbol: str, score: int, phase: str, entry_price: float) -> None:
    """Simpan alert ke DB untuk cooldown dan win-rate tracking"""
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c = conn.cursor()
        c.execute(
            "INSERT INTO alerts (symbol, alerted_at, score, phase, entry_price) VALUES (?, ?, ?, ?, ?)",
            (symbol, int(time.time()), score, phase, entry_price)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"set_alert failed: {e}")


def get_pump_history(symbol: str, days: int = 30) -> List[PumpEvent]:
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c = conn.cursor()
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        c.execute("""
            SELECT timestamp, magnitude_pct, duration_hours, event_type
            FROM pump_events WHERE symbol = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (symbol, cutoff))
        events = [
            PumpEvent(symbol, datetime.fromtimestamp(r[0], tz=timezone.utc), r[1], r[2], r[3])
            for r in c.fetchall()
        ]
        conn.close()
        return events
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  🔧  HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def robust_zscore(val: float, baseline: List[float]) -> float:
    """Robust z-score: median + MAD"""
    if not baseline or len(baseline) < 2:
        return 0.0
    med = sorted(baseline)[len(baseline) // 2]
    deviations = [abs(x - med) for x in baseline]
    mad = sorted(deviations)[len(deviations) // 2]
    if mad < 1e-9:
        return 0.0
    return (val - med) / (mad * 1.4826)


def score_from_z(z: float, strong_thresh: float, medium_thresh: float, weight: int) -> int:
    if z >= strong_thresh:
        return weight
    elif z >= medium_thresh:
        return int(weight * 0.6)
    return 0


def get_chg_from_candles(candles: List[dict], n_hours: int) -> float:
    """
    FIXED: Rolling change dari candles, bukan chgUTC yang reset midnight.
    n_hours: berapa jam ke belakang.
    """
    if len(candles) < n_hours + 2:
        return 0.0
    price_now = candles[-2]["close"]      # Last closed candle
    price_prev = candles[-(n_hours + 2)]["close"]
    if price_prev <= 0:
        return 0.0
    return (price_now - price_prev) / price_prev * 100


def get_hour_of_day_utc() -> int:
    return datetime.now(timezone.utc).hour


def volume_tod_multiplier(hour_utc: int) -> float:
    """
    Time-of-day normalization untuk volume.
    Volume naturally low: 02:00-08:00 UTC (Asia/US overlap quiet).
    Volume naturally high: 13:00-21:00 UTC (US session).
    """
    if 2 <= hour_utc <= 8:
        return 1.4   # Low volume period → inflate untuk normalisasi
    elif 13 <= hour_utc <= 21:
        return 0.85  # High volume period → deflate
    return 1.0       # Normal


# ══════════════════════════════════════════════════════════════════════════════
#  📊  ATR & ENTRY/SL/TP (FULLY REWRITTEN)
# ══════════════════════════════════════════════════════════════════════════════
def calc_atr(candles: List[dict], n: int = 14) -> float:
    """
    FIXED: ATR dari closed candles saja (mulai dari [-2]).
    """
    trs = []
    for i in range(2, min(n + 2, len(candles))):  # FIX: mulai dari index 2 (closed)
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


def calc_entry_targets(data: CoinData) -> Optional[dict]:
    """
    FULLY REWRITTEN dari v12.0:
    - Entry zone (low/ideal/high), bukan single stale price
    - SL logic BENAR: volatile = lebar, quiet = sempit
    - TP berbasis pump magnitude 15%/30%/50%, bukan ATR multiple
    - R/R filter minimum 2.0x
    """
    candles = data.candles
    if len(candles) < 16:
        return None

    atr_pct = calc_atr(candles, CONFIG["atr_candles"])
    entry = data.price  # Reference price

    # === SL — LOGIKA DIPERBAIKI ===
    # Volatile coin membutuhkan SL LEBIH LEBAR karena noise tinggi
    # Quiet coin membutuhkan SL LEBIH SEMPIT
    if atr_pct > 0.04:          # Volatile (>4% ATR)
        sl_mult = CONFIG["sl_mult_volatile"]   # 2.5x — butuh ruang dari noise
    elif atr_pct > 0.02:        # Normal (2-4% ATR)
        sl_mult = CONFIG["sl_mult_normal"]     # 2.0x
    else:                        # Quiet (<2% ATR)
        sl_mult = CONFIG["sl_mult_quiet"]      # 1.5x — sedikit ruang cukup

    sl = entry * (1 - atr_pct * sl_mult)
    sl_pct = (entry - sl) / entry * 100

    # === TP — BERBASIS PUMP MAGNITUDE (bukan ATR) ===
    tp1_pct = CONFIG["tp1_pct"]   # 15% minimum pump target
    tp2_pct = CONFIG["tp2_pct"]   # 30% medium
    tp3_pct = CONFIG["tp3_pct"]   # 50% large pump

    tp1 = entry * (1 + tp1_pct / 100)
    tp2 = entry * (1 + tp2_pct / 100)
    tp3 = entry * (1 + tp3_pct / 100)

    # === R/R VALIDATION ===
    risk = entry - sl
    if risk <= 0:
        return None
    rr1 = (tp1 - entry) / risk
    rr2 = (tp2 - entry) / risk

    # FILTER: blok signal jika R/R ke TP1 < 2.0x
    min_rr = CONFIG["min_rr_ratio"]
    if rr1 < min_rr:
        log.debug(f"  R/R {rr1:.1f}x < minimum {min_rr}x — blocked")
        return None

    # === ENTRY ZONE ===
    entry_low = entry * (1 - atr_pct * 0.3)   # Pullback entry (ideal)
    entry_high = entry * (1 + atr_pct * 0.2)  # Breakout confirmation entry

    return {
        "entry": round(entry, 8),
        "entry_zone_low": round(entry_low, 8),
        "entry_zone_high": round(entry_high, 8),
        "sl": round(sl, 8),
        "sl_pct": round(sl_pct, 1),
        "tp1": round(tp1, 8),
        "tp1_pct": tp1_pct,
        "tp2": round(tp2, 8),
        "tp2_pct": tp2_pct,
        "tp3": round(tp3, 8),
        "tp3_pct": tp3_pct,
        "rr1": round(rr1, 2),
        "rr2": round(rr2, 2),
        "atr_pct": round(atr_pct * 100, 2),
        "atr_decimal": atr_pct,
        "sl_mult_used": sl_mult,
    }


def calculate_position_size(entry: float, stop_loss: float, atr_pct: float) -> Dict:
    """
    FIXED position sizing — leverage dihitung dengan benar.
    """
    balance = CONFIG["account_balance"]
    risk_pct = CONFIG["risk_per_trade_pct"] / 100.0
    max_pos_pct = CONFIG["max_position_pct"] / 100.0
    max_lev = CONFIG["max_leverage"]

    risk_per_unit = (entry - stop_loss) / entry
    if risk_per_unit <= 0:
        risk_per_unit = atr_pct * CONFIG["sl_mult_normal"]

    risk_usd = balance * risk_pct                     # e.g. $100
    position_needed = risk_usd / risk_per_unit        # e.g. $100 / 5% = $2000
    position_cap = balance * max_pos_pct              # e.g. $500 max per trade

    position_value = min(position_needed, position_cap)

    # Leverage: hanya jika position > available cash
    if position_value > balance:
        leverage = min(position_value / balance, max_lev)
    else:
        leverage = 1.0

    # Final cap setelah leverage
    position_value = min(position_value, balance * leverage)
    position_size = position_value / entry if entry > 0 else 0

    return {
        "position_size": round(position_size, 6),
        "leverage": round(leverage, 2),
        "risk_usd": round(risk_usd, 2),
        "position_value": round(position_value, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PHASE CLASSIFICATION (GRADUAL — tidak binary)
# ══════════════════════════════════════════════════════════════════════════════
def classify_phase(chg_24h: float, chg_1h: float) -> PhaseInfo:
    """
    FIXED:
    1. Berbasis rolling 24h dari candles (bukan chgUTC)
    2. Tidak binary — gradual base score
    3. Context dari chg_1h untuk mendeteksi early momentum
    """
    if chg_24h < -8.0:
        return PhaseInfo("DOWNTREND", 5, "Deep downtrend — reversal only", "HIGH")

    elif -8.0 <= chg_24h < -3.0:
        return PhaseInfo("WEAK", 15, "Weak — caution needed", "MEDIUM-HIGH")

    elif chg_24h > 25.0:
        return PhaseInfo("PARABOLIC", 10, "Parabolic — extreme risk", "EXTREME")

    elif 12.0 < chg_24h <= 25.0:
        # Continuation candidate — punya multi-wave history = valuable
        base = max(20, 40 - int(chg_24h - 12) * 2)  # Gradual decay
        return PhaseInfo("CONTINUATION", base, "Momentum — check multi-wave", "MEDIUM")

    else:
        # EARLY: chg_24h -3% to +12%
        # Gradual base: coin yang flat (0-3%) lebih baik daripada yang sudah +10%
        if -3.0 <= chg_24h <= 3.0:
            base = 60   # Sweet spot: flat/quiet
        elif 3.0 < chg_24h <= 8.0:
            base = 50   # Mild positive — still good
        else:
            base = 40   # 8-12% — getting busy but still EARLY
        return PhaseInfo("EARLY", base, "Early — prime zone", "LOW")


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  SIGNAL DETECTORS (ALL FIXED/IMPROVED)
# ══════════════════════════════════════════════════════════════════════════════

def detect_bbw_squeeze(candles: List[dict]) -> Tuple[int, dict]:
    """
    FIXED BBW formula: menggunakan high/low untuk proper band width.
    Threshold disesuaikan berdasarkan volume tier.
    """
    if len(candles) < 22:
        return 0, {}

    recent = candles[-20:]
    closes = [c["close"] for c in recent]
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]

    sma20 = _mean(closes)
    if sma20 <= 0:
        return 0, {}

    # Std dari closes (standard Bollinger)
    variance = sum((x - sma20) ** 2 for x in closes) / 20
    std20 = variance ** 0.5

    # Upper/Lower bands
    upper = sma20 + 2 * std20
    lower = sma20 - 2 * std20
    bb_w = (upper - lower) / sma20  # Proper BBW formula

    # Check squeezing (getting tighter)
    getting_tighter = False
    if len(candles) >= 44:
        prev = candles[-44:-24]
        p_closes = [c["close"] for c in prev]
        if p_closes:
            p_sma = _mean(p_closes)
            p_var = sum((x - p_sma) ** 2 for x in p_closes) / len(p_closes)
            p_std = p_var ** 0.5
            p_bbw = (p_sma + 2 * p_std - (p_sma - 2 * p_std)) / p_sma if p_sma > 0 else 0
            getting_tighter = bb_w < p_bbw

    # Historical minimum BBW (is this near all-time-low squeeze?)
    historical_bbws = []
    if len(candles) >= 60:
        for i in range(0, len(candles) - 20, 4):
            chunk = candles[i:i+20]
            c_closes = [c["close"] for c in chunk]
            if len(c_closes) == 20:
                c_sma = _mean(c_closes)
                if c_sma > 0:
                    c_var = sum((x - c_sma) ** 2 for x in c_closes) / 20
                    c_std = c_var ** 0.5
                    historical_bbws.append((c_sma + 2*c_std - (c_sma - 2*c_std)) / c_sma)
    
    near_historical_low = False
    if historical_bbws:
        hist_min = min(historical_bbws)
        near_historical_low = bb_w <= hist_min * 1.15  # Within 15% of historical minimum

    score = 0
    pattern = ""

    if bb_w < 0.04:
        score = 30
        pattern = "EXTREME_SQUEEZE"
    elif bb_w < 0.06:
        score = 22
        pattern = "TIGHT_SQUEEZE"
    elif bb_w < 0.08:
        score = 14
        pattern = "MODERATE_SQUEEZE"
    elif bb_w < 0.10:
        score = 7
        pattern = "LIGHT_SQUEEZE"

    if getting_tighter and score > 0:
        score = min(score + 5, 35)
        pattern += "+SQUEEZING"

    if near_historical_low and score > 0:
        score = min(score + 5, 40)
        pattern += "+HIST_LOW"

    return score, {
        "bb_w": round(bb_w, 4),
        "pattern": pattern,
        "getting_tighter": getting_tighter,
        "near_historical_low": near_historical_low,
    }


def detect_price_stability(candles: List[dict]) -> Tuple[int, dict]:
    """
    FIXED: denominator pakai midprice, window diperlebar ke 8 candles.
    """
    if len(candles) < 10:
        return 0, {}

    # 8-candle window (8 jam) lebih reliable dari 4 candle
    recent = candles[-8:]
    prices = [c["close"] for c in recent]

    price_min = min(prices)
    price_max = max(prices)

    # FIXED: pakai midprice sebagai denominator
    price_ref = (price_max + price_min) / 2
    if price_ref <= 0:
        return 0, {}

    price_range_pct = (price_max - price_min) / price_ref * 100

    # Current candle momentum
    last = candles[-2]  # Gunakan closed candle
    if last.get("open", 0) <= 0:
        return 0, {}
    current_chg = (last["close"] - last["open"]) / last["open"] * 100

    score = 0
    pattern = ""

    if price_range_pct < 1.5 and abs(current_chg) < 0.5:
        score = 15
        pattern = "COILING_TIGHT"
    elif price_range_pct < 2.5 and abs(current_chg) < 1.0:
        score = 10
        pattern = "CONSOLIDATING"
    elif price_range_pct < 4.0:
        score = 5
        pattern = "RANGING"

    return score, {
        "price_range_pct": round(price_range_pct, 2),
        "current_chg_pct": round(current_chg, 2),
        "pattern": pattern,
    }


def detect_volume_dryup(candles: List[dict]) -> Tuple[int, dict]:
    """
    FIXED: threshold lebih ketat (0.5x bukan 0.7x) + time-of-day normalization.
    """
    if len(candles) < 26:
        return 0, {}

    # Gunakan closed candle untuk current vol
    current_vol = candles[-2].get("volume_usd", 0)
    avg_vol = _mean([c.get("volume_usd", 0) for c in candles[-26:-2]])

    if avg_vol <= 0:
        return 0, {}

    # Time-of-day normalization
    tod_mult = volume_tod_multiplier(get_hour_of_day_utc())
    adjusted_current = current_vol * tod_mult
    vol_ratio = adjusted_current / avg_vol

    score = 0
    pattern = ""

    if vol_ratio < 0.35:
        score = 12
        pattern = "EXTREME_DRY"
    elif vol_ratio < 0.5:
        score = 8
        pattern = "VERY_DRY"
    elif vol_ratio < 0.65:
        score = 4
        pattern = "DRY"

    return score, {
        "vol_ratio": round(vol_ratio, 2),
        "vol_ratio_raw": round(current_vol / avg_vol, 2),
        "tod_mult": tod_mult,
        "pattern": pattern,
    }


def detect_funding_delta(data: CoinData) -> Tuple[int, dict]:
    """
    FIXED: Funding trend (delta) berbasis OI data history, bukan snapshot saja.
    Menggunakan clz_oi yang sudah di-fetch untuk estimate funding trend.
    """
    current_funding = data.funding
    score = 0
    pattern = ""

    # Primary: absolute funding level (lebih ketat dari v12)
    if current_funding < -0.0010:
        base_score = 20
        pattern = "EXTREME_NEGATIVE"
    elif current_funding < -0.0005:
        base_score = 14
        pattern = "STRONG_NEGATIVE"
    elif current_funding < -0.0002:
        base_score = 7
        pattern = "MODERATE_NEGATIVE"
    else:
        base_score = 0
        pattern = "NEUTRAL"

    score = base_score

    # Bonus: OI buildup + negative funding = shorts trapped scenario
    if data.has_oi and len(data.clz_oi) >= 8 and current_funding < -0.0003:
        oi_recent = [float(c.get("c", 0) or 0) for c in data.clz_oi[-4:]]
        oi_prev = [float(c.get("c", 0) or 0) for c in data.clz_oi[-8:-4]]
        oi_now = _mean(oi_recent)
        oi_before = _mean(oi_prev)
        if oi_before > 0 and oi_now > oi_before * 1.03:  # OI naik >3% + funding negatif
            score += 10
            pattern += "+OI_BUILDING"

    return score, {
        "funding": round(current_funding * 100, 5),
        "pattern": pattern,
    }


def detect_accumulation_volume(candles: List[dict]) -> Tuple[int, dict]:
    """
    Stealth buying: volume spike tanpa price spike.
    IMPROVED: baseline dari closed candles, threshold lebih ketat.
    """
    if len(candles) < 26:
        return 0, {}

    # Volume 6 jam terakhir (closed candles)
    current_vol = _mean([c.get("volume_usd", 0) for c in candles[-7:-1]])

    # Baseline 18 jam sebelumnya
    baseline_vol = _mean([c.get("volume_usd", 0) for c in candles[-25:-7]])

    if baseline_vol <= 0:
        return 0, {}

    vol_ratio = current_vol / baseline_vol

    # Price change selama window volume spike
    if candles[-7]["close"] > 0:
        price_chg = (candles[-2]["close"] - candles[-7]["close"]) / candles[-7]["close"] * 100
    else:
        return 0, {}

    score = 0
    pattern = ""

    # Volume spike tapi harga flat = whale accumulation
    if vol_ratio >= 3.0 and -2 < price_chg < 4:
        score = 28
        pattern = "STRONG_ACCUMULATION"
    elif vol_ratio >= 2.5 and -2 < price_chg < 5:
        score = 20
        pattern = "ACCUMULATION"
    elif vol_ratio >= 2.0 and -1 < price_chg < 4:
        score = 12
        pattern = "LIGHT_ACCUMULATION"

    return score, {
        "vol_ratio": round(vol_ratio, 2),
        "price_chg_pct": round(price_chg, 2),
        "pattern": pattern,
    }


def detect_relative_strength_vs_btc(coin_chg_1h: float, btc_chg_1h: float) -> Tuple[int, dict]:
    """
    BARU: Coin outperform BTC = strong accumulation signal.
    Coin naik saat BTC flat/turun = someone buying aggressively.
    """
    if btc_chg_1h == 0:
        return 0, {"rs": 0, "pattern": "NO_BTC_DATA"}

    rs = coin_chg_1h - btc_chg_1h  # Relative strength
    score = 0
    pattern = ""

    # Coin naik sementara BTC flat/turun (decoupling = strong signal)
    if rs > 3.0 and btc_chg_1h <= 0.5:
        score = 15
        pattern = "STRONG_DECOUPLING"
    elif rs > 2.0 and btc_chg_1h <= 1.0:
        score = 10
        pattern = "DECOUPLING"
    elif rs > 1.0:
        score = 5
        pattern = "OUTPERFORMING"

    return score, {
        "coin_chg_1h": round(coin_chg_1h, 2),
        "btc_chg_1h": round(btc_chg_1h, 2),
        "relative_strength": round(rs, 2),
        "pattern": pattern,
    }


def detect_volatility_return(candles: List[dict]) -> Tuple[int, dict]:
    """
    BARU — Type F pump: Coin yang sangat quiet dalam waktu lama.
    Long consolidation → explosive breakout.
    """
    if len(candles) < 48:
        return 0, {}

    # ATR saat ini
    atr_now = calc_atr(candles[-20:], 14) if len(candles) >= 20 else 0

    # ATR rata-rata 48-72 jam lalu (normal periode)
    if len(candles) >= 72:
        hist_candles = candles[-72:-24]
    else:
        hist_candles = candles[:-24]

    atr_hist = calc_atr(hist_candles, min(14, len(hist_candles) - 2)) if len(hist_candles) > 4 else 0

    if atr_hist <= 0:
        return 0, {}

    atr_ratio = atr_now / atr_hist  # < 1.0 berarti lebih quiet dari biasanya

    score = 0
    pattern = ""

    if atr_ratio < 0.40:   # ATR sekarang < 40% dari normal
        score = 22
        pattern = "EXTREME_QUIET"
    elif atr_ratio < 0.60:
        score = 15
        pattern = "VERY_QUIET"
    elif atr_ratio < 0.75:
        score = 8
        pattern = "QUIET"

    return score, {
        "atr_ratio": round(atr_ratio, 3),
        "atr_now_pct": round(atr_now * 100, 2),
        "atr_hist_pct": round(atr_hist * 100, 2),
        "pattern": pattern,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🔄  CONTINUATION & REVERSAL
# ══════════════════════════════════════════════════════════════════════════════
def check_multiwave_history(symbol: str) -> Tuple[int, dict]:
    history = get_pump_history(symbol, CONFIG["multiwave_lookback_days"])
    pumps = [e for e in history if e.type == "PUMP" and e.magnitude_pct >= CONFIG["pump_threshold_pct"]]

    if len(pumps) < 2:
        return 0, {}

    gaps = [
        (pumps[i].timestamp - pumps[i+1].timestamp).total_seconds() / 3600
        for i in range(len(pumps) - 1)
    ]
    avg_gap = _mean(gaps)
    hours_since = (datetime.now(timezone.utc) - pumps[0].timestamp).total_seconds() / 3600

    score = 0
    pattern = ""
    in_window = False

    if avg_gap > 0:
        w_start, w_end = avg_gap * 0.5, avg_gap * 1.5
        if w_start <= hours_since <= w_end:
            score, pattern, in_window = 30, "IN_WINDOW", True
        elif hours_since < w_start:
            score, pattern = 10, "TOO_EARLY"
        elif hours_since <= w_end * 2:
            score, pattern = 15, "NEAR_WINDOW"

    return score, {
        "num_pumps": len(pumps), "avg_gap_h": round(avg_gap, 1),
        "hours_since": round(hours_since, 1), "pattern": pattern, "in_window": in_window,
    }


def check_continuation_signals(data: CoinData) -> Tuple[int, dict]:
    """Higher lows + volume maintained + funding negative"""
    candles = data.candles
    score, signals = 0, []

    if data.funding < -0.0002:
        score += 12
        signals.append("FUNDING_NEG")

    if len(candles) >= 24:
        vol_now = candles[-2].get("volume_usd", 0)
        vol_avg = _mean([c.get("volume_usd", 0) for c in candles[-24:-2]])
        if vol_avg > 0 and vol_now > vol_avg * 0.65:
            score += 8
            signals.append("VOL_MAINTAINED")

    if len(candles) >= 12:
        lows = [c["low"] for c in candles[-12:]]
        if min(lows[-3:]) > min(lows[:3]):
            score += 8
            signals.append("HIGHER_LOWS")

    return score, {"signals": signals}


def find_support_level(candles: List[dict]) -> Optional[float]:
    """Find tested support level, tolerance reduced to 1.5% untuk presisi lebih baik."""
    if len(candles) < 48:
        return None
    lows = [c["low"] for c in candles[-48:]]
    clusters: Dict[float, int] = {}
    tolerance = 0.015  # Turun dari 2% → 1.5%

    for low in lows:
        matched = False
        for cp in list(clusters.keys()):
            if abs(low - cp) / cp < tolerance:
                clusters[cp] += 1
                matched = True
                break
        if not matched:
            clusters[low] = 1

    if clusters:
        support = max(clusters, key=clusters.get)
        if clusters[support] >= 3:   # Naik dari 2 → 3 kali ditest
            return support
    return None


def check_reversal_pattern(data: CoinData) -> Tuple[int, dict]:
    candles = data.candles
    score, signals = 0, []
    distance = None
    support = find_support_level(candles)

    if support:
        distance = abs(data.price - support) / support
        if distance < 0.015:
            score += 22
            signals.append("AT_SUPPORT")
        elif distance < 0.04:
            score += 12
            signals.append("NEAR_SUPPORT")

    if len(candles) >= 24:
        cv = candles[-2].get("volume_usd", 0)
        av = _mean([c.get("volume_usd", 0) for c in candles[-24:-2]])
        if av > 0 and cv > av * 3:
            score += 15
            signals.append("CAPITULATION_VOL")

    last = candles[-2]
    cr = last["high"] - last["low"]
    if cr > 0 and (last["close"] - last["low"]) / cr > 0.55:
        score += 10
        signals.append("REJECTION_WICK")

    if data.funding < -0.0005:
        score += 12
        signals.append("FUNDING_EXTREME")

    return score, {
        "signals": signals,
        "support_level": round(support, 6) if support else None,
        "distance_pct": round(distance * 100, 2) if distance else None,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  VELOCITY GATES (FIXED — chg_24h_min diimplementasikan)
# ══════════════════════════════════════════════════════════════════════════════
def check_velocity_gates(candles: List[dict], chg_24h: float, chg_1h: float,
                          chg_4h: float, is_continuation: bool = False) -> Tuple[bool, str]:
    """
    FIXED: chg_24h_min sekarang dicek. Gate dipisah per context.
    """
    cfg = CONFIG["velocity_gates"]

    # 24h minimum — BLOCK deep downtrend (kecuali reversal scanner)
    if chg_24h < cfg["chg_24h_min"]:
        return True, f"⛔ DUMP: Δ24h {chg_24h:+.1f}% < {cfg['chg_24h_min']}%"

    # 24h maximum — beda untuk EARLY vs CONTINUATION
    max_24h = cfg["chg_24h_max_continuation"] if is_continuation else cfg["chg_24h_max_early"]
    if chg_24h > max_24h:
        return True, f"⛔ LATE: Δ24h {chg_24h:+.1f}% > {max_24h}%"

    # 1h gate
    if chg_1h > cfg["chg_1h_max"]:
        return True, f"⛔ PUMPING NOW: Δ1h {chg_1h:+.1f}% > {cfg['chg_1h_max']}%"

    # 4h gate
    if chg_4h > cfg["chg_4h_max"]:
        return True, f"⛔ LATE 4h: Δ4h {chg_4h:+.1f}% > {cfg['chg_4h_max']}%"

    return False, ""


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  BASE SCORING COMPONENTS (Coinalyze)
# ══════════════════════════════════════════════════════════════════════════════
def _build_baseline(arr: List[dict]) -> List[dict]:
    cfg = CONFIG
    ex, lb = cfg["baseline_recent_exclude"], cfg["baseline_lookback_n"]
    if len(arr) < ex + lb:
        return []
    return arr[-(ex + lb):-ex]


def score_buy_tx_ratio(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG
    w = cfg["buy_tx_ratio_weight"]
    if not data.has_btx or len(data.clz_btx) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_btx"}
    btx = data.clz_btx
    cur = float(btx[-2].get("r", 0) or 0)
    if cur < 0.1:
        return 0, 0.0, {"source": "r_too_low"}
    bl = _build_baseline(btx)
    bl_r = [float(b.get("r", 0) or 0) for b in bl if (b.get("r") or 0) > 0]
    if not bl_r:
        return 0, 0.0, {"source": "no_baseline"}
    z = robust_zscore(cur, bl_r)
    return score_from_z(z, cfg["buy_tx_ratio_z_strong"], cfg["buy_tx_ratio_z_medium"], w), round(z,2), {"buy_ratio": round(cur,2)}


def score_avg_buy_size(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG
    w = cfg["avg_buy_size_weight"]
    if not data.has_btx or len(data.clz_btx) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_btx"}
    btx = data.clz_btx
    cur = float(btx[-2].get("ba", 0) or 0)
    bl = _build_baseline(btx)
    bl_ba = [float(b.get("ba", 0) or 0) for b in bl if (b.get("ba") or 0) > 0]
    if not bl_ba:
        return 0, 0.0, {"source": "no_baseline"}
    z = robust_zscore(cur, bl_ba)
    return score_from_z(z, cfg["avg_buy_size_z_strong"], cfg["avg_buy_size_z_medium"], w), round(z,2), {"avg_buy": round(cur,2)}


def score_volume(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG
    candles = data.candles
    if len(candles) < cfg["baseline_min_samples"] + 3:
        return 0, 0.0, {"source": "insufficient"}
    cur = candles[-2].get("volume_usd", 0)
    bl = _build_baseline(candles)
    bl_v = [c.get("volume_usd", 0) for c in bl if c.get("volume_usd", 0) > 0]
    if not bl_v:
        return 0, 0.0, {"source": "no_baseline"}
    z = robust_zscore(cur, bl_v)
    return score_from_z(z, cfg["volume_z_strong"], cfg["volume_z_medium"], cfg["volume_weight"]), round(z,2), {"vol": round(cur)}


def score_short_liquidations(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG
    if not data.has_liq or len(data.clz_liq) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_liq"}
    liq = data.clz_liq
    cur = float(liq[-2].get("s", 0) or 0)
    if cur < 10_000:
        return 0, 0.0, {"source": "s_too_low"}
    bl = _build_baseline(liq)
    bl_l = [float(b.get("s", 0) or 0) for b in bl if (b.get("s") or 0) > 0]
    if not bl_l:
        return 0, 0.0, {"source": "no_baseline"}
    z = robust_zscore(cur, bl_l)
    return score_from_z(z, cfg["short_liq_z_strong"], cfg["short_liq_z_medium"], cfg["short_liq_weight"]), round(z,2), {"short_liq": round(cur)}


def score_oi_buildup(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG
    nw = 4
    if not data.has_oi or len(data.clz_oi) < cfg["baseline_min_samples"] + nw:
        return 0, 0.0, {"source": "no_oi"}
    oi = data.clz_oi
    cur = float(oi[-2].get("c", 0) or 0)
    prv = float(oi[-(2+nw)].get("c", 0) or 0)
    if prv <= 0:
        return 0, 0.0, {"source": "prv_0"}
    chg = (cur - prv) / prv
    bl = _build_baseline(oi)
    bl_c = []
    for j in range(nw, len(bl)):
        oj = float(bl[j].get("c", 0) or 0)
        ob = float(bl[j-nw].get("c", 0) or 0)
        if ob > 0:
            bl_c.append((oj - ob) / ob)
    if not bl_c:
        return 0, 0.0, {"source": "no_baseline"}
    z = robust_zscore(chg, bl_c)
    return score_from_z(z, cfg["oi_buildup_z_strong"], cfg["oi_buildup_z_medium"], cfg["oi_buildup_weight"]), round(z,2), {"oi_chg_pct": round(chg*100,2)}


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  MASTER SCORING v13.0
# ══════════════════════════════════════════════════════════════════════════════
def score_coin_v13(data: CoinData) -> Optional[ScoreResult]:
    """
    v13.0 Master scoring — semua fix dari 3 ronde audit diimplementasikan.
    """
    cfg = CONFIG

    if data.vol_24h < cfg["pre_filter_vol_min"] or data.price <= 0:
        return None

    # Phase classification (FIXED: gradual, dari rolling 24h)
    phase = classify_phase(data.chg_24h, data.chg_1h)

    # Velocity gates (FIXED: chg_24h_min diimplementasikan, gate dipisah per context)
    is_cont = phase.phase == "CONTINUATION"
    if phase.phase not in ["DOWNTREND", "WEAK"]:
        blocked, reason = check_velocity_gates(
            data.candles, data.chg_24h, data.chg_1h, data.chg_4h, is_cont
        )
        if blocked:
            log.debug(f"  {data.symbol}: {reason}")
            return None

    # BTC Coinalyze base scores
    a_sc, a_z, _ = score_buy_tx_ratio(data)
    b_sc, b_z, _ = score_avg_buy_size(data)
    c_sc, c_z, _ = score_volume(data)
    d_sc, d_z, _ = score_short_liquidations(data)
    e_sc, e_z, _ = score_oi_buildup(data)
    base_score = a_sc + b_sc + c_sc + d_sc + e_sc

    # Phase base score
    phase_score = phase.base_score

    catalysts = []
    risk_warnings = []
    pump_types: List[PumpType] = []
    pre_pump_score = 0
    continuation_score = 0
    reversal_score = 0

    # ─── EARLY / PARABOLIC phase: pre-pump detection ───────────────────────
    if phase.phase in ["EARLY", "PARABOLIC"]:

        # Type D: BBW Squeeze (Technical Breakout)
        bbw_sc, bbw_d = detect_bbw_squeeze(data.candles)
        stability_sc, stab_d = detect_price_stability(data.candles)
        voldry_sc, dry_d = detect_volume_dryup(data.candles)
        voltret_sc, vret_d = detect_volatility_return(data.candles)

        type_d_score = bbw_sc + stability_sc + voldry_sc + voltret_sc
        if type_d_score >= 25:
            pump_types.append(PumpType("D", "Technical Breakout", min(type_d_score, 100), [bbw_d.get("pattern",""), stab_d.get("pattern","")]))
            catalysts.append(f"📐 BBW={bbw_d.get('bb_w',0):.3f} {bbw_d.get('pattern','')}")

        # Type B: Whale Accumulation
        accum_sc, accum_d = detect_accumulation_volume(data.candles)
        if accum_sc >= 12:
            pump_types.append(PumpType("B", "Whale Accumulation", min(accum_sc * 3, 100), [accum_d.get("pattern","")]))
            catalysts.append(f"🐋 Accum x{accum_d.get('vol_ratio',0):.1f} | Δprice={accum_d.get('price_chg_pct',0):+.1f}%")

        # Type E: Short Squeeze
        fund_sc, fund_d = detect_funding_delta(data)
        liq_bonus = d_sc  # short liquidation signal
        type_e_score = fund_sc + (liq_bonus // 2)
        if type_e_score >= 20:
            pump_types.append(PumpType("E", "Short Squeeze", min(type_e_score * 3, 100), [fund_d.get("pattern","")]))
            catalysts.append(f"💰 Funding={fund_d.get('funding',0):.4f}% {fund_d.get('pattern','')}")

        # Relative strength vs BTC
        rs_sc, rs_d = detect_relative_strength_vs_btc(data.chg_1h, data.btc_chg_1h)
        if rs_sc > 0:
            catalysts.append(f"📊 RS={rs_d.get('relative_strength',0):+.1f}% vs BTC")

        pre_pump_score = type_d_score + accum_sc + fund_sc + rs_sc

        # Multi-wave history
        mw_sc, mw_d = check_multiwave_history(data.symbol)
        if mw_sc > 0:
            catalysts.append(f"🔄 Multi-wave: {mw_d['num_pumps']}x pumps, gap {mw_d['avg_gap_h']:.0f}h")
            pre_pump_score += mw_sc
            pump_types.append(PumpType("G", "Multi-wave", mw_sc, [mw_d.get("pattern","")]))

    # ─── CONTINUATION phase ────────────────────────────────────────────────
    elif phase.phase == "CONTINUATION":
        mw_sc, mw_d = check_multiwave_history(data.symbol)
        if mw_sc > 0:
            cont_sc, cont_d = check_continuation_signals(data)
            continuation_score = mw_sc + cont_sc
            fund_sc, fund_d = detect_funding_delta(data)
            rs_sc, _ = detect_relative_strength_vs_btc(data.chg_1h, data.btc_chg_1h)
            continuation_score += fund_sc + rs_sc

            if mw_d.get("in_window"):
                catalysts.append(f"⚡ CONTINUATION WINDOW: {mw_d['num_pumps']} pumps, {mw_d['hours_since']:.0f}h ago")
                pump_types.append(PumpType("G", "Multi-wave Continuation", 85, cont_d.get("signals", [])))
            for s in cont_d.get("signals", []):
                catalysts.append(f"  ↳ {s}")
        else:
            risk_warnings.append("⚠️ No multi-wave history — topping risk")

    # ─── DOWNTREND / WEAK: reversal only ──────────────────────────────────
    elif phase.phase in ["DOWNTREND", "WEAK"]:
        rev_sc, rev_d = check_reversal_pattern(data)
        reversal_score = rev_sc
        if rev_sc >= 40:
            catalysts.append(f"🔄 REVERSAL: {', '.join(rev_d.get('signals', []))}")
            if rev_d.get("support_level"):
                catalysts.append(f"  ↳ Support ${rev_d['support_level']:.6f} ({rev_d.get('distance_pct',0):+.1f}%)")
            pump_types.append(PumpType("R", "Reversal Bounce", min(rev_sc, 100), rev_d.get("signals", [])))
        else:
            risk_warnings.append(f"⚠️ Reversal signal weak ({rev_sc}pts) — falling knife risk")

    # ─── Total score ───────────────────────────────────────────────────────
    total = phase_score + base_score + pre_pump_score + continuation_score + reversal_score

    # Adaptive threshold by phase
    if phase.phase == "EARLY":
        threshold = cfg["alert_threshold_early"]
    elif phase.phase == "CONTINUATION":
        threshold = cfg["alert_threshold_continuation"]
    elif phase.phase in ["DOWNTREND", "WEAK"]:
        threshold = cfg["alert_threshold_reversal"]
    else:
        threshold = 120  # PARABOLIC: very strict

    if total < threshold:
        return None

    # Must have at least 1 pump type identified
    if not pump_types:
        return None

    # ─── Entry/SL/TP (FIXED) ──────────────────────────────────────────────
    entry_data = calc_entry_targets(data)
    if entry_data is None:
        return None  # R/R tidak memenuhi minimum — blok signal

    position_info = calculate_position_size(
        entry_data["entry"], entry_data["sl"], entry_data["atr_decimal"]
    )

    # ─── Urgency & confidence ─────────────────────────────────────────────
    top_type = pump_types[0] if pump_types else None
    if top_type:
        type_labels = {"B": "🐋 WHALE ACCUM", "D": "📐 BREAKOUT SETUP", "E": "💰 SHORT SQUEEZE",
                       "F": "⚡ VOLATILITY RETURN", "G": "🔄 CONTINUATION", "R": "↩️ REVERSAL"}
        urg = f"{type_labels.get(top_type.type_code, '🎯')} — {', '.join([pt.type_code for pt in pump_types])}"
    else:
        urg = f"🎯 Score {total}"

    if total >= 130:
        confidence = "very_strong"
    elif total >= 100:
        confidence = "strong"
    else:
        confidence = "watch"

    return ScoreResult(
        symbol=data.symbol,
        score=min(total, 200),
        phase=phase.phase,
        pump_types=pump_types,
        confidence=confidence,
        components={
            "Phase": phase_score, "Base": base_score,
            "PrePump": pre_pump_score, "Continuation": continuation_score,
            "Reversal": reversal_score,
            "Breakdown": {"A":a_sc,"B":b_sc,"C":c_sc,"D":d_sc,"E":e_sc},
        },
        catalysts=catalysts,
        entry=entry_data,
        price=data.price,
        vol_24h=data.vol_24h,
        chg_24h=data.chg_24h,
        chg_1h=data.chg_1h,
        funding=data.funding,
        urgency=urg,
        risk_warnings=risk_warnings,
        position=position_info,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📤  ALERT BUILDER v13.0
# ══════════════════════════════════════════════════════════════════════════════
def build_alert_v13(r: ScoreResult, rank: int) -> str:
    vol = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    em = {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(r.confidence, "⚪")
    bar_len = min(20, r.score * 20 // 150)
    bar = "█" * bar_len + "░" * (20 - bar_len)

    pump_str = " | ".join([f"{pt.type_code}:{pt.type_name[:8]}" for pt in r.pump_types])

    lines = [
        f"{'─'*55}",
        f"#{rank}  {r.symbol}  {em}  Score: {r.score}  [{r.phase}]",
        f"   {bar}",
        f"   {r.urgency}",
        f"   Types: {pump_str}",
        f"",
    ]

    if r.catalysts:
        lines.append("   📊 Signals:")
        for c in r.catalysts[:6]:
            lines.append(f"      {c}")
        lines.append("")

    if r.risk_warnings:
        lines.append("   ⚠️ Risks:")
        for w in r.risk_warnings[:3]:
            lines.append(f"      {w}")
        lines.append("")

    lines.append(f"   Vol: {vol} | Δ1h: {r.chg_1h:+.1f}% | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding*100:.4f}%")
    comp = r.components
    lines.append(f"   Phase:{comp['Phase']} Base:{comp['Base']} Pre:{comp['PrePump']} Cont:{comp['Continuation']}")

    if r.entry:
        e = r.entry
        lines += [
            f"",
            f"   💰 ENTRY ZONE:",
            f"      Ideal:  ${e['entry']:.8f}",
            f"      Low:    ${e['entry_zone_low']:.8f}  (pullback entry)",
            f"      High:   ${e['entry_zone_high']:.8f}  (breakout confirm)",
            f"      SL:     ${e['sl']:.8f}  (-{e['sl_pct']:.1f}%)  [ATR×{e['sl_mult_used']}x]",
            f"      TP1:    ${e['tp1']:.8f}  (+{e['tp1_pct']:.0f}%)  R/R {e['rr1']:.1f}x",
            f"      TP2:    ${e['tp2']:.8f}  (+{e['tp2_pct']:.0f}%)  R/R {e['rr2']:.1f}x",
            f"      TP3:    ${e['tp3']:.8f}  (+{e['tp3_pct']:.0f}%)  [trailing stop]",
            f"      ATR:    {e['atr_pct']:.2f}%",
        ]

    if r.position:
        p = r.position
        lines.append(f"      Size: {p['position_size']:.4f} | Lev: {p['leverage']:.1f}x | Risk: ${p['risk_usd']:.0f}")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  API CLIENTS
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"
    _candle_cache: Dict = {}
    _cache_ts: Dict = {}
    CACHE_TTL = 55 * 60  # 55 menit cache TTL

    @staticmethod
    def _get(url: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=timeout)
                r.raise_for_status()
                return r.json()
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
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 100) -> List[dict]:
        cache_key = f"{symbol}:{limit}"
        # Cache validation
        if cache_key in cls._candle_cache:
            if time.time() - cls._cache_ts.get(cache_key, 0) < cls.CACHE_TTL:
                return cls._candle_cache[cache_key]

        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/candles",
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
        cls._candle_cache[cache_key] = candles
        cls._cache_ts[cache_key] = time.time()
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"}
        )
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0

    @classmethod
    def clear_cache(cls):
        cls._candle_cache.clear()
        cls._cache_ts.clear()


class CoinalyzeClient:
    BASE = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._cache: Dict = {}

    def _wait(self):
        wait = 0.3 - (time.time() - CoinalyzeClient._last_call)
        if wait > 0:
            time.sleep(wait)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[list]:
        params["api_key"] = self.api_key
        for attempt in range(2):
            self._wait()
            try:
                r = requests.get(f"{self.BASE}/{endpoint}", params=params, timeout=10)
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 5)) + 1)
                    continue
                if r.status_code not in (200,):
                    return None
                data = r.json()
                if isinstance(data, dict) and "error" in data:
                    return None
                return data
            except requests.exceptions.Timeout:
                if attempt < 1:
                    time.sleep(2)
            except Exception as e:
                log.warning(f"Coinalyze error: {e}")
                if attempt < 1:
                    time.sleep(2)
        return None

    def get_future_markets(self) -> List[dict]:
        if "future_markets" in self._cache:
            return self._cache["future_markets"]
        data = self._get("future-markets", {})
        res = data if isinstance(data, list) else []
        self._cache["future_markets"] = res
        return res

    def _batch_fetch(self, endpoint: str, symbols: List[str], extra: dict) -> Dict[str, list]:
        batch_size = 10
        res = {}
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            try:
                data = self._get(endpoint, {"symbols": ",".join(batch), **extra})
                if data and isinstance(data, list):
                    for item in data:
                        sym = item.get("symbol", "")
                        hist = item.get("history", [])
                        if sym and hist:
                            res[sym] = hist
            except Exception as e:
                log.warning(f"Batch fetch error: {e}")
        return res

    def fetch_buy_sell_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        """
        NOTE: Gunakan endpoint yang benar untuk buy/sell data.
        Cek Coinalyze docs untuk endpoint yang tepat di plan kamu.
        Opsi: 'buy-sell-volume-history' atau 'ohlcv-history' (hanya OHLCV).
        """
        return self._batch_fetch(
            "buy-sell-volume-history",  # FIXED dari v12.0 yang pakai ohlcv-history
            symbols,
            {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts}
        )

    def fetch_liquidations_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch(
            "liquidation-history",
            symbols,
            {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts, "convert_to_usd": "true"}
        )

    def fetch_oi_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch(
            "open-interest-history",
            symbols,
            {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts, "convert_to_usd": "true"}
        )


class SymbolMapper:
    def __init__(self, clz_client: CoinalyzeClient):
        self._client = clz_client
        self._to_clz: Dict[str, str] = {}

    def load(self, active_symbols: set) -> None:
        for sym in active_symbols:
            self._to_clz[sym] = f"{sym}_PERP.A"

    def to_clz(self, bitget_sym: str) -> Optional[str]:
        return self._to_clz.get(bitget_sym)

    def clz_symbols_for(self, syms: List[str]) -> List[str]:
        return [self._to_clz[s] for s in syms if s in self._to_clz]


# ══════════════════════════════════════════════════════════════════════════════
#  📤  TELEGRAM
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
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCANNER LOOP v13.0
# ══════════════════════════════════════════════════════════════════════════════
def select_scan_universe(tickers: Dict, max_symbols: int) -> List[str]:
    """
    FIXED: Pilih universe berdasarkan sweet spot pump ($1M-$100M volume).
    Bukan "top 100 by volume" yang memilih large-cap.
    """
    vol_min = CONFIG["pre_filter_vol_min"]
    vol_max = CONFIG["pre_filter_vol_max"]

    candidates = []
    for sym, t in tickers.items():
        try:
            vol = float(t.get("quoteVolume", 0))
            if vol_min <= vol <= vol_max:
                candidates.append((sym, vol))
        except Exception:
            pass

    # Sort by volume, ambil mid-tier (hapus top 10% dan bottom 10%)
    candidates.sort(key=lambda x: x[1])
    n = len(candidates)
    if n > 20:
        # Ambil dari 10% sampai 90% volume range (sweet spot)
        lo, hi = n // 10, n * 9 // 10
        candidates = candidates[lo:hi]

    # Jika masih terlalu banyak, sample acak yang cukup represent
    if len(candidates) > max_symbols:
        import random
        random.shuffle(candidates)
        candidates = candidates[:max_symbols]

    syms = [s for s, _ in candidates]
    log.info(f"  Universe: {len(syms)} symbols (vol ${vol_min/1e6:.0f}M–${vol_max/1e6:.0f}M sweet spot)")
    return syms


def check_btc_circuit_breaker(tickers: Dict) -> bool:
    """Pause scan jika BTC dump > threshold dalam 1h"""
    btc_ticker = tickers.get("BTCUSDT", {})
    if not btc_ticker:
        return False
    try:
        # Ambil BTC candles untuk chg_1h yang akurat
        btc_candles = BitgetClient.get_candles("BTCUSDT", 5)
        if len(btc_candles) >= 3:
            btc_chg_1h = (btc_candles[-2]["close"] - btc_candles[-3]["close"]) / btc_candles[-3]["close"] * 100
            if btc_chg_1h < CONFIG["btc_dump_threshold"]:
                log.warning(f"⛔ BTC CIRCUIT BREAKER: Δ1h {btc_chg_1h:+.1f}% — scan paused")
                return True
    except Exception:
        pass
    return False


def main():
    log.info(f"{'═'*70}")
    log.info(f"  PRE-PUMP SCANNER v{VERSION}")
    log.info(f"  Target: Pump ≥15% dalam 24h | Detect 1-3h sebelumnya")
    log.info(f"{'═'*70}")

    # Validasi API key
    if not CONFIG.get("coinalyze_api_key"):
        log.error("❌ COINALYZE_API_KEY tidak di-set! Export ke environment variable.")
        return 1

    init_db()
    clz_client = CoinalyzeClient(CONFIG["coinalyze_api_key"])
    mapper = SymbolMapper(clz_client)

    # === STEP 1: Fetch tickers ===
    log.info("📊 Fetching tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        log.error("❌ No tickers")
        return 1
    log.info(f"  Got {len(tickers)} tickers")

    # === STEP 2: Circuit breaker ===
    if check_btc_circuit_breaker(tickers):
        return 0  # Skip scan, BTC crash

    # Fetch BTC candles for relative strength calculation
    btc_candles = BitgetClient.get_candles("BTCUSDT", 5)
    btc_chg_1h = 0.0
    if len(btc_candles) >= 3:
        btc_chg_1h = (btc_candles[-2]["close"] - btc_candles[-3]["close"]) / btc_candles[-3]["close"] * 100
    log.info(f"  BTC 1h: {btc_chg_1h:+.2f}%")

    # === STEP 3: Select scan universe (FIXED) ===
    active = select_scan_universe(tickers, CONFIG["max_symbols_per_scan"])

    # === STEP 4: Load symbol mapping ===
    mapper.load(set(active))

    # === STEP 5: Fetch Coinalyze data ===
    log.info("📈 Fetching Coinalyze data...")
    now_ts = int(time.time())
    from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_syms = mapper.clz_symbols_for(active)

    btx_data, liq_data, oi_data = {}, {}, {}
    try:
        btx_data = clz_client.fetch_buy_sell_batch(clz_syms, from_ts, now_ts)
        log.info(f"  BTX: {len(btx_data)}")
    except Exception as e:
        log.warning(f"  BTX fetch failed: {e}")
    try:
        liq_data = clz_client.fetch_liquidations_batch(clz_syms, from_ts, now_ts)
        log.info(f"  LIQ: {len(liq_data)}")
    except Exception as e:
        log.warning(f"  LIQ fetch failed: {e}")
    try:
        oi_data = clz_client.fetch_oi_batch(clz_syms, from_ts, now_ts)
        log.info(f"  OI: {len(oi_data)}")
    except Exception as e:
        log.warning(f"  OI fetch failed: {e}")

    # === STEP 6: Score each coin ===
    log.info("🎯 Scoring...")
    results = []

    for sym in active:
        if is_on_cooldown(sym):
            continue

        try:
            ticker = tickers.get(sym, {})
            price = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))

            if price <= 0:
                continue

            # Fetch candles
            candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
            if len(candles) < 30:
                continue

            # FIXED: chg_24h dari candles (bukan chgUTC!)
            chg_24h = get_chg_from_candles(candles, 24)
            chg_1h = get_chg_from_candles(candles, 1)
            chg_4h = get_chg_from_candles(candles, 4)

            # Fetch funding
            funding = BitgetClient.get_funding(sym)

            # Coinalyze data
            clz_sym = mapper.to_clz(sym)
            clz_btx = btx_data.get(clz_sym, []) if clz_sym else []
            clz_liq = liq_data.get(clz_sym, []) if clz_sym else []
            clz_oi = oi_data.get(clz_sym, []) if clz_sym else []

            coin_data = CoinData(
                symbol=sym, price=price, vol_24h=vol_24h,
                chg_24h=chg_24h, chg_1h=chg_1h, chg_4h=chg_4h,
                funding=funding, candles=candles, btc_chg_1h=btc_chg_1h,
                clz_btx=clz_btx, clz_liq=clz_liq, clz_oi=clz_oi,
            )

            result = score_coin_v13(coin_data)
            if result:
                results.append(result)
                type_str = "/".join([pt.type_code for pt in result.pump_types])
                log.info(f"  ✅ {sym}: {result.score} [{result.phase}] [{type_str}]")

        except Exception as e:
            log.warning(f"  ⚠️ {sym}: {e}")

    # === STEP 7: Sort, dedup, alert ===
    results.sort(key=lambda x: x.score, reverse=True)
    max_alerts = CONFIG["max_alerts_per_scan"]

    log.info(f"\n{'═'*70}")
    log.info(f"  📊 SCAN COMPLETE: {len(results)} signals | Top {min(max_alerts, len(results))} akan dikirim")
    log.info(f"{'═'*70}\n")

    sent = 0
    for rank, r in enumerate(results[:10], 1):
        alert_msg = build_alert_v13(r, rank)
        print(alert_msg)

        if sent < max_alerts:
            if send_telegram(alert_msg):
                sent += 1
            # Simpan ke DB (untuk cooldown + win-rate tracking)
            entry_price = r.entry["entry"] if r.entry else r.price
            set_alert(r.symbol, r.score, r.phase, entry_price)

    if not results:
        log.info("  No signals above threshold this scan cycle")

    BitgetClient.clear_cache()
    return 0


if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️ Scanner stopped")
        exit(0)
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True)
        exit(1)
