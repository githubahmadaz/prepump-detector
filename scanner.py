#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NEXUS-SR v3.1 — Empirically Validated Support Zone Bounce Scanner           ║
║                                                                              ║
║  BASIS DATA EMPIRIS:                                                         ║
║  · Dataset 1: 5.214 TESTING events dari 400+ coins (unbiased, crash market) ║
║  · Dataset 2: 42 events dari targeted pump coins (cross-validation)          ║
║  · Cross-validated: hanya fitur yang KONSISTEN di kedua dataset dipakai      ║
║                                                                              ║
║  ARSITEKTUR (berbasis data, bukan asumsi):                                   ║
║                                                                              ║
║  LAYER 1 — ZONE GATE (binary, Pine Script logic):                            ║
║    G1. Zone state = TESTING (candle[-1] atau candle[-2])                     ║
║    G2. Zone break_count < 3                                                  ║
║    G3. Volume bukan outlier (vol < 5× avg_20)                                ║
║                                                                              ║
║  LAYER 2 — VOLATILITY GATE (binary, dari data empiris):                      ║
║    G4. BBW ≥ 0.050  (Bollinger Band Width — confirmed corr +0.236/+0.262)    ║
║    G5. ATR% ≥ 1.20% (ATR relatif — confirmed corr +0.230/+0.245)            ║
║    ↑ Coin yang tidak lolos gate ini secara mekanis TIDAK BISA 15% dalam 24H  ║
║                                                                              ║
║  LAYER 3 — SCORE (0-100, 3 komponen yang terbukti):                         ║
║    A. Volatility Score   0-50 pts  → BBW + ATR_pct kombinasi                ║
║       Rasio: HIT avg BBW 2.4× MISS avg. Kontribusi terbesar ke precision.   ║
║    B. Volume Score       0-30 pts  → VolCompression + VolZ_4H               ║
║       Dua sub-komponen BERBEDA: compression (recent spike) vs Z-score (hist) ║
║    C. Momentum Score     0-20 pts  → bear_streak + VolRatio                 ║
║       Oversold setup: 3-4 bear candles sebelum entry + current vol normal    ║
║                                                                              ║
║  DIHAPUS (tidak terbukti dari data):                                         ║
║    ✗ RSI oversold       (corr=+0.020, tidak konsisten antar range)           ║
║    ✗ Candle pattern     (HAMMER hit rate 0.7% < base 2.9%)                   ║
║    ✗ Taker buy (btx)    (corr=+0.009, 81% data kosong)                      ║
║    ✗ Funding negatif    (corr=-0.044 large, reversed di targeted)            ║
║    ✗ Zone touch count   (korelasi NEGATIF -0.035)                            ║
║    ✗ OI change          (100% null dari pipeline)                            ║
║    ✗ L/S ratio          (hampir semua no_data)                               ║
║                                                                              ║
║  THRESHOLD:                                                                  ║
║    Normal mode : score ≥ 60                                                  ║
║    Caution mode: score ≥ 75  (BTC+ETH < EMA200 daily, bukan EMA50 weekly)   ║
║    Strong signal: score ≥ 80 → kirim Telegram                               ║
║                                                                              ║
║  REGIME FIX:                                                                 ║
║    Sebelumnya: EMA50 weekly dengan 60 candles → warmup bias → salah         ║
║    Sekarang  : EMA200 daily dengan 300 candles → warmup stabil              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import logging.handlers as _lh
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Logging ───────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch   = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/nexus_sr_v31.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG — semua threshold dari data empiris
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── CREDENTIALS ────────────────────────────────────────────────────────
    "bot_token": os.getenv("BOT_TOKEN"),
    "chat_id":   os.getenv("CHAT_ID"),

    # ── UNIVERSE ───────────────────────────────────────────────────────────
    "min_vol_24h":       50_000,    # $50K floor — singkirkan ghost coins saja
    "min_vol_signal":   500_000,    # $500K minimum untuk sinyal yang dikirim ke Telegram
    "max_vol_24h":  2_000_000_000,
    "gate_chg_24h_max":    60.0,   # skip jika sudah pump >60%

    # ── PINE SCRIPT PARAMS ─────────────────────────────────────────────────
    "lookback_period":       20,   # Pine: lookbackPeriod
    "vol_len":                2,   # Pine: vol_len
    "box_width_multiplier": 1.0,   # Pine: box_withd
    "candle_limit_1h":      200,   # 1H candles (~8 hari)
    "candle_limit_1d":      300,   # FIX: 300 bukan 210 — EMA200 butuh margin warmup
                                   # 300 candles 1D = ~10 bulan data, EMA200 stabil
                                   # di candle ke-201 dst (99 candles margin)

    # ── ZONE GATE ──────────────────────────────────────────────────────────
    "atr_period":           200,   # Pine: ta.atr(200)
    "max_break_count":        3,
    "vol_outlier_mult":     5.0,   # vol > 5× avg = outlier → skip
    "vol_outlier_lookback":  20,   # FIX: dipakai di candles[-2], bukan candles[-1]
    "max_zone_width_pct":    8.0,  # zone width max 8% dari harga — filter low-liquidity

    # ── PUMP REJECTION GATE ────────────────────────────────────────────────
    # Dari audit: 4USDT lolos sebagai STRONG karena volume spike 10.66×
    # padahal bear_streak=0 → ini bukan bounce setup, tapi pump event
    # Pattern: vol_ratio tinggi + tidak ada tekanan jual = pump SEDANG terjadi
    # Sinyal yang benar: ada tekanan jual (bear streak) LALU volume spike
    "pump_reject_vol_ratio":  5.0,  # vol_ratio > ini = potensi pump
    "pump_reject_bear_min":   1,    # bear_streak harus >= ini jika vol tinggi

    # ── MINIMUM COMPONENT SCORE ────────────────────────────────────────────
    # Volume harus mengkonfirmasi sinyal — tidak cukup hanya volatility
    # Dari audit: 龙虾USDT lolos dengan B=8, tidak ada konfirmasi volume
    "min_score_B":           10,    # require B ≥ 10 (dari 30 max)

    # ── VOLATILITY GATE (G4 + G5) — dari data empiris ──────────────────────
    # BBW threshold: top quintile dari large dataset = 0.078
    # Turunkan ke 0.050 untuk meningkatkan recall (capture lebih banyak bounce)
    # Tradeoff: precision sedikit turun tapi tidak kehilangan terlalu banyak hits
    "gate_bbw_min":        0.050,  # Bollinger Band Width minimum
    "gate_atr_pct_min":    1.20,   # ATR sebagai % dari harga minimum

    # ── SCORING PARAMS (dari data empiris) ─────────────────────────────────

    # A. VOLATILITY SCORE (0-50 pts)
    # Dari large dataset: BBW corr=+0.236, ATR_pct corr=+0.230
    # HIT avg BBW = 0.137 vs MISS avg = 0.057 (2.4x ratio)
    # HIT avg ATR% = 3.12 vs MISS avg = 1.40 (2.2x ratio)
    "score_vol_max":        50,    # Total A component
    "bbw_strong":          0.150,  # ≥ ini → full BBW score (above HIT avg)
    "bbw_medium":          0.078,  # ≥ ini → half BBW score (top quintile)
    "atr_strong":          3.12,   # ≥ ini → full ATR score (HIT avg)
    "atr_medium":          1.62,   # ≥ ini → half ATR score (top quintile)
    # Bobot dalam komponen A: BBW 55%, ATR 45% (BBW sedikit lebih kuat)
    "bbw_weight":          0.55,
    "atr_weight":          0.45,

    # B. VOLUME SCORE (0-30 pts)
    # Sub-komponen B1: VolCompression (recent avg / prior avg)
    # Dari large dataset: corr=+0.089 (confirmed), HIT avg=3.76 vs MISS=1.42
    # Sub-komponen B2: VolZ_4H (Z-score vs 4H window)
    # Dari large dataset: corr=+0.083 (confirmed), HIT avg=1.03 vs MISS=0.16
    # BERBEDA: B1=recent spike vs baseline, B2=historical anomaly 4H window
    "score_vol_micro_max":  30,
    "vol_comp_strong":     3.76,   # ≥ HIT avg → full B1 score
    "vol_comp_medium":     1.82,   # ≥ top quintile → half B1 score
    "vol_z4h_strong":      2.0,    # ≥ ini → full B2 score (7.8% hit rate)
    "vol_z4h_medium":      0.729,  # ≥ ini → half B2 score (threshold optimal)
    "vol_comp_weight":     0.55,   # B1 sedikit lebih kuat dari B2
    "vol_z4h_weight":      0.45,

    # C. MOMENTUM SCORE (0-20 pts)
    # Sub-komponen C1: Bear streak (consecutive bearish candles masuk zone)
    # Dari large dataset: corr=+0.030, streak 3-4 = hit 3.9-4.7% vs base 2.9%
    # Sub-komponen C2: VolRatio (current vol vs 20-bar avg)
    # Dari large dataset: corr=+0.068, ratio ≥1.44 = 4.3% hit rate
    "score_momentum_max":   20,
    "bear_streak_strong":    4,    # ≥ 4 consecutive bear candles
    "bear_streak_medium":    2,    # ≥ 2 consecutive bear candles
    "vol_ratio_strong":    2.0,    # ≥ 2× avg → full C2 score
    "vol_ratio_medium":    1.44,   # ≥ 1.44× avg → half C2 score (threshold optimal)
    "bear_weight":         0.40,   # C1 lebih lemah dari C2
    "vol_ratio_weight":    0.60,

    # ── THRESHOLD ─────────────────────────────────────────────────────────
    "score_threshold_normal":  60,
    "score_threshold_caution": 75,  # aktif jika BTC+ETH < EMA200 daily
    "score_strong":            80,  # → kirim Telegram

    # ── REGIME CHECK (FIX dari bug sebelumnya) ─────────────────────────────
    # Bug lama: EMA50 weekly dengan 60 candles → warmup bias → selalu NORMAL
    # Fix: EMA200 daily dengan 210 candles → cukup warmup untuk akurasi
    "ema_regime_period":      200,  # EMA200 daily

    # ── OUTPUT ────────────────────────────────────────────────────────────
    "top_n":                   10,
    "max_alerts":               5,
    "alert_cooldown_sec":    3600,
    "cooldown_file": "/tmp/nexus_sr_v31_state.json",
    "sleep_between_coins":    0.2,
}

MANUAL_EXCLUDE: set = set()


# ══════════════════════════════════════════════════════════════════════════════
#  💾  STATE FILE
# ══════════════════════════════════════════════════════════════════════════════
def _load_state() -> dict:
    try:
        path = CONFIG["cooldown_file"]
        if os.path.exists(path):
            with open(path) as f:
                state = json.load(f)
            now = time.time()
            state["cooldowns"] = {
                k: v for k, v in state.get("cooldowns", {}).items()
                if now - v < CONFIG["alert_cooldown_sec"]
            }
            return state
    except Exception:
        pass
    return {"cooldowns": {}}

def _save_state(state: dict) -> None:
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_state = _load_state()
log.info(f"State loaded: {len(_state['cooldowns'])} cooldowns")

def is_on_cooldown(sym: str) -> bool:
    return (time.time() - _state["cooldowns"].get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym: str) -> None:
    _state["cooldowns"][sym] = time.time()
    _save_state(_state)


# ══════════════════════════════════════════════════════════════════════════════
#  📐  MATH UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def _mean(arr: list) -> float:
    return sum(arr) / len(arr) if arr else 0.0

def _std(arr: list) -> float:
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / len(arr))

def _zscore(value: float, series: list, min_samples: int = 10) -> float:
    if len(series) < min_samples:
        return 0.0
    sigma = _std(series)
    return (value - _mean(series)) / sigma if sigma > 0 else 0.0

def _linear_score(value: float, strong: float, medium: float, weight: float) -> float:
    """
    Interpolasi linear [0, weight] dari value relatif terhadap strong/medium.
    value >= strong  → weight (full)
    value >= medium  → weight/2 .. weight (partial)
    value >= 0       → 0 .. weight/2 (minimal)
    value < 0        → 0
    """
    if value >= strong:
        return weight
    if value >= medium:
        ratio = (value - medium) / (strong - medium)
        return weight * 0.5 + ratio * weight * 0.5
    if value >= 0:
        ratio = value / medium if medium > 0 else 0.0
        return ratio * weight * 0.5
    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"
    _candle_cache: Dict = {}

    @staticmethod
    def _get(url: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=timeout)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    log.warning("Bitget rate limit — tunggu 30s")
                    time.sleep(30); continue
                if e.response.status_code not in (400, 404):
                    log.warning(f"Bitget HTTP {e.response.status_code}")
                break
            except Exception:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        """GET /api/v2/mix/market/tickers — semua USDT-Futures."""
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {d["symbol"]: d for d in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, granularity: str = "1H",
                    limit: int = 200) -> List[dict]:
        """
        GET /api/v2/mix/market/candles
        Mendukung limit > 200 via pagination otomatis.
        """
        cache_key = f"{symbol}:{granularity}:{limit}"
        if cache_key in cls._candle_cache:
            return cls._candle_cache[cache_key]

        def _parse(raw_rows: list) -> List[dict]:
            out = []
            for row in raw_rows:
                try:
                    vol = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                    out.append({
                        "ts":    int(row[0]),
                        "open":  float(row[1]),
                        "high":  float(row[2]),
                        "low":   float(row[3]),
                        "close": float(row[4]),
                        "vol":   vol,
                    })
                except (IndexError, ValueError):
                    continue
            return out

        if limit <= 200:
            data = cls._get(
                f"{cls.BASE}/api/v2/mix/market/candles",
                params={"symbol": symbol, "productType": "USDT-FUTURES",
                        "granularity": granularity, "limit": limit}
            )
            if not data or data.get("code") != "00000":
                return []
            result = sorted(_parse(data.get("data", [])), key=lambda x: x["ts"])
            cls._candle_cache[cache_key] = result
            return result

        # Pagination untuk limit > 200
        collected: Dict[int, dict] = {}
        end_time = None
        for _ in range(math.ceil(limit / 200)):
            params = {"symbol": symbol, "productType": "USDT-FUTURES",
                      "granularity": granularity, "limit": 200}
            if end_time is not None:
                params["endTime"] = str(end_time)
            data = cls._get(f"{cls.BASE}/api/v2/mix/market/candles", params=params)
            if not data or data.get("code") != "00000":
                break
            raw = data.get("data", [])
            if not raw:
                break
            for c in _parse(raw):
                collected[c["ts"]] = c
            if len(raw) < 200:
                break
            end_time = min(c["ts"] for c in _parse(raw)) - 1
            if len(collected) >= limit:
                break
            time.sleep(0.15)

        result = sorted(collected.values(), key=lambda x: x["ts"])[-limit:]
        cls._candle_cache[cache_key] = result
        return result

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  📊  INDICATORS (Pine Script compatible)
# ══════════════════════════════════════════════════════════════════════════════

def _wilder_ema(values: list, period: int) -> list:
    """Wilder's RMA — Pine's ta.atr internal."""
    if not values:
        return []
    alpha  = 1.0 / period
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1.0 - alpha) * result[-1])
    return result

def _std_ema(values: list, period: int) -> list:
    """Standard EMA — Pine's ta.ema."""
    if not values:
        return []
    alpha  = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1.0 - alpha) * result[-1])
    return result

def compute_delta_volume(candles: List[dict]) -> List[float]:
    """Pine: upAndDownVolume() — lines 23-38. Signed volume per candle."""
    result = []
    is_buy = True
    for c in candles:
        if   c["close"] > c["open"]: is_buy = True
        elif c["close"] < c["open"]: is_buy = False
        result.append(c["vol"] if is_buy else -c["vol"])
    return result

def compute_vol_thresholds(dv: List[float], vol_len: int) -> Tuple[List[float], List[float]]:
    """Pine: vol_hi = ta.highest(Vol/2.5, vol_len), vol_lo = ta.lowest(...)"""
    n      = len(dv)
    scaled = [v / 2.5 for v in dv]
    vol_hi = [max(scaled[max(0, i - vol_len + 1): i + 1]) for i in range(n)]
    vol_lo = [min(scaled[max(0, i - vol_len + 1): i + 1]) for i in range(n)]
    return vol_hi, vol_lo

def compute_atr(candles: List[dict], period: int = 200) -> List[float]:
    """Pine: ta.atr(200) — Wilder's smoothed ATR."""
    trs = []
    for i, c in enumerate(candles):
        pc  = candles[i-1]["close"] if i > 0 else c["close"]
        trs.append(max(c["high"] - c["low"],
                       abs(c["high"] - pc),
                       abs(c["low"]  - pc)))
    return _wilder_ema(trs, period)

def compute_ema(values: list, period: int) -> List[float]:
    return _std_ema(values, period)

def compute_bbw(candles: List[dict], period: int = 20, mult: float = 2.0) -> List[float]:
    """
    Bollinger Band Width = (upper - lower) / middle.
    Normalized: output adalah desimal relatif terhadap SMA.
    Dari data empiris: HIT avg BBW = 0.137 vs MISS avg = 0.057 (2.4× ratio).
    """
    closes = [c["close"] for c in candles]
    n      = len(closes)
    result = [0.0] * n
    for i in range(period - 1, n):
        window = closes[i - period + 1: i + 1]
        sma    = sum(window) / period
        sd     = math.sqrt(sum((x - sma) ** 2 for x in window) / period)
        result[i] = (mult * 2 * sd / sma) if sma > 0 else 0.0
    return result

def compute_vol_compression(candles: List[dict], fast: int = 5,
                             slow: int = 20) -> float:
    """
    Volume compression: avg vol fast period / avg vol slow period.
    Dari data empiris: HIT avg = 3.76 vs MISS avg = 1.42.
    Nilai > 1.0 = volume recent lebih tinggi dari baseline (spike).
    """
    if len(candles) < slow + fast + 1:
        return 1.0
    vols      = [c["vol"] for c in candles]
    recent    = vols[-fast:]
    prior_end = len(vols) - fast
    prior     = vols[max(0, prior_end - slow): prior_end]
    avg_r     = _mean(recent)
    avg_p     = _mean(prior)
    return avg_r / avg_p if avg_p > 0 else 1.0

def compute_vol_z4h(candles: List[dict], window: int = 96) -> float:
    """
    Volume Z-score vs 4H window (96 candles 1H = 4 hari).
    Dari data empiris: HIT avg Z = 1.03 vs MISS avg = 0.16.
    Gunakan candle[-2] (confirmed bar, bukan live).
    """
    vols = [c["vol"] for c in candles]
    if len(vols) < window + 5:
        return 0.0
    cur_vol  = vols[-2]
    baseline = vols[-(window + 5):-5]
    return _zscore(cur_vol, baseline, min_samples=20)

def compute_vol_ratio(candles: List[dict], period: int = 20) -> float:
    """
    Current vol / rolling avg vol (period candles sebelumnya).
    Dari data empiris: HIT avg = 2.63 vs MISS avg = 1.38.
    Gunakan candle[-2].
    """
    vols = [c["vol"] for c in candles]
    if len(vols) < period + 2:
        return 1.0
    cur_vol  = vols[-2]
    baseline = vols[-(period + 2):-2]
    avg      = _mean(baseline)
    return cur_vol / avg if avg > 0 else 1.0

def compute_atr_pct(candles: List[dict], atr_arr: List[float]) -> float:
    """ATR sebagai % dari harga saat ini. Dari data: HIT avg=3.12% vs MISS=1.40%."""
    price = candles[-1]["close"]
    if price <= 0 or not atr_arr:
        return 0.0
    return atr_arr[-1] / price * 100

def compute_bear_streak(candles: List[dict]) -> int:
    """
    Berapa candle bearish berturut-turut sebelum candle terakhir.
    Dari data: streak 3-4 = hit 3.9-4.7% vs base 2.9%.
    """
    streak = 0
    for c in reversed(candles[:-1]):   # exclude last (live)
        if c["close"] < c["open"]:
            streak += 1
        else:
            break
        if streak >= 6:
            break
    return streak


# ══════════════════════════════════════════════════════════════════════════════
#  📐  ENTRY / SL / TP  (per-coin, bukan fixed %)
# ══════════════════════════════════════════════════════════════════════════════

def find_resistance_levels(candles: List[dict], entry: float,
                           atr: float, lookback: int = 10) -> List[float]:
    """
    Cari pivot highs di atas entry price — kandidat TP resistance.
    Lookback lebih kecil (10) dari zone detection (20) karena
    resistance tidak harus sekuat support.

    Juga tambahkan swing high dari 100 candle terakhir jika relevan.
    """
    highs = [c["high"] for c in candles]
    n     = len(highs)
    found: set = set()

    for i in range(lookback, n - lookback):
        val = highs[i]
        if val < entry * 1.003:          # minimal 0.3% di atas entry
            continue
        if (val >= max(highs[max(0, i - lookback): i]) and
                val >= max(highs[i + 1: i + lookback + 1])):
            found.add(round(val, 10))

    # Recent swing high (last 100 candles) sebagai kandidat tambahan
    window = min(100, n)
    recent_max = max(highs[-window:])
    if recent_max > entry * 1.01:        # minimal 1% di atas entry
        found.add(round(recent_max, 10))

    # Hanya yang benar-benar di atas entry
    return sorted(r for r in found if r > entry * 1.002)


def compute_trade_setup(
    candles:  List[dict],
    zone:     dict,
    atr_arr:  List[float],
    bbw:      float,
    price:    float,
) -> dict:
    """
    Hitung Entry, SL, TP per coin berdasarkan kondisi masing-masing.

    ── ENTRY ─────────────────────────────────────────────────────────────────
    · LIMIT di zone_top  → jika harga masih di atas zone_top
    · MARKET di harga saat ini → jika sudah di dalam zone

    ── SL ────────────────────────────────────────────────────────────────────
    · zone_bottom − 1.0×ATR
    · Rasionale: zone_bottom = pivot_low − ATR_formasi.
      Turun 1 ATR lagi dari sana = struktur zone benar-benar rusak.
      Lebih akurat dari fixed % karena mempertimbangkan volatility coin.

    ── TP (per-coin, 3 metode) ───────────────────────────────────────────────
    Metode 1 — ATR Projection (basis volatility regime dari BBW):
      BBW ≥ 0.150  → 5.0×ATR   (sangat volatile, potensi move besar)
      BBW ≥ 0.078  → 3.5×ATR   (volatile)
      BBW ≥ 0.050  → 2.5×ATR   (medium-high, minimum gate)

    Metode 2 — Resistance Level (pivot high terdekat di atas entry):
      Dipakai jika R:R ≥ 2.0 dari SL yang sudah dihitung.
      Jika resistance terlalu dekat (R:R < 2.0), gunakan ATR projection.

    Metode 3 — Minimum R:R floor = 2.0:1
      TP tidak pernah kurang dari entry + 2.0 × risk.

    Final TP = nilai tertinggi yang masih logis:
      Jika resistance ada DAN lebih dekat dari ATR target DAN R:R ≥ 2.0:
        → gunakan resistance (lebih konservatif, lebih mungkin tercapai)
      Else:
        → ATR projection (lebih agresif)
      Kemudian enforce min R:R 2.0:1.
    """
    atr = atr_arr[-1] if atr_arr else price * 0.02

    # ── ENTRY ──────────────────────────────────────────────────────────────
    zone_top = zone["zone_top"]
    zone_bot = zone["zone_bottom"]

    if price <= zone_top:
        entry      = price       # sudah dalam zone → masuk market
        entry_type = "MARKET"
    else:
        entry      = zone_top    # di atas zone → limit di zone_top
        entry_type = "LIMIT"

    # ── SL ─────────────────────────────────────────────────────────────────
    sl = zone_bot - atr * 1.0
    sl = max(sl, price * 0.0001)   # tidak boleh negatif/nol

    risk = entry - sl
    if risk <= 0:
        risk = atr                 # fallback: risk = 1 ATR

    # ── RESISTANCE ─────────────────────────────────────────────────────────
    res_levels = find_resistance_levels(candles, entry, atr)

    # ── TP METODE 1: ATR Projection ─────────────────────────────────────────
    if   bbw >= 0.150: atr_mult, regime_label = 5.0, "volatile"
    elif bbw >= 0.078: atr_mult, regime_label = 3.5, "high"
    else:              atr_mult, regime_label = 2.5, "medium"

    tp_atr = entry + atr * atr_mult

    # ── TP METODE 2: Resistance level ──────────────────────────────────────
    tp_res    = None
    res_label = ""
    for res in res_levels:
        rr_res = (res - entry) / risk
        if rr_res >= 2.0:          # hanya pakai jika R:R ≥ 2.0
            tp_res    = res
            res_label = f"R:R{rr_res:.1f}"
            break

    # ── TP METODE 3: Floor R:R = 2.0 ───────────────────────────────────────
    tp_floor = entry + risk * 2.0

    # ── FINAL TP SELECTION ──────────────────────────────────────────────────
    if tp_res is not None and tp_res < tp_atr:
        # Resistance lebih dekat dari ATR target
        # Gunakan resistance (lebih konservatif tapi lebih realistis)
        tp        = tp_res
        tp_method = f"Resist {res_label}"
    else:
        # Pakai ATR projection
        tp        = tp_atr
        extra     = f"+Res@{tp_res:.5f}" if tp_res else ""
        tp_method = f"ATR×{atr_mult:.1f}({regime_label}){extra}"

    # Pastikan minimum R:R = 2.0
    if tp < tp_floor:
        tp        = tp_floor
        tp_method = "MinRR2.0"

    # ── METRICS ────────────────────────────────────────────────────────────
    risk_pct   = (entry - sl) / entry * 100    if entry > 0 else 0
    reward_pct = (tp    - entry) / entry * 100 if entry > 0 else 0
    rr         = reward_pct / risk_pct         if risk_pct > 0 else 0

    return {
        "entry":      round(entry, 8),
        "entry_type": entry_type,
        "sl":         round(sl, 8),
        "tp":         round(tp, 8),
        "tp_method":  tp_method,
        "risk_pct":   round(risk_pct, 2),
        "reward_pct": round(reward_pct, 2),
        "rr":         round(rr, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🟩  ZONE DETECTION  (Pine Script identical)
# ══════════════════════════════════════════════════════════════════════════════

def find_pivot_lows(lows: List[float], lookback: int) -> List[Optional[float]]:
    """Pine: ta.pivotlow(src, lookbackPeriod, lookbackPeriod) — line 67."""
    n      = len(lows)
    result = [None] * n
    for i in range(lookback, n - lookback):
        val = lows[i]
        if val <= min(lows[i - lookback: i]) and \
           val <= min(lows[i + 1: i + lookback + 1]):
            result[i] = val
    return result

def detect_support_zones(candles: List[dict]) -> List[dict]:
    """
    Pine: calcSupportResistance() — support branch.
    Pivot low + Vol > vol_hi → zone terbentuk.
    Hitung touch_count dan break_count dari sisa candle.
    """
    cfg  = CONFIG
    lb   = cfg["lookback_period"]
    vl   = cfg["vol_len"]
    bw   = cfg["box_width_multiplier"]
    ap   = cfg["atr_period"]

    if len(candles) < lb * 2 + 10:
        return []

    dv      = compute_delta_volume(candles)
    vh, _   = compute_vol_thresholds(dv, vl)
    atr     = compute_atr(candles, ap)
    lows    = [c["low"] for c in candles]
    pivots  = find_pivot_lows(lows, lb)

    zones = []
    n     = len(candles)

    for i, piv in enumerate(pivots):
        if piv is None:
            continue
        if dv[i] <= vh[i]:              # Pine line 74: Vol > vol_hi
            continue
        if math.isnan(atr[i]) or atr[i] <= 0:
            continue

        zone_top    = piv
        zone_bottom = piv - atr[i] * bw
        break_count = 0
        touch_count = 0
        in_zone     = False
        broke_at    = n

        for j in range(i + lb, n):
            if j >= broke_at:
                break
            c_low  = candles[j]["low"]
            c_high = candles[j]["high"]
            if not in_zone:
                if zone_bottom <= c_low <= zone_top:
                    in_zone      = True
                    touch_count += 1
            else:
                if c_low > zone_top:
                    in_zone = False
                elif c_high < zone_bottom:
                    break_count += 1
                    in_zone  = False
                    broke_at = j

        zones.append({
            "zone_top":    zone_top,
            "zone_bottom": zone_bottom,
            "touch_count": touch_count,
            "break_count": break_count,
        })

    return zones

def get_zone_state(zone: dict, c_low: float, c_high: float) -> str:
    """Pine: brekout_sup/sup_holds — lines 105-120."""
    top = zone["zone_top"]; bot = zone["zone_bottom"]
    if c_high < bot:             return "BROKEN"
    if bot <= c_low <= top:      return "TESTING"
    return "VALID"


# ══════════════════════════════════════════════════════════════════════════════
#  🌍  REGIME CHECK  (fix: EMA200 daily, bukan EMA50 weekly)
# ══════════════════════════════════════════════════════════════════════════════
def is_caution_mode(btc_1d: List[dict], eth_1d: List[dict]) -> Tuple[bool, dict]:
    """
    CAUTION MODE: BTC dan ETH KEDUANYA di bawah EMA200 daily.

    Dual method untuk robustness:
    - Method 1: EMA200 jika data >= 200 bars
    - Method 2: SMA50 fallback jika data < 200 tapi >= 50
    Returns (is_caution, debug_dict) agar caller bisa log detail.
    """
    def _check(candles: List[dict], name: str) -> Tuple[bool, str]:
        if not candles:
            return False, f"{name}: no data"
        closes = [c["close"] for c in candles]
        price  = closes[-1]
        n      = len(closes)
        if n >= 200:
            ema = _std_ema(closes, 200)
            below = price < ema[-1]
            return below, f"{name} ${price:,.0f} vs EMA200 ${ema[-1]:,.0f} ({'BELOW' if below else 'ABOVE'})"
        elif n >= 50:
            sma = sum(closes[-50:]) / 50
            below = price < sma
            return below, f"{name} ${price:,.0f} vs SMA50 ${sma:,.0f} fallback n={n} ({'BELOW' if below else 'ABOVE'})"
        return False, f"{name}: n={n} insufficient"

    btc_below, btc_msg = _check(btc_1d, "BTC")
    eth_below, eth_msg = _check(eth_1d, "ETH")
    caution = btc_below and eth_below
    return caution, {"btc": btc_msg, "eth": eth_msg, "caution": caution}


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  SCORING  (3 komponen, total 100 pts, semua dari data empiris)
# ══════════════════════════════════════════════════════════════════════════════

def score_A_volatility(bbw: float, atr_pct: float) -> Tuple[int, dict]:
    """
    Komponen A: Volatility Score (0-50 pts)
    BASIS DATA: BBW corr=+0.236, ATR_pct corr=+0.230 (terkuat di kedua dataset)
    HIT avg BBW = 0.137 (2.4× MISS avg 0.057)
    HIT avg ATR_pct = 3.12% (2.2× MISS avg 1.40%)

    Sub-A1: BBW score (0-27.5 pts = 55% dari 50)
    Sub-A2: ATR_pct score (0-22.5 pts = 45% dari 50)
    """
    cfg = CONFIG

    # A1: BBW
    a1_raw = _linear_score(bbw, cfg["bbw_strong"], cfg["bbw_medium"], 1.0)
    a1_pts = round(a1_raw * cfg["score_vol_max"] * cfg["bbw_weight"])

    # A2: ATR_pct
    a2_raw = _linear_score(atr_pct, cfg["atr_strong"], cfg["atr_medium"], 1.0)
    a2_pts = round(a2_raw * cfg["score_vol_max"] * cfg["atr_weight"])

    total = min(a1_pts + a2_pts, cfg["score_vol_max"])
    return total, {"a1_bbw": a1_pts, "a2_atr": a2_pts,
                   "bbw": round(bbw, 4), "atr_pct": round(atr_pct, 2)}


def score_B_volume(vol_compression: float, vol_z4h: float) -> Tuple[int, dict]:
    """
    Komponen B: Volume Score (0-30 pts)
    BASIS DATA: vol_compression corr=+0.089, vol_z4h corr=+0.083 (confirmed kedua dataset)

    Sub-B1: Vol Compression (0-16.5 pts = 55% dari 30)
    Mengukur: spike volume RECENT vs baseline (akumulasi short-term)
    HIT avg = 3.76 vs MISS avg = 1.42

    Sub-B2: Vol Z-score 4H (0-13.5 pts = 45% dari 30)
    Mengukur: anomali volume vs window 4H historis (akumulasi longer-term)
    HIT avg Z = 1.03 vs MISS avg = 0.16
    BERBEDA dari B1: B1=recent spike, B2=historical anomaly
    """
    cfg = CONFIG

    # B1: Vol Compression
    b1_raw = _linear_score(vol_compression, cfg["vol_comp_strong"],
                           cfg["vol_comp_medium"], 1.0)
    b1_pts = round(b1_raw * cfg["score_vol_micro_max"] * cfg["vol_comp_weight"])

    # B2: Vol Z-score 4H
    b2_raw = _linear_score(vol_z4h, cfg["vol_z4h_strong"], cfg["vol_z4h_medium"], 1.0)
    b2_pts = round(b2_raw * cfg["score_vol_micro_max"] * cfg["vol_z4h_weight"])

    total = min(b1_pts + b2_pts, cfg["score_vol_micro_max"])
    return total, {"b1_comp": b1_pts, "b2_z4h": b2_pts,
                   "vol_comp": round(vol_compression, 3),
                   "vol_z4h": round(vol_z4h, 3)}


def score_C_momentum(bear_streak: int, vol_ratio: float) -> Tuple[int, dict]:
    """
    Komponen C: Momentum Score (0-20 pts)
    BASIS DATA: bear_streak corr=+0.030, vol_ratio corr=+0.068 (confirmed kedua dataset)

    Sub-C1: Bear Streak (0-8 pts = 40% dari 20)
    Mengukur: berapa candle bearish berturut-turut sebelum entry
    Data: streak 3-4 = 3.9-4.7% hit rate vs base 2.9%
    Ini adalah oversold setup — harga sudah ditekan cukup untuk bisa bounce

    Sub-C2: Vol Ratio (0-12 pts = 60% dari 20)
    Mengukur: apakah ada volume lebih saat masuk zone
    Data: vol_ratio ≥1.44 = kombinasi BBW+ATR+VolRatio = 12.7% hit rate
    BERBEDA dari B1/B2: ini adalah CURRENT bar vol vs recent avg (bukan spike detection)
    """
    cfg = CONFIG

    # C1: Bear streak
    # streak ≥ 4 = full, streak ≥ 2 = half, streak < 2 = minimal
    if bear_streak >= cfg["bear_streak_strong"]:
        c1_raw = 1.0
    elif bear_streak >= cfg["bear_streak_medium"]:
        c1_raw = 0.5 + 0.5 * (bear_streak - cfg["bear_streak_medium"]) / \
                       (cfg["bear_streak_strong"] - cfg["bear_streak_medium"])
    else:
        c1_raw = bear_streak / cfg["bear_streak_medium"] * 0.5 \
                 if cfg["bear_streak_medium"] > 0 else 0.0

    c1_pts = round(c1_raw * cfg["score_momentum_max"] * cfg["bear_weight"])

    # C2: Vol ratio
    c2_raw = _linear_score(vol_ratio, cfg["vol_ratio_strong"],
                           cfg["vol_ratio_medium"], 1.0)
    c2_pts = round(c2_raw * cfg["score_momentum_max"] * cfg["vol_ratio_weight"])

    total = min(c1_pts + c2_pts, cfg["score_momentum_max"])
    return total, {"c1_bear": c1_pts, "c2_volr": c2_pts,
                   "bear_streak": bear_streak,
                   "vol_ratio": round(vol_ratio, 3)}


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM & OUTPUT
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(msg: str) -> bool:
    bot  = CONFIG["bot_token"]
    chat = CONFIG["chat_id"]
    if not bot or not chat:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def build_table(results: list, caution: bool) -> str:
    mode = "⚠️ CAUTION (thr=75)" if caution else "✅ NORMAL (thr=60)"
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "",
        "═" * 150,
        f"  NEXUS-SR v3.1  |  Bitget Futures  |  {now}  |  {mode}",
        "═" * 150,
        f"  {'#':>2}  {'Coin':<13} {'Price':>12}  "
        f"{'Score':>5}  {'A':>3} {'B':>3} {'C':>3}  "
        f"{'BBW':>6} {'ATR%':>5}  "
        f"{'Entry':>12} {'Type':>5}  "
        f"{'SL':>12} {'Risk%':>5}  "
        f"{'TP':>12} {'Rwd%':>5}  {'R:R':>4}  TP-Method",
        "─" * 150,
    ]
    for i, r in enumerate(results, 1):
        raw = r["symbol"].replace("USDT", "")
        lines.append(
            f"  {i:>2}  {raw:<13} {r['price']:>12.6f}  "
            f"{r['score']:>5.1f}  {r['sa']:>3} {r['sb']:>3} {r['sc']:>3}  "
            f"{r['bbw']:>6.4f} {r['atr_pct']:>4.1f}%  "
            f"{r['entry']:>12.6f} {r['entry_type']:>5}  "
            f"{r['sl']:>12.6f} {r['risk_pct']:>4.1f}%  "
            f"{r['tp']:>12.6f} {r['reward_pct']:>4.1f}%  {r['rr']:>4.1f}  "
            f"{r['tp_method']:<28}  {r['strength']}"
        )
    lines += [
        "─" * 150,
        "  A=Volatility(0-50) B=Volume(0-30) C=Momentum(0-20) | "
        "Pump gate: vol>5x+bear=0 reject | MinB=10",
        "═" * 150,
    ]
    return "\n".join(lines)


def build_approaching_table(approaching: list) -> str:
    """Coin dalam 5% di atas zone_top — belum TESTING tapi mendekati."""
    if not approaching:
        return ""
    lines = [
        "",
        "  ↘  APPROACHING ZONES  (0-5% di atas zone_top, belum masuk)",
        "  " + "─" * 90,
        f"  {'#':>2}  {'Coin':<13} {'Price':>12} {'ZoneTop':>10} {'ZoneBot':>10} "
        f"{'Dist%':>6}  {'BBW':>6} {'ATR%':>6}  Vol24h(M)",
        "  " + "─" * 90,
    ]
    for i, a in enumerate(sorted(approaching, key=lambda x: x["dist_pct"]), 1):
        raw = a["symbol"].replace("USDT", "")
        gate_ok = "✓" if a["gate_ok"] else "✗"
        lines.append(
            f"  {i:>2}  {raw:<13} {a['price']:>12.6f} {a['zone_top']:>10.5f} "
            f"{a['zone_bot']:>10.5f} {a['dist_pct']:>5.2f}%  "
            f"{a['bbw']:>6.4f} {a['atr_pct']:>5.2f}%  "
            f"${a['vol_24h_m']:.1f}M  gate:{gate_ok}"
        )
    lines.append("  " + "─" * 90)
    lines.append("  ✓ = lolos gate BBW+ATR  ✗ = belum cukup volatil untuk 15% move")
    return "\n".join(lines)


def build_telegram_msg(results: list, caution: bool,
                       n_tested: int, n_candidates: int) -> str:
    mode = "⚠️ CAUTION" if caution else "🟢 NORMAL"
    now  = datetime.now(timezone.utc).strftime("%H:%M UTC")
    txt  = (
        f"🎯 <b>NEXUS-SR v3.1</b> [{now}]\n"
        f"Mode: {mode} | {len(results)}/{n_tested} signals | Universe: {n_candidates}\n"
        f"{'─'*28}\n"
    )
    for i, r in enumerate(results, 1):
        raw = r["symbol"].replace("USDT", "")
        # Emoji kekuatan R:R
        rr_emoji = "🔥" if r["rr"] >= 3 else "✅" if r["rr"] >= 2 else "⚠️"
        txt += (
            f"{i}. <b>{raw}</b>  [{r['strength']}]  Score:<b>{r['score']:.0f}</b>\n"
            f"   Zone: <code>{r['zone_bot']:.5f} – {r['zone_top']:.5f}</code>\n"
            f"   📥 Entry ({r['entry_type']}): <code>{r['entry']:.6f}</code>\n"
            f"   🛑 SL: <code>{r['sl']:.6f}</code> "
            f"(<b>-{r['risk_pct']:.1f}%</b>)\n"
            f"   🎯 TP: <code>{r['tp']:.6f}</code> "
            f"(<b>+{r['reward_pct']:.1f}%</b>) "
            f"{rr_emoji} R:R <b>{r['rr']:.2f}</b>\n"
            f"   📐 <i>{r['tp_method']}</i>\n"
            f"   BBW:{r['bbw']:.4f} ATR:{r['atr_pct']:.1f}% "
            f"VC:{r['vol_comp']:.2f}x Bear:{r['bear_streak']}\n\n"
        )
    txt += (
        "📊 <i>Basis: 5.214 events empiris | "
        "TP per-coin: ATR×regime atau resistance\n"
        "⚠️ Paper mode — verifikasi sebelum live trade.</i>"
    )
    return txt


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    cfg      = CONFIG
    start_ts = time.time()

    log.info("=" * 70)
    log.info(f"  NEXUS-SR v3.1 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 70)

    # ── 1. Regime check (FIX: EMA200 daily) ───────────────────────────────
    log.info("Fetching BTC/ETH daily data untuk regime check …")
    btc_1d  = BitgetClient.get_candles("BTCUSDT", "1D", cfg["candle_limit_1d"])
    eth_1d  = BitgetClient.get_candles("ETHUSDT", "1D", cfg["candle_limit_1d"])
    caution, regime_debug = is_caution_mode(btc_1d, eth_1d)
    threshold = cfg["score_threshold_caution"] if caution else cfg["score_threshold_normal"]
    log.info(f"  {regime_debug['btc']}")
    log.info(f"  {regime_debug['eth']}")
    log.info(f"Market regime: {'⚠️ CAUTION (thr=75)' if caution else '✅ NORMAL (thr=60)'}")

    # ── 2. Fetch tickers ────────────────────────────────────────────────
    log.info("Fetching Bitget USDT-Futures tickers …")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        send_telegram("⚠️ NEXUS-SR v3: Gagal fetch tickers")
        return
    log.info(f"Tickers received: {len(tickers)}")

    # ── 3. Build candidates ────────────────────────────────────────────
    skip_stats   = defaultdict(int)
    candidates   = []

    for sym, t in tickers.items():
        if sym in MANUAL_EXCLUDE:
            skip_stats["excluded"] += 1; continue
        if is_on_cooldown(sym):
            skip_stats["cooldown"] += 1; continue
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = abs(float(t.get("change24h", 0)) * 100)
        except Exception:
            skip_stats["parse_err"] += 1; continue
        if vol < cfg["min_vol_24h"]:
            skip_stats["vol_low"] += 1; continue
        if vol > cfg["max_vol_24h"]:
            skip_stats["vol_high"] += 1; continue
        if chg > cfg["gate_chg_24h_max"]:
            skip_stats["pumped"] += 1; continue
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    # ── 4. Phase 1: candle fetch + zone detection + gate filtering ──────
    log.info("Phase 1: candle fetch + zone detection …")
    BitgetClient.clear_cache()

    testing_candidates = []
    all_approaching    = []

    for idx, (sym, ticker) in enumerate(candidates):
        if (idx + 1) % 100 == 0:
            log.info(f"  Phase 1: {idx+1}/{len(candidates)}")

        try:
            price   = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            if price <= 0:
                skip_stats["no_price"] += 1; continue

            candles = BitgetClient.get_candles(sym, "1H", cfg["candle_limit_1h"])
            if len(candles) < cfg["lookback_period"] * 2 + 30:
                skip_stats["candle_short"] += 1; continue

            # ── Zone Gate G3: volume outlier ──────────────────────────────
            # FIX: gunakan candles[-2] (confirmed bar), bukan candles[-1] (live)
            # Candle live volume bisa undercount jika jam baru dimulai
            lb  = cfg["vol_outlier_lookback"]
            cur = candles[-2]["vol"]                           # ← FIX: [-2] bukan [-1]
            avg = _mean([c["vol"] for c in candles[-lb-2:-2]]) # ← baseline sebelum bar itu
            if avg > 0 and cur > cfg["vol_outlier_mult"] * avg:
                skip_stats["vol_outlier"] += 1; continue

            # ── Pre-compute indicators (one pass per coin) ────────────────
            atr_arr  = compute_atr(candles, cfg["atr_period"])
            bbw_arr  = compute_bbw(candles, 20)
            atr_pct  = compute_atr_pct(candles, atr_arr)
            bbw      = bbw_arr[-1]

            # ── Zone detection ────────────────────────────────────────────
            zones = detect_support_zones(candles)
            if not zones:
                skip_stats["no_zones"] += 1; continue

            # ── Zone Gate G1+G2: TESTING state, break_count < 3 ──────────
            testing_zones = []
            for z in zones:
                if z["break_count"] >= cfg["max_break_count"]:
                    continue
                for lookback_i in (1, 2):
                    if lookback_i >= len(candles):
                        continue
                    c = candles[-lookback_i]
                    if get_zone_state(z, c["low"], c["high"]) == "TESTING":
                        testing_zones.append(z)
                        break

            # ── Approaching zones (pre-alert, tidak kirim telegram) ───────
            if not testing_zones:
                for z in zones:
                    if z["break_count"] >= cfg["max_break_count"]:
                        continue
                    state = get_zone_state(z, candles[-1]["low"], candles[-1]["high"])
                    if state == "VALID":
                        dist_pct = (price - z["zone_top"]) / price * 100
                        if 0 < dist_pct <= 5.0:
                            gate_ok = (bbw >= cfg["gate_bbw_min"] and
                                       atr_pct >= cfg["gate_atr_pct_min"])
                            all_approaching.append({
                                "symbol":    sym,
                                "price":     price,
                                "zone_top":  z["zone_top"],
                                "zone_bot":  z["zone_bottom"],
                                "dist_pct":  round(dist_pct, 2),
                                "bbw":       round(bbw, 4),
                                "atr_pct":   round(atr_pct, 2),
                                "vol_24h_m": round(vol_24h / 1_000_000, 1),
                                "gate_ok":   gate_ok,
                            })
                skip_stats["no_testing"] += 1; continue

            # ── Volatility Gate G4+G5 (dari data empiris) ─────────────────
            # Coin yang tidak lolos gate ini secara mekanis tidak bisa 15% move
            if bbw < cfg["gate_bbw_min"]:
                skip_stats["gate_bbw"] += 1; continue
            if atr_pct < cfg["gate_atr_pct_min"]:
                skip_stats["gate_atr"] += 1; continue

            testing_candidates.append({
                "sym":     sym,
                "ticker":  ticker,
                "candles": candles,
                "zones":   testing_zones,
                "atr_arr": atr_arr,
                "bbw_arr": bbw_arr,
                "atr_pct": atr_pct,
                "bbw":     bbw,
                "vol_24h": vol_24h,
            })

        except Exception as e:
            log.debug(f"  Error Phase1 {sym}: {e}")

        time.sleep(cfg["sleep_between_coins"])

    log.info(
        f"Phase 1 done: {len(testing_candidates)} pass all gates | "
        f"approaching: {len(all_approaching)} | "
        f"skip: {dict(skip_stats)}"
    )

    # ── 5. Phase 2: score each testing candidate ────────────────────────
    log.info(f"Phase 2: scoring {len(testing_candidates)} candidates …")
    results = []

    for d in testing_candidates:
        sym     = d["sym"]
        candles = d["candles"]
        zones   = d["zones"]
        atr_pct = d["atr_pct"]
        bbw     = d["bbw"]

        try:
            price = float(d["ticker"].get("lastPr", 0))
            if price <= 0:
                continue

            # Pilih zona terbaik: paling dekat harga (sudah dalam TESTING state)
            best_zone = min(zones, key=lambda z: abs(z["zone_top"] - price))

            # ── Zone width guard: filter low-liquidity coins ──────────────
            zone_width_pct = (best_zone["zone_top"] - best_zone["zone_bottom"]) \
                             / price * 100 if price > 0 else 999
            if zone_width_pct > cfg["max_zone_width_pct"]:
                log.info(
                    f"  {sym}: zone width {zone_width_pct:.1f}% > "
                    f"{cfg['max_zone_width_pct']}% → low liquidity, skip"
                )
                continue

            # Compute score components
            vol_comp    = compute_vol_compression(candles)
            vol_z4h     = compute_vol_z4h(candles)
            vol_ratio   = compute_vol_ratio(candles)
            bear_streak = compute_bear_streak(candles)

            sa, det_a = score_A_volatility(bbw, atr_pct)
            sb, det_b = score_B_volume(vol_comp, vol_z4h)
            sc, det_c = score_C_momentum(bear_streak, vol_ratio)

            total = sa + sb + sc

            # ── Pump rejection gate ───────────────────────────────────────
            # Pattern: volume spike BESAR + tidak ada bear streak = pump event
            # Bounce legitimate: ada tekanan jual (bear streak) LALU volume masuk
            # Referensi audit: 4USDT vol_ratio=10.66x, bear=0 → pump, bukan bounce
            is_pump = (vol_ratio > cfg["pump_reject_vol_ratio"] and
                       bear_streak < cfg["pump_reject_bear_min"])
            if is_pump:
                log.info(
                    f"  {sym}: PUMP REJECTED — "
                    f"vol_ratio={vol_ratio:.2f}x bear={bear_streak} "
                    f"(pattern: spike without prior selling = pump event)"
                )
                continue

            # ── Minimum B score gate ──────────────────────────────────────
            # Volume harus mengkonfirmasi sinyal — volatility saja tidak cukup
            # Referensi audit: 龙虾USDT lolos dengan B=8, hanya dari volatility
            if sb < cfg["min_score_B"]:
                log.info(
                    f"  {sym}: B={sb} < min {cfg['min_score_B']} — "
                    f"no volume confirmation, skip"
                )
                continue

            log.info(
                f"  {sym}: score={total} (thr={threshold}) | "
                f"A={sa}(BBW={bbw:.4f},ATR={atr_pct:.2f}%) "
                f"B={sb}(VC={vol_comp:.2f}x,VZ4h={vol_z4h:.2f}) "
                f"C={sc}(bear={bear_streak},vr={vol_ratio:.2f}x) | "
                f"Zone={best_zone['zone_top']:.5f} Price={price:.5f}"
            )

            if total < threshold:
                log.info(f"    ❌ {sym} gagal threshold")
                continue

            strength = ("STRONG"   if total >= cfg["score_strong"]           else
                        "MODERATE" if total >= cfg["score_threshold_normal"] else
                        "WEAK")

            # ── Trade setup (Entry / SL / TP per-coin) ───────────────────
            trade = compute_trade_setup(
                candles  = candles,
                zone     = best_zone,
                atr_arr  = d["atr_arr"],
                bbw      = bbw,
                price    = price,
            )

            log.info(
                f"    ✅ {sym} lolos! strength={strength} | "
                f"Entry={trade['entry_type']}@{trade['entry']:.5f} "
                f"SL={trade['sl']:.5f}(-{trade['risk_pct']:.1f}%) "
                f"TP={trade['tp']:.5f}(+{trade['reward_pct']:.1f}%) "
                f"R:R={trade['rr']:.2f} [{trade['tp_method']}]"
            )

            results.append({
                "symbol":       sym,
                "price":        round(price, 8),
                "zone_top":     round(best_zone["zone_top"], 8),
                "zone_bot":     round(best_zone["zone_bottom"], 8),
                # Trade setup
                "entry":        trade["entry"],
                "entry_type":   trade["entry_type"],
                "sl":           trade["sl"],
                "tp":           trade["tp"],
                "tp_method":    trade["tp_method"],
                "risk_pct":     trade["risk_pct"],
                "reward_pct":   trade["reward_pct"],
                "rr":           trade["rr"],
                # Score
                "score":        round(total, 1),
                "sa":           sa, "sb": sb, "sc": sc,
                # Indicators
                "bbw":          round(bbw, 4),
                "atr_pct":      round(atr_pct, 2),
                "vol_comp":     round(vol_comp, 3),
                "vol_z4h":      round(vol_z4h, 3),
                "vol_ratio":    round(vol_ratio, 3),
                "bear_streak":  bear_streak,
                "zone_width_pct": round(zone_width_pct, 1),
                "touches":      best_zone["touch_count"],
                "breaks":       best_zone["break_count"],
                "strength":     strength,
                "caution":      caution,
                "vol_24h_m":    round(d["vol_24h"] / 1_000_000, 1),
            })

        except Exception as e:
            log.warning(f"  Error Phase2 {sym}: {e}")

    # ── 6. Sort + output ────────────────────────────────────────────────
    # Sort: Score DESC → A(volatility) DESC → B(volume) DESC
    results.sort(key=lambda x: (-x["score"], -x["sa"], -x["sb"]))
    top = results[:cfg["top_n"]]

    elapsed = round(time.time() - start_ts, 1)
    log.info(
        f"\nSignals: {len(results)} | Shown: {len(top)} | "
        f"Approaching: {len(all_approaching)} | Time: {elapsed}s"
    )

    # ── Terminal output ─────────────────────────────────────────────────
    print(build_table(top, caution))
    print(build_approaching_table(all_approaching[:10]))

    if not top:
        log.info("Tidak ada sinyal saat ini.")
        _save_state(_state)
        return

    # ── Telegram ────────────────────────────────────────────────────────
    strong  = [r for r in top if r["strength"] == "STRONG"]
    # Filter Telegram: hanya coin dengan vol ≥ min_vol_signal (kurangi noise low-cap)
    tg_pool = [r for r in (strong if strong else top)
               if r["vol_24h_m"] * 1_000_000 >= cfg["min_vol_signal"]]
    targets = tg_pool[:cfg["max_alerts"]] if tg_pool else []

    if targets:
        ok = send_telegram(
            build_telegram_msg(targets, caution,
                               len(testing_candidates), len(candidates))
        )
        if ok:
            log.info(f"📤 Telegram: {len(targets)} signals sent")
            for r in targets:
                set_cooldown(r["symbol"])

    _save_state(_state)
    log.info(f"=== SELESAI — {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)
    run_scan()
