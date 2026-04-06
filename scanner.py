#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  BACKTEST + GRID SEARCH — Pre-Pump Scanner v14.5                           ║
║                                                                              ║
║  METODOLOGI:                                                                 ║
║  1. Fetch historical Bitget candles 1H (~31 hari / 750 candles)             ║
║  2. Pre-compute raw features di setiap candle (sliding window)               ║
║  3. Grid search param combinations tanpa API call ulang (pure math)          ║
║  4. Outcome: pump ≥15% dalam 6h, 12h, 24h setelah signal                   ║
║  5. Metrics: Precision, Recall, F1, Signal Rate                              ║
║  6. Output: best_config.json + backtest_results.csv                         ║
║                                                                              ║
║  CATATAN:                                                                    ║
║  • Backtest ini hanya menggunakan Bitget candles (Tier 3 signals)            ║
║  • Coinalyze (Tier 1/2) tidak tersedia untuk historical replay               ║
║  • Fokus grid search: threshold + bobot Tier 3 + Type D/B/F triggers        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import csv
import json
import logging
import math
import sys
import time
import itertools
import requests

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/backtest_v14.log"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  BACKTEST SETTINGS
# ══════════════════════════════════════════════════════════════════════════════

# Universe: ambil dari Bitget top symbols berdasarkan volume
UNIVERSE_SIZE        = 60          # jumlah simbol yang di-backtest
MIN_VOL_24H          = 2_000_000   # filter minimum volume (USD)
MAX_VOL_24H          = 200_000_000
CANDLE_LIMIT         = 750         # ~31 hari @ 1H
MIN_LOOKBACK         = 80          # candles minimum sebelum mulai simulasi
PUMP_THRESHOLD_PCT   = 15.0        # outcome: pump ≥ X% dianggap True
LOOKAHEAD_HOURS      = [6, 12, 24] # cek pump di H+6, H+12, H+24
MIN_SIGNALS_FOR_EVAL = 3           # skip simbol dengan sinyal < ini
COOLDOWN_HOURS       = 4           # skip candle setelah sinyal aktif

# ── Default CONFIG (sama persis seperti scanner v14.5) ───────────────────────
BASE_CONFIG: Dict = {
    "ls_ratio_weight":           35,
    "buy_vol_ratio_weight":      30,
    "funding_trend_weight":      25,
    "funding_snapshot_weight":   15,
    "predicted_funding_weight":  20,
    "oi_buildup_weight":         20,
    "short_liq_weight":          20,
    "liq_cascade_weight":        15,
    "bbw_squeeze_weight":        15,
    "accumulation_weight":       15,
    "price_stability_weight":    10,
    "volume_dryup_weight":       10,
    "volatility_return_weight":  15,
    "rs_btc_weight":             12,
    "alert_threshold_early":     85,
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal":  80,
    "bv_ratio_strong":           0.62,
    "bv_ratio_moderate":         0.55,
    "ls_long_extreme_low":       0.38,
    "ls_long_low":               0.44,
    "ls_long_normal":            0.50,
    "ls_long_high":              0.58,
    "whale_accum_bv_min":        8,
    "whale_accum_accum_min":     5,
    "short_squeeze_ls_min":      10,
    "short_squeeze_liq_min":     6,
    "short_squeeze_fund_min":    7,
    "velocity_gates": {
        "chg_1h_max":              4.0,
        "chg_4h_max":              8.0,
        "chg_24h_max_early":       12.0,
        "chg_24h_max_continuation":30.0,
        "chg_24h_min":            -8.0,
    },
}

# ── Grid search parameter space ───────────────────────────────────────────────
# Fokus pada parameter yang paling berpengaruh pada precision/recall
PARAM_GRID: Dict[str, List] = {
    "alert_threshold_early":    [70, 80, 85, 90, 100],
    "bbw_squeeze_weight":       [10, 13, 15, 18, 20],
    "accumulation_weight":      [10, 13, 15, 18],
    "volatility_return_weight": [10, 13, 15, 18],
    "volume_dryup_weight":      [7,  10, 13],
    "price_stability_weight":   [7,  10, 13],
}
# Kombinasi grid = 5×5×4×4×3×3 = 3600 — manageable dengan pre-computed features


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class RawFeatures:
    """Pre-computed raw features untuk satu (symbol, candle_idx)."""
    sym:          str
    candle_idx:   int
    ts:           int          # timestamp candle
    close_price:  float
    chg_24h:      float
    chg_1h:       float
    chg_4h:       float
    # Tier 3 raw values (bukan scores — bisa di-rescore dengan param berbeda)
    bb_w:         float        # Bollinger Band Width
    bb_tighter:   bool
    range_pct:    float        # price stability range %
    curr_chg:     float        # candle body change %
    vol_ratio:    float        # current vol / avg vol (accumulation)
    price_chg_6h: float        # price change 6h (untuk accum filter)
    dry_ratio:    float        # volume dry-up ratio
    atr_ratio:    float        # volatility return ratio
    rs_1h:        float        # relative strength vs BTC 1h
    btc_chg_1h:   float        # BTC 1h change
    # Future prices untuk outcome checking
    max_hi_6h:    float = 0.0
    max_hi_12h:   float = 0.0
    max_hi_24h:   float = 0.0


@dataclass
class SignalRecord:
    sym: str
    ts: int
    score: int
    phase: str
    pump_types: str
    tp_6h: bool
    tp_12h: bool
    tp_24h: bool
    max_pct_6h: float
    max_pct_12h: float
    max_pct_24h: float


@dataclass
class BacktestResult:
    n_signals: int
    n_tp_6h: int
    n_tp_12h: int
    n_tp_24h: int
    precision_6h: float
    precision_12h: float
    precision_24h: float
    recall_24h: float
    f1_24h: float
    signal_rate: float   # signals per symbol per day
    avg_score: float


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET API
# ══════════════════════════════════════════════════════════════════════════════

BITGET_BASE = "https://api.bitget.com"

def bitget_get(url: str, params: dict, retries: int = 3) -> Optional[dict]:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                time.sleep(10)
                continue
            break
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)
    return None


def fetch_tickers() -> Dict[str, dict]:
    data = bitget_get(f"{BITGET_BASE}/api/v2/mix/market/tickers",
                      {"productType": "USDT-FUTURES"})
    if not data or data.get("code") != "00000":
        return {}
    return {item["symbol"]: item for item in data.get("data", [])}


def fetch_candles_full(symbol: str, limit: int = 750) -> List[dict]:
    """Fetch historical hourly candles. Bitget v2 max 1000 per request."""
    data = bitget_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        {"symbol": symbol, "productType": "USDT-FUTURES",
         "granularity": "1H", "limit": min(limit, 1000)},
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for row in data.get("data", []):
        try:
            vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
            candles.append({
                "ts":         int(row[0]),
                "open":       float(row[1]),
                "high":       float(row[2]),
                "low":        float(row[3]),
                "close":      float(row[4]),
                "volume_usd": vol_usd,
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


# ══════════════════════════════════════════════════════════════════════════════
#  🔧  PURE MATH HELPERS (dikopi dari scanner, tanpa global CONFIG)
# ══════════════════════════════════════════════════════════════════════════════

def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def _calc_atr(candles: List[dict], n: int = 14) -> float:
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


def _get_chg(candles: List[dict], n_hours: int) -> float:
    if len(candles) < n_hours + 2:
        return 0.0
    now_price  = candles[-2]["close"]
    prev_price = candles[-(n_hours + 2)]["close"]
    if prev_price <= 0:
        return 0.0
    return (now_price - prev_price) / prev_price * 100


def _classify_phase(chg_24h: float) -> Tuple[str, int]:
    """Returns (phase_name, base_score)."""
    if chg_24h < -8.0:
        return "DOWNTREND", 5
    elif chg_24h < -3.0:
        return "WEAK", 15
    elif chg_24h > 25.0:
        return "PARABOLIC", 10
    elif chg_24h > 12.0:
        base = max(20, 40 - int(chg_24h - 12) * 2)
        return "CONTINUATION", base
    else:
        if abs(chg_24h) <= 3.0:
            base = 60
        elif chg_24h <= 8.0:
            base = 50
        else:
            base = 40
        return "EARLY", base


def _check_velocity(chg_24h, chg_1h, chg_4h, phase: str, cfg: dict) -> bool:
    """Returns True if BLOCKED by velocity gate."""
    vg = cfg["velocity_gates"]
    if phase in ["DOWNTREND", "WEAK"]:
        return False
    is_cont = phase == "CONTINUATION"
    if chg_24h < vg["chg_24h_min"]:
        return True
    max_24h = vg["chg_24h_max_continuation"] if is_cont else vg["chg_24h_max_early"]
    if chg_24h > max_24h:
        return True
    if chg_1h > vg["chg_1h_max"]:
        return True
    if chg_4h > vg["chg_4h_max"]:
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  📐  FEATURE EXTRACTOR (raw values, bukan score)
# ══════════════════════════════════════════════════════════════════════════════

def extract_raw_features(sym: str, candles: List[dict],
                          btc_candles: List[dict], idx: int) -> Optional[RawFeatures]:
    """
    Ekstrak raw features dari candles[:idx+1] untuk satu titik waktu.
    Output berupa nilai mentah (bb_w, range_pct, dll) yang nanti di-rescore
    saat grid search tanpa harus fetch data lagi.
    """
    window = candles[:idx + 1]
    if len(window) < MIN_LOOKBACK:
        return None

    close_price = window[-2]["close"] if len(window) >= 2 else 0.0
    if close_price <= 0:
        return None

    chg_24h = _get_chg(window, 24)
    chg_1h  = _get_chg(window, 1)
    chg_4h  = _get_chg(window, 4)

    # ── BBW Squeeze ───────────────────────────────────────────────────────────
    closes_20 = [c["close"] for c in window[-20:]] if len(window) >= 20 else []
    bb_w, bb_tighter = 0.0, False
    if len(closes_20) == 20:
        sma = _mean(closes_20)
        if sma > 0:
            std = math.sqrt(sum((x - sma) ** 2 for x in closes_20) / 20)
            bb_w = (4 * std) / sma
            if len(window) >= 44:
                prev_c = [c["close"] for c in window[-44:-24]]
                if prev_c:
                    p_sma = _mean(prev_c)
                    if p_sma > 0:
                        p_std = math.sqrt(sum((x - p_sma) ** 2 for x in prev_c) / len(prev_c))
                        p_bbw = (4 * p_std) / p_sma
                        bb_tighter = bb_w < p_bbw

    # ── Price Stability ────────────────────────────────────────────────────────
    range_pct, curr_chg = 0.0, 0.0
    if len(window) >= 10:
        recent = window[-9:-1]
        closes = [c["close"] for c in recent]
        lo, hi = min(closes), max(closes)
        ref = (lo + hi) / 2
        if ref > 0:
            range_pct = (hi - lo) / ref * 100
        last = window[-2]
        if last.get("open", 0) > 0:
            curr_chg = (last["close"] - last["open"]) / last["open"] * 100

    # ── Volume Dry-up ─────────────────────────────────────────────────────────
    dry_ratio = 1.0
    if len(window) >= 26:
        cur_vol = window[-2].get("volume_usd", 0)
        avg_vol = _mean([c.get("volume_usd", 0) for c in window[-26:-2]])
        if avg_vol > 0:
            dry_ratio = cur_vol / avg_vol

    # ── Accumulation ──────────────────────────────────────────────────────────
    vol_ratio, price_chg_6h = 0.0, 0.0
    if len(window) >= 26 and window[-7]["close"] > 0:
        cur_v  = _mean([c.get("volume_usd", 0) for c in window[-7:-1]])
        base_v = _mean([c.get("volume_usd", 0) for c in window[-25:-7]])
        if base_v > 0:
            vol_ratio = cur_v / base_v
        price_chg_6h = (window[-2]["close"] - window[-7]["close"]) / window[-7]["close"] * 100

    # ── Volatility Return ─────────────────────────────────────────────────────
    atr_ratio = 1.0
    if len(window) >= 50:
        atr_now  = _calc_atr(window[-22:], 14)
        atr_hist = _calc_atr(window[-72:-24], 14) if len(window) >= 74 else _calc_atr(window[:-24], 14)
        if atr_hist > 0:
            atr_ratio = atr_now / atr_hist

    # ── RS vs BTC ─────────────────────────────────────────────────────────────
    btc_chg_1h, rs_1h = 0.0, 0.0
    if len(btc_candles) >= 3:
        ts_target = window[-2]["ts"]
        # Cari candle BTC yang paling dekat dengan ts_target
        btc_ref = min(btc_candles, key=lambda c: abs(c["ts"] - ts_target))
        btc_idx = btc_candles.index(btc_ref)
        if btc_idx >= 3:
            btc_now  = btc_candles[btc_idx]["close"]
            btc_prev = btc_candles[btc_idx - 2]["close"]
            if btc_prev > 0:
                btc_chg_1h = (btc_now - btc_prev) / btc_prev * 100
        rs_1h = chg_1h - btc_chg_1h

    # ── Future prices untuk outcome ───────────────────────────────────────────
    future = candles[idx + 1:idx + 1 + 24]
    ref_close = close_price
    max_hi_6h  = max((c["high"] for c in future[:6]),  default=ref_close)
    max_hi_12h = max((c["high"] for c in future[:12]), default=ref_close)
    max_hi_24h = max((c["high"] for c in future[:24]), default=ref_close)

    return RawFeatures(
        sym=sym, candle_idx=idx, ts=window[-2]["ts"],
        close_price=close_price,
        chg_24h=chg_24h, chg_1h=chg_1h, chg_4h=chg_4h,
        bb_w=bb_w, bb_tighter=bb_tighter,
        range_pct=range_pct, curr_chg=curr_chg,
        vol_ratio=vol_ratio, price_chg_6h=price_chg_6h,
        dry_ratio=dry_ratio, atr_ratio=atr_ratio,
        rs_1h=rs_1h, btc_chg_1h=btc_chg_1h,
        max_hi_6h=max_hi_6h, max_hi_12h=max_hi_12h, max_hi_24h=max_hi_24h,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  SCORER (menggunakan pre-computed features + config params)
# ══════════════════════════════════════════════════════════════════════════════

def score_features(feat: RawFeatures, cfg: dict) -> Tuple[int, List[str]]:
    """
    Hitung skor dari raw features menggunakan cfg yang diberikan.
    Return (total_score, pump_types_list).
    Tidak ada API call — murni math dari pre-computed features.
    """
    phase, phase_score = _classify_phase(feat.chg_24h)
    blocked = _check_velocity(feat.chg_24h, feat.chg_1h, feat.chg_4h, phase, cfg)
    if blocked:
        return 0, []

    pump_types = []

    # ── BBW Squeeze ───────────────────────────────────────────────────────────
    w_bbw = cfg["bbw_squeeze_weight"]
    bb_w = feat.bb_w
    if bb_w < 0.04:
        bbw_sc = w_bbw
    elif bb_w < 0.06:
        bbw_sc = int(w_bbw * 0.8)
    elif bb_w < 0.08:
        bbw_sc = int(w_bbw * 0.55)
    elif bb_w < 0.10:
        bbw_sc = int(w_bbw * 0.28)
    else:
        bbw_sc = 0
    if feat.bb_tighter and bbw_sc > 0:
        bbw_sc = min(bbw_sc + 5, w_bbw + 5)

    # ── Price Stability ────────────────────────────────────────────────────────
    w_stab = cfg["price_stability_weight"]
    rp = feat.range_pct
    if rp < 1.5 and abs(feat.curr_chg) < 0.5:
        stab_sc = w_stab
    elif rp < 2.5:
        stab_sc = int(w_stab * 0.67)
    elif rp < 4.0:
        stab_sc = int(w_stab * 0.33)
    else:
        stab_sc = 0

    # ── Volume Dry-up ─────────────────────────────────────────────────────────
    w_dry = cfg["volume_dryup_weight"]
    dr = feat.dry_ratio
    if dr < 0.35:
        dry_sc = w_dry
    elif dr < 0.50:
        dry_sc = int(w_dry * 0.7)
    elif dr < 0.65:
        dry_sc = int(w_dry * 0.4)
    else:
        dry_sc = 0

    # ── Accumulation ──────────────────────────────────────────────────────────
    w_accum = cfg["accumulation_weight"]
    vr = feat.vol_ratio
    pc = feat.price_chg_6h
    if vr >= 3.0 and -2 < pc < 4:
        accum_sc = w_accum
    elif vr >= 2.5 and -2 < pc < 5:
        accum_sc = int(w_accum * 0.75)
    elif vr >= 2.0 and -1 < pc < 4:
        accum_sc = int(w_accum * 0.5)
    else:
        accum_sc = 0

    # ── Volatility Return ─────────────────────────────────────────────────────
    w_vret = cfg["volatility_return_weight"]
    ar = feat.atr_ratio
    if ar < 0.40:
        vret_sc = w_vret
    elif ar < 0.60:
        vret_sc = int(w_vret * 0.7)
    elif ar < 0.75:
        vret_sc = int(w_vret * 0.4)
    else:
        vret_sc = 0

    # ── RS vs BTC ─────────────────────────────────────────────────────────────
    w_rs = cfg["rs_btc_weight"]
    rs = feat.rs_1h
    btc = feat.btc_chg_1h
    if rs > 3.0 and btc <= 0.5:
        rs_sc = w_rs
    elif rs > 2.0:
        rs_sc = int(w_rs * 0.67)
    elif rs > 1.0:
        rs_sc = int(w_rs * 0.33)
    else:
        rs_sc = 0

    tier3 = bbw_sc + stab_sc + dry_sc + accum_sc + vret_sc + rs_sc
    total = phase_score + tier3

    # ── Pump Types ────────────────────────────────────────────────────────────
    # Type D: Technical Breakout
    if bbw_sc >= (cfg["bbw_squeeze_weight"] * 0.9) and (stab_sc >= 8 or dry_sc >= 6):
        pump_types.append("D")
    # Type F: Volatility Return
    if vret_sc >= 10:
        pump_types.append("F")
    # Type B: Whale Accum (tanpa Coinalyze bv_sc, gunakan proxy accum)
    if accum_sc >= cfg["accumulation_weight"] * 0.75:
        pump_types.append("B")

    if not pump_types:
        return 0, []

    # ── Threshold check ───────────────────────────────────────────────────────
    if phase == "EARLY":
        threshold = cfg["alert_threshold_early"]
    elif phase == "CONTINUATION":
        threshold = cfg["alert_threshold_continuation"]
    elif phase in ["DOWNTREND", "WEAK"]:
        threshold = cfg["alert_threshold_reversal"]
    else:
        threshold = 110

    if total < threshold:
        return 0, []

    return total, pump_types


# ══════════════════════════════════════════════════════════════════════════════
#  📊  SIMULATION ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def simulate(all_features: List[RawFeatures], cfg: dict) -> List[SignalRecord]:
    """
    Jalankan simulasi lengkap pada pre-computed features dengan cfg tertentu.
    Terapkan cooldown antar-signal per simbol.
    """
    signals: List[SignalRecord] = []
    # Group by symbol
    by_sym: Dict[str, List[RawFeatures]] = {}
    for f in all_features:
        by_sym.setdefault(f.sym, []).append(f)

    for sym, feats in by_sym.items():
        feats_sorted = sorted(feats, key=lambda x: x.candle_idx)
        last_signal_ts = 0

        for feat in feats_sorted:
            # Cooldown: skip jika sinyal terakhir dalam COOLDOWN_HOURS
            hours_since_last = (feat.ts - last_signal_ts) / 3600
            if last_signal_ts > 0 and hours_since_last < COOLDOWN_HOURS:
                continue

            score, pump_types = score_features(feat, cfg)
            if score <= 0 or not pump_types:
                continue

            # Outcome
            ref = feat.close_price
            tp_6h  = feat.max_hi_6h  >= ref * (1 + PUMP_THRESHOLD_PCT / 100) if ref > 0 else False
            tp_12h = feat.max_hi_12h >= ref * (1 + PUMP_THRESHOLD_PCT / 100) if ref > 0 else False
            tp_24h = feat.max_hi_24h >= ref * (1 + PUMP_THRESHOLD_PCT / 100) if ref > 0 else False
            pct_6h  = (feat.max_hi_6h  / ref - 1) * 100 if ref > 0 else 0.0
            pct_12h = (feat.max_hi_12h / ref - 1) * 100 if ref > 0 else 0.0
            pct_24h = (feat.max_hi_24h / ref - 1) * 100 if ref > 0 else 0.0

            signals.append(SignalRecord(
                sym=sym, ts=feat.ts, score=score,
                phase=_classify_phase(feat.chg_24h)[0],
                pump_types="/".join(pump_types),
                tp_6h=tp_6h, tp_12h=tp_12h, tp_24h=tp_24h,
                max_pct_6h=round(pct_6h, 2),
                max_pct_12h=round(pct_12h, 2),
                max_pct_24h=round(pct_24h, 2),
            ))
            last_signal_ts = feat.ts

    return signals


def compute_metrics(signals: List[SignalRecord],
                    n_symbols: int, days: float) -> BacktestResult:
    n = len(signals)
    if n == 0:
        return BacktestResult(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    tp6  = sum(1 for s in signals if s.tp_6h)
    tp12 = sum(1 for s in signals if s.tp_12h)
    tp24 = sum(1 for s in signals if s.tp_24h)

    precision_6h  = tp6  / n
    precision_12h = tp12 / n
    precision_24h = tp24 / n

    # Recall: berapa banyak pump ≥15% yang kita tangkap?
    # Estimasi total pump events dari TP+FN (kita tidak tahu FN secara langsung)
    # Proxy: assume precision_24h * n / (true_pump_rate)
    # Untuk recall, kita hitung dari symbol-days dengan pump
    # Simple proxy: recall = TP24 / (TP24 + FN)
    # FN = pump events yang terjadi tapi tidak ter-signal → sulit tanpa ground truth
    # Kita pakai recall estimasi: TP24 / expected_pumps
    # Expected pumps per symbol per month: ~2 (empirical rough estimate)
    expected_pumps = n_symbols * days / 30 * 2  # rough: 2 pumps/symbol/month
    recall_24h = min(tp24 / max(expected_pumps, 1), 1.0)

    f1_24h = 0.0
    if (precision_24h + recall_24h) > 0:
        f1_24h = 2 * precision_24h * recall_24h / (precision_24h + recall_24h)

    signal_rate = n / (n_symbols * days) if n_symbols * days > 0 else 0.0
    avg_score   = _mean([s.score for s in signals]) if signals else 0.0

    return BacktestResult(
        n_signals=n, n_tp_6h=tp6, n_tp_12h=tp12, n_tp_24h=tp24,
        precision_6h=round(precision_6h, 4),
        precision_12h=round(precision_12h, 4),
        precision_24h=round(precision_24h, 4),
        recall_24h=round(recall_24h, 4),
        f1_24h=round(f1_24h, 4),
        signal_rate=round(signal_rate, 4),
        avg_score=round(avg_score, 2),
    )


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  GRID SEARCH
# ══════════════════════════════════════════════════════════════════════════════

def run_grid_search(all_features: List[RawFeatures],
                    n_symbols: int, days: float) -> List[dict]:
    """
    Jalankan grid search atas semua kombinasi parameter.
    Setiap kombinasi hanya operasi math di atas pre-computed features — sangat cepat.
    """
    param_names  = list(PARAM_GRID.keys())
    param_values = list(PARAM_GRID.values())
    total_combos = 1
    for v in param_values:
        total_combos *= len(v)

    log.info(f"Grid search: {len(param_names)} params, {total_combos} kombinasi")

    results = []
    for i, combo in enumerate(itertools.product(*param_values)):
        cfg = dict(BASE_CONFIG)
        for name, val in zip(param_names, combo):
            cfg[name] = val

        signals  = simulate(all_features, cfg)
        metrics  = compute_metrics(signals, n_symbols, days)

        row = {
            "combo_id":              i,
            "n_signals":             metrics.n_signals,
            "precision_24h":         metrics.precision_24h,
            "precision_12h":         metrics.precision_12h,
            "precision_6h":          metrics.precision_6h,
            "recall_24h":            metrics.recall_24h,
            "f1_24h":                metrics.f1_24h,
            "signal_rate_per_symday":metrics.signal_rate,
            "avg_score":             metrics.avg_score,
        }
        for name, val in zip(param_names, combo):
            row[name] = val

        results.append(row)

        if (i + 1) % 200 == 0:
            log.info(f"  Grid search progress: {i+1}/{total_combos}")

    # Sort by F1 desc, precision_24h desc
    results.sort(key=lambda x: (x["f1_24h"], x["precision_24h"]), reverse=True)
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  📤  OUTPUT WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_signals_csv(signals: List[SignalRecord], path: str):
    if not signals:
        log.warning("No signals to write.")
        return
    with open(path, "w", newline="") as f:
        fieldnames = ["sym","ts","datetime","score","phase","pump_types",
                      "tp_6h","tp_12h","tp_24h","max_pct_6h","max_pct_12h","max_pct_24h"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for s in signals:
            dt = datetime.fromtimestamp(s.ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            w.writerow({
                "sym": s.sym, "ts": s.ts, "datetime": dt,
                "score": s.score, "phase": s.phase, "pump_types": s.pump_types,
                "tp_6h": int(s.tp_6h), "tp_12h": int(s.tp_12h), "tp_24h": int(s.tp_24h),
                "max_pct_6h": s.max_pct_6h, "max_pct_12h": s.max_pct_12h,
                "max_pct_24h": s.max_pct_24h,
            })
    log.info(f"  Signals CSV: {path}")


def write_grid_csv(results: List[dict], path: str):
    if not results:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)
    log.info(f"  Grid CSV: {path}")


def write_best_config(best_params: dict, baseline_metrics: BacktestResult,
                      best_metrics: BacktestResult, path: str):
    output = {
        "scanner_version": "14.5",
        "backtest_date":   datetime.now(timezone.utc).isoformat(),
        "backtest_period_days": round(CANDLE_LIMIT / 24, 1),
        "universe_size":   UNIVERSE_SIZE,
        "pump_threshold_pct": PUMP_THRESHOLD_PCT,
        "baseline": {
            "n_signals":      baseline_metrics.n_signals,
            "precision_24h":  baseline_metrics.precision_24h,
            "precision_12h":  baseline_metrics.precision_12h,
            "f1_24h":         baseline_metrics.f1_24h,
            "signal_rate":    baseline_metrics.signal_rate,
        },
        "best": {
            "n_signals":      best_metrics.n_signals,
            "precision_24h":  best_metrics.precision_24h,
            "precision_12h":  best_metrics.precision_12h,
            "f1_24h":         best_metrics.f1_24h,
            "signal_rate":    best_metrics.signal_rate,
        },
        "improvement": {
            "precision_24h_delta": round(best_metrics.precision_24h - baseline_metrics.precision_24h, 4),
            "f1_24h_delta":        round(best_metrics.f1_24h - baseline_metrics.f1_24h, 4),
        },
        "recommended_config_changes": best_params,
        "note": (
            "Config changes di atas hanya untuk parameter Tier 3 (Bitget candles). "
            "Parameter Tier 1/2 (Coinalyze) tidak di-backtest karena historical data tidak tersedia. "
            "Apply perubahan ini ke CONFIG scanner v14.5 untuk parameter yang sesuai."
        ),
    }
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    log.info(f"  Best config: {path}")


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("═" * 68)
    log.info("  BACKTEST + GRID SEARCH — Pre-Pump Scanner v14.5")
    log.info(f"  Universe: {UNIVERSE_SIZE} syms | Candles: {CANDLE_LIMIT}h | "
             f"Pump target: ≥{PUMP_THRESHOLD_PCT}%")
    log.info("═" * 68)

    # ── Step 1: Fetch Bitget universe ─────────────────────────────────────────
    log.info("📊 Step 1: Fetching Bitget tickers...")
    tickers = fetch_tickers()
    if not tickers:
        log.error("❌ No tickers from Bitget")
        return 1
    log.info(f"  Got {len(tickers)} tickers")

    # Filter & sort by volume
    STOCK_BLACKLIST = {
        "HOODUSDT","COINUSDT","MSTRUSDT","NVDAUSDT","AAPLUSDT","GOOGLUSDT",
        "AMZNUSDT","METAUSDT","QQQUSDT","MCDUSDT","PIUSDT","WMTUSDT",
        "BGBUSDT","TSLAUSDT","CRCLUSDT","SPYUSDT","GLDUSDT","MSFTUSDT",
        "PLTRUSDT","INTCUSDT","XAUSDT","USDCUSDT","TRXUSDT","BZUSDT",
    }
    candidates = []
    for sym, t in tickers.items():
        if sym.strip().upper() in STOCK_BLACKLIST:
            continue
        try:
            vol = float(t.get("quoteVolume", 0))
            if MIN_VOL_24H <= vol <= MAX_VOL_24H:
                candidates.append((sym, vol))
        except Exception:
            pass
    candidates.sort(key=lambda x: x[1], reverse=True)
    universe = [s for s, _ in candidates[:UNIVERSE_SIZE]]
    log.info(f"  Universe selected: {len(universe)} symbols")

    # ── Step 2: Fetch BTC candles (reference) ─────────────────────────────────
    log.info("₿  Step 2: Fetching BTC candles...")
    btc_candles = fetch_candles_full("BTCUSDT", CANDLE_LIMIT)
    log.info(f"  BTC: {len(btc_candles)} candles")

    # ── Step 3: Fetch + pre-compute features per symbol ───────────────────────
    log.info(f"📈 Step 3: Fetching candles + pre-computing features ({len(universe)} symbols)...")
    all_features: List[RawFeatures] = []
    failed_syms = []

    for i, sym in enumerate(universe):
        try:
            candles = fetch_candles_full(sym, CANDLE_LIMIT)
            if len(candles) < MIN_LOOKBACK + 25:
                log.warning(f"  ⚠️ {sym}: only {len(candles)} candles — skip")
                failed_syms.append(sym)
                continue

            sym_features = 0
            for idx in range(MIN_LOOKBACK, len(candles) - 24):
                feat = extract_raw_features(sym, candles, btc_candles, idx)
                if feat is not None:
                    all_features.append(feat)
                    sym_features += 1

            log.info(f"  [{i+1:3d}/{len(universe)}] {sym}: {len(candles)} candles → {sym_features} feature points")
            time.sleep(0.15)  # gentle rate limit
        except Exception as e:
            log.warning(f"  ⚠️ {sym}: {e}")
            failed_syms.append(sym)

    valid_syms = len(universe) - len(failed_syms)
    days = CANDLE_LIMIT / 24
    log.info(f"  Total features: {len(all_features):,} | Valid symbols: {valid_syms} | Days: {days:.1f}")

    if not all_features:
        log.error("❌ No features extracted — check API connection")
        return 1

    # ── Step 4: Baseline backtest (default config) ────────────────────────────
    log.info("\n🎯 Step 4: Baseline backtest (default config v14.5)...")
    baseline_signals  = simulate(all_features, BASE_CONFIG)
    baseline_metrics  = compute_metrics(baseline_signals, valid_syms, days)

    log.info(f"  Baseline results:")
    log.info(f"    Signals:       {baseline_metrics.n_signals}")
    log.info(f"    Precision 6h:  {baseline_metrics.precision_6h:.1%}")
    log.info(f"    Precision 12h: {baseline_metrics.precision_12h:.1%}")
    log.info(f"    Precision 24h: {baseline_metrics.precision_24h:.1%}")
    log.info(f"    F1 24h:        {baseline_metrics.f1_24h:.4f}")
    log.info(f"    Signal rate:   {baseline_metrics.signal_rate:.3f} /sym/day")
    log.info(f"    Avg score:     {baseline_metrics.avg_score:.1f}")

    # Write baseline signals CSV
    write_signals_csv(baseline_signals, "/tmp/baseline_signals.csv")

    # ── Step 5: Grid search ───────────────────────────────────────────────────
    log.info(f"\n🔍 Step 5: Grid search...")
    t0 = time.time()
    grid_results = run_grid_search(all_features, valid_syms, days)
    elapsed = time.time() - t0
    log.info(f"  Grid search done in {elapsed:.1f}s")

    # Write full grid results
    write_grid_csv(grid_results, "/tmp/grid_search_results.csv")

    # ── Step 6: Analyze best config ───────────────────────────────────────────
    log.info("\n🏆 Step 6: Top 10 configurations by F1...")
    param_names = list(PARAM_GRID.keys())

    for rank, row in enumerate(grid_results[:10], 1):
        params_str = " | ".join(f"{k}={row[k]}" for k in param_names)
        log.info(
            f"  #{rank:2d}  F1={row['f1_24h']:.4f}  P24={row['precision_24h']:.1%} "
            f"P12={row['precision_12h']:.1%}  n={row['n_signals']}  "
            f"rate={row['signal_rate_per_symday']:.3f}  |  {params_str}"
        )

    # Best config
    best_row = grid_results[0]
    best_params = {k: best_row[k] for k in param_names}
    best_cfg = dict(BASE_CONFIG)
    best_cfg.update(best_params)
    best_signals = simulate(all_features, best_cfg)
    best_metrics = compute_metrics(best_signals, valid_syms, days)

    log.info(f"\n  Best vs Baseline:")
    log.info(f"    Precision 24h: {baseline_metrics.precision_24h:.1%} → {best_metrics.precision_24h:.1%}  "
             f"({best_metrics.precision_24h - baseline_metrics.precision_24h:+.1%})")
    log.info(f"    F1 24h:        {baseline_metrics.f1_24h:.4f} → {best_metrics.f1_24h:.4f}  "
             f"({best_metrics.f1_24h - baseline_metrics.f1_24h:+.4f})")
    log.info(f"    Signals:       {baseline_metrics.n_signals} → {best_metrics.n_signals}")

    # Write outputs
    write_signals_csv(best_signals, "/tmp/best_config_signals.csv")
    write_best_config(best_params, baseline_metrics, best_metrics, "/tmp/best_config.json")

    # ── Step 7: Parameter sensitivity analysis ────────────────────────────────
    log.info("\n📊 Step 7: Parameter sensitivity analysis...")
    for param in param_names:
        values = PARAM_GRID[param]
        f1_by_val = {}
        for val in values:
            # Average F1 across all combos with this param value
            rows = [r for r in grid_results if r[param] == val]
            f1_by_val[val] = _mean([r["f1_24h"] for r in rows])
        sorted_vals = sorted(f1_by_val.items(), key=lambda x: x[1], reverse=True)
        best_val, best_f1 = sorted_vals[0]
        bar_str = " ".join(f"{v}={'█'*int(f*100)}{f:.3f}" for v, f in sorted_vals)
        log.info(f"  {param:35s} best={best_val}  ({bar_str})")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info(f"\n{'═'*68}")
    log.info("  RINGKASAN REKOMENDASI CONFIG v14.5")
    log.info(f"{'═'*68}")
    log.info("  Parameter yang perlu diubah di CONFIG scanner:")
    for k, v in best_params.items():
        baseline_v = BASE_CONFIG.get(k, "N/A")
        changed = " ← BERUBAH" if v != baseline_v else ""
        log.info(f"    {k:40s} {baseline_v} → {v}{changed}")
    log.info(f"\n  Output files:")
    log.info(f"    /tmp/baseline_signals.csv     — sinyal dengan config default")
    log.info(f"    /tmp/best_config_signals.csv  — sinyal dengan config optimal")
    log.info(f"    /tmp/grid_search_results.csv  — semua {len(grid_results)} kombinasi")
    log.info(f"    /tmp/best_config.json         — rekomendasi config final")
    log.info(f"{'═'*68}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️  Stopped by user")
        sys.exit(0)
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True)
        sys.exit(1)
