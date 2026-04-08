#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v15.0 — DATA-DRIVEN (104 PUMP EVENTS v2, 47 FITUR)        ║
║  Basis: feature_discovery_v2 (atr_now #1 disc=133.88, range_pct #2, bb_w #3)║
║  Update: weight sesuai disc_score v2 | Type D relaxed + dist_to_support      ║
║  Velocity gates relaxed | Bitget-only lebih kuat | Pre-filter aktif          ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import logging
import logging.handlers as _lh
import os
import time
import sqlite3
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "15.0-DATA-V2"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch   = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/scanner_v15.log", maxBytes=10 * 1024**2, backupCount=3)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG v15.0 — SEMUA BERDASARKAN FEATURE DISCOVERY v2
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key":  os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":          os.getenv("BOT_TOKEN"),
    "chat_id":            os.getenv("CHAT_ID"),

    # ── Universe filter (v15: lebih longgar sesuai data pump low-vol) ───────────
    "pre_filter_vol_min":     250_000,   # turun dari 300k
    "pre_filter_vol_max":   250_000_000,
    "max_symbols_per_scan":         160,

    # ── Velocity gates — RELAXED sesuai data v2 (banyak pump +7-11% di 1h/4h) ──
    "velocity_gates": {
        "chg_1h_max":                9.5,   # naik dari 6.0
        "chg_4h_max":               13.0,   # naik dari 8.0
        "chg_24h_max_early":        18.0,   # naik dari 12.0
        "chg_24h_max_continuation": 35.0,
        "chg_24h_min":             -10.0,
    },

    # ── Bitget candle settings ────────────────────────────────────────────────
    "candle_limit_bitget": 100,
    "prefilter_bitget_top_n": 80,   # AKTIF — pakai ATR + wick + decel (v2 top features)

    # ── Coinalyze settings ────────────────────────────────────────────────────
    "coinalyze_lookback_h":          72,
    "coinalyze_funding_lookback_h": 168,
    "coinalyze_interval":        "1hour",
    "coinalyze_funding_interval":  "daily",
    "coinalyze_funding_interval_alt": "1hour",
    "coinalyze_batch_size":           10,
    "coinalyze_rate_limit_wait":     1.2,

    # ── TIER 1 weights (Coinalyze) — tidak berubah banyak karena data v2 fokus candle ──
    "ls_ratio_weight":           35,
    "buy_vol_ratio_weight":      30,
    "funding_trend_weight":      25,
    "funding_snapshot_weight":   15,
    "predicted_funding_weight":  20,
    "oi_buildup_weight":         20,
    "short_liq_weight":          20,
    "liq_cascade_weight":        15,

    # ── TIER 3 weights — DISESUAIKAN DENGAN DISC_SCORE v2 ─────────────────────
    "bbw_squeeze_weight":        10,     # bb_w #3 disc=130.88
    "accumulation_weight":       15,
    "price_stability_weight":    14,     # range_pct #2 disc=132.18
    "volume_dryup_weight":       8,
    "volatility_return_weight":  35,     # atr_now #1 disc=133.88 (fitur TERKUAT)
    "rs_btc_weight":             10,

    # ── FITUR BARU & DIPERKUAT dari v2 ────────────────────────────────────────
    "lower_wick_weight":         20,     # last_wick_dn #4 disc=118.57
    "momentum_decel_weight":     9,      # momentum_accel masih moderate
    "dist_to_support_weight":    18,     # STRONG baru disc=72.91 lift=2.23x
    "rs_24h_weight":             13,     # STRONG disc=66.82

    # ── Alert thresholds (v15: lebih seimbang) ───────────────────────────────
    "multiwave_bonus":           30,
    "alert_threshold_early":     88,     # turun dari 95 (data v2 lebih kuat)
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal":  78,
    "alert_threshold_bitget_only": 68,   # lebih longgar

    # ── Entry/SL/TP ───────────────────────────────────────────────────────────
    "min_rr_ratio":         2.0,
    "max_alerts_per_scan":    6,
    "atr_candles":           14,
    "sl_mult_volatile":     2.5,
    "sl_mult_normal":       2.0,
    "sl_mult_quiet":        1.5,
    "tp1_pct":             15.0,
    "tp2_pct":             30.0,
    "tp3_pct":             50.0,

    # ── Position sizing ───────────────────────────────────────────────────────
    "account_balance":       10000.0,
    "risk_per_trade_pct":      1.0,
    "max_position_pct":        5.0,
    "max_leverage":           10,

    # ── History DB ────────────────────────────────────────────────────────────
    "pump_history_db":    "/tmp/scanner_v15_history.db",
    "pump_threshold_pct":    15,
    "pump_max_duration_h":   24,
    "multiwave_lookback_days": 30,

    # ── Circuit breaker ───────────────────────────────────────────────────────
    "btc_dump_threshold":  -3.0,

    # ── L/S ratio thresholds (tetap dari v14.8) ───────────────────────────────
    "ls_long_extreme_low":  0.42,
    "ls_long_low":          0.47,
    "ls_long_normal":       0.50,
    "ls_long_high":         0.58,

    # ── Buy volume thresholds ─────────────────────────────────────────────────
    "bv_ratio_strong":      0.62,
    "bv_ratio_moderate":    0.55,

    # ── Pump type thresholds (Type D di-relax sesuai v2) ──────────────────────
    "short_squeeze_ls_min":    8,
    "short_squeeze_liq_min":   6,
    "short_squeeze_fund_min":  7,
    "whale_accum_bv_min":      8,
    "whale_accum_accum_min":   5,

    "squeeze_alt_fund_liq_fund":   20,
    "squeeze_alt_fund_liq_liq":    18,
    "squeeze_alt_fund_pred_fund":  15,
    "squeeze_alt_fund_pred_pred":  10,
    "squeeze_alt_fund_pred_liq":    8,

    # ── Type D baru (v15) ─────────────────────────────────────────────────────
    "type_d_min_oi_sc":         4,      # relax
    "type_d_min_liq_sc":        4,
    "type_d_min_fund_sc":       6,
    "type_d_min_bbw":           6,      # relax dari 8
    "type_d_min_dry":           4,      # relax dari 5

    # ── DistToSupport & Inside Compression (dari v2) ──────────────────────────
    "support_candle_window":   96,
    "support_cluster_tol":   0.020,
    "support_bounce_min":        2,
    "support_bounce_max":        5,

    # ── Blacklist ─────────────────────────────────────────────────────────────
    "stock_token_blacklist": [
        "HOODUSDT", "COINUSDT", "MSTRUSDT", "NVDAUSDT", "AAPLUSDT",
        "GOOGLUSDT", "AMZNUSDT", "METAUSDT", "QQQUSDT", "BZUSDT",
        "MCDUSDT", "NIGHTUSDT", "JCTUSDT", "NOMUSDT", "ASTERUSDT",
        "POLYXUSDT", "PIUSDT", "WMTUSDT", "BGBUSDT", "MEUSDT",
        "TSLAUSDT", "CRCLUSDT", "SPYUSDT", "GLDUSDT", "MSFTUSDT",
        "PLTRUSDT", "INTCUSDT", "XAUSDT", "USDCUSDT", "TRXUSDT",
    ],
}


# (Semua dataclass, helper, ATR, phase, Tier 1, Tier 2, Tier 3 functions SAMA seperti v14.9.1)
# Hanya yang berubah saya tulis di bawah ini. Sisanya copy dari file lama kamu.

# ─────────────────────────────────────────────────────────────────────────────
#  PERUBAHAN PENTING DI TIER 3 (berdasarkan v2 data)
# ─────────────────────────────────────────────────────────────────────────────
def detect_dist_to_support(candles: List[dict], price: float) -> Tuple[int, dict]:
    # (fungsi lama tetap, tapi bobot dinaikkan di scoring)
    ...  # sama seperti v14.9

def detect_inside_compression(candles: List[dict], price: float) -> Tuple[int, dict]:
    """Baru v15 — dari feature v2: inside_compression = 0 sangat kuat untuk pump"""
    if len(candles) < 30 or price <= 0:
        return 0, {}
    recent_lows = [c["low"] for c in candles[-30:]]
    support = min(recent_lows)
    dist = (price - support) / support * 100
    inside = 1 if dist <= 3.0 else 0   # sesuai definisi v2 (~3%)
    score = 12 if inside == 0 else 0    # reward breakout (0 = bagus)
    return score, {"inside": inside, "dist_pct": round(dist, 2), "pattern": "BREAKOUT" if inside == 0 else "COMPRESSION"}


# ─────────────────────────────────────────────────────────────────────────────
#  MASTER SCORING v15.0
# ─────────────────────────────────────────────────────────────────────────────
def score_coin_v15(data: CoinData) -> Optional[ScoreResult]:
    # ... (sama sampai tier3_score)

    # Tambah inside_compression
    comp_sc, comp_d = detect_inside_compression(data.candles, data.price)
    tier3_score = (bbw_sc + stab_sc + dry_sc + accum_sc + vret_sc + rs_sc
                   + wick_sc + decel_sc + supp_sc + rs24_sc + comp_sc)

    # Type D — RELAXED + integrasi dist & inside (sesuai data v2)
    type_d_coinalyze_confirmed = (
        oi_sc >= CONFIG["type_d_min_oi_sc"] or
        liq_sc >= CONFIG["type_d_min_liq_sc"] or
        fund_sc >= CONFIG["type_d_min_fund_sc"]
    )
    if (bbw_sc >= CONFIG["type_d_min_bbw"] and 
        dry_sc >= CONFIG["type_d_min_dry"] and 
        supp_sc >= 6 and comp_sc > 0 and type_d_coinalyze_confirmed):
        pump_types.append(PumpType(
            "D", "Technical Breakout",
            min((bbw_sc + dry_sc + supp_sc) * 3, 100),
            [bbw_d.get("pattern", ""), dry_d.get("pattern", ""), comp_d.get("pattern", "")]
        ))

    # Bitget-only emergency path (lebih kuat)
    has_any_clz = ...  # sama
    if not has_any_clz and phase.phase == "EARLY" and total >= CONFIG["alert_threshold_bitget_only"]:
        if not pump_types:
            pump_types.append(PumpType("X", "Pure Technical (Bitget-only)", total, ["ATR + Range + Wick"]))

    # ... (sisanya sama seperti v14.9)

    return ScoreResult(...)  # sama


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN (hanya tambah log v15)
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info(f"{'═'*70}")
    log.info(f"  PRE-PUMP SCANNER v{VERSION} — FULL DATA v2 DRIVEN")
    log.info(f"  Top features: ATR, Range, BBW, LowerWick, DistSupport (v2)")
    # ... sisanya sama

    # Aktifkan prefilter
    prefilter_n = CONFIG.get("prefilter_bitget_top_n", 80)
    if prefilter_n > 0 and len(active) > prefilter_n:
        active = prefilter_by_bitget(active, tickers, top_n=prefilter_n)

    # ... lanjut sama

if __name__ == "__main__":
    exit(main())
