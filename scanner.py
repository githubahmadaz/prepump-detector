#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v15.0 — DATA-DRIVEN v2 (104 PUMP EVENTS, 47 FITUR)        ║
║  Basis: feature_discovery_v2.log + feature_importance_v2.csv                ║
║  Update: bobot sesuai disc_score | Type D relaxed | dist_to_support + inside_compression
║  Velocity gates relaxed | Pre-filter Bitget aktif (80) | Bitget-only lebih kuat
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
#  ⚙️  CONFIG v15.0 (100% berdasarkan feature_discovery_v2)
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token": os.getenv("BOT_TOKEN"),
    "chat_id": os.getenv("CHAT_ID"),

    "pre_filter_vol_min": 250_000,
    "pre_filter_vol_max": 250_000_000,
    "max_symbols_per_scan": 160,

    "velocity_gates": {
        "chg_1h_max": 9.5,
        "chg_4h_max": 13.0,
        "chg_24h_max_early": 18.0,
        "chg_24h_max_continuation": 35.0,
        "chg_24h_min": -10.0,
    },

    "candle_limit_bitget": 100,
    "prefilter_bitget_top_n": 80,          # ← AKTIF (ATR + Wick + Decel)

    "coinalyze_lookback_h": 72,
    "coinalyze_funding_lookback_h": 168,
    "coinalyze_interval": "1hour",
    "coinalyze_funding_interval": "daily",
    "coinalyze_funding_interval_alt": "1hour",
    "coinalyze_batch_size": 10,
    "coinalyze_rate_limit_wait": 1.2,

    # Tier 1 (Coinalyze) tetap
    "ls_ratio_weight": 35, "buy_vol_ratio_weight": 30, "funding_trend_weight": 25,
    "funding_snapshot_weight": 15, "predicted_funding_weight": 20,
    "oi_buildup_weight": 20, "short_liq_weight": 20, "liq_cascade_weight": 15,

    # Tier 3 — Disesuaikan dengan disc_score v2
    "bbw_squeeze_weight": 10,
    "accumulation_weight": 15,
    "price_stability_weight": 14,
    "volume_dryup_weight": 8,
    "volatility_return_weight": 35,      # ATR #1
    "rs_btc_weight": 10,

    "lower_wick_weight": 20,
    "momentum_decel_weight": 9,
    "dist_to_support_weight": 18,        # STRONG baru
    "rs_24h_weight": 13,

    "multiwave_bonus": 30,
    "alert_threshold_early": 88,
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal": 78,
    "alert_threshold_bitget_only": 68,

    "min_rr_ratio": 2.0,
    "max_alerts_per_scan": 6,
    "atr_candles": 14,
    "sl_mult_volatile": 2.5,
    "sl_mult_normal": 2.0,
    "sl_mult_quiet": 1.5,
    "tp1_pct": 15.0, "tp2_pct": 30.0, "tp3_pct": 50.0,

    "account_balance": 10000.0,
    "risk_per_trade_pct": 1.0,
    "max_position_pct": 5.0,
    "max_leverage": 10,

    "pump_history_db": "/tmp/scanner_v15_history.db",
    "pump_threshold_pct": 15,
    "pump_max_duration_h": 24,
    "multiwave_lookback_days": 30,

    "btc_dump_threshold": -3.0,

    "ls_long_extreme_low": 0.42, "ls_long_low": 0.47, "ls_long_normal": 0.50, "ls_long_high": 0.58,
    "bv_ratio_strong": 0.62, "bv_ratio_moderate": 0.55,

    "short_squeeze_ls_min": 8, "short_squeeze_liq_min": 6, "short_squeeze_fund_min": 7,
    "whale_accum_bv_min": 8, "whale_accum_accum_min": 5,

    "squeeze_alt_fund_liq_fund": 20, "squeeze_alt_fund_liq_liq": 18,
    "squeeze_alt_fund_pred_fund": 15, "squeeze_alt_fund_pred_pred": 10, "squeeze_alt_fund_pred_liq": 8,

    "type_d_min_oi_sc": 4, "type_d_min_liq_sc": 4, "type_d_min_fund_sc": 6,
    "type_d_min_bbw": 6, "type_d_min_dry": 4,

    "support_candle_window": 96, "support_cluster_tol": 0.020,
    "support_bounce_min": 2, "support_bounce_max": 5,

    "stock_token_blacklist": [ ... ]  # sama seperti v14.9 kamu (saya singkat)
}

# === SEMUA DATACLASS, HELPER, ATR, PHASE, TIER 1, TIER 2, TIER 3 LAINNYA SAMA DENGAN v14.9.1 ===
# (Untuk menghemat ruang, saya tidak copy ulang 900+ baris yang tidak berubah.
# Kamu tinggal paste seluruh bagian dari v14.9.1 kamu mulai dari @dataclass sampai fungsi detect_rs_24h)

# Tambahan fungsi BARU v15.0 (dari data v2)
def detect_inside_compression(candles: List[dict], price: float) -> Tuple[int, dict]:
    if len(candles) < 30 or price <= 0:
        return 0, {}
    recent_lows = [c["low"] for c in candles[-30:] if c["low"] > 0]
    if not recent_lows:
        return 0, {}
    support = min(recent_lows)
    dist = (price - support) / support * 100
    inside = 1 if dist <= 3.0 else 0
    score = 12 if inside == 0 else 0   # reward breakout (0 = bagus)
    return score, {"inside": inside, "dist_pct": round(dist, 2), "pattern": "BREAKOUT" if inside == 0 else "COMPRESSION"}

# === MASTER SCORING v15.0 (hanya bagian ini yang diubah) ===
def score_coin_v15(data: CoinData) -> Optional[ScoreResult]:
    # ... (sama seperti score_coin_v14 sampai tier3_score)

    # Tambah inside_compression
    comp_sc, comp_d = detect_inside_compression(data.candles, data.price)
    tier3_score += comp_sc

    # Type D relaxed + integrasi dist & inside
    type_d_coinalyze_confirmed = (oi_sc >= CONFIG["type_d_min_oi_sc"] or liq_sc >= CONFIG["type_d_min_liq_sc"] or fund_sc >= CONFIG["type_d_min_fund_sc"])
    if bbw_sc >= CONFIG["type_d_min_bbw"] and dry_sc >= CONFIG["type_d_min_dry"] and supp_sc >= 6 and comp_sc > 0 and type_d_coinalyze_confirmed:
        pump_types.append(PumpType("D", "Technical Breakout", min((bbw_sc + dry_sc + supp_sc) * 3, 100), [...]))

    # Bitget-only emergency path
    has_any_clz = clz.has_ohlcv or clz.has_oi or clz.has_liq or clz.has_ls or clz.has_funding_hist
    if not has_any_clz and phase.phase == "EARLY" and total >= CONFIG["alert_threshold_bitget_only"]:
        if not pump_types:
            pump_types.append(PumpType("X", "Pure Technical (Bitget-only)", total, ["ATR + Range + Wick"]))

    # ... sisanya sama seperti v14.9 (threshold check, entry, return ScoreResult)

    return ScoreResult(...)   # sama

# === MAIN — PERBAIKAN KRITIS (active didefinisikan dulu) ===
def main():
    log.info(f"{'═'*70}")
    log.info(f"  PRE-PUMP SCANNER v{VERSION} — FULL DATA v2 DRIVEN")
    log.info(f"  Top features: ATR, Range, BBW, LowerWick, DistSupport (v2)")
    log.info(f"{'═'*70}")

    init_db()
    clz = CoinalyzeClient(CONFIG["coinalyze_api_key"])

    tickers = BitgetClient.get_tickers()
    if not tickers:
        log.error("❌ No tickers from Bitget")
        return 1

    # BTC data
    btc_candles = BitgetClient.get_candles("BTCUSDT", 30)
    btc_chg_1h = get_chg_from_candles(btc_candles, 1) if len(btc_candles) >= 3 else 0.0
    btc_chg_24h = get_chg_from_candles(btc_candles, 24) if len(btc_candles) >= 26 else 0.0

    if btc_chg_1h < CONFIG["btc_dump_threshold"]:
        log.warning(f"⛔ BTC CIRCUIT BREAKER: {btc_chg_1h:+.1f}% — scan paused")
        return 0

    # Universe
    active = select_universe(tickers)          # ← DITARUH DULU SEBELUM PREFILTER

    # Pre-filter (80 simbol terbaik)
    prefilter_n = CONFIG.get("prefilter_bitget_top_n", 80)
    if prefilter_n > 0 and len(active) > prefilter_n:
        log.info(f"⚡ Pre-filter Bitget top {prefilter_n} (ATR+Wick+Decel)...")
        active = prefilter_by_bitget(active, tickers, top_n=prefilter_n)

    clz.build_symbol_maps(active)
    now_ts = int(time.time())
    from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_data = clz.fetch_all_data(active, from_ts, now_ts)

    # Scoring
    results = []
    for sym in active:
        if is_on_cooldown(sym):
            continue
        # ... (sama seperti v14.9 — gunakan score_coin_v15)
        result = score_coin_v15(coin_data)   # ganti jadi v15
        if result:
            results.append(result)

    # Kirim alert
    results.sort(key=lambda x: x.score, reverse=True)
    sent = 0
    for rank, r in enumerate(results[:10], 1):
        msg = build_alert_v14(r, rank)   # atau buat build_alert_v15 kalau mau
        print(msg)
        if sent < CONFIG["max_alerts_per_scan"]:
            if send_telegram(msg):
                sent += 1
            set_alert(r.symbol, r.score, r.phase, r.entry["entry"] if r.entry else r.price)

    BitgetClient.clear_cache()
    return 0

if __name__ == "__main__":
    try:
        exit(main())
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True)
        exit(1)
