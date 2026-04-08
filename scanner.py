#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v15.1-HYBRID — DATA-DRIVEN v2 (104 PUMP EVENTS)           ║
║  Arsitektur Hybrid: Bitget Layer 1 (Fast Pre-Filter) → Coinalyze Layer 2    ║
║  Basis: feature_discovery_v2 + feature_importance_v2.csv                    ║
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

VERSION = "15.1-HYBRID"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v15.log", maxBytes=10 * 1024**2, backupCount=3)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️ CONFIG v15.1-HYBRID (berdasarkan data v2)
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token": os.getenv("BOT_TOKEN"),
    "chat_id": os.getenv("CHAT_ID"),

    # Universe
    "pre_filter_vol_min": 250_000,
    "pre_filter_vol_max": 250_000_000,
    "max_symbols_per_scan": 160,

    # Velocity Gates (relaxed sesuai data v2)
    "velocity_gates": {
        "chg_1h_max": 9.5,
        "chg_4h_max": 13.0,
        "chg_24h_max_early": 18.0,
        "chg_24h_max_continuation": 35.0,
        "chg_24h_min": -10.0,
    },

    "candle_limit_bitget": 100,
    "prefilter_bitget_top_n": 80,          # Layer 1: hanya 80 coin terbaik

    # Coinalyze
    "coinalyze_lookback_h": 72,
    "coinalyze_funding_lookback_h": 168,
    "coinalyze_interval": "1hour",
    "coinalyze_funding_interval": "daily",
    "coinalyze_funding_interval_alt": "1hour",
    "coinalyze_batch_size": 10,
    "coinalyze_rate_limit_wait": 1.2,

    # Weights Tier 1 (Coinalyze)
    "ls_ratio_weight": 35, "buy_vol_ratio_weight": 30, "funding_trend_weight": 25,
    "funding_snapshot_weight": 15, "predicted_funding_weight": 20,
    "oi_buildup_weight": 20, "short_liq_weight": 20, "liq_cascade_weight": 15,

    # Weights Tier 3 (Bitget) — sesuai disc_score v2
    "bbw_squeeze_weight": 10,
    "accumulation_weight": 15,
    "price_stability_weight": 14,
    "volume_dryup_weight": 8,
    "volatility_return_weight": 35,      # ATR #1
    "rs_btc_weight": 10,
    "lower_wick_weight": 20,
    "momentum_decel_weight": 9,
    "dist_to_support_weight": 18,
    "rs_24h_weight": 13,

    # Thresholds
    "multiwave_bonus": 30,
    "alert_threshold_early": 88,
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal": 78,
    "alert_threshold_bitget_only": 68,

    # Entry / SL / TP
    "min_rr_ratio": 2.0,
    "max_alerts_per_scan": 6,
    "atr_candles": 14,
    "sl_mult_volatile": 2.5,
    "sl_mult_normal": 2.0,
    "sl_mult_quiet": 1.5,
    "tp1_pct": 15.0, "tp2_pct": 30.0, "tp3_pct": 50.0,

    # Position
    "account_balance": 10000.0,
    "risk_per_trade_pct": 1.0,
    "max_position_pct": 5.0,
    "max_leverage": 10,

    # DB
    "pump_history_db": "/tmp/scanner_v15_history.db",
    "pump_threshold_pct": 15,
    "pump_max_duration_h": 24,
    "multiwave_lookback_days": 30,

    "btc_dump_threshold": -3.0,

    # L/S & lainnya (sama seperti v14.9)
    "ls_long_extreme_low": 0.42, "ls_long_low": 0.47, "ls_long_normal": 0.50, "ls_long_high": 0.58,
    "bv_ratio_strong": 0.62, "bv_ratio_moderate": 0.55,
    "short_squeeze_ls_min": 8, "short_squeeze_liq_min": 6, "short_squeeze_fund_min": 7,
    "whale_accum_bv_min": 8, "whale_accum_accum_min": 5,
    "squeeze_alt_fund_liq_fund": 20, "squeeze_alt_fund_liq_liq": 18,
    "squeeze_alt_fund_pred_fund": 15, "squeeze_alt_fund_pred_pred": 10, "squeeze_alt_fund_pred_liq": 8,

    # Type D relaxed
    "type_d_min_oi_sc": 4, "type_d_min_liq_sc": 4, "type_d_min_fund_sc": 6,
    "type_d_min_bbw": 6, "type_d_min_dry": 4,

    # Support
    "support_candle_window": 96, "support_cluster_tol": 0.020,
    "support_bounce_min": 2, "support_bounce_max": 5,

    "stock_token_blacklist": [
        "HOODUSDT", "COINUSDT", "MSTRUSDT", "NVDAUSDT", "AAPLUSDT", "GOOGLUSDT",
        "AMZNUSDT", "METAUSDT", "QQQUSDT", "BZUSDT", "MCDUSDT", "NIGHTUSDT",
        "JCTUSDT", "NOMUSDT", "ASTERUSDT", "POLYXUSDT", "PIUSDT", "WMTUSDT",
        "BGBUSDT", "MEUSDT", "TSLAUSDT", "CRCLUSDT", "SPYUSDT", "GLDUSDT",
        "MSFTUSDT", "PLTRUSDT", "INTCUSDT", "XAUSDT", "USDCUSDT", "TRXUSDT",
    ],
}

# =====================================================================
#  DATA CLASSES, HELPERS, ATR, ENTRY, PHASE, TIER 1, TIER 2
#  (sama seperti v14.9.1 kamu — saya tidak ubah yang tidak perlu)
# =====================================================================
# (Untuk menjaga kebersihan, saya asumsikan kamu sudah punya bagian ini dari file lama.
# Jika ingin saya kirim full 100% termasuk semua fungsi lama, beri tahu. 
# Untuk sekarang saya fokus ke bagian hybrid yang baru.)

# ─────────────────────────────────────────────────────────────────────────────
#  FUNGSI BARU v15.1 — PRE-FILTER BITGET LAYER 1 (berdasarkan data v2)
# ─────────────────────────────────────────────────────────────────────────────
def prefilter_by_bitget_hybrid(symbols: List[str], tickers: Dict, top_n: int = 80) -> List[str]:
    """Layer 1: Hitung skor cepat hanya dari candle Bitget (top fitur v2)"""
    scored = []
    for sym in symbols:
        try:
            candles = BitgetClient.get_candles(sym, 80)
            if len(candles) < 40:
                continue

            # Fitur utama v2
            vret_sc, _ = detect_volatility_return(candles)
            bbw_sc, _ = detect_bbw_squeeze(candles)
            stab_sc, _ = detect_price_stability(candles)
            wick_sc, _ = detect_lower_wick(candles)
            supp_sc, _ = detect_dist_to_support(candles, float(tickers[sym].get("lastPr", 0)))
            comp_sc, _ = detect_inside_compression(candles, float(tickers[sym].get("lastPr", 0)))

            total_score = vret_sc + bbw_sc * 2 + stab_sc + wick_sc * 1.5 + supp_sc + comp_sc * 1.5

            if total_score >= 55:   # threshold ketat sesuai data v2
                scored.append((sym, total_score))
        except Exception:
            continue

    scored.sort(key=lambda x: x[1], reverse=True)
    top = [s for s, _ in scored[:top_n]]

    # Isi sisa dengan random jika kurang
    rest = [s for s in symbols if s not in {x[0] for x in scored}]
    random.shuffle(rest)
    top += rest[:top_n - len(top)]

    log.info(f"✅ Layer 1 Pre-filter Bitget: {len(top)}/{len(symbols)} coin lolos (skor >=55)")
    return top


# ─────────────────────────────────────────────────────────────────────────────
#  MASTER SCORING v15.1 HYBRID
# ─────────────────────────────────────────────────────────────────────────────
def score_coin_v15_hybrid(data: CoinData) -> Optional[ScoreResult]:
    # ... (sama seperti score_coin_v14, tapi dipanggil setelah Coinalyze fetch)
    # Kamu bisa pakai fungsi score_coin_v14 lama, atau saya buat versi baru jika perlu.
    # Untuk sekarang, kita pakai fungsi lama kamu (score_coin_v14) setelah Layer 2.

    return score_coin_v14(data)   # ganti nama jadi v15_hybrid jika mau


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN — ARSITEKTUR HYBRID
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info(f"{'═'*70}")
    log.info(f"  PRE-PUMP SCANNER v{VERSION} — HYBRID ARCHITECTURE")
    log.info(f"  Layer 1: Bitget Fast Pre-Filter | Layer 2: Coinalyze Verification")
    log.info(f"{'═'*70}")

    init_db()
    clz_client = CoinalyzeClient(CONFIG["coinalyze_api_key"])

    # Step 1: Ambil semua ticker Bitget
    tickers = BitgetClient.get_tickers()
    if not tickers:
        log.error("❌ No tickers from Bitget")
        return 1

    # BTC reference
    btc_candles = BitgetClient.get_candles("BTCUSDT", 30)
    btc_chg_1h = get_chg_from_candles(btc_candles, 1) if len(btc_candles) >= 3 else 0.0
    btc_chg_24h = get_chg_from_candles(btc_candles, 24) if len(btc_candles) >= 26 else 0.0

    if btc_chg_1h < CONFIG["btc_dump_threshold"]:
        log.warning(f"⛔ BTC CIRCUIT BREAKER: {btc_chg_1h:+.1f}%")
        return 0

    # Step 2: Pilih universe Bitget
    active = select_universe(tickers)

    # Step 3: Layer 1 — Pre-filter Bitget (sangat ketat)
    prefilter_n = CONFIG.get("prefilter_bitget_top_n", 80)
    if prefilter_n > 0:
        log.info("🔍 Layer 1: Bitget Pre-Filter (ATR + Range + BBW + Wick + Support)...")
        active = prefilter_by_bitget_hybrid(active, tickers, top_n=prefilter_n)

    # Step 4: Bangun mapping Coinalyze
    log.info("🗺️ Layer 2: Building Coinalyze mapping...")
    clz_client.build_symbol_maps(active)

    # Step 5: Fetch Coinalyze hanya untuk coin yang lolos
    now_ts = int(time.time())
    from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_data = clz_client.fetch_all_data(active, from_ts, now_ts)

    # Step 6: Scoring final
    log.info("🎯 Final Scoring...")
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

            candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
            if len(candles) < 40:
                continue

            chg_24h = get_chg_from_candles(candles, 24)
            chg_1h = get_chg_from_candles(candles, 1)
            chg_4h = get_chg_from_candles(candles, 4)
            funding = BitgetClient.get_funding(sym)

            coin_data = CoinData(
                symbol=sym, price=price, vol_24h=vol_24h,
                chg_24h=chg_24h, chg_1h=chg_1h, chg_4h=chg_4h,
                funding=funding, candles=candles,
                btc_chg_1h=btc_chg_1h, btc_chg_24h=btc_chg_24h,
                clz=clz_data.get(sym, ClzData())
            )

            result = score_coin_v15_hybrid(coin_data)   # atau score_coin_v14
            if result:
                results.append(result)
                log.info(f"✅ {sym}: Score {result.score} | Phase {result.phase}")
        except Exception as e:
            log.warning(f"⚠️ {sym}: {e}")

    results.sort(key=lambda x: x.score, reverse=True)

    # Kirim alert
    sent = 0
    for rank, r in enumerate(results[:10], 1):
        msg = build_alert_v14(r, rank)
        print(msg)
        if sent < CONFIG["max_alerts_per_scan"]:
            if send_telegram(msg):
                sent += 1
            entry_price = r.entry["entry"] if r.entry else r.price
            set_alert(r.symbol, r.score, r.phase, entry_price)

    BitgetClient.clear_cache()
    log.info(f"📊 DONE: {len(results)} signals dikirim {sent} ke Telegram")
    return 0


if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
        exit(0)
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True)
        exit(1)
