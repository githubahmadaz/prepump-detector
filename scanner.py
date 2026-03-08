"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  QUANTITATIVE PUMP DETECTION SCANNER v19                                     ║
║                                                                              ║
║  UPGRADE v19 — 20 Perbaikan dari audit v18:                                 ║
║                                                                              ║
║  FIX #1  — ADAPTIVE ENTRY ENGINE (Market Microstructure)                    ║
║    v18: entry = VWAP (unrealistis jika harga di bawah VWAP)                 ║
║    v19: entry = min(VWAP, price + ATR*0.25)                                 ║
║         selalu dekat harga saat ini, RR lebih besar                         ║
║                                                                              ║
║  FIX #2  — LIQUIDITY-AWARE STOP LOSS                                         ║
║    v18: SL = entry - ATR * 2.5 (ignores liquidity structure)                ║
║    v19: swing_low = lowest low 20 candle                                    ║
║         SL = min(swing_low - ATR*0.5, entry - ATR*1.5)                     ║
║         menghindari zona sweep likuiditas                                   ║
║                                                                              ║
║  FIX #3  — DYNAMIC TAKE PROFIT (ATR-Based)                                  ║
║    v18: TP1=1.5×ATR / TP2=3×ATR / TP3=5×ATR                                ║
║    v19: TP1=2×ATR / TP2=3.5×ATR / TP3=5×ATR                                ║
║         RR otomatis dihitung ulang                                          ║
║                                                                              ║
║  FIX #4  — AI-STYLE WEIGHTED SCORING ENGINE                                  ║
║    v18: linear additive scoring (semua bobot sama)                          ║
║    v19: score = 0.30×vol + 0.20×accel + 0.20×momentum +                    ║
║                 0.15×liquidity + 0.10×breakout + 0.05×rsi                  ║
║         setiap komponen dinormalisasi 0−100                                 ║
║                                                                              ║
║  FIX #5  — LOGISTIC PROBABILITY MODEL v19                                    ║
║    v18: prob berbasis z-score feature sum                                   ║
║    v19: P(pump) = 1/(1+exp(−0.08×(weighted_score − 55)))                   ║
║         k=0.08, threshold=55 — lebih akurat untuk score 0-100              ║
║                                                                              ║
║  FIX #6  — TREND CONTEXT FILTER (EMA20/EMA50)                               ║
║    v19 baru: hitung EMA20 dan EMA50                                         ║
║    - if EMA20 < EMA50: kurangi score (downtrend konteks)                   ║
║    - if price < EMA50: REJECT sinyal (strong downtrend gate)                ║
║                                                                              ║
║  FIX #7  — STRICT VWAP BIAS GATE                                             ║
║    v18: tolerance 97% VWAP (price > vwap*0.97)                             ║
║    v19: if price < VWAP → REJECT sinyal (pump mulai di atas VWAP)          ║
║                                                                              ║
║  FIX #8  — MOMENTUM VALIDATION (5m price change)                            ║
║    v19 baru: if price_change_5m <= 0 → REJECT sinyal                       ║
║    memastikan momentum positif jangka pendek                                ║
║                                                                              ║
║  FIX #9  — FAKE PUMP: WICK RATIO + BUY RATIO GATE                           ║
║    v18: 4-kondisi fake pump (penalti)                                       ║
║    v19: + wick_ratio = (high-close)/(high-low) > 0.4 → REJECT              ║
║         + buy_ratio < 55% → REJECT (distribusi terdeteksi)                 ║
║                                                                              ║
║  FIX #10 — VOLUME Z-SCORE (Abnormal Activity Detector)                      ║
║    v19 baru: z = (vol - mean_vol) / std_vol                                 ║
║    if z > 3 → boost volume_score (deteksi anomali statistik)               ║
║                                                                              ║
║  FIX #11 — MICRO BREAKOUT DETECTION                                          ║
║    v19 baru: highest_high_20 = max high 20 candle terakhir                  ║
║    if price > highest_high_20 → boost breakout_score                       ║
║                                                                              ║
║  FIX #12 — ORDERBOOK IMBALANCE                                               ║
║    v19 baru: imbalance = bid_volume / ask_volume                            ║
║    if imbalance > 1.5 → boost liquidity_score (bullish pressure)           ║
║                                                                              ║
║  FIX #13 — NOISE FILTER (Illiquid & Low-Volatility Rejection)               ║
║    v19 baru: if 24h_volume < $20M → REJECT (illiquid)                      ║
║    v19 baru: if ATR < 0.3% → REJECT (terlalu flat)                         ║
║                                                                              ║
║  FIX #14 — EARLY PUMP DETECTION                                              ║
║    v19 baru: if vol_acceleration AND price > VWAP AND range_pos < 40%      ║
║    → score boost (deteksi pre-pump lebih awal)                             ║
║                                                                              ║
║  FIX #15 — SIGNAL RANKING (score × probability)                             ║
║    v18: sort by score saja                                                  ║
║    v19: rank_value = weighted_score × (pump_prob/100)                      ║
║         sinyal berkualitas tinggi naik ke atas                             ║
║                                                                              ║
║  FIX #16 — WHALE ACCUMULATION DETECTION                                      ║
║    v19 baru: vol naik + harga sideways + volatilitas turun                 ║
║    → deteksi akumulasi diam-diam sebelum pump besar                        ║
║                                                                              ║
║  FIX #17 — TELEGRAM HTML ESCAPE FIX                                          ║
║    v18: error "can't parse entities" — karakter spesial tidak di-escape    ║
║    v19: html.escape() pada konten dinamis sebelum dikirim                  ║
║                                                                              ║
║  FIX #18 — SIGNAL THRESHOLD DINAIKKAN                                        ║
║    v18: MIN_SCORE = 40 (terlalu banyak false signal)                       ║
║    v19: MIN_SCORE = 55 (hanya sinyal berkualitas tinggi)                   ║
║                                                                              ║
║  FIX #19 — TARGET KUALITAS SINYAL                                            ║
║    v19 target: False signal rate < 35%, Pump detection rate > 65%          ║
║                                                                              ║
║  FIX #20 — WARISAN v18: vol spike 3 tier, buy pressure, whale v18,          ║
║    micro momentum 5m, OI persistence, funding guard, BB/ATR/HTF accum,    ║
║    pump timing ETA, market regime, BTC correlation                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests
import time
import os
import math
import json
import logging
import logging.handlers as _lh
import html                          # FIX v19 #17: Telegram HTML escape
import statistics                    # FIX v19 #10: Volume Z-Score
from datetime import datetime, timezone
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging ───────────────────────────────────────────────────────────────────
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v19.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v19 — log aktif: /tmp/scanner_v19.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_alert":          8,
    "max_alerts_per_run":       15,

    # ── Volume 24h total (USD) ────────────────────────────────────────────────
    "min_vol_24h":          10_000,
    "max_vol_24h":     200_000_000,   # v19: diperlebar dari 50M → 200M
    "pre_filter_vol":       10_000,

    # ── FIX v19 #13 — NOISE FILTER: Volume & ATR minimum ─────────────────────
    # Koin dengan volume < $20M terlalu illiquid → false signal tinggi
    # Koin dengan ATR < 0.3% terlalu flat → tidak ada pump potential
    "noise_min_vol_24h":  20_000_000,   # $20M minimum untuk signal valid
    "noise_min_atr_pct":        0.30,   # ATR harus >= 0.3% dari harga

    # ── Open Interest minimum filter ──────────────────────────────────────────
    "min_oi_usd":          100_000,   # minimal $100K OI

    # ── Gate perubahan harga 24h ──────────────────────────────────────────────
    "gate_chg_24h_max":          8.0,
    "gate_chg_24h_min":        -15.0,   # hanya skip dump besar

    # ── FIX v19 #7 — STRICT VWAP GATE ────────────────────────────────────────
    # v18: tolerance 97% (price > vwap*0.97) — terlalu longgar
    # v19: price harus >= VWAP (pump starts above VWAP)
    "vwap_gate_tolerance":      1.00,   # FIX v19: strict, was 0.97

    # ── Gate uptrend usia ─────────────────────────────────────────────────────
    "gate_uptrend_max_hours":   10,

    # ── Gate RSI overbought ───────────────────────────────────────────────────
    "gate_rsi_max":             72.0,

    # ── Gate BB Position ──────────────────────────────────────────────────────
    "gate_bb_pos_max":          1.05,

    # ── FIX v19 #6 — TREND CONTEXT FILTER (EMA20/EMA50) ─────────────────────
    # EMA20 < EMA50 = downtrend konteks → reduce score
    # price < EMA50 = strong downtrend → REJECT sinyal
    "ema_trend_score_penalty":  -8,     # penalty jika EMA20 < EMA50
    "ema_trend_gate":           True,   # aktifkan gate price < EMA50

    # ── FIX v19 #9 — FAKE PUMP: WICK RATIO GATE ─────────────────────────────
    # Upper wick > 40% dari range = bearish rejection / whale trap
    "wick_ratio_gate_max":       0.40,  # reject jika upper_wick/range > 0.4
    "buy_ratio_gate_min":        0.55,  # reject jika buy_ratio < 55%

    # ── Funding rate scoring ──────────────────────────────────────────────────
    "funding_penalty_avg":     0.0003,   # > +0.03% → penalti -2
    "funding_bonus_avg":      -0.0002,   # < -0.02% → bonus +2
    "funding_bonus_cumul":    -0.001,    # cumul < -0.1% → bonus +1
    "funding_streak_min":       5,

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h":               168,
    "candle_4h":                48,

    # ── Entry / SL ────────────────────────────────────────────────────────────
    "entry_bos_buffer":        0.0005,
    "sl_swing_lookback":       12,
    "sl_swing_buffer":         0.003,
    "sl_atr_mult_min":         1.0,
    "sl_atr_mult_max":         3.0,
    "max_sl_pct":              8.0,
    "min_sl_pct":              0.5,

    # ── Operasional ───────────────────────────────────────────────────────────
    "alert_cooldown_sec":     1800,
    "sleep_coins":             0.8,
    "sleep_error":             3.0,
    "cooldown_file":          "./cooldown.json",
    "funding_snapshot_file":  "./funding.json",
    "oi_snapshot_file":       "./oi_snapshot.json",

    # ══════════════════════════════════════════════════════════════════════════
    #  BOBOT SKOR v18 (DIPERTAHANKAN — basis additive score)
    # ══════════════════════════════════════════════════════════════════════════

    # ── BB Squeeze ────────────────────────────────────────────────────────────
    "bb_squeeze_threshold":    0.04,
    "score_bb_squeeze":        4,

    # ── ATR Contracting ───────────────────────────────────────────────────────
    "atr_contract_ratio":      0.75,
    "score_atr_contracting":   3,

    # ── Energy Build-Up ───────────────────────────────────────────────────────
    "energy_oi_change_min":    5.0,
    "energy_vol_ratio_min":    1.5,
    "energy_range_max_pct":    2.5,
    "score_energy_buildup":    4,

    # ── Smart Money Accumulation ──────────────────────────────────────────────
    "accum_vol_ratio":         1.5,
    "accum_price_range_max":   2.0,
    "accum_atr_lookback_long": 24,
    "accum_atr_lookback_short": 6,
    "accum_atr_contract_ratio": 0.75,
    "accum_max_pos_in_range":  0.70,
    "score_accumulation":      4,
    "score_vol_compression":   4,

    # ── HTF Accumulation 4H ───────────────────────────────────────────────────
    "htf_atr_contract_ratio":  0.85,
    "htf_vol_ratio_min":       1.3,
    "htf_range_max_pct":       3.0,
    "htf_max_pos_in_range":    0.75,
    "score_htf_accumulation":  3,

    # ── Liquidity Sweep ───────────────────────────────────────────────────────
    "liq_sweep_lookback":      20,
    "liq_sweep_wick_min_pct":  0.3,
    "score_liquidity_sweep":   3,

    # ── OI Expansion ─────────────────────────────────────────────────────────
    "oi_change_min_pct":       3.0,
    "oi_strong_pct":          10.0,
    "score_oi_expansion":      3,
    "score_oi_strong":         5,

    # ── Volume dengan konteks arah ────────────────────────────────────────────
    "vol_ratio_threshold":     1.5,
    "vol_bullish_min_ratio":   0.6,
    "score_vol_bullish":       2,

    # ── Volume Acceleration ───────────────────────────────────────────────────
    "vol_accel_threshold":     0.5,
    "score_vol_accel":         2,

    # ── RSI ideal pre-pump = 40–60 ────────────────────────────────────────────
    "rsi_ideal_min":           40.0,
    "rsi_ideal_max":           60.0,
    "score_rsi_ideal":         2,

    # ── Higher Low ────────────────────────────────────────────────────────────
    "higher_low_lookback":     16,
    "score_higher_low":        2,

    # ── BOS Up ───────────────────────────────────────────────────────────────
    "bos_lookback":            8,
    "score_bos_up":            1,

    # ── Funding scoring ───────────────────────────────────────────────────────
    "score_funding_avg_neg":   2,
    "score_funding_cumul":     2,
    "score_funding_neg_pct":   3,
    "score_funding_streak":    3,

    # ── BTC Outperformance ────────────────────────────────────────────────────
    "btc_bearish_threshold":  -3.0,
    "btc_bullish_threshold":   3.0,
    "outperform_min_delta":    2.0,
    "score_outperform":        3,

    # ── Threshold lainnya ─────────────────────────────────────────────────────
    "above_vwap_rate_min":     0.6,
    "ema_gap_threshold":       1.0,

    # ══════════════════════════════════════════════════════════════════════════
    #  v18 FEATURES (DIPERTAHANKAN)
    # ══════════════════════════════════════════════════════════════════════════

    # ── Volume Spike Detection (Phase 1 pre-pump) ─────────────────────────────
    "vol_spike_window":        20,
    "vol_spike_low":           1.5,
    "vol_spike_mid":           2.0,
    "vol_spike_high":          3.0,
    "score_vol_spike_low":     8,
    "score_vol_spike_mid":     15,
    "score_vol_spike_high":    22,

    # ── Momentum Acceleration (Phase 3 pre-pump) ──────────────────────────────
    "accel_strong_threshold":  0.005,
    "score_accel_positive":    10,
    "score_accel_strong":      18,

    # ── Buy Pressure (Phase 2 pre-pump) ──────────────────────────────────────
    "buy_pressure_window":     8,
    "buy_pressure_low":        0.55,
    "buy_pressure_mid":        0.65,
    "buy_pressure_high":       0.75,
    "score_buy_pressure_low":  6,
    "score_buy_pressure_mid":  12,
    "score_buy_pressure_high": 20,

    # ── Whale Order Detection ─────────────────────────────────────────────────
    "whale_vol_mult":          5.0,
    "score_whale_order":       8,

    # ── Fake Pump Filter ──────────────────────────────────────────────────────
    "fake_pump_price_min":     0.3,
    "fake_pump_buy_max":       0.50,
    "penalty_fake_pump":       -10,

    # ── FIX v19 #5 — Logistic Probability Model v19 ──────────────────────────
    # P(pump) = 1 / (1 + exp(-k * (weighted_score - threshold)))
    # k=0.08, threshold=55 — dikalibrasi untuk weighted_score 0-100
    "prob_k":                  0.08,   # steepness logistic curve
    "prob_threshold":          55.0,   # decision boundary (pump vs no-pump)

    # ── v18 feature-based prob weights (DIPERTAHANKAN sebagai fallback) ───────
    "prob_center":             50,
    "prob_scale":              8,

    # ── FIX v19 #18 — Signal Threshold DINAIKKAN ─────────────────────────────
    # v18: WATCHLIST=40, ALERT=55, STRONG=70
    # v19: WATCHLIST=55, ALERT=68, STRONG=80 (lebih ketat, kurangi false signal)
    "score_watchlist":         55,    # FIX v19: was 40
    "score_alert":             68,    # FIX v19: was 55
    "score_strong_alert":      80,    # FIX v19: was 70

    # ── Entry Regime Detection ────────────────────────────────────────────────
    "breakout_buy_ratio_min":  0.60,
    "breakout_vol_ratio_min":  1.80,
    "mean_rev_rsi_max":        45.0,
    "mean_rev_range_max":      0.20,
    "entry_breakout_atr_mult": 0.15,
    "entry_mean_rev_atr_mult": 0.20,
    "sl_atr_base":             1.5,    # FIX v19 #2: was 2.5 → 1.5 (Step 2)
    "sl_atr_volatile":         2.0,    # FIX v19 #2: was 3.0 → 2.0 (Step 2)

    # ── FIX v19 #3 — TP ATR Multipliers (diperlebar) ─────────────────────────
    "tp1_atr_mult":            2.0,    # FIX v19: was 1.5 → 2.0
    "tp2_atr_mult":            3.5,    # FIX v19: was 3.0 → 3.5
    "tp3_atr_mult":            5.0,    # sama

    # ── v18 Entry model ───────────────────────────────────────────────────────
    "entry_pullback_atr_mult": 0.30,
    "entry_sweep_atr_mult":    0.20,
    "entry_retest_buffer":     0.005,

    # ── FIX v19 #1 — Adaptive Entry Multiplier ───────────────────────────────
    "entry_atr_buffer_mult":   0.25,   # entry = min(VWAP, price + ATR*0.25)

    # ── v18 Micro Momentum (5m candles) ──────────────────────────────────────
    "micro_mom_candles":       12,
    "micro_accel_strong":      0.003,
    "score_micro_accel":       15,
    "score_micro_accel_pos":   8,

    # ── v18 Whale detection upgrade ──────────────────────────────────────────
    "whale_vol_mult_v18":      3.0,
    "whale_buy_ratio_min":     0.65,
    "whale_oi_change_min":     2.0,
    "score_whale_v18":         10,

    # ── v18 Pump Probability feature weights ─────────────────────────────────
    "prob_w_vol_spike":        0.9,
    "prob_w_buy_press":        1.2,
    "prob_w_mom_accel":        1.5,
    "prob_w_oi_change":        0.7,
    "prob_w_bb_squeeze":       0.6,
    "prob_w_atr_contract":     0.5,
    "prob_w_fake_pump":       -1.0,

    # ── v18 Pump Timing ETA ───────────────────────────────────────────────────
    "timing_w_vol_accel":      0.4,
    "timing_w_buy_ratio":      0.3,
    "timing_w_oi_accel":       0.2,
    "timing_w_momentum":       0.1,
    "timing_eta_5min":         0.75,
    "timing_eta_10min":        0.55,
    "timing_eta_30min":        0.35,
    "timing_eta_60min":        0.20,

    # ── v18 Fake Pump upgrade ─────────────────────────────────────────────────
    "fake_price_spike_min":    0.005,
    "fake_oi_flat_max":        1.5,
    "fake_sell_press_max":     0.48,
    "fake_penalty_mild":      -5,
    "fake_penalty_strong":    -12,
    "fake_penalty_severe":    -20,

    # ══════════════════════════════════════════════════════════════════════════
    #  FIX v19 #4 — AI WEIGHTED SCORING BOBOT
    # ══════════════════════════════════════════════════════════════════════════
    # Setiap komponen dinormalisasi 0-100, lalu dikombinasi dengan bobot:
    "wscore_w_volume":         0.30,   # volume_score weight
    "wscore_w_acceleration":   0.20,   # acceleration_score weight
    "wscore_w_momentum":       0.20,   # momentum_score weight (buy pressure)
    "wscore_w_liquidity":      0.15,   # liquidity_score weight
    "wscore_w_breakout":       0.10,   # breakout_score weight
    "wscore_w_rsi":            0.05,   # rsi_score weight

    # ── FIX v19 #10 — Volume Z-Score ─────────────────────────────────────────
    "vol_zscore_threshold":     3.0,   # z > 3 = abnormal activity
    "vol_zscore_lookback":     20,     # candle lookback untuk mean/std
    "vol_zscore_score_boost":  15,     # boost ke volume_score (normalized)

    # ── FIX v19 #11 — Micro Breakout ─────────────────────────────────────────
    "micro_breakout_lookback":  20,    # highest high dari 20 candle terakhir
    "micro_breakout_score":     60,    # breakout_score jika breakout valid

    # ── FIX v19 #12 — Orderbook Imbalance ────────────────────────────────────
    "ob_imbalance_min":         1.5,   # bid/ask ratio > 1.5 = bullish
    "ob_imbalance_score":       15,    # tambahan ke liquidity_score

    # ── FIX v19 #14 — Early Pump Detection ───────────────────────────────────
    # vol_acceleration + price > VWAP + range_position < 40%
    "early_pump_range_max":     0.40,  # range_pos < 40% = di bawah setengah range
    "early_pump_score_boost":   12,    # boost ke additive score

    # ── FIX v19 #16 — Whale Accumulation Detection ───────────────────────────
    # vol naik + harga sideways + volatilitas turun
    "whale_accum_vol_ratio":    1.3,   # vol ratio minimal untuk akumulasi
    "whale_accum_range_max":    1.5,   # price range < 1.5% = sideways
    "whale_accum_atr_ratio":    0.80,  # ATR menurun (short/long ratio < 0.8)
    "whale_accum_score":         5,    # bonus ke liquidity_score
}

MANUAL_EXCLUDE = set()

EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    # ── Tier 1: Large Cap Altcoin (OI & volume tertinggi) ────────────────────
    "DOGEUSDT", "ADAUSDT", "XMRUSDT", "LINKUSDT", "XLMUSDT", "HBARUSDT",
    "LTCUSDT", "AVAXUSDT", "SHIBUSDT", "SUIUSDT", "TONUSDT",
    "UNIUSDT", "DOTUSDT", "TAOUSDT", "AAVEUSDT", "PEPEUSDT",
    "ETCUSDT", "NEARUSDT", "ONDOUSDT", "POLUSDT", "ICPUSDT", "ATOMUSDT",
    "ENAUSDT", "KASUSDT", "ALGOUSDT", "RENDERUSDT", "FILUSDT", "APTUSDT",
    "ARBUSDT", "JUPUSDT", "SEIUSDT", "STXUSDT", "DYDXUSDT", "VIRTUALUSDT",

    # ── Tier 2: Mid Cap (OI signifikan, aktif di futures) ────────────────────
    "FETUSDT", "INJUSDT", "PYTHUSDT", "GRTUSDT", "TIAUSDT", "LDOUSDT",
    "OPUSDT", "ENSUSDT", "AXSUSDT", "PENDLEUSDT", "WIFUSDT", "SANDUSDT",
    "MANAUSDT", "COMPUSDT", "GALAUSDT", "RAYUSDT", "RUNEUSDT", "EGLDUSDT",
    "SNXUSDT", "ARUSDT", "CRVUSDT", "IMXUSDT", "EIGENUSDT", "JTOUSDT",
    "CELOUSDT", "MASKUSDT", "APEUSDT", "MOVEUSDT", "MINAUSDT", "SONICUSDT",
    "KAIAUSDT", "HYPEUSDT", "WLDUSDT", "STRKUSDT", "CFXUSDT", "BOMEUSDT",

    # ── Tier 3: Aktif trading, OI > threshold ────────────────────────────────
    "FLOKIUSDT", "CAKEUSDT", "CHZUSDT", "HNTUSDT", "ROSEUSDT", "IOTXUSDT",
    "ANKRUSDT", "ZILUSDT", "ONTUSDT", "ENJUSDT", "GMTUSDT", "NOTUSDT",
    "PEOPLEUSDT", "METISUSDT", "AIXBTUSDT", "GOATUSDT", "PNUTUSDT",
    "GRASSUSDT", "POPCATUSDT", "ORDIUSDT", "MOODENGUSDT", "BIOUSDT",
    "MAGICUSDT", "REZUSDT", "ARPAUSDT", "ACTUSDT", "USUALUSDT",
    "SLPUSDT", "XAIUSDT", "BLURUSDT", "ARKMUSDT", "API3USDT", "AGLDUSDT",
    "TNSRUSDT", "LAYERUSDT", "ANIMEUSDT", "YGGUSDT", "THEUSDT",
}

GRAN_MAP    = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
BITGET_BASE = "https://api.bitget.com"
_cache      = {}

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
# ══════════════════════════════════════════════════════════════════════════════
def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception:
        pass
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  💾  FUNDING SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots = {}
_btc_candles_cache = {"ts": 0, "data": []}

def load_funding_snapshots():
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                _funding_snapshots = json.load(f)
    except Exception:
        _funding_snapshots = {}

def save_all_funding_snapshots():
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception:
        pass

def add_funding_snapshot(symbol, funding_rate):
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({
        "ts":      time.time(),
        "funding": funding_rate,
    })
    # Simpan hanya 48 snapshot terakhir per coin
    if len(_funding_snapshots[symbol]) > 48:
        _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

# ══════════════════════════════════════════════════════════════════════════════
#  💾  OI SNAPSHOTS — FIX v18: PERSISTEN KE DISK
# ══════════════════════════════════════════════════════════════════════════════
_oi_snapshot = {}

def load_oi_snapshots():
    """
    FIX v18: Load OI snapshot dari disk saat startup.
    Sebelumnya (v15.7) _oi_snapshot hanya in-memory → reset tiap restart
    → OI change selalu is_new=True → energy_buildup dan OI scoring tidak pernah
    aktif di run pertama setelah restart.
    """
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            # Buang snapshot yang sudah lebih dari 2 jam (stale data)
            now = time.time()
            _oi_snapshot = {
                sym: v for sym, v in data.items()
                if now - v.get("ts", 0) < 7200
            }
            log.info(f"OI snapshots loaded: {len(_oi_snapshot)} coins")
        else:
            _oi_snapshot = {}
    except Exception:
        _oi_snapshot = {}

def save_oi_snapshots():
    """FIX v18: Simpan OI snapshot ke disk setelah tiap scan."""
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(_oi_snapshot, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=10):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit — tunggu 15s, lalu retry")
                time.sleep(15)
                continue   # retry setelah 429
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg):
    """
    FIX v19 #17 — Telegram HTML Escape.
    v18: error "Bad Request: can't parse entities" terjadi karena karakter
         spesial (<, >, &) dalam signal text tidak di-escape.
    v19: clean_telegram_html() memastikan konten plain-text di-escape
         sedangkan tag HTML (<b>, <code>, <i>) tetap valid.
    """
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN atau CHAT_ID tidak ada!")
        return False
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
            # FIX v19: Fallback ke plain text jika HTML parse error
            if "can't parse entities" in r.text or "Bad Request" in r.text:
                log.warning("Telegram: HTML parse error — retry tanpa parse_mode")
                plain_msg = msg.replace("<b>","").replace("</b>","") \
                               .replace("<i>","").replace("</i>","") \
                               .replace("<code>","").replace("</code>","")
                r2 = requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    data={"chat_id": CHAT_ID, "text": plain_msg},
                    timeout=15,
                )
                return r2.status_code == 200
            return False
        return True
    except Exception as e:
        log.warning(f"Telegram exception: {e}")
        return False


def _escape_html(text):
    """
    FIX v19 #17 — Escape karakter HTML dalam teks dinamis (signal, label).
    Gunakan pada konten yang berasal dari kalkulasi/API, bukan tag HTML manual.
    """
    if not isinstance(text, str):
        text = str(text)
    return html.escape(text, quote=False)

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════
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
        params={
            "symbol":       symbol,
            "granularity":  g,
            "limit":        str(limit),
            "productType":  "usdt-futures",
        },
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({
                "ts":         int(c[0]),
                "open":       float(c[1]),
                "high":       float(c[2]),
                "low":        float(c[3]),
                "close":      float(c[4]),
                "volume":     float(c[5]),
                "volume_usd": vol_usd,
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    _cache[key] = (time.time(), candles)
    return candles

def get_funding(symbol):
    """Ambil funding rate terkini. Guard: cek data["data"] tidak kosong."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d_list = data.get("data") or []
            if d_list:
                return float(d_list[0].get("fundingRate", 0))
        except Exception:
            pass
    return 0.0

def get_btc_candles_cached(limit=48):
    """Cache candle BTCUSDT 1h selama 5 menit — hemat ~100 API call per scan."""
    global _btc_candles_cache
    if time.time() - _btc_candles_cache["ts"] < 300 and _btc_candles_cache["data"]:
        return _btc_candles_cache["data"]
    candles = get_candles("BTCUSDT", "1h", limit)
    if candles:
        _btc_candles_cache = {"ts": time.time(), "data": candles}
    return candles

def get_funding_stats(symbol):
    """Hitung statistik funding dari snapshot in-memory."""
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None
    all_rates = [s["funding"] for s in snaps]
    last6     = all_rates[-6:]
    avg6      = sum(last6) / len(last6)
    cumul     = sum(last6)
    neg_pct   = sum(1 for f in last6 if f < 0) / len(last6) * 100
    streak    = 0
    for f in reversed(all_rates):
        if f < 0:
            streak += 1
        else:
            break
    return {
        "avg":          avg6,
        "cumulative":   cumul,
        "neg_pct":      neg_pct,
        "streak":       streak,
        "basis":        all_rates[-1] * 100,
        "current":      all_rates[-1],
        "sample_count": len(all_rates),
    }

def get_open_interest(symbol):
    """Ambil Open Interest dari Bitget Futures API. Guard: cek list tidak kosong."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d = data["data"]
            if isinstance(d, list) and d:
                d = d[0]
            elif isinstance(d, list):
                return 0.0
            if "openInterestList" in d:
                oi_list = d.get("openInterestList") or []
                if oi_list:
                    oi = float(oi_list[0].get("openInterest", 0))
                else:
                    oi = float(d.get("openInterest", d.get("holdingAmount", 0)))
            else:
                oi = float(d.get("openInterest", d.get("holdingAmount", 0)))
            price = float(d.get("indexPrice", d.get("lastPr", 0)) or 0)
            if 0 < oi < 1e9 and price > 0:
                return oi * price
            return oi
        except Exception:
            pass
    return 0.0

def get_oi_change(symbol):
    """
    FIX v18: Hitung % perubahan OI menggunakan snapshot yang sudah di-load dari disk.
    Sebelumnya (v15.7) _oi_snapshot hanya in-memory sehingga selalu is_new=True
    di setiap restart — menyebabkan energy_buildup dan OI scoring tidak pernah aktif.
    """
    global _oi_snapshot
    oi_now = get_open_interest(symbol)
    prev   = _oi_snapshot.get(symbol)
    if prev is None or oi_now <= 0:
        if oi_now > 0:
            _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
        return {"oi_now": oi_now, "oi_prev": 0.0, "change_pct": 0.0, "is_new": True}
    oi_prev    = prev["oi"]
    change_pct = ((oi_now - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0.0
    _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
    return {
        "oi_now":     round(oi_now, 2),
        "oi_prev":    round(oi_prev, 2),
        "change_pct": round(change_pct, 2),
        "is_new":     False,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📊  INDIKATOR TEKNIKAL
# ══════════════════════════════════════════════════════════════════════════════

def _calc_ema_series(values, period):
    if len(values) < period:
        return None
    alpha   = 2.0 / (period + 1)
    ema_val = sum(values[:period]) / period
    for v in values[period:]:
        ema_val = alpha * v + (1.0 - alpha) * ema_val
    return ema_val

def calc_ema_gap(candles, period=20):
    """EMA gap = close / EMA(period)."""
    if len(candles) < period + 1:
        return 1.0
    closes  = [c["close"] for c in candles]
    ema_val = _calc_ema_series(closes, period)
    if ema_val is None or ema_val == 0:
        return 1.0
    return candles[-1]["close"] / ema_val

def calc_bbw(candles, period=20):
    """BB Width (desimal) dan posisi harga dalam band (0=bawah, 1=atas)."""
    if len(candles) < period:
        return 0.0, 0.5
    closes   = [c["close"] for c in candles[-period:]]
    mean     = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std      = math.sqrt(variance)
    bb_upper = mean + 2 * std
    bb_lower = mean - 2 * std
    bbw      = (bb_upper - bb_lower) / mean if mean > 0 else 0.0
    if bb_upper == bb_lower:
        bb_pct = 0.5
    else:
        bb_pct = (candles[-1]["close"] - bb_lower) / (bb_upper - bb_lower)
    return bbw, bb_pct

def calc_atr_pct(candles, period=14):
    """ATR sebagai % dari harga close terakhir."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return 0.0
    atr = sum(trs) / len(trs)
    cur = candles[-1]["close"]
    return (atr / cur * 100) if cur > 0 else 0.0

def calc_atr_abs(candles, period=14):
    """ATR dalam nilai absolut untuk kalkulasi entry/SL."""
    if len(candles) < period + 1:
        return candles[-1]["close"] * 0.01
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else candles[-1]["close"] * 0.01

def _atr_n(candles, n):
    """Helper: hitung ATR untuk n candle terakhir."""
    trs = []
    for i in range(1, min(n + 1, len(candles))):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 0.0

def calc_atr_contracting(candles):
    """
    Deteksi kompresi volatilitas: ATR jangka pendek < ATR jangka panjang.
    ATR_short (6c) / ATR_long (24c) < threshold = energi menumpuk sebelum ekspansi.
    """
    atr_s = _atr_n(candles, CONFIG["accum_atr_lookback_short"])   # 6 candle
    atr_l = _atr_n(candles, CONFIG["accum_atr_lookback_long"])    # 24 candle
    if atr_l <= 0:
        return {"is_contracting": False, "ratio": 1.0}
    ratio = atr_s / atr_l
    return {
        "is_contracting": ratio <= CONFIG["atr_contract_ratio"],
        "ratio":          round(ratio, 3),
    }

def calc_vwap(candles, lookback=24):
    """VWAP rolling 24 candle."""
    n = min(lookback, len(candles))
    if n == 0:
        return candles[-1]["close"] if candles else 0.0
    recent = candles[-n:]
    cum_tv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in recent)
    cum_v  = sum(c["volume"] for c in recent)
    return (cum_tv / cum_v) if cum_v > 0 else candles[-1]["close"]

def get_rsi(candles, period=14):
    """RSI Wilder."""
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))

def detect_bos_up(candles, lookback=None):
    """
    Break of Structure ke atas.
    FIX v18: lookback default dinaikkan 3 → 8 candle.
    Lookback 3 candle = 3 jam saja → BOS trivial, hampir selalu True.
    Lookback 8 candle = lebih bermakna secara struktur.
    """
    if lookback is None:
        lookback = CONFIG["bos_lookback"]
    if len(candles) < lookback + 1:
        return False, 0.0
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level  = max(prev_highs)
    return candles[-1]["close"] > bos_level, bos_level

def higher_low_detected(candles):
    """
    Higher Low: low candle terakhir > min(low N candle sebelumnya).
    FIX v18: lookback dinaikkan 6 → 16 candle.
    Lookback 6 candle rentan noise. 16 candle lebih bermakna secara teknikal.
    """
    lookback = CONFIG["higher_low_lookback"]
    if len(candles) < lookback + 1:
        return False
    lows = [c["low"] for c in candles[-(lookback + 1):]    ]
    return lows[-1] > min(lows[:-1])

def calc_swing_range_position(candles, lookback=48):
    """
    Hitung posisi harga saat ini dalam swing range lookback candle.
    Return 0.0 (bawah = accumulation zone) hingga 1.0 (atas = distribusi zone).
    """
    n      = min(lookback, len(candles))
    recent = candles[-n:]
    if not recent:
        return 0.5
    swing_low  = min(c["low"]  for c in recent)
    swing_high = max(c["high"] for c in recent)
    swing_range = swing_high - swing_low
    if swing_range <= 0:
        return 0.5
    price = candles[-1]["close"]
    pos   = (price - swing_low) / swing_range
    return round(min(max(pos, 0.0), 1.0), 3)

def calc_candle_direction_ratio(candles, lookback=6):
    """
    Rasio candle bullish dalam lookback candle terakhir (0.0–1.0).
    Candle bullish = close > open.
    """
    if len(candles) < lookback:
        return 0.5
    recent   = candles[-lookback:]
    bullish  = sum(1 for c in recent if c["close"] >= c["open"])
    return bullish / len(recent)

# ══════════════════════════════════════════════════════════════════════════════
#  📊  NEW v18 INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def calc_volume_spike(candles):
    """
    v18 — Volume Spike Detection (Phase 1 pre-pump).
    Baseline = avg 20 candle, current = avg 3 candle terbaru.
    Tier: 1.5x→+8 | 2x→+15 | 3x→+22
    """
    window = CONFIG["vol_spike_window"]
    if len(candles) < window + 3:
        return {"ratio": 0.0, "score": 0, "label": "Data kurang", "tier": 0,
                "current_vol": 0.0, "avg_vol": 0.0}
    current_vol      = sum(c["volume_usd"] for c in candles[-3:]) / 3
    baseline_candles = candles[-(window + 3):-3]
    avg_vol          = sum(c["volume_usd"] for c in baseline_candles) / len(baseline_candles) if baseline_candles else 0.0
    if avg_vol <= 0:
        return {"ratio": 0.0, "score": 0, "label": "Vol baseline 0", "tier": 0,
                "current_vol": current_vol, "avg_vol": 0.0}
    ratio = current_vol / avg_vol
    if ratio >= CONFIG["vol_spike_high"]:
        return {"ratio": round(ratio,2), "score": CONFIG["score_vol_spike_high"],
                "label": f"🚀 Volume Spike {ratio:.1f}x — PUMP LIKELY (Phase 1 kuat)", "tier": 3,
                "current_vol": current_vol, "avg_vol": avg_vol}
    elif ratio >= CONFIG["vol_spike_mid"]:
        return {"ratio": round(ratio,2), "score": CONFIG["score_vol_spike_mid"],
                "label": f"📈 Volume Spike {ratio:.1f}x — Akumulasi kuat (Phase 1)", "tier": 2,
                "current_vol": current_vol, "avg_vol": avg_vol}
    elif ratio >= CONFIG["vol_spike_low"]:
        return {"ratio": round(ratio,2), "score": CONFIG["score_vol_spike_low"],
                "label": f"📊 Volume Spike {ratio:.1f}x — Early interest (Phase 1)", "tier": 1,
                "current_vol": current_vol, "avg_vol": avg_vol}
    return {"ratio": round(ratio,2), "score": 0, "label": f"Volume normal {ratio:.1f}x", "tier": 0,
            "current_vol": current_vol, "avg_vol": avg_vol}


def calc_micro_momentum(candles_5m):
    """
    FIX v18 #3 — Micro Momentum Engine (5m/15m/1h).

    Pump altcoin sering terjadi dalam 5−20 menit.
    Menggunakan candle 5m untuk deteksi momentum jauh lebih responsif:

      mom_5m  = % change candle 5m terbaru vs 1 candle sebelumnya
      mom_15m = % change rata-rata 3 candle 5m terakhir (= 15 menit)
      mom_1h  = % change rata-rata 12 candle 5m terakhir (= 1 jam)

    Formula percepatan:
      accel = mom_5m × 2 + mom_15m − mom_1h

    Jika accel > 0 → momentum sedang menguat (pre-pump signal)
    """
    if len(candles_5m) < 13:
        return {
            "mom_5m": 0.0, "mom_15m": 0.0, "mom_1h": 0.0,
            "accel": 0.0, "score": 0, "label": "Data 5m kurang",
            "is_accelerating": False, "is_strong": False,
        }

    closes = [c["close"] for c in candles_5m]

    # mom_5m: % change 1 candle 5m terbaru
    mom_5m = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] > 0 else 0.0

    # mom_15m: avg % change 3 candle terakhir
    c3 = closes[-4] if len(closes) >= 4 else closes[0]
    mom_15m = (closes[-1] - c3) / c3 / 3 if c3 > 0 else 0.0

    # mom_1h: avg % change 12 candle terakhir
    c12 = closes[-13] if len(closes) >= 13 else closes[0]
    mom_1h = (closes[-1] - c12) / c12 / 12 if c12 > 0 else 0.0

    # acceleration formula
    accel = mom_5m * 2 + mom_15m - mom_1h

    strong_threshold = CONFIG["micro_accel_strong"]

    if accel > strong_threshold:
        score = CONFIG["score_micro_accel"]
        label = (f"⚡ Micro Momentum KUAT — 5m:{mom_5m*100:+.3f}% "
                 f"15m:{mom_15m*100:+.3f}% 1h:{mom_1h*100:+.3f}% "
                 f"accel:{accel*100:+.3f}%")
        is_accel = True
        is_strong = True
    elif accel > 0:
        score = CONFIG["score_micro_accel_pos"]
        label = (f"📈 Micro Momentum positif — accel:{accel*100:+.3f}% "
                 f"(5m:{mom_5m*100:+.3f}%)")
        is_accel = True
        is_strong = False
    else:
        score = 0
        label = f"Micro momentum flat/negatif ({accel*100:+.3f}%)"
        is_accel = False
        is_strong = False

    return {
        "mom_5m":          round(mom_5m, 6),
        "mom_15m":         round(mom_15m, 6),
        "mom_1h":          round(mom_1h, 6),
        "accel":           round(accel, 6),
        "score":           score,
        "label":           label,
        "is_accelerating": is_accel,
        "is_strong":       is_strong,
    }


def calc_momentum_acceleration(candles):
    """
    1h momentum acceleration (dipertahankan sebagai secondary signal).
    Digunakan sebagai input pump probability dan timing model.
    """
    if len(candles) < 7:
        return {"acceleration": 0.0, "score": 0, "label": "Data kurang",
                "is_accelerating": False, "is_strong": False,
                "momentum1": 0.0, "momentum2": 0.0}
    price_now = candles[-1]["close"]
    price_3h  = candles[-4]["close"]
    price_6h  = candles[-7]["close"]
    if price_3h <= 0 or price_6h <= 0:
        return {"acceleration": 0.0, "score": 0, "label": "Data invalid",
                "is_accelerating": False, "is_strong": False,
                "momentum1": 0.0, "momentum2": 0.0}
    momentum1    = (price_now - price_3h) / price_3h
    momentum2    = (price_3h  - price_6h) / price_6h
    acceleration = momentum1 - momentum2
    strong_th    = CONFIG["accel_strong_threshold"]
    if acceleration > strong_th:
        score, is_accel, is_strong = CONFIG["score_accel_strong"], True, True
        label = (f"⚡ 1h Momentum Accel KUAT {acceleration*100:+.2f}% — "
                 f"m1={momentum1*100:+.2f}% > m2={momentum2*100:+.2f}%")
    elif acceleration > 0:
        score, is_accel, is_strong = CONFIG["score_accel_positive"], True, False
        label = f"📈 1h Momentum Accel {acceleration*100:+.2f}%"
    else:
        score, is_accel, is_strong = 0, False, False
        label = f"Momentum 1h melambat {acceleration*100:+.2f}%"
    return {
        "acceleration":    round(acceleration, 5),
        "momentum1":       round(momentum1, 5),
        "momentum2":       round(momentum2, 5),
        "score":           score,
        "label":           label,
        "is_accelerating": is_accel,
        "is_strong":       is_strong,
    }


def calc_buy_pressure(candles_15m):
    """
    v18 — Buy Pressure dari 15m candles.
    Estimasi buy_volume dari posisi close dalam candle range.
    Tier: >55%→+6 | >65%→+12 | >75%→+20
    """
    window = CONFIG["buy_pressure_window"]
    if len(candles_15m) < window:
        return {"buy_ratio": 0.5, "buy_pct": 50.0, "score": 0,
                "label": "Data 15m kurang", "is_bullish": False, "tier": 0}
    recent    = candles_15m[-window:]
    total_vol = sum(c["volume"] for c in recent)
    if total_vol <= 0:
        return {"buy_ratio": 0.5, "buy_pct": 50.0, "score": 0,
                "label": "Volume 15m nol", "is_bullish": False, "tier": 0}
    buy_vol = 0.0
    for c in recent:
        rng = c["high"] - c["low"]
        frac = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        buy_vol += c["volume"] * frac
    buy_ratio = buy_vol / total_vol
    if buy_ratio >= CONFIG["buy_pressure_high"]:
        return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                "score": CONFIG["score_buy_pressure_high"],
                "label": f"🐳 Buy Pressure {buy_ratio*100:.0f}% — PUMP PHASE", "is_bullish": True, "tier": 3}
    elif buy_ratio >= CONFIG["buy_pressure_mid"]:
        return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                "score": CONFIG["score_buy_pressure_mid"],
                "label": f"💰 Buy Pressure {buy_ratio*100:.0f}% — Whale activity", "is_bullish": True, "tier": 2}
    elif buy_ratio >= CONFIG["buy_pressure_low"]:
        return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                "score": CONFIG["score_buy_pressure_low"],
                "label": f"📦 Buy Pressure {buy_ratio*100:.0f}% — Accumulation", "is_bullish": True, "tier": 1}
    return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
            "score": 0, "label": f"Buy Pressure rendah {buy_ratio*100:.0f}%",
            "is_bullish": False, "tier": 0}


def detect_whale_order(candles_15m, oi_data):
    """
    FIX v18 #2 — Whale Detection Diperkuat.

    v18: volume > 5× avg (terlalu kasar)
    v18: whale = vol > 3× AND buy_ratio > 65% AND OI naik
    Ini filter market maker, liquidation spike, dan fake pump.
    """
    window = 10
    if len(candles_15m) < window + 1:
        return {"is_whale": False, "mult": 0.0, "score": 0,
                "label": "Data kurang", "confidence": "LOW"}

    recent_vol = candles_15m[-1]["volume"]
    avg_vol    = sum(c["volume"] for c in candles_15m[-(window+1):-1]) / window
    if avg_vol <= 0:
        return {"is_whale": False, "mult": 0.0, "score": 0,
                "label": "Avg vol 0", "confidence": "LOW"}

    mult = recent_vol / avg_vol

    # Hitung buy ratio candle terakhir
    c = candles_15m[-1]
    rng = c["high"] - c["low"]
    buy_frac = (c["close"] - c["low"]) / rng if rng > 0 else 0.5

    # Cek OI naik
    oi_rising = (not oi_data.get("is_new", True) and
                 oi_data.get("change_pct", 0) >= CONFIG["whale_oi_change_min"])

    cond_vol      = mult >= CONFIG["whale_vol_mult_v18"]       # vol > 3x
    cond_buy      = buy_frac >= CONFIG["whale_buy_ratio_min"]  # buy > 65%
    cond_oi       = oi_rising                                   # OI naik

    n_cond = sum([cond_vol, cond_buy, cond_oi])

    if n_cond >= 3:
        # Semua 3 kondisi: whale confirmed
        score      = CONFIG["score_whale_v18"]
        confidence = "HIGH"
        label      = (f"🐳 WHALE CONFIRMED {mult:.1f}x vol + buy {buy_frac*100:.0f}% "
                      f"+ OI naik — big player masuk!")
        is_whale   = True
    elif n_cond == 2 and cond_vol and cond_buy:
        # Vol + Buy (tanpa OI data): probable whale
        score      = CONFIG["score_whale_v18"] // 2
        confidence = "MEDIUM"
        label      = (f"🐳 Whale Probable {mult:.1f}x vol + buy {buy_frac*100:.0f}% "
                      f"(OI belum konfirmasi)")
        is_whale   = True
    else:
        score, is_whale, confidence = 0, False, "LOW"
        label = f"Order biasa {mult:.1f}x (vol={cond_vol}, buy={cond_buy}, OI={cond_oi})"

    return {
        "is_whale":   is_whale,
        "mult":       round(mult, 1),
        "buy_frac":   round(buy_frac, 3),
        "oi_rising":  oi_rising,
        "score":      score,
        "label":      label,
        "confidence": confidence,
        "n_cond":     n_cond,
    }


def detect_fake_pump(candles, buy_pressure_ratio, oi_data):
    """
    FIX v18 #7 — Fake Pump Filter Diperkuat.

    v18: price_up AND buy<50% → fake (terlalu sederhana)
    v18: fake_score berbasis 4 kondisi bertingkat:
      1. price_spike: harga naik > 0.5% dalam 3h
      2. vol_spike: volume naik tapi buy rendah
      3. OI_flat: OI tidak naik (tidak ada posisi baru)
      4. sell_pressure: buy_ratio < 48%

    Penalti proporsional: 2 kondisi→−5, 3 kondisi→−12, 4 kondisi→−20
    """
    if len(candles) < 4:
        return {"is_fake": False, "penalty": 0, "label": "", "n_cond": 0}

    price_now = candles[-1]["close"]
    price_3h  = candles[-4]["close"]
    if price_3h <= 0:
        return {"is_fake": False, "penalty": 0, "label": "", "n_cond": 0}

    price_chg_3h = (price_now - price_3h) / price_3h

    # Kondisi 1: Price spike
    cond_price_spike = price_chg_3h > CONFIG["fake_price_spike_min"]

    # Kondisi 2: Volume anomali (vol naik tapi bukan buy)
    vol_anomaly = False
    if len(candles) >= 8:
        avg_vol = sum(c["volume_usd"] for c in candles[-8:-3]) / 5
        cur_vol = sum(c["volume_usd"] for c in candles[-3:]) / 3
        vol_anomaly = cur_vol > avg_vol * 1.5 and buy_pressure_ratio < 0.55

    # Kondisi 3: OI flat (tidak ada posisi baru = tidak ada institutional)
    oi_flat = (oi_data.get("is_new", True) or
               abs(oi_data.get("change_pct", 0)) < CONFIG["fake_oi_flat_max"])

    # Kondisi 4: Sell pressure dominan
    sell_pressure = buy_pressure_ratio < CONFIG["fake_sell_press_max"]

    # Hitung n kondisi yang aktif
    conditions = [cond_price_spike, vol_anomaly, oi_flat, sell_pressure]
    n_cond = sum(conditions)

    # Hanya flag sebagai fake jika minimal price_spike + 1 kondisi lain
    if not cond_price_spike or n_cond < 2:
        return {"is_fake": False, "penalty": 0, "label": "", "n_cond": n_cond}

    if n_cond >= 4:
        penalty = CONFIG["fake_penalty_severe"]
        severity = "SEVERE"
    elif n_cond == 3:
        penalty = CONFIG["fake_penalty_strong"]
        severity = "STRONG"
    else:
        penalty = CONFIG["fake_penalty_mild"]
        severity = "MILD"

    label = (
        f"⚠️ FAKE PUMP {severity} ({n_cond}/4 cond) — "
        f"harga +{price_chg_3h*100:.2f}% tapi: "
        f"vol_anomaly={vol_anomaly}, OI_flat={oi_flat}, "
        f"sell_press={sell_pressure} (buy={buy_pressure_ratio*100:.0f}%)"
    )

    return {
        "is_fake":           n_cond >= 2,
        "penalty":           penalty,
        "label":             label,
        "n_cond":            n_cond,
        "severity":          severity,
        "price_chg_3h":      round(price_chg_3h * 100, 2),
        "cond_price_spike":  cond_price_spike,
        "cond_vol_anomaly":  vol_anomaly,
        "cond_oi_flat":      oi_flat,
        "cond_sell_pressure": sell_pressure,
    }


def calc_pump_probability_v18(vol_spike, buy_press, micro_mom, oi_data, bb_squeeze,
                               atr_contracting, fake_pump):
    """
    FIX v18 #1 — Probability Model Berbasis Feature (bukan raw score).

    v18: prob = logistic(score) — score adalah penjumlahan heuristik,
         bisa tinggi tanpa pump nyata.

    v18: z = Σ(bobot × feature_normalized)
         prob = 1 / (1 + exp(-z))

    Feature normalization:
      - vol_spike_ratio: [0,3+] → normalized ke [0,1] max=3
      - buy_pressure: [0,1] → already normalized
      - micro_accel: [-∞,+∞] → clamp ke [-0.01, 0.01] → scale ke [-1,1]
      - OI_change: [-∞,+∞] → clamp ke [-10,10] → scale
      - bb_squeeze: boolean [0,1]
      - atr_contract: boolean [0,1]
      - fake_pump: boolean [0,1]
    """
    # Normalize features ke [0, 1] atau [-1, 1]
    vs_norm   = min(vol_spike.get("ratio", 0) / 3.0, 1.0)
    bp_norm   = buy_press.get("buy_ratio", 0.5)
    ma_raw    = micro_mom.get("accel", 0)
    ma_norm   = max(-1.0, min(1.0, ma_raw / 0.01))   # scale: 1% accel = max
    oi_raw    = oi_data.get("change_pct", 0) if not oi_data.get("is_new", True) else 0
    oi_norm   = max(-1.0, min(1.0, oi_raw / 10.0))   # scale: 10% OI = max
    bb_norm   = 1.0 if bb_squeeze else 0.0
    atr_norm  = 1.0 if atr_contracting else 0.0
    fake_norm = 1.0 if fake_pump.get("is_fake", False) else 0.0

    w = CONFIG
    z = (w["prob_w_vol_spike"]  * vs_norm
       + w["prob_w_buy_press"]  * bp_norm
       + w["prob_w_mom_accel"]  * ma_norm
       + w["prob_w_oi_change"]  * oi_norm
       + w["prob_w_bb_squeeze"] * bb_norm
       + w["prob_w_atr_contract"] * atr_norm
       + w["prob_w_fake_pump"]  * fake_norm)

    # Logistic transform — z adalah raw feature sum, bukan score
    # Center sekitar z=1.0 (threshold decision boundary)
    prob = 1.0 / (1.0 + math.exp(-(z - 1.0) * 2.5))
    return round(prob * 100, 1)


def calc_pump_timing_eta(vol_accel, buy_ratio, oi_data, micro_mom):
    """
    FIX v18 #6 — Pump Timing Model / ETA.

    timing_score = 0.4×vol_accel + 0.3×buy_ratio + 0.2×OI_accel + 0.1×mom_micro

    Output ETA berdasarkan timing_score:
      > 0.75 → 5 menit
      > 0.55 → 10 menit
      > 0.35 → 30 menit
      > 0.20 → 60 menit
      else   → > 1 jam (setup fase awal)
    """
    # Normalize inputs ke [0, 1]
    va_norm  = min(max(vol_accel, 0) / 2.0, 1.0)   # vol accel 0-200%
    bp_norm  = min(max(buy_ratio, 0), 1.0)
    oi_raw   = oi_data.get("change_pct", 0) if not oi_data.get("is_new", True) else 0
    oi_norm  = min(max(oi_raw, 0) / 10.0, 1.0)     # OI accel 0-10%
    ma_raw   = micro_mom.get("accel", 0)
    ma_norm  = min(max(ma_raw, 0) / 0.01, 1.0)     # micro accel 0-1%

    w = CONFIG
    timing = (w["timing_w_vol_accel"] * va_norm
            + w["timing_w_buy_ratio"] * bp_norm
            + w["timing_w_oi_accel"]  * oi_norm
            + w["timing_w_momentum"]  * ma_norm)

    if timing >= CONFIG["timing_eta_5min"]:
        eta = "~5 menit"
        eta_emoji = "🔥🔥🔥"
        urgency = "IMMINENT"
    elif timing >= CONFIG["timing_eta_10min"]:
        eta = "~10 menit"
        eta_emoji = "🔥🔥"
        urgency = "VERY SOON"
    elif timing >= CONFIG["timing_eta_30min"]:
        eta = "~30 menit"
        eta_emoji = "🔥"
        urgency = "SOON"
    elif timing >= CONFIG["timing_eta_60min"]:
        eta = "~60 menit"
        eta_emoji = "⏳"
        urgency = "BUILDING"
    else:
        eta = "> 1 jam"
        eta_emoji = "📦"
        urgency = "EARLY SETUP"

    return {
        "timing_score": round(timing, 3),
        "eta":          eta,
        "eta_emoji":    eta_emoji,
        "urgency":      urgency,
        "va_norm":      round(va_norm, 3),
        "bp_norm":      round(bp_norm, 3),
        "oi_norm":      round(oi_norm, 3),
        "ma_norm":      round(ma_norm, 3),
    }


def get_alert_level_v19(score):
    """
    FIX v19 #18 — Threshold Dinaikkan untuk Kurangi False Signal.

    v18: WATCHLIST=40, ALERT=55, STRONG=70
    v19: WATCHLIST=55, ALERT=68, STRONG=80

    Threshold lebih tinggi memastikan hanya sinyal berkualitas
    yang diteruskan ke Telegram, mengurangi false signal rate.
    """
    if score >= CONFIG["score_strong_alert"]:
        return "STRONG ALERT"
    elif score >= CONFIG["score_alert"]:
        return "ALERT"
    elif score >= CONFIG["score_watchlist"]:
        return "WATCHLIST"
    return "IGNORE"


def get_alert_level_v18(score):
    """v18: threshold lama — dipertahankan untuk kompatibilitas internal."""
    if score >= CONFIG["score_strong_alert"]:
        return "STRONG ALERT"
    elif score >= CONFIG["score_alert"]:
        return "ALERT"
    elif score >= CONFIG["score_watchlist"]:
        return "WATCHLIST"
    return "IGNORE"


def calc_market_regime(candles, vwap, buy_ratio, vol_ratio, rsi, price_pos):
    """
    v18 — Market Regime: PULLBACK / SWEEP / BREAKOUT / NEUTRAL.
    Extends v18 dengan mode PULLBACK dan SWEEP.
    """
    price_now = candles[-1]["close"]

    above_vwap   = price_now > vwap
    buy_strong   = buy_ratio >= CONFIG["breakout_buy_ratio_min"]
    vol_strong   = vol_ratio >= CONFIG["breakout_vol_ratio_min"]
    below_vwap   = price_now < vwap
    oversold     = rsi < CONFIG["mean_rev_rsi_max"]
    low_range    = price_pos < CONFIG["mean_rev_range_max"]

    if above_vwap and buy_strong and vol_strong:
        return "BREAKOUT"
    elif below_vwap and oversold and low_range:
        return "PULLBACK"
    elif below_vwap and low_range:
        return "SWEEP"
    return "NEUTRAL"


def calc_entry_v19(candles, vwap, price_now, atr_abs_val, market_regime, sr,
                   rsi, buy_ratio, vol_ratio, price_pos, alert_level, bos_level,
                   liq_sweep):
    """
    FIX v19 #1, #2, #3 — Entry Engine, Stop Loss, Take Profit v19.

    FIX #1 — ADAPTIVE ENTRY (Market Microstructure Based):
      entry = min(VWAP, price + ATR * 0.25)
      Memastikan entry selalu dekat harga saat ini, tidak terlalu jauh dari VWAP.
      Ini menghindari entry unrealistis ketika harga sudah jauh di bawah VWAP.

    FIX #2 — LIQUIDITY-AWARE STOP LOSS:
      swing_low_20 = lowest low dari 20 candle terakhir
      SL = min(swing_low_20 - ATR*0.5, entry - ATR*1.5)
      SL ditempatkan di bawah zona likuiditas untuk menghindari stop hunt.

    FIX #3 — DYNAMIC TAKE PROFIT (ATR-Based):
      TP1 = entry + ATR * 2.0   (lebih besar dari v18 1.5x)
      TP2 = entry + ATR * 3.5   (lebih besar dari v18 3.0x)
      TP3 = entry + ATR * 5.0   (sama, + liquidity void check)
      RR otomatis dihitung ulang berdasarkan entry/SL aktual.
    """
    atr = atr_abs_val
    atr_pct_now = (atr / price_now * 100) if price_now > 0 else 2.0

    # ── FIX v19 #1: Adaptive Entry ────────────────────────────────────────────
    # entry = min(VWAP, price + ATR*0.25)
    # Logika: ambil yang lebih rendah antara VWAP dan price+buffer kecil
    # Ini memastikan entry selalu realistis dan dekat harga saat ini
    entry_adaptive  = min(vwap, price_now + atr * CONFIG["entry_atr_buffer_mult"])
    entry_regime    = None
    entry_reason    = "ADAPTIVE"

    # Override dengan regime-specific entry jika lebih baik
    if market_regime == "PULLBACK":
        entry_regime = vwap - atr * CONFIG["entry_pullback_atr_mult"]
        if entry_regime < price_now * 0.995:   # terlalu jauh di bawah
            entry_regime = None

    elif market_regime == "SWEEP" and liq_sweep and liq_sweep.get("is_sweep"):
        sweep_low    = liq_sweep.get("sweep_low", price_now * 0.98)
        entry_regime = sweep_low + atr * CONFIG["entry_sweep_atr_mult"]
        if entry_regime > price_now:
            entry_regime = None

    elif market_regime == "BREAKOUT":
        res_levels = []
        if sr and sr.get("resistance"):
            res_levels = [rv["level"] for rv in sr["resistance"]
                          if rv["level"] > price_now * 0.998]
        if res_levels:
            breakout_lvl = min(res_levels)
            entry_regime = breakout_lvl * (1.0 + CONFIG["entry_retest_buffer"])
            entry_reason = f"BREAKOUT retest {_fmt_price(breakout_lvl)}"
        elif bos_level > 0 and bos_level < price_now * 1.05:
            entry_regime = bos_level * (1.0 + CONFIG["entry_bos_buffer"])
            entry_reason = "BREAKOUT BOS"

    # Pilih entry terbaik: gunakan regime jika ada, fallback ke adaptive
    if entry_regime is not None and entry_regime > 0:
        # Adaptive: min(regime, adaptive) — selalu ambil yang lebih konservatif
        entry = min(entry_regime, entry_adaptive)
        entry_reason = f"{market_regime} + ADAPTIVE min({_fmt_price(entry_regime)},{_fmt_price(entry_adaptive)})"
    else:
        entry = entry_adaptive
        entry_reason = f"ADAPTIVE — min(VWAP={_fmt_price(vwap)}, price+ATR*0.25={_fmt_price(price_now + atr*0.25)})"

    # Pastikan entry tidak di bawah harga terlalu jauh
    if entry < price_now * 0.97:
        entry = price_now * 0.998
        entry_reason += " [clamp: tidak terlalu jauh dari harga]"

    # ── FIX v19 #2: Liquidity-Aware Stop Loss ─────────────────────────────────
    # swing_low = lowest low dari 20 candle terakhir
    # SL = min(swing_low - ATR*0.5, entry - ATR*1.5)
    # Menempatkan SL di bawah zona likuiditas (below market maker sweep zone)
    lkb_20       = min(20, len(candles) - 1)
    swing_low_20 = min(c["low"] for c in candles[-lkb_20:]) if lkb_20 > 0 else entry * 0.97

    sl_liquidity = swing_low_20 - atr * 0.5        # di bawah swing low
    sl_atr_base  = entry - atr * CONFIG["sl_atr_base"]   # 1.5x ATR dari entry

    # Ambil yang lebih rendah (lebih defensif)
    sl = min(sl_liquidity, sl_atr_base)

    # Clamp SL dalam batas persentase
    sl = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))   # max loss 8%
    sl = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))   # min loss 0.5%

    if sl >= entry:
        sl = entry * 0.975

    # ── FIX v19 #3: Dynamic Take Profit (ATR-Based) ───────────────────────────
    # TP1 = entry + ATR * 2.0   (lebih besar dari v18 1.5x → RR lebih baik)
    # TP2 = entry + ATR * 3.5   (lebih besar dari v18 3.0x)
    # TP3 = entry + ATR * 5.0   (sama, + liquidity void detection)
    tp1 = entry + atr * CONFIG["tp1_atr_mult"]   # 2.0x
    tp2 = entry + atr * CONFIG["tp2_atr_mult"]   # 3.5x
    tp3 = entry + atr * CONFIG["tp3_atr_mult"]   # 5.0x

    # TP3 cek liquidity void (area tanpa resistance = target ideal)
    if sr and sr.get("resistance"):
        res_above = sorted([rv["level"] for rv in sr["resistance"] if rv["level"] > entry])
        if len(res_above) >= 2:
            gap = res_above[1] - res_above[0]
            if gap / res_above[0] > 0.05:   # gap > 5% = liquidity void
                tp3 = max(tp3, res_above[1])
        if res_above and res_above[0] > tp1:
            tp1 = max(tp1, res_above[0])
        if len(res_above) >= 2 and res_above[1] > tp2:
            tp2 = max(tp2, res_above[1])

    tp1 = max(tp1, entry * 1.010)   # minimal +1%
    tp2 = max(tp2, tp1   * 1.010)
    tp3 = max(tp3, tp2   * 1.015)

    # ── Risk-Reward Calculation ────────────────────────────────────────────────
    risk = entry - sl
    rr1  = round((tp1 - entry) / risk, 1) if risk > 0 else 0.0
    rr2  = round((tp2 - entry) / risk, 1) if risk > 0 else 0.0
    rr3  = round((tp3 - entry) / risk, 1) if risk > 0 else 0.0

    return {
        "entry":         round(entry, 8),
        "sl":            round(sl, 8),
        "sl_pct":        round((entry - sl) / entry * 100, 2),
        "t1":            round(tp1, 8),
        "t2":            round(tp2, 8),
        "t3":            round(tp3, 8),
        "rr":            rr1,
        "rr2":           rr2,
        "rr3":           rr3,
        "rr_str":        f"{rr1:.1f}",
        "rr2_str":       f"{rr2:.1f}",
        "rr3_str":       f"{rr3:.1f}",
        "vwap":          round(vwap, 8),
        "bos_level":     round(bos_level, 8),
        "alert_level":   alert_level,
        "gain_t1_pct":   round((tp1 - entry) / entry * 100, 1),
        "gain_t2_pct":   round((tp2 - entry) / entry * 100, 1),
        "gain_t3_pct":   round((tp3 - entry) / entry * 100, 1),
        "atr_abs":       round(atr, 8),
        "atr_pct":       round(atr_pct_now, 2),
        "sl_method":     entry_reason,
        "market_regime": market_regime,
        "swing_low_20":  round(swing_low_20, 8),
        "sl_liquidity":  round(sl_liquidity, 8),
        "trail_note":    "Trailing: TP1→SL=Entry | TP2→SL=TP1 | TP3 free run",
        "used_resistance": bool(sr and sr.get("resistance")),
        "t1_source":     f"ATR×{CONFIG['tp1_atr_mult']}",
        "t2_source":     f"ATR×{CONFIG['tp2_atr_mult']}",
        "t3_source":     f"ATR×{CONFIG['tp3_atr_mult']} / Liq Void",
        "atr_pct_abs":   round(atr / entry * 100, 2) if entry > 0 else 0.0,
    }

def calc_accumulation_phase(candles):
    """
    Deteksi fase akumulasi smart money.

    Kondisi akumulasi VALID (v15.7/15.8):
      1. Volume 4 candle terbaru > 1.5x baseline 24 candle sebelumnya
      2. Price range 12 candle < 2% (harga sideways)
      3. ATR short < 75% ATR long (volatilitas menyempit)
      4. BB Width menyempit dibanding 12 candle lalu
      5. Posisi harga dalam swing range < 70% (bukan distribusi atas)

    FIX v18: is_vol_compress hanya aktif secara scoring jika is_accumulating=False
    (isolasi scoring di master_score — lihat bagian scoring).
    """
    if len(candles) < 36:
        return {
            "is_accumulating": False, "is_vol_compress": False,
            "vol_ratio_4h": 0.0, "price_range_pct": 0.0,
            "atr_contract": 1.0, "bbw_contracting": False,
            "price_pos": 0.5, "phase_label": "Data kurang",
        }

    # Volume: rata-rata 4 candle terbaru vs baseline 24 candle sebelumnya
    vol_4h  = sum(c["volume_usd"] for c in candles[-4:]) / 4
    vol_24h = sum(c["volume_usd"] for c in candles[-28:-4]) / 24 if len(candles) >= 28 else vol_4h
    vol_ratio_4h = (vol_4h / vol_24h) if vol_24h > 0 else 1.0

    # Price range 12 candle
    r12             = candles[-12:]
    hi12            = max(c["high"] for c in r12)
    lo12            = min(c["low"]  for c in r12)
    mid12           = (hi12 + lo12) / 2
    price_range_pct = ((hi12 - lo12) / mid12 * 100) if mid12 > 0 else 99.0

    # ATR contracting
    atr_s        = _atr_n(candles, CONFIG["accum_atr_lookback_short"])
    atr_l        = _atr_n(candles, CONFIG["accum_atr_lookback_long"])
    atr_contract = (atr_s / atr_l) if atr_l > 0 else 1.0

    # BB contracting
    bbw_now, _  = calc_bbw(candles)
    bbw_12h, _  = calc_bbw(candles[:-12]) if len(candles) > 32 else (bbw_now, 0.0)
    bbw_contracting = (bbw_now < bbw_12h * 0.85) if bbw_12h > 0 else False

    # Posisi harga dalam swing range 48 candle
    price_pos = calc_swing_range_position(candles, lookback=48)

    vol_rising      = vol_ratio_4h    >= CONFIG["accum_vol_ratio"]
    price_sideways  = price_range_pct <= CONFIG["accum_price_range_max"]
    atr_shrinking   = atr_contract    <= CONFIG["accum_atr_contract_ratio"]
    is_vol_compress = atr_shrinking and bbw_contracting
    price_in_zone   = price_pos < CONFIG["accum_max_pos_in_range"]
    is_accumulating = vol_rising and price_sideways and price_in_zone

    if is_accumulating and is_vol_compress:
        label = (
            f"🏦 AKUMULASI + COMPRESSION — vol {vol_ratio_4h:.1f}x, "
            f"range {price_range_pct:.1f}%, pos {price_pos:.0%} dari range"
        )
    elif is_accumulating:
        label = (
            f"📦 AKUMULASI — vol {vol_ratio_4h:.1f}x, "
            f"sideways {price_range_pct:.1f}%, pos {price_pos:.0%}"
        )
    elif is_vol_compress:
        label = f"🗜️ VOLATILITY COMPRESSION — ATR {atr_contract:.2f}x dari baseline"
    else:
        label = "—"

    return {
        "is_accumulating":  is_accumulating,
        "is_vol_compress":  is_vol_compress,
        "vol_ratio_4h":     round(vol_ratio_4h, 2),
        "price_range_pct":  round(price_range_pct, 2),
        "atr_contract":     round(atr_contract, 3),
        "bbw_contracting":  bbw_contracting,
        "price_pos":        price_pos,
        "phase_label":      label,
    }

def calc_htf_accumulation(candles_4h):
    """
    HTF Accumulation Filter — deteksi akumulasi di timeframe 4H.

    Kondisi:
      1. ATR 4H terkini < 85% ATR rata-rata (kompresi volatilitas TF besar)
      2. Volume 4H terbaru > 1.3x rata-rata
      3. Range 8 candle 4H < 3%
      4. Posisi harga 4H < 75% swing range 4H (bukan distribusi)
    """
    if len(candles_4h) < 16:
        return {
            "is_htf_accum": False, "atr_ratio": 1.0,
            "vol_ratio": 1.0, "range_pct": 99.0,
            "price_pos": 0.5, "label": "Data 4H tidak cukup",
        }

    atr_recent = _atr_n(candles_4h, 4)
    atr_avg    = _atr_n(candles_4h, 12)
    atr_ratio  = (atr_recent / atr_avg) if atr_avg > 0 else 1.0

    vol_recent = sum(c["volume_usd"] for c in candles_4h[-2:]) / 2
    vol_avg    = (sum(c["volume_usd"] for c in candles_4h[-10:-2]) / 8
                  if len(candles_4h) >= 10 else vol_recent)
    vol_ratio  = (vol_recent / vol_avg) if vol_avg > 0 else 1.0

    r8        = candles_4h[-8:]
    hi8       = max(c["high"] for c in r8)
    lo8       = min(c["low"]  for c in r8)
    mid8      = (hi8 + lo8) / 2
    range_pct = ((hi8 - lo8) / mid8 * 100) if mid8 > 0 else 99.0

    price_pos = calc_swing_range_position(candles_4h, lookback=32)

    atr_compressed = atr_ratio  <= CONFIG["htf_atr_contract_ratio"]
    vol_building   = vol_ratio  >= CONFIG["htf_vol_ratio_min"]
    price_sideways = range_pct  <= CONFIG["htf_range_max_pct"]
    price_in_zone  = price_pos  <  CONFIG["htf_max_pos_in_range"]

    is_htf_accum = atr_compressed and vol_building and price_sideways and price_in_zone

    if is_htf_accum:
        label = (
            f"🕯️ 4H HTF Akumulasi — ATR {atr_ratio:.2f}, "
            f"vol {vol_ratio:.1f}x, range {range_pct:.1f}%, pos {price_pos:.0%}"
        )
    elif atr_compressed and price_sideways and price_in_zone:
        label = f"🕯️ 4H Konsolidasi (vol belum naik) — range {range_pct:.1f}%"
    else:
        label = "—"

    return {
        "is_htf_accum": is_htf_accum,
        "atr_ratio":    round(atr_ratio, 3),
        "vol_ratio":    round(vol_ratio, 2),
        "range_pct":    round(range_pct, 2),
        "price_pos":    price_pos,
        "label":        label,
    }

def detect_liquidity_sweep(candles, lookback=None):
    """
    Liquidity Sweep Detection — stop hunt sebelum reversal/pump.
    Pola: harga turun di bawah support, lalu candle close kembali di atas
    support dengan wick panjang → market maker sudah selesai ambil likuiditas.
    """
    if lookback is None:
        lookback = CONFIG["liq_sweep_lookback"]
    if len(candles) < lookback + 3:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "Data kurang"}

    reference_candles = candles[-(lookback + 3):-3]
    if not reference_candles:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "—"}

    lows_sorted   = sorted(c["low"] for c in reference_candles)
    support_level = sum(lows_sorted[:3]) / 3

    sweep_detected = False
    sweep_candle   = None
    sweep_low_val  = 0.0

    for candle in candles[-3:]:
        candle_range = candle["high"] - candle["low"]
        if candle_range <= 0:
            continue
        wick_bottom = (candle["open"] - candle["low"]
                       if candle["close"] > candle["open"]
                       else candle["close"] - candle["low"])
        wick_pct   = wick_bottom / candle_range

        went_below   = candle["low"]   < support_level
        closed_above = candle["close"] > support_level
        has_wick     = wick_pct >= CONFIG["liq_sweep_wick_min_pct"]

        if went_below and closed_above and has_wick:
            sweep_detected = True
            sweep_candle   = candle
            sweep_low_val  = candle["low"]
            break

    if sweep_detected and sweep_candle is not None:
        depth_pct = (support_level - sweep_low_val) / support_level * 100
        label = (
            f"🎯 Liquidity Sweep — low ${sweep_low_val:.6g} tembus support "
            f"${support_level:.6g} ({depth_pct:.2f}%), close kembali di atas"
        )
    else:
        label = "—"

    return {
        "is_sweep":  sweep_detected,
        "sweep_low": round(sweep_low_val, 8),
        "support":   round(support_level, 8),
        "label":     label,
    }

def detect_energy_buildup(candles_1h, oi_data):
    """
    Energy Build-Up Detector — "OI Build + Volume Bullish + Price Stuck".

    Pola absorption: market maker menyerap order sambil membangun posisi.
    Harga DITAHAN (sideways) meski volume dan OI naik = klasik pre-pump.

    Kondisi deteksi:
      1. OI naik > 5% (posisi baru dibangun) — FIX v18: sekarang bisa aktif
         karena OI snapshot di-load dari disk antar-run.
      2. Vol rata-rata 3h terbaru > 1.5x baseline 24h
      3. Price range 3h < 2.5% (harga tidak bergerak)
      4. Minimal 2 dari 3 candle terbaru bullish (buying pressure)
    """
    if len(candles_1h) < 24:
        return {
            "is_buildup": False, "is_strong": False,
            "oi_change": 0.0, "vol_ratio": 0.0, "range_pct": 0.0,
            "label": "Data tidak cukup",
        }

    # Kondisi 1: OI naik (sekarang bisa berfungsi karena snapshot persisten)
    oi_change = oi_data.get("change_pct", 0.0)
    oi_rising = (not oi_data.get("is_new", True)) and oi_change >= CONFIG["energy_oi_change_min"]

    # Kondisi 2: volume rata-rata 3 candle terbaru vs baseline
    recent_3 = candles_1h[-3:]
    vol_3h_avg = sum(c["volume_usd"] for c in recent_3) / 3
    baseline   = candles_1h[-24:-3]
    avg_vol    = sum(c["volume_usd"] for c in baseline) / len(baseline) if baseline else vol_3h_avg
    vol_ratio  = (vol_3h_avg / avg_vol) if avg_vol > 0 else 1.0
    vol_rising = vol_ratio >= CONFIG["energy_vol_ratio_min"]

    # Kondisi 3: harga tidak bergerak
    hi3       = max(c["high"]  for c in recent_3)
    lo3       = min(c["low"]   for c in recent_3)
    mid3      = (hi3 + lo3) / 2
    range_pct = ((hi3 - lo3) / mid3 * 100) if mid3 > 0 else 99.0
    price_stuck = range_pct <= CONFIG["energy_range_max_pct"]

    # Kondisi 4: mayoritas candle terbaru bullish
    bullish_count   = sum(1 for c in recent_3 if c["close"] >= c["open"])
    candles_bullish = bullish_count >= 2

    is_buildup = oi_rising and vol_rising and price_stuck and candles_bullish
    is_strong  = False   # akan di-set dari master_score jika funding <= 0

    if is_buildup:
        label = (
            f"⚡ ENERGY BUILD-UP — OI +{oi_change:.1f}%, vol {vol_ratio:.1f}x "
            f"(3h avg), range {range_pct:.1f}%, candle {bullish_count}/3 hijau"
        )
    else:
        conds = sum([oi_rising, vol_rising, price_stuck, candles_bullish])
        label = (
            f"— ({conds}/4 kondisi: OI={oi_rising}, vol={vol_rising}, "
            f"stuck={price_stuck}, bullish={candles_bullish})"
        )

    return {
        "is_buildup":       is_buildup,
        "is_strong":        is_strong,
        "oi_change":        round(oi_change, 2),
        "vol_ratio":        round(vol_ratio, 2),
        "range_pct":        round(range_pct, 2),
        "candles_bullish":  candles_bullish,
        "oi_rising":        oi_rising,
        "vol_rising":       vol_rising,
        "price_stuck":      price_stuck,
        "label":            label,
    }

def calc_uptrend_age(candles):
    """Berapa jam harga naik berturut-turut. Pre-pump ideal = streak pendek atau 0."""
    if len(candles) < 4:
        return {"age_hours": 0, "is_fresh": False, "is_late": False}
    streak = 0
    for i in range(len(candles) - 1, 0, -1):
        if candles[i]["close"] > candles[i-1]["close"]:
            streak += 1
        else:
            break
    return {
        "age_hours": streak,
        "is_fresh":  1 <= streak <= 8,
        "is_late":   streak > CONFIG["gate_uptrend_max_hours"],
    }

def calc_support_resistance(candles, lookback=48, n_levels=3):
    """Level S/R dari pivot point 48 candle terakhir."""
    if len(candles) < 10:
        return {"resistance": [], "support": [], "nearest_res": None, "nearest_sup": None}
    n      = min(lookback, len(candles))
    recent = candles[-n:]
    price  = candles[-1]["close"]

    pivots_high, pivots_low = [], []
    for i in range(1, len(recent) - 1):
        h, l = recent[i]["high"], recent[i]["low"]
        if h > recent[i-1]["high"] and h > recent[i+1]["high"]:
            pivots_high.append(h)
        if l < recent[i-1]["low"]  and l < recent[i+1]["low"]:
            pivots_low.append(l)

    def cluster_levels(levels, cluster_pct=0.005):
        if not levels:
            return []
        levels   = sorted(levels)
        clusters = []
        current  = [levels[0]]
        for lv in levels[1:]:
            if (lv - current[-1]) / current[-1] < cluster_pct:
                current.append(lv)
            else:
                clusters.append((sum(current) / len(current), len(current)))
                current = [lv]
        clusters.append((sum(current) / len(current), len(current)))
        clusters.sort(key=lambda x: -x[1])
        return [round(lv, 8) for lv, _ in clusters[:n_levels]]

    res_all  = cluster_levels(pivots_high)
    sup_all  = cluster_levels(pivots_low)
    resistance = sorted([r for r in res_all if r > price * 1.001])[:n_levels]
    support    = sorted([s for s in sup_all if s < price * 0.999], reverse=True)[:n_levels]

    def fmt(lv, ref):
        return {"level": round(lv, 8), "gap_pct": round((lv - ref) / ref * 100, 1)}

    return {
        "resistance":  [fmt(r, price) for r in resistance],
        "support":     [fmt(s, price) for s in support],
        "nearest_res": fmt(resistance[0], price) if resistance else None,
        "nearest_sup": fmt(support[0], price)    if support    else None,
    }

def calc_volume_ratio(candles, lookback=24):
    """Rasio volume candle terakhir vs rata-rata lookback candle sebelumnya."""
    if len(candles) < lookback + 1:
        return 0.0
    avg_vol = sum(c["volume_usd"] for c in candles[-(lookback + 1):-1]) / lookback
    if avg_vol <= 0:
        return 0.0
    return candles[-1]["volume_usd"] / avg_vol

def calc_volume_acceleration(candles):
    """Volume acceleration: vol 1h terbaru vs rata-rata 3h sebelumnya."""
    if len(candles) < 4:
        return 0.0
    vol_1h = candles[-1]["volume_usd"]
    vol_3h = sum(c["volume_usd"] for c in candles[-4:-1]) / 3
    if vol_3h <= 0:
        return 0.0
    return (vol_1h - vol_3h) / vol_3h


# ══════════════════════════════════════════════════════════════════════════════
#  📊  NEW v19 INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def calc_ema20_ema50(candles):
    """
    FIX v19 #6 — Trend Context Filter.

    Hitung EMA20 dan EMA50 dari close price candle 1h.
    Digunakan untuk mendeteksi konteks tren dan memfilter sinyal pada downtrend.

    Rule:
      - if EMA20 < EMA50: reduce score (downtrend konteks)
      - if price < EMA50: REJECT sinyal (strong downtrend gate)
    """
    if len(candles) < 50:
        price = candles[-1]["close"] if candles else 0.0
        return {
            "ema20": price, "ema50": price,
            "is_downtrend": False, "is_strong_downtrend": False,
            "ema20_above_ema50": True, "label": "Data kurang",
        }
    closes  = [c["close"] for c in candles]
    ema20   = _calc_ema_series(closes, 20)
    ema50   = _calc_ema_series(closes, 50)
    price   = candles[-1]["close"]
    if ema20 is None or ema50 is None or ema50 == 0:
        return {
            "ema20": price, "ema50": price,
            "is_downtrend": False, "is_strong_downtrend": False,
            "ema20_above_ema50": True, "label": "EMA calc error",
        }
    ema20_above = ema20 >= ema50
    price_above_ema50 = price >= ema50
    is_downtrend        = not ema20_above        # EMA20 < EMA50
    is_strong_downtrend = not price_above_ema50  # price < EMA50

    if is_strong_downtrend:
        label = (f"🔻 Strong Downtrend — Price {_fmt_price(price)} < EMA50 {_fmt_price(ema50)} "
                 f"| EMA20={_fmt_price(ema20)}")
    elif is_downtrend:
        label = (f"⚠️ Downtrend Context — EMA20 {_fmt_price(ema20)} < EMA50 {_fmt_price(ema50)} "
                 f"(score dikurangi)")
    else:
        gap_pct = (ema20 - ema50) / ema50 * 100
        label = (f"✅ Uptrend Context — EMA20 > EMA50 (+{gap_pct:.2f}%) "
                 f"| price {_fmt_price(price)}")

    return {
        "ema20":                round(ema20, 8),
        "ema50":                round(ema50, 8),
        "price_vs_ema50_pct":   round((price - ema50) / ema50 * 100, 2),
        "ema20_vs_ema50_pct":   round((ema20 - ema50) / ema50 * 100, 2),
        "is_downtrend":         is_downtrend,
        "is_strong_downtrend":  is_strong_downtrend,
        "ema20_above_ema50":    ema20_above,
        "price_above_ema50":    price_above_ema50,
        "label":                label,
    }


def calc_volume_zscore(candles):
    """
    FIX v19 #10 — Volume Z-Score.

    Deteksi aktivitas trading yang tidak normal secara statistik:
      z = (volume_sekarang - mean_volume) / std_volume

    Jika z > 3 → abnormal activity → boost volume_score.
    Z-score lebih robust daripada simple ratio karena memperhitungkan
    distribusi volume historis, bukan hanya rata-rata.
    """
    lookback = CONFIG["vol_zscore_lookback"]
    if len(candles) < lookback + 2:
        return {"z": 0.0, "mean_vol": 0.0, "std_vol": 0.0,
                "current_vol": 0.0, "is_anomaly": False, "label": "Data kurang"}

    baseline_vols = [c["volume_usd"] for c in candles[-(lookback + 1):-1]]
    current_vol   = candles[-1]["volume_usd"]

    if len(baseline_vols) < 2:
        return {"z": 0.0, "mean_vol": 0.0, "std_vol": 0.0,
                "current_vol": current_vol, "is_anomaly": False, "label": "Data tidak cukup"}

    mean_vol = statistics.mean(baseline_vols)
    try:
        std_vol  = statistics.stdev(baseline_vols)
    except statistics.StatisticsError:
        std_vol  = 0.0

    if std_vol <= 0 or mean_vol <= 0:
        return {"z": 0.0, "mean_vol": mean_vol, "std_vol": std_vol,
                "current_vol": current_vol, "is_anomaly": False, "label": "Std vol nol"}

    z           = (current_vol - mean_vol) / std_vol
    is_anomaly  = z > CONFIG["vol_zscore_threshold"]   # z > 3

    if is_anomaly:
        label = (f"📊 Volume Z-Score {z:.1f}σ — ANOMALI STATISTIK! "
                 f"(vol {current_vol/mean_vol:.1f}x mean)")
    elif z > 2:
        label = f"📊 Volume Z-Score {z:.1f}σ — elevated (belum anomali)"
    else:
        label = f"Volume Z-Score {z:.1f}σ — normal"

    return {
        "z":            round(z, 2),
        "mean_vol":     round(mean_vol, 2),
        "std_vol":      round(std_vol, 2),
        "current_vol":  round(current_vol, 2),
        "is_anomaly":   is_anomaly,
        "label":        label,
    }


def calc_micro_breakout(candles):
    """
    FIX v19 #11 — Micro Breakout Detection.

    Hitung highest_high dari 20 candle terakhir.
    Jika harga saat ini melebihi highest_high_20 → breakout valid.

    Ini lebih sensitif daripada BOS yang membutuhkan candle close di atas level.
    Micro breakout bisa menjadi konfirmasi awal pump.
    """
    lookback = CONFIG["micro_breakout_lookback"]
    if len(candles) < lookback + 1:
        return {"is_breakout": False, "highest_high_20": 0.0,
                "price": 0.0, "gap_pct": 0.0, "label": "Data kurang"}

    reference     = candles[-(lookback + 1):-1]
    highest_high  = max(c["high"] for c in reference)
    price_now     = candles[-1]["close"]
    high_now      = candles[-1]["high"]

    is_breakout   = price_now > highest_high
    gap_pct       = (price_now - highest_high) / highest_high * 100 if highest_high > 0 else 0.0

    if is_breakout:
        label = (f"🔺 Micro Breakout — Price {_fmt_price(price_now)} > "
                 f"High{lookback} {_fmt_price(highest_high)} (+{gap_pct:.2f}%)")
    elif high_now > highest_high:
        label = (f"⚡ Wick Break — High {_fmt_price(high_now)} > "
                 f"High{lookback} {_fmt_price(highest_high)} (close belum konfirmasi)")
    else:
        gap_to_break = (highest_high - price_now) / price_now * 100 if price_now > 0 else 0
        label = f"— High{lookback}: {_fmt_price(highest_high)} (jarak {gap_to_break:.2f}%)"

    return {
        "is_breakout":      is_breakout,
        "highest_high_20":  round(highest_high, 8),
        "price":            round(price_now, 8),
        "gap_pct":          round(gap_pct, 3),
        "label":            label,
    }


def get_orderbook_imbalance(symbol):
    """
    FIX v19 #12 — Orderbook Imbalance.

    Ambil depth orderbook dari Bitget dan hitung rasio bid/ask volume.
    imbalance = total_bid_qty / total_ask_qty

    Jika imbalance > 1.5 → lebih banyak buyer → bullish pressure.
    Ini adalah konfirmasi real-time bahwa permintaan melebihi penawaran.
    """
    try:
        data = safe_get(
            f"{BITGET_BASE}/api/v2/mix/market/merge-depth",
            params={
                "symbol":      symbol,
                "productType": "usdt-futures",
                "precision":   "scale0",
                "limit":       "20",
            },
        )
        if not data or data.get("code") != "00000":
            return {"imbalance": 1.0, "bid_vol": 0.0, "ask_vol": 0.0,
                    "is_bullish": False, "label": "Data tidak tersedia"}

        d       = data.get("data", {})
        bids    = d.get("bids", [])
        asks    = d.get("asks", [])

        if not bids or not asks:
            return {"imbalance": 1.0, "bid_vol": 0.0, "ask_vol": 0.0,
                    "is_bullish": False, "label": "Bids/asks kosong"}

        # Sum volume dari 20 level teratas
        bid_vol = sum(float(b[1]) for b in bids[:20] if len(b) >= 2)
        ask_vol = sum(float(a[1]) for a in asks[:20] if len(a) >= 2)

        if ask_vol <= 0:
            return {"imbalance": 1.0, "bid_vol": bid_vol, "ask_vol": 0.0,
                    "is_bullish": False, "label": "Ask volume nol"}

        imbalance  = bid_vol / ask_vol
        min_imbal  = CONFIG["ob_imbalance_min"]
        is_bullish = imbalance > min_imbal

        if is_bullish:
            label = (f"📗 OB Imbalance {imbalance:.2f}x — "
                     f"bid {bid_vol:.0f} vs ask {ask_vol:.0f} = BULLISH PRESSURE")
        elif imbalance < (1 / min_imbal):
            label = (f"📕 OB Imbalance {imbalance:.2f}x — "
                     f"ask mendominasi = bearish pressure")
        else:
            label = f"OB Imbalance {imbalance:.2f}x — netral"

        return {
            "imbalance":   round(imbalance, 3),
            "bid_vol":     round(bid_vol, 2),
            "ask_vol":     round(ask_vol, 2),
            "is_bullish":  is_bullish,
            "label":       label,
        }
    except Exception as e:
        log.debug(f"OB imbalance error: {e}")
        return {"imbalance": 1.0, "bid_vol": 0.0, "ask_vol": 0.0,
                "is_bullish": False, "label": f"Error: {str(e)[:40]}"}


def detect_whale_accumulation(candles):
    """
    FIX v19 #16 — Whale Accumulation Detection.

    Kondisi whale accumulation (diam-diam sebelum pump besar):
      1. Volume naik: rata-rata vol 3 candle > 1.3x baseline 12 candle
      2. Harga sideways: price range 6 candle < 1.5%
      3. Volatilitas menurun: ATR_short / ATR_long < 0.80

    Pattern ini adalah "absorb & hold" — whale menyerap supply
    sambil menjaga harga flat, lalu akan pump mendadak.
    """
    if len(candles) < 20:
        return {"is_accumulating": False, "vol_rising": False,
                "price_sideways": False, "vol_decreasing": False,
                "score_boost": 0, "label": "Data kurang"}

    # Kondisi 1: volume naik
    vol_3c      = sum(c["volume_usd"] for c in candles[-3:]) / 3
    vol_base    = sum(c["volume_usd"] for c in candles[-15:-3]) / 12 if len(candles) >= 15 else vol_3c
    vol_ratio   = vol_3c / vol_base if vol_base > 0 else 1.0
    vol_rising  = vol_ratio >= CONFIG["whale_accum_vol_ratio"]

    # Kondisi 2: harga sideways (range 6 candle < 1.5%)
    recent_6    = candles[-6:]
    hi6         = max(c["high"]  for c in recent_6)
    lo6         = min(c["low"]   for c in recent_6)
    mid6        = (hi6 + lo6) / 2
    range_pct   = (hi6 - lo6) / mid6 * 100 if mid6 > 0 else 99.0
    price_sideways = range_pct <= CONFIG["whale_accum_range_max"]

    # Kondisi 3: volatilitas menurun (ATR short/long < 0.80)
    atr_s       = _atr_n(candles, 3)
    atr_l       = _atr_n(candles, 12)
    atr_ratio   = (atr_s / atr_l) if atr_l > 0 else 1.0
    vol_dec     = atr_ratio <= CONFIG["whale_accum_atr_ratio"]

    n_cond      = sum([vol_rising, price_sideways, vol_dec])
    is_accum    = n_cond >= 2   # minimal 2 dari 3 kondisi

    if is_accum:
        score_boost = CONFIG["whale_accum_score"]
        label = (f"🐳 Whale Accumulation ({n_cond}/3 cond) — "
                 f"vol {vol_ratio:.1f}x, range {range_pct:.1f}%, ATR ratio {atr_ratio:.2f}")
    else:
        score_boost = 0
        label = (f"— Whale Accum {n_cond}/3 "
                 f"(vol={vol_rising}, sideways={price_sideways}, atr_dec={vol_dec})")

    return {
        "is_accumulating": is_accum,
        "vol_rising":      vol_rising,
        "price_sideways":  price_sideways,
        "vol_decreasing":  vol_dec,
        "vol_ratio":       round(vol_ratio, 2),
        "range_pct":       round(range_pct, 2),
        "atr_ratio":       round(atr_ratio, 3),
        "n_cond":          n_cond,
        "score_boost":     score_boost,
        "label":           label,
    }


def calc_wick_ratio(candles):
    """
    FIX v19 #9 — Wick Ratio untuk Fake Pump Gate.

    wick_ratio = (high - close) / (high - low)

    Upper wick besar menunjukkan:
    - Whale menjual ke atas (distribusi)
    - Bearish rejection di resistance
    - Jebakan (bull trap / stop hunt atas)

    Jika wick_ratio > 0.4 pada candle terbaru → sinyal ditolak.
    """
    if len(candles) < 2:
        return {"wick_ratio": 0.0, "is_bearish_wick": False, "label": "Data kurang"}

    c        = candles[-1]
    hi       = c["high"]
    lo       = c["low"]
    cl       = c["close"]
    rng      = hi - lo

    if rng <= 0:
        return {"wick_ratio": 0.0, "is_bearish_wick": False,
                "label": "Range nol (candle doji)"}

    wick_ratio       = (hi - cl) / rng
    is_bearish_wick  = wick_ratio > CONFIG["wick_ratio_gate_max"]

    if is_bearish_wick:
        label = (f"⚠️ Bearish Upper Wick {wick_ratio:.2f} > {CONFIG['wick_ratio_gate_max']} "
                 f"— distribusi/whale trap terdeteksi")
    else:
        label = f"Wick ratio {wick_ratio:.2f} — OK (batas {CONFIG['wick_ratio_gate_max']})"

    return {
        "wick_ratio":      round(wick_ratio, 3),
        "is_bearish_wick": is_bearish_wick,
        "label":           label,
    }


def calc_early_pump_signal(candles, vwap, vol_spike, price_pos_48):
    """
    FIX v19 #14 — Early Pump Detection.

    Kondisi deteksi pump lebih awal (sebelum price breakout):
      1. Volume acceleration aktif (vol_spike tier >= 1)
      2. Harga di atas VWAP (demand zone)
      3. Posisi dalam range < 40% (masih di bawah tengah range)

    Ketiga kondisi sekaligus menunjukkan:
    "Ada minat beli signifikan, harga masih murah relatif terhadap range,
     dan di atas VWAP → setup ideal sebelum pump"
    """
    price_now = candles[-1]["close"] if candles else 0.0
    above_vwap = price_now > vwap
    vol_accel  = vol_spike.get("tier", 0) >= 1
    low_range  = price_pos_48 < CONFIG["early_pump_range_max"]  # < 40%

    is_early   = above_vwap and vol_accel and low_range

    if is_early:
        label = (f"⚡ EARLY PUMP SIGNAL — price > VWAP ✓ | "
                 f"vol {vol_spike.get('ratio',0):.1f}x ✓ | "
                 f"range pos {price_pos_48:.0%} < 40% ✓")
    else:
        conds = sum([above_vwap, vol_accel, low_range])
        label = (f"Early pump {conds}/3 "
                 f"(vwap={above_vwap}, vol={vol_accel}, range={low_range})")

    return {
        "is_early_pump":  is_early,
        "above_vwap":     above_vwap,
        "vol_accel":      vol_accel,
        "low_range":      low_range,
        "price_pos_48":   price_pos_48,
        "label":          label,
    }


def calc_weighted_score_v19(vol_spike, micro_mom, mom_accel, buy_press,
                             liq_sweep, accum, htf_accum, bos_up, rsi,
                             vol_zscore, micro_breakout, whale_accum,
                             ob_imbalance, energy):
    """
    FIX v19 #4 — AI-Style Weighted Scoring Engine.

    Menggantikan linear additive scoring dengan sistem berbobot:
      score = 0.30 * volume_score      (Phase 1: tanda paling kuat)
            + 0.20 * acceleration_score (Phase 3: momentum 5m)
            + 0.20 * momentum_score     (Phase 2: buy pressure)
            + 0.15 * liquidity_score    (struktur pasar)
            + 0.10 * breakout_score     (konfirmasi level)
            + 0.05 * rsi_score          (kondisi RSI)

    Setiap komponen dinormalisasi ke 0-100 sebelum digabungkan.
    Hasil akhir adalah weighted_score dalam range 0-100.
    """

    # ── 1. Volume Score (0-100) ───────────────────────────────────────────────
    # Basis: vol_spike ratio. Bobot tertinggi (30%) karena volume adalah
    # leading indicator paling reliable untuk pump detection.
    vol_ratio = vol_spike.get("ratio", 0)
    if   vol_ratio >= 4.0: vs = 100
    elif vol_ratio >= 3.0: vs = 82
    elif vol_ratio >= 2.5: vs = 70
    elif vol_ratio >= 2.0: vs = 58
    elif vol_ratio >= 1.5: vs = 40
    elif vol_ratio >= 1.0: vs = 20
    else:                  vs = 0
    # Z-score boost: anomali statistik = signal lebih kuat
    if vol_zscore.get("is_anomaly", False):
        vs = min(100, vs + CONFIG["vol_zscore_score_boost"])
    volume_score = vs

    # ── 2. Acceleration Score (0-100) ─────────────────────────────────────────
    # Prioritaskan micro_mom 5m (lebih responsif), fallback ke 1h mom
    if   micro_mom.get("is_strong",        False): acc_s = 90
    elif micro_mom.get("is_accelerating",  False): acc_s = 62
    elif mom_accel.get("is_strong",        False): acc_s = 42
    elif mom_accel.get("is_accelerating",  False): acc_s = 25
    else:                                           acc_s =  0
    acceleration_score = acc_s

    # ── 3. Momentum Score (0-100) — Buy Pressure ──────────────────────────────
    # Buy pressure adalah proxy terbaik untuk demand dominance
    buy_ratio = buy_press.get("buy_ratio", 0.5)
    if   buy_ratio >= 0.75: mom_s = 100
    elif buy_ratio >= 0.70: mom_s = 85
    elif buy_ratio >= 0.65: mom_s = 70
    elif buy_ratio >= 0.60: mom_s = 55
    elif buy_ratio >= 0.55: mom_s = 40
    elif buy_ratio >= 0.50: mom_s = 25
    else:                   mom_s =  0
    momentum_score = mom_s

    # ── 4. Liquidity Score (0-100) ────────────────────────────────────────────
    # Kombinasi faktor struktur pasar
    liq_s = 0
    if liq_sweep.get("is_sweep",         False): liq_s += 35
    if accum.get("is_accumulating",      False): liq_s += 25
    if htf_accum.get("is_htf_accum",     False): liq_s += 20
    if whale_accum.get("is_accumulating",False): liq_s += 18
    if energy.get("is_buildup",          False): liq_s += 15
    # Orderbook imbalance: konfirmasi real-time
    if ob_imbalance.get("is_bullish",    False): liq_s += CONFIG["ob_imbalance_score"]
    liquidity_score = min(100, liq_s)

    # ── 5. Breakout Score (0-100) ─────────────────────────────────────────────
    # Micro breakout = konfirmasi level paling kuat
    brk_s = 0
    if micro_breakout.get("is_breakout", False): brk_s += CONFIG["micro_breakout_score"]
    if bos_up:                                    brk_s += 25
    breakout_score = min(100, brk_s)

    # ── 6. RSI Score (0-100) ─────────────────────────────────────────────────
    # Zona ideal pre-pump: RSI 45-60 (momentum ada, belum overbought)
    if   45  <= rsi <= 60:  rsi_s = 100
    elif 42  <= rsi <  45:  rsi_s = 75
    elif 60  <  rsi <= 65:  rsi_s = 65
    elif 38  <= rsi <  42:  rsi_s = 50
    elif 65  <  rsi <= 70:  rsi_s = 30
    else:                   rsi_s =  5
    rsi_score = rsi_s

    # ── Weighted Sum ──────────────────────────────────────────────────────────
    w = CONFIG
    weighted_score = (
        w["wscore_w_volume"]       * volume_score +
        w["wscore_w_acceleration"] * acceleration_score +
        w["wscore_w_momentum"]     * momentum_score +
        w["wscore_w_liquidity"]    * liquidity_score +
        w["wscore_w_breakout"]     * breakout_score +
        w["wscore_w_rsi"]          * rsi_score
    )

    return {
        "weighted_score":      round(weighted_score, 1),
        "volume_score":        volume_score,
        "acceleration_score":  acceleration_score,
        "momentum_score":      momentum_score,
        "liquidity_score":     liquidity_score,
        "breakout_score":      breakout_score,
        "rsi_score":           rsi_score,
    }


def calc_pump_probability_v19(weighted_score):
    """
    FIX v19 #5 — Logistic Probability Model v19.

    Menggantikan feature-sum probability dengan model berbasis weighted_score:

      P(pump) = 1 / (1 + exp(-k * (weighted_score - threshold)))

    Parameter (dikalibrasi untuk score 0-100):
      k         = 0.08   (steepness: lebih tajam dari v18)
      threshold = 55     (decision boundary — 50% probability di score 55)

    Interpretasi:
      score 40 → prob ~18%  (rendah)
      score 55 → prob ~50%  (boundary)
      score 65 → prob ~73%  (cukup tinggi)
      score 75 → prob ~86%  (tinggi)
      score 85 → prob ~93%  (sangat tinggi)
    """
    k         = CONFIG["prob_k"]
    threshold = CONFIG["prob_threshold"]
    prob      = 1.0 / (1.0 + math.exp(-k * (weighted_score - threshold)))
    return round(prob * 100, 1)

def check_volume_consistent(candles, lookback=3, min_ratio=1.5):
    """Volume tinggi harus konsisten ≥ 2 candle, bukan hanya 1 spike."""
    if len(candles) < 24:
        return False
    avg_vol   = sum(c["volume_usd"] for c in candles[-24:]) / 24
    if avg_vol <= 0:
        return False
    recent    = candles[-lookback:]
    above_avg = sum(1 for c in recent if c["volume_usd"] > avg_vol * min_ratio)
    return above_avg >= max(1, lookback // 2)

def calc_btc_correlation(coin_candles, btc_candles, lookback=24):
    """Pearson correlation coin vs BTC untuk mendeteksi pergerakan independen."""
    if not coin_candles or not btc_candles or len(coin_candles) < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": 0, "risk_note": "Data tidak cukup"}

    n   = min(lookback, len(coin_candles), len(btc_candles))
    c_c = coin_candles[-n:]
    c_b = btc_candles[-n:]

    def pct_changes(candles):
        return [(candles[i]["close"] - candles[i-1]["close"]) / candles[i-1]["close"]
                for i in range(1, len(candles)) if candles[i-1]["close"] > 0]

    cc = pct_changes(c_c)
    cb = pct_changes(c_b)
    mn = min(len(cc), len(cb))
    if mn < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": mn, "risk_note": "Data tidak cukup"}

    cc, cb = cc[-mn:], cb[-mn:]
    mc, mb = sum(cc) / mn, sum(cb) / mn
    num    = sum((x - mc) * (y - mb) for x, y in zip(cc, cb))
    sd_c   = (sum((x - mc)**2 for x in cc)) ** 0.5
    sd_b   = (sum((y - mb)**2 for y in cb)) ** 0.5

    corr = 0.0 if sd_c < 1e-10 or sd_b < 1e-10 else max(-1.0, min(1.0, num / (sd_c * sd_b)))

    if corr >= 0.75:
        label, emoji = "CORRELATED",  "🔗"
        risk_note    = "⚠️ Ikuti BTC! Jika BTC dump → exit cepat"
    elif corr >= 0.40:
        label, emoji = "MODERATE",    "〰️"
        risk_note    = "🔶 Sebagian ikuti BTC — pantau jika BTC turun"
    else:
        label, emoji = "INDEPENDENT", "🚀"
        risk_note    = "✅ Pergerakan independen — lebih tahan dump BTC"

    btc_chg  = ((c_b[-1]["close"] - c_b[0]["close"]) / c_b[0]["close"] * 100
                if len(c_b) >= 2 and c_b[0]["close"] > 0 else 0.0)
    coin_chg = ((c_c[-1]["close"] - c_c[0]["close"]) / c_c[0]["close"] * 100
                if len(c_c) >= 2 and c_c[0]["close"] > 0 else 0.0)

    if btc_chg <= CONFIG["btc_bearish_threshold"]:
        btc_regime, btc_re = "BEARISH", "🔻"
        btc_rn = f"⚠️ BTC bearish ({btc_chg:+.1f}%/{mn}h) — risiko tinggi"
    elif btc_chg >= CONFIG["btc_bullish_threshold"]:
        btc_regime, btc_re = "BULLISH", "🟢"
        btc_rn = f"✅ BTC bullish ({btc_chg:+.1f}%/{mn}h) — kondisi favorable"
    else:
        btc_regime, btc_re = "SIDEWAYS", "⬜"
        btc_rn = f"BTC sideways ({btc_chg:+.1f}%/{mn}h) — altcoin bisa independen"

    delta = coin_chg - btc_chg
    if delta >= CONFIG["outperform_min_delta"] and coin_chg > 0:
        op_label, op_emoji = "OUTPERFORM",   "🚀"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% (+{delta:.1f}%)"
    elif delta <= -CONFIG["outperform_min_delta"]:
        op_label, op_emoji = "UNDERPERFORM", "📉"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% ({delta:.1f}%)"
    else:
        op_label, op_emoji = "IN-LINE",      "〰️"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}%"

    return {
        "correlation":      round(corr, 3),
        "label":            label,
        "emoji":            emoji,
        "lookback":         mn,
        "risk_note":        risk_note,
        "btc_regime":       btc_regime,
        "btc_regime_emoji": btc_re,
        "btc_regime_note":  btc_rn,
        "btc_period_chg":   round(btc_chg, 2),
        "coin_period_chg":  round(coin_chg, 2),
        "outperform_label": op_label,
        "outperform_emoji": op_emoji,
        "outperform_note":  op_note,
        "delta_vs_btc":     round(delta, 2),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def find_swing_low_sl(candles, lookback=None):
    """Cari swing low terbaru dalam lookback candle sebagai dasar SL."""
    if lookback is None:
        lookback = CONFIG["sl_swing_lookback"]
    n = min(lookback, len(candles) - 1)
    if n < 2:
        return None
    recent_lows = [c["low"] for c in candles[-(n + 1):-1]]
    return min(recent_lows) * (1.0 - CONFIG["sl_swing_buffer"])

def calc_entry(candles, bos_level, alert_level, vwap, price_now, atr_abs_val=None, sr=None):
    """
    Entry / SL / Target — v18

    Entry:
      HIGH  → di atas BOS level + buffer kecil
      MEDIUM → VWAP atau market price

    SL:
      Swing low 12 candle, clamp [1x–3x ATR] dan [0.5%–8%]

    Target (3-tier, per-coin dinamis):
      Tier 1: Resistance pivot 48–168 candle
      Tier 2: ATR projection per-coin
      Tier 3: Fibonacci swing projection (fallback)
    """
    if atr_abs_val is None:
        atr_abs_val = calc_atr_abs(candles)

    # ── Entry ─────────────────────────────────────────────────────────────────
    gap_to_vwap_pct = (price_now - vwap) / vwap * 100 if vwap > 0 else 0

    if alert_level == "HIGH" and bos_level > 0 and bos_level < price_now * 1.05:
        entry        = bos_level * (1.0 + CONFIG["entry_bos_buffer"])
        entry_reason = "BOS breakout"
    elif gap_to_vwap_pct <= 2.0:
        entry        = max(vwap, price_now)
        entry_reason = "VWAP pullback"
    else:
        entry        = price_now * 1.001
        entry_reason = "market price"

    if entry < price_now:
        entry = price_now * 1.001

    # ── SL ────────────────────────────────────────────────────────────────────
    sl_swing = find_swing_low_sl(candles, lookback=12)
    if sl_swing is None or sl_swing >= entry:
        sl_swing = entry - atr_abs_val * 2.0

    sl_floor = entry - atr_abs_val * CONFIG["sl_atr_mult_max"]
    sl_ceil  = entry - atr_abs_val * CONFIG["sl_atr_mult_min"]
    sl       = max(sl_swing, sl_floor)
    sl       = min(sl, sl_ceil)
    sl       = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl       = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry:
        sl = entry * 0.98

    # ── Target — kumpulkan resistance pivot ───────────────────────────────────
    res_levels = []

    if sr and sr.get("resistance"):
        for rv in sr["resistance"]:
            if rv["level"] > entry * 1.005:
                res_levels.append(rv["level"])

    lookback_long = min(168, len(candles))
    recent_long   = candles[-lookback_long:]
    pivot_highs   = []
    for i in range(2, len(recent_long) - 2):
        h = recent_long[i]["high"]
        if (h > recent_long[i-1]["high"] and h > recent_long[i-2]["high"] and
                h > recent_long[i+1]["high"] and h > recent_long[i+2]["high"]):
            pivot_highs.append(h)

    if pivot_highs:
        pivot_highs = sorted(set(pivot_highs))
        clusters, cur = [], [pivot_highs[0]]
        for ph in pivot_highs[1:]:
            if (ph - cur[-1]) / cur[-1] < 0.015:
                cur.append(ph)
            else:
                clusters.append(sum(cur) / len(cur))
                cur = [ph]
        clusters.append(sum(cur) / len(cur))
        for c_lv in clusters:
            if c_lv > entry * 1.005 and c_lv not in res_levels:
                res_levels.append(c_lv)

    res_levels = sorted(set(res_levels))

    # Swing range untuk proyeksi Fibonacci
    swing_low_val  = min(c["low"]  for c in recent_long)
    swing_high_val = max(c["high"] for c in recent_long)
    swing_range    = swing_high_val - swing_low_val
    price_pos_pct  = ((entry - swing_low_val) / swing_range) if swing_range > 0 else 0.5

    # ATR floor adaptif berdasarkan posisi harga dalam range
    if price_pos_pct < 0.4:
        atr_mult_t1, atr_mult_t2 = 3.5, 6.5
    elif price_pos_pct < 0.6:
        atr_mult_t1, atr_mult_t2 = 2.5, 5.0
    else:
        atr_mult_t1, atr_mult_t2 = 1.5, 3.0

    atr_floor_t1 = entry + atr_abs_val * atr_mult_t1
    atr_floor_t2 = entry + atr_abs_val * atr_mult_t2

    if res_levels:
        t1, t1_source = res_levels[0], "R1 pivot"
        if len(res_levels) >= 2:
            t2, t2_source = res_levels[1], "R2 pivot"
        else:
            t2, t2_source = t1 * 1.272, "R1 × 1.272"
        if t1 < atr_floor_t1:
            t1, t1_source = atr_floor_t1, f"ATR×{atr_mult_t1:.1f} (R1 terlalu dekat)"
        if t2 < atr_floor_t2:
            t2, t2_source = atr_floor_t2, f"ATR×{atr_mult_t2:.1f}"
    else:
        swing_valid = swing_range > atr_abs_val * 2 and swing_low_val < entry
        if swing_valid:
            t1, t1_source = entry + swing_range * 0.382, "Fib 38.2% swing"
            t2, t2_source = entry + swing_range * 0.618, "Fib 61.8% swing"
        else:
            t1, t1_source = atr_floor_t1, f"ATR×{atr_mult_t1:.1f}"
            t2, t2_source = atr_floor_t2, f"ATR×{atr_mult_t2:.1f}"
        if t1 < atr_floor_t1:
            t1 = atr_floor_t1
        if t2 < atr_floor_t2:
            t2 = atr_floor_t2

    if t2 <= t1:
        t2        = t1 * (1 + (atr_abs_val / entry) * atr_mult_t1)
        t2_source = "T1 + ATR ext"

    t1 = max(t1, entry * 1.005)
    t2 = max(t2, t1   * 1.005)

    risk   = entry - sl
    rr_val = round((t1 - entry) / risk, 1) if risk > 0 else 0.0

    return {
        "entry":           round(entry, 8),
        "sl":              round(sl, 8),
        "sl_pct":          round((entry - sl) / entry * 100, 2),
        "t1":              round(t1, 8),
        "t2":              round(t2, 8),
        "rr":              rr_val,
        "rr_str":          f"{rr_val:.1f}",
        "vwap":            round(vwap, 8),
        "bos_level":       round(bos_level, 8),
        "alert_level":     alert_level,
        "gain_t1_pct":     round((t1 - entry) / entry * 100, 1),
        "gain_t2_pct":     round((t2 - entry) / entry * 100, 1),
        "atr_abs":         round(atr_abs_val, 8),
        "sl_method":       entry_reason,
        "used_resistance": len(res_levels) > 0,
        "n_res_levels":    len(res_levels),
        "t1_source":       t1_source,
        "t2_source":       t2_source,
        "atr_pct_abs":     round(atr_abs_val / entry * 100, 2),
        "swing_range_pct": round(swing_range / entry * 100, 1) if entry > 0 else 0.0,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    c1h   = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c4h   = get_candles(symbol, "4h",  CONFIG["candle_4h"])
    c15m  = get_candles(symbol, "15m", 30)   # 15m candles untuk buy pressure
    c5m   = get_candles(symbol, "5m",  36)   # NEW v18: 5m candles untuk micro momentum

    if len(c1h) < 48:
        log.info(f"  {symbol}: Candle 1h tidak cukup ({len(c1h)} < 48)")
        return None

    try:
        vol_24h   = float(ticker.get("quoteVolume", 0))
        chg_24h   = float(ticker.get("change24h",  0)) * 100
        price_now = float(ticker.get("lastPr",      0)) or c1h[-1]["close"]
    except Exception:
        return None

    if vol_24h <= 0 or price_now <= 0:
        return None

    # ── GATE 0: Open Interest minimum ────────────────────────────────────────
    oi_data = get_oi_change(symbol)
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]:
        log.info(
            f"  {symbol}: OI ${oi_data['oi_now']:,.0f} < ${CONFIG['min_oi_usd']:,} "
            f"— GATE GAGAL (coin illiquid)"
        )
        return None

    # ── GATE 1: Funding — ambil dan simpan snapshot ───────────────────────────
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)
    fstats  = get_funding_stats(symbol)
    if fstats is None:
        fstats = {
            "avg": funding, "cumulative": funding, "neg_pct": 0.0,
            "streak": 0, "basis": funding * 100, "current": funding,
            "sample_count": 1,
        }
        log.info(f"  {symbol}: Funding snapshot baru (1 data) — lanjut scan")

    # ── GATE 2: VWAP dengan toleransi ────────────────────────────────────────
    vwap            = calc_vwap(c1h, lookback=24)
    vwap_gate_level = vwap * CONFIG["vwap_gate_tolerance"]
    if price_now < vwap_gate_level:
        log.info(
            f"  {symbol}: Harga ${price_now:.6g} < VWAP gate ${vwap_gate_level:.6g} "
            f"— GATE GAGAL"
        )
        return None

    # ── Hitung semua indikator ────────────────────────────────────────────────
    bbw, bb_pct      = calc_bbw(c1h)
    atr_pct          = calc_atr_pct(c1h)
    atr_abs_val      = calc_atr_abs(c1h)
    atr_contr        = calc_atr_contracting(c1h)
    rsi              = get_rsi(c1h[-48:])
    bos_up, bos_level = detect_bos_up(c1h)
    higher_low       = higher_low_detected(c1h)
    vol_ratio        = calc_volume_ratio(c1h)
    vol_accel        = calc_volume_acceleration(c1h)
    vol_consistent   = check_volume_consistent(c1h)
    uptrend          = calc_uptrend_age(c1h)
    sr               = calc_support_resistance(c1h)
    btc_candles      = get_btc_candles_cached(48)
    btc_corr         = calc_btc_correlation(c1h, btc_candles, lookback=24)
    accum            = calc_accumulation_phase(c1h)
    htf_accum        = calc_htf_accumulation(c4h)
    liq_sweep        = detect_liquidity_sweep(c1h)
    energy           = detect_energy_buildup(c1h, oi_data)
    price_pos_48     = calc_swing_range_position(c1h, lookback=48)
    candle_dir_ratio = calc_candle_direction_ratio(c1h, lookback=6)

    # ── NEW v19: Phase 1-2-3 + Micro Momentum + Timing ──────────────────────
    vol_spike    = calc_volume_spike(c1h)
    micro_mom    = calc_micro_momentum(c5m)          # 5m micro momentum
    mom_accel    = calc_momentum_acceleration(c1h)   # 1h secondary signal
    buy_press    = calc_buy_pressure(c15m)
    whale_order  = detect_whale_order(c15m, oi_data) # upgraded whale
    fake_pump    = detect_fake_pump(c1h, buy_press["buy_ratio"], oi_data)

    # ── NEW v19: Additional Indicators ───────────────────────────────────────
    ema_trend    = calc_ema20_ema50(c1h)              # FIX v19 #6: EMA trend
    vol_zscore   = calc_volume_zscore(c1h)            # FIX v19 #10: Z-score
    micro_brkout = calc_micro_breakout(c1h)           # FIX v19 #11: Breakout
    ob_imbalance = get_orderbook_imbalance(symbol)    # FIX v19 #12: OB
    whale_accum  = detect_whale_accumulation(c1h)     # FIX v19 #16: Whale accum
    wick_data    = calc_wick_ratio(c1h)               # FIX v19 #9: Wick ratio

    # Set energy.is_strong jika funding negatif
    if energy["is_buildup"] and fstats.get("current", 1) <= 0:
        energy["is_strong"] = True
        energy["label"]     = energy["label"] + " 🔥 + funding negatif (squeeze)"

    # Rate candle di atas VWAP (6 candle terbaru)
    above_vwap_rate = 0.0
    if len(c1h) >= 6:
        above           = sum(1 for c in c1h[-6:] if c["close"] > vwap)
        above_vwap_rate = above / 6

    # Price change 1h
    price_chg = 0.0
    if len(c1h) >= 2 and c1h[-2]["close"] > 0:
        price_chg = (c1h[-1]["close"] - c1h[-2]["close"]) / c1h[-2]["close"] * 100

    # Price change 5m (untuk momentum gate)
    price_change_5m = 0.0
    if len(c5m) >= 2 and c5m[-2]["close"] > 0:
        price_change_5m = (c5m[-1]["close"] - c5m[-2]["close"]) / c5m[-2]["close"] * 100

    # ── GATE 3: Uptrend tidak terlalu tua ────────────────────────────────────
    if uptrend["is_late"]:
        log.info(
            f"  {symbol}: Uptrend sudah {uptrend['age_hours']}h — "
            f"terlalu tua, kemungkinan distribusi (GATE GAGAL)"
        )
        return None

    # ── GATE 4: RSI tidak overbought ─────────────────────────────────────────
    if rsi >= CONFIG["gate_rsi_max"]:
        log.info(
            f"  {symbol}: RSI {rsi:.1f} ≥ {CONFIG['gate_rsi_max']} — "
            f"overbought (GATE GAGAL)"
        )
        return None

    # ── GATE 5: BB Position tidak di puncak ──────────────────────────────────
    if bb_pct >= CONFIG["gate_bb_pos_max"]:
        log.info(
            f"  {symbol}: BB pos {bb_pct*100:.0f}% — overbought BB (GATE GAGAL)"
        )
        return None

    # ── GATE 6: Harga tidak di zona distribusi atas ───────────────────────────
    if price_pos_48 > 0.85:
        log.info(
            f"  {symbol}: Posisi harga {price_pos_48:.0%} dari swing range — "
            f"zona distribusi atas (GATE GAGAL)"
        )
        return None

    # ── FIX v19 #13 — GATE 7: Noise Filter — Volume & ATR Minimum ────────────
    # Tolak koin illiquid dan flat (terlalu berisik, tidak pump-able)
    if vol_24h < CONFIG["noise_min_vol_24h"]:
        log.info(
            f"  {symbol}: Vol 24h ${vol_24h/1e6:.1f}M < ${CONFIG['noise_min_vol_24h']/1e6:.0f}M "
            f"— FIX v19 #13 NOISE FILTER (GATE GAGAL)"
        )
        return None
    if atr_pct < CONFIG["noise_min_atr_pct"]:
        log.info(
            f"  {symbol}: ATR {atr_pct:.3f}% < {CONFIG['noise_min_atr_pct']}% "
            f"— FIX v19 #13 NOISE FILTER terlalu flat (GATE GAGAL)"
        )
        return None

    # ── FIX v19 #6 — GATE 8: Trend Context — Reject Strong Downtrend ─────────
    # price < EMA50 = strong downtrend → pump sangat kecil kemungkinannya
    if CONFIG["ema_trend_gate"] and ema_trend["is_strong_downtrend"]:
        log.info(
            f"  {symbol}: Price < EMA50 — FIX v19 #6 STRONG DOWNTREND GATE "
            f"({ema_trend['label']}) (GATE GAGAL)"
        )
        return None

    # ── FIX v19 #7 — GATE 9: Strict VWAP Bias ────────────────────────────────
    # Pump setup harus dimulai di atas VWAP
    # v18: toleransi 97% VWAP — terlalu longgar
    # v19: price harus >= VWAP (strict)
    if price_now < vwap * CONFIG["vwap_gate_tolerance"]:
        log.info(
            f"  {symbol}: Harga ${price_now:.6g} < VWAP ${vwap:.6g} "
            f"— FIX v19 #7 STRICT VWAP GATE (GATE GAGAL)"
        )
        return None

    # ── FIX v19 #8 — GATE 10: Momentum Validation ────────────────────────────
    # Jika momentum 5m <= 0 → tidak ada urgency untuk masuk
    if price_change_5m <= 0:
        log.info(
            f"  {symbol}: price_change_5m={price_change_5m:.3f}% ≤ 0 "
            f"— FIX v19 #8 MOMENTUM GATE (GATE GAGAL)"
        )
        return None

    # ── FIX v19 #9 — GATE 11: Wick Ratio (Bearish Rejection / Whale Trap) ────
    if wick_data["is_bearish_wick"]:
        log.info(
            f"  {symbol}: Wick ratio {wick_data['wick_ratio']:.2f} > "
            f"{CONFIG['wick_ratio_gate_max']} — "
            f"FIX v19 #9 FAKE PUMP WICK GATE (GATE GAGAL)"
        )
        return None

    # ── FIX v19 #9 — GATE 12: Buy Ratio Gate ─────────────────────────────────
    # buy_ratio < 55% → distribusi / sell pressure dominan
    if buy_press.get("buy_ratio", 0.5) < CONFIG["buy_ratio_gate_min"]:
        log.info(
            f"  {symbol}: Buy ratio {buy_press.get('buy_ratio',0)*100:.0f}% < "
            f"{CONFIG['buy_ratio_gate_min']*100:.0f}% "
            f"— FIX v19 #9 BUY RATIO GATE (GATE GAGAL)"
        )
        return None

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v19 — ADDITIVE BASE + WEIGHTED SCORE OVERLAY
    # ══════════════════════════════════════════════════════════════════════════
    score   = 0
    signals = []

    # ── 0a. Volume Spike (Phase 1) ────────────────────────────────────────────
    if vol_spike["tier"] > 0:
        score += vol_spike["score"]
        signals.append(vol_spike["label"])

    # ── 0b. Buy Pressure (Phase 2) ────────────────────────────────────────────
    if buy_press["tier"] > 0:
        score += buy_press["score"]
        signals.append(buy_press["label"])

    # ── 0c. Micro Momentum (Phase 3 — 5m responsif) ──────────────────────────
    if micro_mom["is_accelerating"]:
        score += micro_mom["score"]
        signals.append(micro_mom["label"])

    # ── 0d. 1h Momentum Acceleration (secondary signal) ──────────────────────
    if mom_accel["is_accelerating"] and not micro_mom["is_accelerating"]:
        score += mom_accel["score"]
        signals.append(mom_accel["label"])

    # ── 0e. Whale Order Detection ─────────────────────────────────────────────
    if whale_order["is_whale"]:
        score += whale_order["score"]
        signals.append(whale_order["label"])

    # ── 0f. Fake Pump Penalty (bertingkat) ───────────────────────────────────
    if fake_pump["is_fake"]:
        score += fake_pump["penalty"]   # nilai negatif −5/−12/−20
        signals.append(fake_pump["label"])

    # ── 1. BB Squeeze ─────────────────────────────────────────────────────────
    bb_squeeze = bbw < CONFIG["bb_squeeze_threshold"]
    if bb_squeeze:
        score += CONFIG["score_bb_squeeze"]
        signals.append(
            f"🗜️ BB Squeeze aktif (BBW {bbw*100:.2f}% < {CONFIG['bb_squeeze_threshold']*100:.0f}%) "
            f"— kompresi energi sebelum breakout"
        )

    # ── 2. ATR Contracting ────────────────────────────────────────────────────
    if atr_contr["is_contracting"]:
        score += CONFIG["score_atr_contracting"]
        signals.append(
            f"📉 ATR Menyempit — rasio {atr_contr['ratio']:.2f} "
            f"(ATR 6c = {atr_contr['ratio']*100:.0f}% dari ATR 24c) — energi menumpuk"
        )

    # ── 3. Energy Build-Up ────────────────────────────────────────────────────
    if energy["is_buildup"]:
        score += CONFIG["score_energy_buildup"]
        signals.append(energy["label"])
        if energy["is_strong"]:
            score   += 2
            signals.append("⭐ Energy Build-Up + Funding Negatif = squeeze probability tinggi")

    # ── 4. Smart Money Accumulation + Volatility Compression ─────────────────
    if accum["is_accumulating"] and accum["is_vol_compress"]:
        score += CONFIG["score_accumulation"] + CONFIG["score_vol_compression"]
        signals.append(
            f"🏦 AKUMULASI + VOL COMPRESSION — vol {accum['vol_ratio_4h']:.1f}x, "
            f"range {accum['price_range_pct']:.1f}%, pos {accum['price_pos']:.0%}"
        )
    elif accum["is_accumulating"]:
        score += CONFIG["score_accumulation"]
        signals.append(
            f"📦 Smart Money Accumulation — vol {accum['vol_ratio_4h']:.1f}x, "
            f"sideways {accum['price_range_pct']:.1f}%, pos {accum['price_pos']:.0%}"
        )
    elif accum["is_vol_compress"]:
        score += CONFIG["score_vol_compression"]
        signals.append(
            f"🗜️ Volatility Compression — ATR {accum['atr_contract']:.2f}x dari baseline"
        )

    # ── 5. HTF Accumulation 4H ────────────────────────────────────────────────
    if htf_accum["is_htf_accum"]:
        score += CONFIG["score_htf_accumulation"]
        signals.append(htf_accum["label"])

    # ── 6. Liquidity Sweep ────────────────────────────────────────────────────
    if liq_sweep["is_sweep"]:
        score += CONFIG["score_liquidity_sweep"]
        signals.append(liq_sweep["label"])

    # ── 7. OI Expansion ───────────────────────────────────────────────────────
    if not energy["is_buildup"]:
        if not oi_data["is_new"] and oi_data["oi_now"] > 0:
            chg = oi_data["change_pct"]
            if chg >= CONFIG["oi_strong_pct"]:
                score += CONFIG["score_oi_strong"]
                signals.append(f"📈 OI Expansion KUAT +{chg:.1f}% — posisi leverage besar dibangun")
            elif chg >= CONFIG["oi_change_min_pct"]:
                score += CONFIG["score_oi_expansion"]
                signals.append(f"📊 OI Expansion +{chg:.1f}% — akumulasi posisi futures")
        elif oi_data["is_new"] and oi_data["oi_now"] > 0:
            signals.append(
                f"📊 OI baseline ${oi_data['oi_now']/1e6:.2f}M (snapshot pertama)"
            )
    else:
        if oi_data["oi_now"] > 0:
            ov     = oi_data["oi_now"]
            os_str = (f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K")
            chg_str = (f"+{oi_data['change_pct']:.1f}%" if not oi_data.get("is_new")
                       else "baseline")
            signals.append(f"📊 OI: {os_str} ({chg_str}) — sudah termasuk dalam Energy Build-Up")

    # ── 8. Volume dengan konteks arah harga ──────────────────────────────────
    if vol_ratio > CONFIG["vol_ratio_threshold"] and vol_consistent:
        if candle_dir_ratio >= CONFIG["vol_bullish_min_ratio"]:
            score += CONFIG["score_vol_bullish"]
            signals.append(
                f"🟢 Volume {vol_ratio:.1f}x rata-rata + {candle_dir_ratio*100:.0f}% candle "
                f"bullish — buying pressure konsisten"
            )
        else:
            signals.append(
                f"⚠️ Volume {vol_ratio:.1f}x tapi {candle_dir_ratio*100:.0f}% candle "
                f"bullish — kemungkinan distribusi/short, skor TIDAK ditambah"
            )

    # ── 9. Volume Acceleration dengan konteks arah ────────────────────────────
    if vol_accel > CONFIG["vol_accel_threshold"] and vol_consistent:
        last_candle_bullish = c1h[-1]["close"] >= c1h[-1]["open"]
        if last_candle_bullish:
            score += CONFIG["score_vol_accel"]
            signals.append(
                f"📈 Volume acceleration {vol_accel*100:.0f}% — candle terbaru bullish"
            )
        else:
            signals.append(
                f"⚠️ Volume acceleration {vol_accel*100:.0f}% tapi candle terbaru merah "
                f"— kemungkinan distribusi agresif"
            )

    # ── 10. RSI ideal pre-pump = 40–60 ────────────────────────────────────────
    rsi_in_ideal_zone = CONFIG["rsi_ideal_min"] <= rsi <= CONFIG["rsi_ideal_max"]
    if rsi_in_ideal_zone:
        score += CONFIG["score_rsi_ideal"]
        signals.append(
            f"📊 RSI {rsi:.1f} — zona ideal pre-pump (40–60): "
            f"belum overbought, momentum mulai terbentuk"
        )
    elif rsi < CONFIG["rsi_ideal_min"]:
        signals.append(f"📊 RSI {rsi:.1f} — oversold (bisa reversal, tapi belum konfirmasi)")
    else:
        signals.append(f"📊 RSI {rsi:.1f} — di atas zona ideal, momentum sudah berjalan")

    # ── 11. Higher Low ────────────────────────────────────────────────────────
    if higher_low:
        score += CONFIG["score_higher_low"]
        signals.append("🔼 Higher Low terdeteksi — struktur bullish awal mulai terbentuk")

    # ── 12. BOS Up ────────────────────────────────────────────────────────────
    if bos_up:
        score += CONFIG["score_bos_up"]
        signals.append(
            f"🔺 BOS Up (level {_fmt_price(bos_level)}) — breakout minor, "
            f"konfirmasi struktur berbalik"
        )

    # ── 13. Funding rate ──────────────────────────────────────────────────────
    f_avg = fstats["avg"]
    funding_avg_neg_active = False

    if f_avg <= CONFIG["funding_bonus_avg"]:
        score += CONFIG["score_funding_avg_neg"]
        funding_avg_neg_active = True
        signals.append(f"⭐ Funding avg {f_avg:.6f} — sangat negatif (short squeeze setup)")
    elif fstats["cumulative"] <= CONFIG["funding_bonus_cumul"]:
        score += CONFIG["score_funding_cumul"]
        signals.append(f"Funding kumulatif {fstats['cumulative']:.5f} — akumulasi negatif")
    elif f_avg < 0:
        signals.append(f"Funding avg {f_avg:.6f} — negatif ringan (favorable)")
    elif f_avg >= CONFIG["funding_penalty_avg"]:
        score -= 2
        signals.append(
            f"⚠️ Funding avg {f_avg:.6f} — sangat positif (penalti: overbought)"
        )
    else:
        signals.append(f"Funding avg {f_avg:.6f} — netral")

    if not funding_avg_neg_active:
        if fstats["neg_pct"] >= 70 and fstats["sample_count"] >= 3:
            score += CONFIG["score_funding_neg_pct"]
            signals.append(
                f"Funding negatif {fstats['neg_pct']:.0f}% dari {fstats['sample_count']} periode"
            )
        if fstats["streak"] >= CONFIG["funding_streak_min"]:
            score += CONFIG["score_funding_streak"]
            signals.append(
                f"Funding streak negatif {fstats['streak']}x berturut "
                f"({fstats['sample_count']} total data)"
            )
    else:
        if fstats["neg_pct"] >= 70 and fstats["sample_count"] >= 3:
            signals.append(
                f"Funding negatif {fstats['neg_pct']:.0f}% "
                f"(sudah dihitung dalam avg_neg)"
            )

    # ── 14. BTC Outperformance ────────────────────────────────────────────────
    if btc_corr.get("outperform_label") == "OUTPERFORM":
        score += CONFIG["score_outperform"]
        signals.append(
            f"🚀 OUTPERFORM BTC — coin {btc_corr['coin_period_chg']:+.1f}% vs BTC "
            f"{btc_corr['btc_period_chg']:+.1f}% ({btc_corr['delta_vs_btc']:+.1f}%)"
        )

    # ── FIX v19 #6: EMA Trend Penalty ────────────────────────────────────────
    # EMA20 < EMA50 = downtrend context → kurangi score
    if ema_trend["is_downtrend"] and not ema_trend["is_strong_downtrend"]:
        score += CONFIG["ema_trend_score_penalty"]  # nilai negatif
        signals.append(
            f"⚠️ Downtrend Context — EMA20 < EMA50 "
            f"(score penalty {CONFIG['ema_trend_score_penalty']})"
        )
    else:
        signals.append(ema_trend["label"])

    # ── FIX v19 #10: Volume Z-Score ───────────────────────────────────────────
    if vol_zscore["is_anomaly"]:
        signals.append(vol_zscore["label"])
        # Z-score boost sudah dimasukkan ke weighted_score, bukan additive score

    # ── FIX v19 #11: Micro Breakout Detection ────────────────────────────────
    if micro_brkout["is_breakout"]:
        signals.append(micro_brkout["label"])

    # ── FIX v19 #12: Orderbook Imbalance ─────────────────────────────────────
    if ob_imbalance["is_bullish"]:
        signals.append(ob_imbalance["label"])

    # ── FIX v19 #14: Early Pump Detection ────────────────────────────────────
    early_pump = calc_early_pump_signal(c1h, vwap, vol_spike, price_pos_48)
    if early_pump["is_early_pump"]:
        score += CONFIG["early_pump_score_boost"]
        signals.append(early_pump["label"])

    # ── FIX v19 #16: Whale Accumulation ──────────────────────────────────────
    if whale_accum["is_accumulating"]:
        signals.append(whale_accum["label"])

    # ══════════════════════════════════════════════════════════════════════════
    #  ALERT LEVEL v19 — weighted score + logistic probability + timing ETA
    # ══════════════════════════════════════════════════════════════════════════

    # FIX v19 #4: AI-Style Weighted Scoring (normalized 0-100)
    wscore_data = calc_weighted_score_v19(
        vol_spike, micro_mom, mom_accel, buy_press,
        liq_sweep, accum, htf_accum, bos_up, rsi,
        vol_zscore, micro_brkout, whale_accum, ob_imbalance, energy
    )
    weighted_score = wscore_data["weighted_score"]

    # FIX v19 #5: Logistic Probability berbasis weighted_score
    pump_prob = calc_pump_probability_v19(weighted_score)

    # FIX v18: Pump Timing ETA (dipertahankan)
    pump_timing = calc_pump_timing_eta(
        vol_accel, buy_press["buy_ratio"], oi_data, micro_mom
    )

    # FIX v19 #18: Alert level dari threshold v19 (dinaikkan)
    alert_level_v19 = get_alert_level_v19(weighted_score)

    # Backward compat: map ke format lama
    alert_level_v18 = alert_level_v19

    # Pump type detection (Phase-based)
    if vol_spike["tier"] >= 2 and buy_press["tier"] >= 2 and mom_accel["is_strong"]:
        pump_type = "Phase 1+2+3 Full — PUMP IMMINENT"
    elif vol_spike["tier"] >= 2 and buy_press["tier"] >= 2:
        pump_type = "Phase 1+2 — Volume + Buy Pressure"
    elif vol_spike["tier"] >= 3:
        pump_type = "Phase 1 STRONG — Volume Spike 3x+"
    elif whale_order["is_whale"] and buy_press["tier"] >= 2:
        pump_type = "Whale Entry + Buy Pressure"
    elif mom_accel["is_strong"] and vol_spike["tier"] >= 1:
        pump_type = "Momentum Acceleration + Vol Spike"
    elif energy["is_buildup"] and energy["is_strong"]:
        pump_type = "Energy Build-Up + Short Squeeze"
    elif liq_sweep["is_sweep"] and htf_accum["is_htf_accum"]:
        pump_type = "Liquidity Sweep + HTF Accumulation"
    elif accum["is_accumulating"] and accum["is_vol_compress"] and atr_contr["is_contracting"]:
        pump_type = "Smart Money Accumulation + ATR Compression"
    elif bb_squeeze and energy["is_buildup"]:
        pump_type = "BB Squeeze + Energy Build-Up"
    elif energy["is_buildup"]:
        pump_type = "Energy Build-Up (OI+Vol+Price Stuck)"
    elif vol_spike["tier"] >= 1:
        pump_type = "Volume Spike — Early Interest"
    elif accum["is_accumulating"]:
        pump_type = "Smart Money Accumulation"
    elif htf_accum["is_htf_accum"]:
        pump_type = "HTF Accumulation Build-Up"
    elif liq_sweep["is_sweep"]:
        pump_type = "Liquidity Sweep Reversal"
    elif bb_squeeze and atr_contr["is_contracting"]:
        pump_type = "BB Squeeze + ATR Compression"
    elif (not oi_data["is_new"] and oi_data["change_pct"] >= CONFIG["oi_strong_pct"]
          and accum["is_vol_compress"]):
        pump_type = "OI Expansion Kuat + Vol Compression"
    else:
        pump_type = "Accumulation Setup"

    # Map alert_level_v19 ke format lama (HIGH/MEDIUM) untuk kompatibilitas
    if alert_level_v19 == "STRONG ALERT":
        alert_level = "HIGH"
    elif alert_level_v19 in ("ALERT", "WATCHLIST"):
        alert_level = "MEDIUM"
    else:
        alert_level = "LOW"

    # ── Entry & Target v19 — Adaptive Entry + Liquidity-Aware SL + Wider TP ──
    market_regime = calc_market_regime(
        c1h, vwap, buy_press["buy_ratio"], vol_ratio, rsi, price_pos_48
    )
    entry_data = calc_entry_v19(
        c1h, vwap, price_now, atr_abs_val, market_regime, sr,
        rsi, buy_press["buy_ratio"], vol_ratio, price_pos_48,
        alert_level_v19, bos_level, liq_sweep
    )

    # ── FIX v19 #15: Signal Ranking (score × probability) ────────────────────
    # rank_value digunakan untuk sort — menggabungkan quality dan confidence
    rank_value = weighted_score * (pump_prob / 100.0)

    # FIX v19 #18: gunakan threshold WATCHLIST v19 (55) sebagai minimum
    min_score = CONFIG["score_watchlist"]
    if weighted_score >= min_score:
        return {
            "symbol":          symbol,
            "score":           score,             # additive raw score (v18 compat)
            "weighted_score":  weighted_score,    # FIX v19 #4: normalized 0-100
            "wscore_data":     wscore_data,        # breakdown komponen
            "rank_value":      round(rank_value, 2),  # FIX v19 #15: ranking key
            "signals":         signals,
            "entry":           entry_data,
            "price":           price_now,
            "chg_24h":         chg_24h,
            "vol_24h":         vol_24h,
            "rsi":             round(rsi, 1),
            "bbw":             round(bbw * 100, 2),
            "bb_pct":          round(bb_pct, 2),
            "bb_squeeze":      bb_squeeze,
            "atr_pct":         round(atr_pct, 2),
            "atr_contracting": atr_contr["is_contracting"],
            "atr_ratio":       atr_contr["ratio"],
            "above_vwap_rate": round(above_vwap_rate * 100, 1),
            "vwap":            round(vwap, 8),
            "bos_up":          bos_up,
            "bos_level":       round(bos_level, 8),
            "higher_low":      higher_low,
            "funding_stats":   fstats,
            "pump_type":       pump_type,
            "alert_level":     alert_level,
            "alert_level_v18": alert_level_v19,   # v18 compat key → v19 value
            "alert_level_v19": alert_level_v19,   # NEW v19
            "pump_prob":       pump_prob,          # FIX v19 #5: logistic prob
            "vol_ratio":       round(vol_ratio, 2),
            "vol_accel":       round(vol_accel * 100, 1),
            "vol_consistent":  vol_consistent,
            "candle_dir_ratio": round(candle_dir_ratio * 100, 1),
            "price_pos_48":    price_pos_48,
            "uptrend_age":     uptrend["age_hours"],
            "sr":              sr,
            "btc_corr":        btc_corr,
            "accum":           accum,
            "htf_accum":       htf_accum,
            "liq_sweep":       liq_sweep,
            "energy":          energy,
            "oi_data":         oi_data,
            # v18 phase signals + timing (dipertahankan)
            "vol_spike":       vol_spike,
            "buy_press":       buy_press,
            "micro_mom":       micro_mom,
            "mom_accel":       mom_accel,
            "whale_order":     whale_order,
            "fake_pump":       fake_pump,
            "market_regime":   market_regime,
            "pump_timing":     pump_timing,
            # FIX v19: new indicators
            "ema_trend":       ema_trend,          # #6
            "vol_zscore":      vol_zscore,         # #10
            "micro_brkout":    micro_brkout,       # #11
            "ob_imbalance":    ob_imbalance,       # #12
            "whale_accum":     whale_accum,        # #16
            "wick_data":       wick_data,          # #9
            "price_change_5m": round(price_change_5m, 3),  # #8
        }
    else:
        log.info(f"  {symbol}: Weighted Score {weighted_score:.1f} < {min_score} (threshold v19) — dilewati")
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def _fmt_price(p):
    """Format harga otomatis sesuai magnitudo."""
    if p == 0:
        return "0"
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.5f}"
    return f"{p:.8f}"

def build_alert(r, rank=None):
    """
    Pesan Telegram v19 — diperkaya dengan weighted score, logistic prob,
    EMA trend, Z-score, micro breakout, OB imbalance.

    FIX v19 #17: html.escape() pada semua konten dinamis untuk mencegah
    error "Bad Request: can't parse entities".
    """
    level_v19 = r.get("alert_level_v19", r.get("alert_level_v18", "ALERT"))
    if level_v19 == "STRONG ALERT":
        level_icon = "🔥"
    elif level_v19 == "ALERT":
        level_icon = "📡"
    else:
        level_icon = "👁"

    e   = r["entry"]
    bc  = r.get("btc_corr", {})
    sr  = r.get("sr", {})
    oi  = r.get("oi_data", {})
    vs  = r.get("vol_spike", {})
    bp  = r.get("buy_press", {})
    ma  = r.get("mom_accel", {})
    wo  = r.get("whale_order", {})
    fp  = r.get("fake_pump", {})
    wsd = r.get("wscore_data", {})
    et  = r.get("ema_trend", {})

    p     = r["price"]
    entry = e["entry"]
    sl    = e["sl"]
    t1    = e["t1"]
    t2    = e["t2"]
    t3    = e.get("t3", t2)

    weighted_score = r.get("weighted_score", r.get("score", 0))
    pump_prob      = r.get("pump_prob", 0)
    rank_value     = r.get("rank_value", 0)
    market_regime  = e.get("market_regime", r.get("market_regime", "NEUTRAL"))

    pt    = r.get("pump_timing", {})
    eta   = pt.get("eta", "?")
    eta_e = pt.get("eta_emoji", "")
    urg   = pt.get("urgency", "")

    # FIX v19 #17: Escape dynamic symbol name
    sym_escaped = _escape_html(r["symbol"])

    msg  = f"{level_icon} <b>{sym_escaped} — {level_v19}</b>  #{rank}\n"
    msg += f"<b>Score :</b> {r['score']} (W:{weighted_score:.0f}/100)  |  <b>Prob:</b> {pump_prob}%\n"
    msg += f"<b>Rank  :</b> {rank_value:.1f}  |  <b>ETA :</b> {eta_e} {eta} ({urg})\n"

    # FIX v19 #17: Escape pump_type yang bisa mengandung karakter spesial
    pump_type_escaped = _escape_html(r.get("pump_type", ""))
    msg += f"<b>Type  :</b> {pump_type_escaped}\n"
    msg += f"<b>Regime:</b> {market_regime}  |  <b>Scan:</b> {utc_now()}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    # Harga & kondisi pasar
    msg += f"<b>Harga :</b> <code>{_fmt_price(p)}</code>  ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>VWAP  :</b> <code>{_fmt_price(r['vwap'])}</code>"
    gap_vwap = (p - r['vwap']) / r['vwap'] * 100 if r['vwap'] > 0 else 0
    msg += f"  ({gap_vwap:+.1f}% vs harga)\n"
    msg += (
        f"<b>Posisi:</b> {r['price_pos_48']:.0%} range  |  "
        f"RSI: {r['rsi']}  |  "
        f"{r['candle_dir_ratio']:.0f}% candle hijau\n"
    )

    # FIX v19 #6: EMA Trend Context
    ema_label = _escape_html(et.get("label", "—"))
    msg += f"<b>Trend :</b> {ema_label[:60]}\n"

    # FIX v19 #4: Weighted Score Breakdown
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"<b>📊 Score v19 [{weighted_score:.0f}/100 → Prob:{pump_prob}%]:</b>\n"
    msg += (
        f"  Vol:{wsd.get('volume_score',0):.0f}  "
        f"Accel:{wsd.get('acceleration_score',0):.0f}  "
        f"Mom:{wsd.get('momentum_score',0):.0f}  "
        f"Liq:{wsd.get('liquidity_score',0):.0f}  "
        f"Brk:{wsd.get('breakout_score',0):.0f}  "
        f"RSI:{wsd.get('rsi_score',0):.0f}\n"
    )

    # Phase Signals v18 (dipertahankan)
    mm   = r.get("micro_mom", {})

    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>📊 Phase Signals:</b>\n"
    vol_str   = f"{vs.get('ratio', 0):.1f}x"   if vs else "—"
    buy_str   = f"{bp.get('buy_pct', 0):.0f}%" if bp else "—"

    # Micro momentum (5m)
    if mm and mm.get("is_accelerating"):
        m5   = mm.get("mom_5m", 0) * 100
        accel_val = mm.get("accel", 0) * 100
        micro_str = f"⚡{accel_val:+.3f}% (5m:{m5:+.3f}%)"
    else:
        micro_str = "—"

    accel_1h = (f"+{ma.get('acceleration',0)*100:.2f}%"
                if ma and ma.get("is_accelerating") else "—")

    wh_conf   = wo.get("confidence","") if wo else ""
    whale_str = (f"✅ {wo.get('mult',0):.1f}x [{wh_conf}]"
                 if wo and wo.get("is_whale") else "—")

    fake_sev = fp.get("severity","") if fp and fp.get("is_fake") else ""
    fake_str = (f"⚠️ FAKE [{fake_sev}] ({fp.get('n_cond',0)}/4)"
                if fp and fp.get("is_fake") else "✅ Clean")

    timing_score = pt.get("timing_score", 0) if pt else 0

    # FIX v19 #10: Z-score
    vz   = r.get("vol_zscore", {})
    z_str = (f"⚡{vz.get('z',0):.1f}σ ANOMALI"
             if vz.get("is_anomaly") else f"{vz.get('z',0):.1f}σ")

    # FIX v19 #11: Micro Breakout
    mb   = r.get("micro_brkout", {})
    mb_str = "✅ BREAKOUT" if mb.get("is_breakout") else "—"

    # FIX v19 #12: OB Imbalance
    ob   = r.get("ob_imbalance", {})
    ob_str = (f"📗{ob.get('imbalance',0):.2f}x BULLISH"
              if ob.get("is_bullish") else f"{ob.get('imbalance',1.0):.2f}x")

    msg += (
        f"  Vol Spike   : {vol_str}  Z:{z_str}\n"
        f"  Buy Pressure: {buy_str}\n"
        f"  Micro Mom5m : {micro_str}\n"
        f"  Mom 1h      : {accel_1h}\n"
        f"  Whale       : {whale_str}\n"
        f"  Fake Filter : {fake_str}\n"
        f"  OB Imbalance: {ob_str}\n"
        f"  Breakout20  : {mb_str}\n"
        f"  Timing Score: {timing_score:.2f}\n"
    )

    # Entry / SL / TP v19
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    sl_method = _escape_html(e.get("sl_method", ""))[:60]
    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(entry)}</code>  [{sl_method}]\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(sl)}</code>  (-{e['sl_pct']:.2f}%)\n"
    rr3_str = e.get("rr3_str", e.get("rr2_str", "?"))
    msg += f"🎯 <b>TP1   :</b> <code>{_fmt_price(t1)}</code>  (+{e['gain_t1_pct']:.1f}%)  RR:{e['rr_str']}x\n"
    msg += f"🎯 <b>TP2   :</b> <code>{_fmt_price(t2)}</code>  (+{e['gain_t2_pct']:.1f}%)  RR:{e.get('rr2_str','?')}x\n"
    msg += f"🎯 <b>TP3   :</b> <code>{_fmt_price(t3)}</code>  (+{e.get('gain_t3_pct', 0):.1f}%)  RR:{rr3_str}x\n"
    msg += f"⚖️ <b>ATR   :</b> {e.get('atr_pct', r.get('atr_pct',0)):.2f}%  |  SL: -{e['sl_pct']:.2f}%\n"
    msg += f"📌 <i>{_escape_html(e.get('trail_note',''))}</i>\n"

    # BTC correlation
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    if bc.get("correlation") is not None:
        msg += (
            f"{bc.get('btc_regime_emoji','❓')} <b>BTC:</b> {bc.get('btc_regime','?')}"
            f"  ({bc.get('btc_period_chg',0):+.1f}%/{bc.get('lookback',24)}h)\n"
        )
        msg += (
            f"{bc.get('outperform_emoji','〰️')} <b>vs BTC:</b> {bc.get('outperform_label','?')} "
            f"| Coin {bc.get('coin_period_chg',0):+.1f}% vs BTC {bc.get('btc_period_chg',0):+.1f}%\n"
        )
    else:
        msg += "📊 <b>vs BTC:</b> data tidak tersedia\n"

    # Support & Resistance
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    res_list = sr.get("resistance", []) if sr else []
    sup_list = sr.get("support",    []) if sr else []
    if res_list:
        for rv in res_list[:2]:
            msg += f"🔴 R <code>{_fmt_price(rv['level'])}</code>  ({rv['gap_pct']:+.1f}%)\n"
    msg += f"▶ NOW <code>{_fmt_price(p)}</code>\n"
    if sup_list:
        for sv in sup_list[:2]:
            msg += f"🟢 S <code>{_fmt_price(sv['level'])}</code>  ({sv['gap_pct']:+.1f}%)\n"

    # OI
    if oi.get("oi_now", 0) > 0:
        ov     = oi["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs     = f"({oi['change_pct']:+.1f}%)" if not oi.get("is_new") else "(baseline)"
        msg += f"📈 <b>OI:</b> {os_str} {cs}\n"

    # Sinyal teknikal prioritas
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>Sinyal:</b>\n"
    priority_signals = []
    keywords = [
        "Phase", "Volume Spike", "Buy Pressure", "Acceleration",
        "Whale", "FAKE", "AKUMULASI", "BUILD-UP", "Squeeze", "Sweep",
        "BOS", "Funding", "HTF", "OI", "ATR", "BB", "Z-Score",
        "Breakout", "Imbalance", "EARLY PUMP", "EMA", "Downtrend"
    ]
    for s in r["signals"]:
        if any(kw in s for kw in keywords):
            priority_signals.append(s)
        if len(priority_signals) >= 8:
            break
    for s in priority_signals:
        s_short  = s[:90] + "…" if len(s) > 90 else s
        # FIX v19 #17: escape signal text
        s_escaped = _escape_html(s_short)
        msg += f"• {s_escaped}\n"

    msg += f"\n<i>Scanner v19 | ⚠️ Bukan financial advice</i>"
    return msg

def build_summary(results):
    """
    FIX v19 #15 — Summary diurutkan berdasarkan rank_value (score × prob).
    Menampilkan weighted_score dan pump_prob per coin.
    """
    msg = f"\U0001f4cb <b>TOP CANDIDATES Scanner v19 \u2014 {utc_now()}</b>\n{chr(9473)*28}\n"
    for i, r in enumerate(results, 1):
        vol_str    = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                      else f"${r['vol_24h']/1e3:.0f}K")
        lv19       = r.get("alert_level_v19", r.get("alert_level_v18", "ALERT"))
        prob       = r.get("pump_prob", 0)
        wscore     = r.get("weighted_score", r.get("score", 0))
        rank_val   = r.get("rank_value", 0)
        level_icon = "\U0001f525" if lv19 == "STRONG ALERT" else ("\U0001f4e1" if lv19 == "ALERT" else "\U0001f441")
        vs_ratio   = r.get("vol_spike", {}).get("ratio", 0)
        bp_pct     = r.get("buy_press", {}).get("buy_pct", 0)
        whale_tag  = " \U0001f433" if r.get("whale_order", {}).get("is_whale") else ""
        fake_tag   = " \u26a0\ufe0f"  if r.get("fake_pump",   {}).get("is_fake")  else ""
        accel_tag  = " \u26a1"   if r.get("mom_accel",   {}).get("is_accelerating") else ""
        early_tag  = " \U0001f50d" if r.get("vol_zscore",  {}).get("is_anomaly")  else ""
        sym_esc    = _escape_html(r["symbol"])
        msg += (
            f"{i}. {level_icon} <b>{sym_esc}</b> "
            f"[{lv19} | W:{wscore:.0f}/100 | Prob:{prob}% | Rank:{rank_val:.1f}]\n"
        )
        pt_r   = r.get("pump_timing", {})
        eta_r  = pt_r.get("eta", "?")
        eta_er = pt_r.get("eta_emoji", "")
        msg += (
            f"   Vol:{vs_ratio:.1f}x | Buy:{bp_pct:.0f}%{whale_tag}{accel_tag}{fake_tag}{early_tag} | "
            f"RSI:{r['rsi']} | ETA:{eta_er}{eta_r} | "
            f"TP1:+{r['entry']['gain_t1_pct']}% TP3:+{r['entry'].get('gain_t3_pct',0):.1f}%\n"
        )
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found      = []
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
            vol   = float(ticker.get("quoteVolume", 0))
            chg   = float(ticker.get("change24h",   0)) * 100
            price = float(ticker.get("lastPr",       0))
        except Exception:
            filtered_stats["parse_error"] += 1
            continue

        if vol < CONFIG["pre_filter_vol"]:
            filtered_stats["vol_too_low"] += 1
            continue

        if vol > CONFIG["max_vol_24h"]:
            filtered_stats["vol_too_high"] += 1
            continue

        # FIX v18: dilonggarkan dari 5% → 8%
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

    total      = len(WHITELIST_SYMBOLS)
    will_scan  = len(all_candidates)
    n_excluded = (filtered_stats.get("excluded_keyword", 0)
                  + filtered_stats.get("manual_exclude", 0))
    n_filtered = sum(v for k, v in filtered_stats.items()
                     if k not in ("excluded_keyword", "manual_exclude"))
    accounted  = will_scan + n_excluded + n_filtered + len(not_found)

    log.info(f"\n📊 SCAN SUMMARY Scanner v19:")
    log.info(f"   Whitelist total  : {total} coins")
    log.info(f"   ✅ Will scan     : {will_scan} ({will_scan/total*100:.1f}%)")
    log.info(f"   🚫 Excluded kw   : {n_excluded}")
    log.info(f"   ❌ Filtered      : {n_filtered}")
    log.info(f"   ⚠️  Not in Bitget : {len(not_found)}")
    log.info(f"   ✔️  Akuntabel     : {accounted}/{total}")
    log.info(f"\n📋 Filter breakdown:")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"   {k:25s}: {v}")
    if not_found:
        sample = ", ".join(not_found[:10])
        log.info(f"\n   Missing sample   : {sample}"
                 f"{' ...' if len(not_found) > 10 else ''}")
    est_secs = will_scan * CONFIG["sleep_coins"]
    log.info(f"\n⏱️  Est. scan time: {est_secs:.0f}s (~{est_secs/60:.1f} min)")
    log.info("=" * 70 + "\n")
    return all_candidates

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== QUANTITATIVE PUMP DETECTION SCANNER v19 — {utc_now()} ===")

    load_funding_snapshots()
    log.info(f"Funding snapshots loaded: {len(_funding_snapshots)} coins di memori")

    load_oi_snapshots()

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker dari Bitget: {len(tickers)}")

    candidates = build_candidate_list(tickers)
    results    = []

    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except Exception:
            vol = 0.0

        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")

        try:
            res = master_score(sym, t)
            if res:
                log.info(
                    f"  ✅ W-Score={res['weighted_score']:.0f}/100 | "
                    f"Prob={res['pump_prob']}% | Rank={res['rank_value']:.1f} | "
                    f"{res['alert_level_v19']} | {res['pump_type']} | "
                    f"pos:{res['price_pos_48']:.0%} | "
                    f"T1:+{res['entry']['gain_t1_pct']}%"
                )
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    save_all_funding_snapshots()
    log.info("Funding snapshots disimpan ke disk.")

    save_oi_snapshots()
    log.info("OI snapshots disimpan ke disk.")

    # FIX v19 #15: Sort berdasarkan rank_value = weighted_score × probability
    # Ini memastikan sinyal berkualitas TINGGI dan berkeyakinan TINGGI naik ke atas
    results.sort(key=lambda x: x.get("rank_value", 0), reverse=True)
    log.info(f"\nLolos threshold v19 (W-Score >= {CONFIG['score_watchlist']}): {len(results)} coin")

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
            log.info(
                f"✅ Alert #{rank}: {r['symbol']} W-Score={r['weighted_score']:.0f} "
                f"Prob={r['pump_prob']}% Rank={r['rank_value']:.1f} "
                f"Level={r['alert_level_v19']}"
            )
        time.sleep(2)

    log.info(f"=== SELESAI Scanner v19 — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  QUANTITATIVE PUMP DETECTION SCANNER v19                    ║")
    log.info("║  20 Upgrades: Weighted Score, Logistic Prob, EMA Filter,    ║")
    log.info("║  Adaptive Entry, Liquidity SL, Noise Filter, Early Pump     ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
