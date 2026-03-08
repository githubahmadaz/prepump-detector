"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  QUANTITATIVE PUMP DETECTION SCANNER v18                                     ║
║                                                                              ║
║  UPGRADE v18 — 7 perbaikan dari laporan evaluasi v18:                        ║
║                                                                              ║
║  FIX #1 — PROBABILITY MODEL BERBASIS FEATURE (bukan score)                  ║
║    v18: prob = logistic(score) — score adalah penjumlahan heuristik          ║
║    v18: z = Σ(bobot × feature_norm)  lalu  prob = 1/(1+exp(-z))             ║
║    Bobot: vol_spike×0.9, buy_press×1.2, mom_accel×1.5,                     ║
║           OI_change×0.7, BB_squeeze×0.6, ATR_contract×0.5, fake×−1.0       ║
║                                                                              ║
║  FIX #2 — WHALE DETECTION DIPERKUAT                                          ║
║    v18: volume > 5× avg (terlalu kasar, market maker bisa trigger)           ║
║    v18: whale = vol>3× AND buy_ratio>65% AND OI_naik                        ║
║         → jauh lebih akurat, filter MM & liquidation spike                  ║
║                                                                              ║
║  FIX #3 — MICRO MOMENTUM ENGINE (5m/15m/1h)                                 ║
║    v18: mom_3h − mom_6h (terlalu lambat untuk pump 5−20 menit)              ║
║    v18: mom_5m, mom_15m, mom_1h dari candle 5m                              ║
║         accel = mom_5m×2 + mom_15m − mom_1h                                ║
║                                                                              ║
║  FIX #4 — ENTRY ENGINE DIPERBAIKI (RR lebih besar)                          ║
║    v18: entry ≈ price atau VWAP (RR kecil)                                  ║
║    v18: PULLBACK  → entry = VWAP − 0.3×ATR                                 ║
║         SWEEP     → entry = liq_sweep_low + 0.2×ATR                        ║
║         BREAKOUT  → entry = breakout retest zone + 0.15×ATR                ║
║                                                                              ║
║  FIX #5 — TARGET TP DIPERLEBAR (altcoin pump 5−20%)                         ║
║    v18: TP1=1×ATR / TP2=2×ATR / TP3=4×ATR (terlalu konservatif)            ║
║    v18: TP1=1.5×ATR / TP2=3×ATR / TP3=5×ATR                                ║
║         + TP3 cek liquidity void (area tanpa resistance)                    ║
║         RR target: 2.5−6× (realistis untuk altcoin pump)                   ║
║                                                                              ║
║  FIX #6 — PUMP TIMING MODEL / ETA (fitur baru)                              ║
║    timing = 0.4×vol_accel + 0.3×buy_ratio + 0.2×OI_accel + 0.1×mom_micro  ║
║    Output: Pump ETA → 5 min / 10 min / 30 min / 60 min / > 1h              ║
║                                                                              ║
║  FIX #7 — FAKE PUMP FILTER DIPERKUAT (model baru)                           ║
║    v18: price_up AND buy<50% → fake                                          ║
║    v18: fake_score = price_spike×w1 + vol_spike×w2 + OI_flat×w3             ║
║                    + sell_pressure×w4 (bertingkat, penalti proporsional)    ║
║                                                                              ║
║  WARISAN v18: vol spike 3 tier, buy pressure, logistic prob, whale base,    ║
║               OI persistence, funding guard, BB/ATR/HTF accumulation       ║
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
    "/tmp/scanner_v18.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v18 — log aktif: /tmp/scanner_v18.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_alert":          8,
    "max_alerts_per_run":       15,

    # ── Volume 24h total (USD) ────────────────────────────────────────────────
    "min_vol_24h":          10_000,
    "max_vol_24h":      50_000_000,
    "pre_filter_vol":       10_000,

    # ── Open Interest minimum filter ──────────────────────────────────────────
    "min_oi_usd":          100_000,   # minimal $100K OI

    # ── Gate perubahan harga 24h ──────────────────────────────────────────────
    # FIX v18: dilonggarkan dari 5% → 8%.
    # 5% terlalu ketat di pasar bullish — coin yang baru mulai pump +5-7%
    # masih bisa pre-pump, belum tentu distribusi.
    # 8% masih memfilter coin yang sudah pump besar.
    "gate_chg_24h_max":          8.0,
    "gate_chg_24h_min":        -15.0,   # hanya skip dump besar

    # ── VWAP Gate Tolerance ───────────────────────────────────────────────────
    "vwap_gate_tolerance":      0.97,   # price > vwap * 0.97

    # ── Gate uptrend usia ─────────────────────────────────────────────────────
    "gate_uptrend_max_hours":   10,

    # ── Gate RSI overbought ───────────────────────────────────────────────────
    "gate_rsi_max":             72.0,

    # ── Gate BB Position ──────────────────────────────────────────────────────
    "gate_bb_pos_max":          1.05,

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
    # FIX v18: OI snapshot sekarang persisten ke disk
    "oi_snapshot_file":       "./oi_snapshot.json",

    # ══════════════════════════════════════════════════════════════════════════
    #  BOBOT SKOR v18
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
    # FIX v18: score_vol_compression hanya aktif jika is_accumulating=False
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
    # FIX v18: lookback dinaikkan 6 → 16 candle (lebih bermakna)
    "higher_low_lookback":     16,
    "score_higher_low":        2,

    # ── BOS Up ───────────────────────────────────────────────────────────────
    # FIX v18: lookback dinaikkan 3 → 8 candle (BOS lebih bermakna)
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
    #  UPGRADE v18: NEW FEATURES
    # ══════════════════════════════════════════════════════════════════════════

    # ── Volume Spike Detection (Phase 1 pre-pump) ─────────────────────────────
    # Formula: current_volume_20 / avg_volume_20
    "vol_spike_window":        20,       # candle baseline window
    "vol_spike_low":           1.5,      # threshold low  → +8
    "vol_spike_mid":           2.0,      # threshold mid  → +15
    "vol_spike_high":          3.0,      # threshold high → +22
    "score_vol_spike_low":     8,
    "score_vol_spike_mid":     15,
    "score_vol_spike_high":    22,

    # ── Momentum Acceleration (Phase 3 pre-pump) ──────────────────────────────
    # acceleration = (price_now-price_3h) - (price_3h-price_6h)
    "accel_strong_threshold":  0.005,    # 0.5% percepatan → strong
    "score_accel_positive":    10,       # acceleration > 0
    "score_accel_strong":      18,       # acceleration > strong_threshold

    # ── Buy Pressure (Phase 2 pre-pump) ──────────────────────────────────────
    # buy_ratio = buy_volume / total_volume (dari 15m candles)
    "buy_pressure_window":     8,        # 8 candle 15m = 2 jam
    "buy_pressure_low":        0.55,     # >55% accumulation → +6
    "buy_pressure_mid":        0.65,     # >65% whale activity → +12
    "buy_pressure_high":       0.75,     # >75% pump phase → +20
    "score_buy_pressure_low":  6,
    "score_buy_pressure_mid":  12,
    "score_buy_pressure_high": 20,

    # ── Whale Order Detection ─────────────────────────────────────────────────
    # Deteksi via volume spike besar pada 15m candle terbaru
    "whale_vol_mult":          5.0,      # 5x avg 15m volume = whale
    "score_whale_order":       8,

    # ── Fake Pump Filter ──────────────────────────────────────────────────────
    # Harga naik tapi buy_ratio < 50% → spoof / distribusi
    "fake_pump_price_min":     0.3,      # price naik minimal 0.3% dalam 3h
    "fake_pump_buy_max":       0.50,     # buy_ratio < 50%
    "penalty_fake_pump":       -10,

    # ── Logistic Probability Model ────────────────────────────────────────────
    # prob = 1 / (1 + exp(-(score - center) / scale))
    "prob_center":             50,       # score 50 → prob 50%
    "prob_scale":              8,        # steepness

    # ── Signal Threshold v18 ──────────────────────────────────────────────────
    "score_watchlist":         40,       # 40–55 → WATCHLIST
    "score_alert":             55,       # 55–70 → ALERT
    "score_strong_alert":      70,       # 70+   → STRONG ALERT

    # ── Entry Regime Detection ────────────────────────────────────────────────
    "breakout_buy_ratio_min":  0.60,     # buy_ratio > 60% untuk breakout mode
    "breakout_vol_ratio_min":  1.80,     # vol_ratio > 1.8x untuk breakout mode
    "mean_rev_rsi_max":        45.0,     # RSI < 45 untuk mean reversion mode
    "mean_rev_range_max":      0.20,     # range_pos < 20% untuk mean reversion
    "entry_breakout_atr_mult": 0.15,     # entry = resistance + 0.15×ATR
    "entry_mean_rev_atr_mult": 0.20,     # entry = support + 0.20×ATR
    "sl_atr_base":             2.5,      # SL = entry - ATR × 2.5
    "sl_atr_volatile":         3.0,      # SL untuk coin volatile
    "tp1_atr_mult":            1.5,      # v18: TP1 = entry + ATR × 1.5
    "tp2_atr_mult":            3.0,      # v18: TP2 = entry + ATR × 3
    "tp3_atr_mult":            5.0,      # v18: TP3 = entry + ATR × 5

    # ── v18 Entry model ───────────────────────────────────────────────────────
    "entry_pullback_atr_mult": 0.30,     # PULLBACK: entry = VWAP - 0.3×ATR
    "entry_sweep_atr_mult":    0.20,     # SWEEP: entry = sweep_low + 0.2×ATR
    "entry_retest_buffer":     0.005,    # BREAKOUT retest: +0.5% di atas breakout

    # ── v18 Micro Momentum (5m candles) ──────────────────────────────────────
    "micro_mom_candles":       12,       # berapa 5m candle untuk rata2 1h
    "micro_accel_strong":      0.003,    # 0.3% acceleration = strong
    "score_micro_accel":       15,       # strong micro accel score
    "score_micro_accel_pos":   8,        # positive micro accel score

    # ── v18 Whale detection upgrade ──────────────────────────────────────────
    "whale_vol_mult_v18":      3.0,      # vol > 3× (bukan 5×)
    "whale_buy_ratio_min":     0.65,     # buy_ratio > 65%
    "whale_oi_change_min":     2.0,      # OI harus naik minimal 2%
    "score_whale_v18":         10,       # skor whale yang lebih ketat tapi akurat

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
    "timing_eta_5min":         0.75,     # timing > 0.75 → ETA 5 min
    "timing_eta_10min":        0.55,     # timing > 0.55 → ETA 10 min
    "timing_eta_30min":        0.35,     # timing > 0.35 → ETA 30 min
    "timing_eta_60min":        0.20,     # timing > 0.20 → ETA 60 min

    # ── v18 Fake Pump upgrade ─────────────────────────────────────────────────
    "fake_price_spike_min":    0.005,    # harga naik > 0.5% dalam 3h
    "fake_oi_flat_max":        1.5,      # OI change < 1.5% = OI flat
    "fake_sell_press_max":     0.48,     # buy_ratio < 48% = sell pressure
    "fake_penalty_mild":      -5,        # 2 kondisi fake
    "fake_penalty_strong":    -12,       # 3 kondisi fake
    "fake_penalty_severe":    -20,       # 4 kondisi fake (semua terpenuhi)
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
            return False
        return True
    except Exception as e:
        log.warning(f"Telegram exception: {e}")
        return False

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


def get_alert_level_v18(score):
    """v18: threshold sama dengan v18."""
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


def calc_entry_v18(candles, vwap, price_now, atr_abs_val, market_regime, sr,
                   rsi, buy_ratio, vol_ratio, price_pos, alert_level, bos_level,
                   liq_sweep):
    """
    FIX v18 #4 & #5 — Entry Engine Diperbaiki + TP Diperlebar.

    Entry modes:
      PULLBACK  → entry = VWAP − 0.3×ATR  (tunggu pullback)
      SWEEP     → entry = liq_sweep_low + 0.2×ATR
      BREAKOUT  → entry = breakout_retest + 0.15×ATR
      NEUTRAL   → entry = support + 0.2×ATR

    TP v18 (diperlebar untuk altcoin pump 5−20%):
      TP1 = 1.5×ATR | TP2 = 3×ATR | TP3 = 5×ATR
      + TP3 cek liquidity void (area gap resistance)
    """
    atr = atr_abs_val
    atr_pct_now = (atr / price_now * 100) if price_now > 0 else 2.0

    # ── Entry berbasis regime ─────────────────────────────────────────────────
    if market_regime == "PULLBACK":
        entry        = vwap - atr * CONFIG["entry_pullback_atr_mult"]
        entry_reason = f"PULLBACK — VWAP {_fmt_price(vwap)} − 0.3×ATR"
        if entry >= price_now:          # jika entry > harga sekarang, pakai harga
            entry = price_now * 0.998

    elif market_regime == "SWEEP" and liq_sweep and liq_sweep.get("is_sweep"):
        sweep_low    = liq_sweep.get("sweep_low", price_now * 0.98)
        entry        = sweep_low + atr * CONFIG["entry_sweep_atr_mult"]
        entry_reason = f"SWEEP — Low {_fmt_price(sweep_low)} + 0.2×ATR"
        if entry > price_now:
            entry = price_now * 0.998

    elif market_regime == "BREAKOUT":
        res_levels = []
        if sr and sr.get("resistance"):
            res_levels = [rv["level"] for rv in sr["resistance"]
                          if rv["level"] > price_now * 0.998]
        if res_levels:
            breakout_lvl = min(res_levels)
            entry        = breakout_lvl * (1.0 + CONFIG["entry_retest_buffer"])
            entry_reason = f"BREAKOUT retest — R1 {_fmt_price(breakout_lvl)} + buffer"
        elif bos_level > 0 and bos_level < price_now * 1.05:
            entry        = bos_level * (1.0 + CONFIG["entry_bos_buffer"])
            entry_reason = "BREAKOUT — BOS retest"
        else:
            entry        = price_now * 1.001
            entry_reason = "BREAKOUT — market"

    else:  # NEUTRAL
        sup_levels = []
        if sr and sr.get("support"):
            sup_levels = [sv["level"] for sv in sr["support"]
                          if sv["level"] < price_now]
        if sup_levels:
            support      = max(sup_levels)
            entry        = support + atr * CONFIG["entry_mean_rev_atr_mult"]
            entry_reason = f"NEUTRAL — S1 {_fmt_price(support)} + 0.2×ATR"
        else:
            entry        = vwap - atr * 0.1   # slight below vwap
            entry_reason = "NEUTRAL — VWAP basis"
        if entry > price_now:
            entry = price_now * 0.999

    # ── SL ────────────────────────────────────────────────────────────────────
    sl_mult = CONFIG["sl_atr_volatile"] if atr_pct_now > 3.0 else CONFIG["sl_atr_base"]
    sl      = entry - atr * sl_mult
    sl      = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl      = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry:
        sl = entry * 0.975

    # ── TP v18: 1.5× / 3× / 5× ATR (diperlebar) ──────────────────────────────
    tp1 = entry + atr * CONFIG["tp1_atr_mult"]   # 1.5×
    tp2 = entry + atr * CONFIG["tp2_atr_mult"]   # 3.0×
    tp3 = entry + atr * CONFIG["tp3_atr_mult"]   # 5.0×

    # TP3 cek liquidity void: area tanpa resistance signifikan
    if sr and sr.get("resistance"):
        res_above = sorted([rv["level"] for rv in sr["resistance"] if rv["level"] > entry])
        # Jika ada gap besar antar resistance, pakai batas atas gap sebagai TP3
        if len(res_above) >= 2:
            gap = res_above[1] - res_above[0]
            if gap / res_above[0] > 0.05:   # gap > 5% = liquidity void
                tp3 = max(tp3, res_above[1])
        if res_above and res_above[0] > tp1:
            tp1 = max(tp1, res_above[0])
        if len(res_above) >= 2 and res_above[1] > tp2:
            tp2 = max(tp2, res_above[1])

    tp1 = max(tp1, entry * 1.008)
    tp2 = max(tp2, tp1   * 1.01)
    tp3 = max(tp3, tp2   * 1.02)

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

    # ── NEW v18: Phase 1-2-3 + Micro Momentum + Timing ──────────────────────
    vol_spike    = calc_volume_spike(c1h)
    micro_mom    = calc_micro_momentum(c5m)          # v18: 5m micro momentum
    mom_accel    = calc_momentum_acceleration(c1h)   # 1h secondary signal
    buy_press    = calc_buy_pressure(c15m)
    whale_order  = detect_whale_order(c15m, oi_data) # v18: upgraded whale
    fake_pump    = detect_fake_pump(c1h, buy_press["buy_ratio"], oi_data)  # v18: upgraded fake

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

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v18
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

    # ── 0c. NEW v18: Micro Momentum (Phase 3 — 5m responsif) ─────────────────
    if micro_mom["is_accelerating"]:
        score += micro_mom["score"]
        signals.append(micro_mom["label"])

    # ── 0d. 1h Momentum Acceleration (secondary signal) ──────────────────────
    if mom_accel["is_accelerating"] and not micro_mom["is_accelerating"]:
        # Hanya aktif jika micro_mom tidak aktif (hindari double count)
        score += mom_accel["score"]
        signals.append(mom_accel["label"])

    # ── 0e. Whale Order Detection (v18 upgraded) ──────────────────────────────
    if whale_order["is_whale"]:
        score += whale_order["score"]
        signals.append(whale_order["label"])

    # ── 0f. Fake Pump Penalty (v18 upgraded, bertingkat) ─────────────────────
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
    # FIX v18: isolasi scoring — vol_compression TIDAK dapat skor jika
    # is_accumulating sudah aktif (mencegah double counting sinyal ATR).
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
        # FIX v18: hanya aktif jika is_accumulating=False
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
    # Guard: skip jika energy_buildup aktif (OI sudah dihitung di sana).
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
            f"konfirmasi struktur berbalik (skor rendah: idealnya deteksi sebelum BOS)"
        )

    # ── 13. Funding rate ──────────────────────────────────────────────────────
    # FIX v18: mutual exclusion guard — funding_neg_pct dan funding_streak
    # hanya aktif jika funding_avg_neg TIDAK aktif, mencegah double scoring
    # dari sumber yang sama (funding negatif) hingga +8 poin.
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

    # FIX v18: neg_pct dan streak hanya aktif jika avg_neg TIDAK aktif
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
        # Jika avg_neg sudah aktif, tetap tampilkan info tapi tanpa skor tambahan
        if fstats["neg_pct"] >= 70 and fstats["sample_count"] >= 3:
            signals.append(
                f"Funding negatif {fstats['neg_pct']:.0f}% "
                f"(sudah dihitung dalam avg_neg — tidak ditambah lagi)"
            )
        if fstats["streak"] >= CONFIG["funding_streak_min"]:
            signals.append(
                f"Funding streak {fstats['streak']}x "
                f"(sudah dihitung dalam avg_neg — tidak ditambah lagi)"
            )

    # ── 14. BTC Outperformance ────────────────────────────────────────────────
    if btc_corr.get("outperform_label") == "OUTPERFORM":
        score += CONFIG["score_outperform"]
        signals.append(
            f"🚀 OUTPERFORM BTC — coin {btc_corr['coin_period_chg']:+.1f}% vs BTC "
            f"{btc_corr['btc_period_chg']:+.1f}% ({btc_corr['delta_vs_btc']:+.1f}%)"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  ALERT LEVEL v18 — feature-based probability + timing ETA
    # ══════════════════════════════════════════════════════════════════════════

    # v18 FIX #1: Probability berbasis feature (bukan raw score)
    pump_prob = calc_pump_probability_v18(
        vol_spike, buy_press, micro_mom, oi_data,
        bb_squeeze, atr_contr["is_contracting"], fake_pump
    )

    # v18 FIX #6: Pump Timing ETA
    pump_timing = calc_pump_timing_eta(
        vol_accel, buy_press["buy_ratio"], oi_data, micro_mom
    )

    # Alert level dari threshold (sama dengan v18)
    alert_level_v18 = get_alert_level_v18(score)

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

    # Map alert_level_v18 ke format lama (HIGH/MEDIUM) untuk kompatibilitas
    if alert_level_v18 == "STRONG ALERT":
        alert_level = "HIGH"
    elif alert_level_v18 in ("ALERT", "WATCHLIST"):
        alert_level = "MEDIUM"
    else:
        alert_level = "LOW"

    # ── Entry & Target v18 — Market Regime + Sweep + Wide TP ────────────────
    market_regime = calc_market_regime(
        c1h, vwap, buy_press["buy_ratio"], vol_ratio, rsi, price_pos_48
    )
    entry_data = calc_entry_v18(
        c1h, vwap, price_now, atr_abs_val, market_regime, sr,
        rsi, buy_press["buy_ratio"], vol_ratio, price_pos_48,
        alert_level_v18, bos_level, liq_sweep
    )

    # v18: gunakan threshold WATCHLIST (40) sebagai minimum
    min_score = CONFIG["score_watchlist"]
    if score >= min_score:
        return {
            "symbol":          symbol,
            "score":           score,
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
            "alert_level_v18": alert_level_v18,   # NEW v18
            "pump_prob":       pump_prob,          # NEW v18: logistic probability
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
            # v18 phase signals + timing
            "vol_spike":       vol_spike,
            "buy_press":       buy_press,
            "micro_mom":       micro_mom,      # NEW v18: 5m micro momentum
            "mom_accel":       mom_accel,
            "whale_order":     whale_order,
            "fake_pump":       fake_pump,
            "market_regime":   market_regime,
            "pump_timing":     pump_timing,    # NEW v18: ETA model
        }
    else:
        log.info(f"  {symbol}: Skor {score} < {min_score} (WATCHLIST threshold) — dilewati")
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
    """Pesan Telegram v18 — format diperkaya dengan Phase signals & logistic prob."""
    level_v18 = r.get("alert_level_v18", "ALERT")
    if level_v18 == "STRONG ALERT":
        level_icon = "🔥"
    elif level_v18 == "ALERT":
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

    p     = r["price"]
    entry = e["entry"]
    sl    = e["sl"]
    t1    = e["t1"]
    t2    = e["t2"]
    t3    = e.get("t3", t2)

    pump_prob    = r.get("pump_prob", 0)
    market_regime = e.get("market_regime", r.get("market_regime", "NEUTRAL"))

    # Header
    pt    = r.get("pump_timing", {})
    eta   = pt.get("eta", "?")
    eta_e = pt.get("eta_emoji", "")
    urg   = pt.get("urgency", "")

    msg  = f"{level_icon} <b>{r['symbol']} — {level_v18}</b>  #{rank}\n"
    msg += f"<b>Score :</b> {r['score']}  |  <b>Pump Prob:</b> {pump_prob}%\n"
    msg += f"<b>ETA   :</b> {eta_e} {eta} ({urg})\n"
    msg += f"<b>Type  :</b> {r['pump_type']}\n"
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

    # Phase Signals v18
    mm   = r.get("micro_mom", {})

    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>📊 Phase Signals v18:</b>\n"
    vol_str   = f"{vs.get('ratio', 0):.1f}x"   if vs else "—"
    buy_str   = f"{bp.get('buy_pct', 0):.0f}%" if bp else "—"
    # Micro momentum (5m)
    if mm and mm.get("is_accelerating"):
        m5   = mm.get("mom_5m", 0) * 100
        accel_val = mm.get("accel", 0) * 100
        micro_str = f"⚡{accel_val:+.3f}% (5m:{m5:+.3f}%)"
    else:
        micro_str = "—"
    # 1h momentum fallback
    accel_1h = (f"+{ma.get('acceleration',0)*100:.2f}%"
                if ma and ma.get("is_accelerating") else "—")
    # Whale confidence
    wh_conf   = wo.get("confidence","") if wo else ""
    whale_str = (f"✅ {wo.get('mult',0):.1f}x [{wh_conf}]"
                 if wo and wo.get("is_whale") else "—")
    # Fake severity
    fake_sev = fp.get("severity","") if fp and fp.get("is_fake") else ""
    fake_str = (f"⚠️ FAKE [{fake_sev}] ({fp.get('n_cond',0)}/4)"
                if fp and fp.get("is_fake") else "✅ Clean")
    # Timing
    timing_score = pt.get("timing_score", 0) if pt else 0
    msg += (
        f"  Vol Spike   : {vol_str}\n"
        f"  Buy Pressure: {buy_str}\n"
        f"  Micro Mom5m : {micro_str}\n"
        f"  Mom 1h      : {accel_1h}\n"
        f"  Whale       : {whale_str}\n"
        f"  Fake Filter : {fake_str}\n"
        f"  Timing Score: {timing_score:.2f}\n"
    )

    # Entry / SL / TP v18
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(entry)}</code>  [{e.get('sl_method','')}]\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(sl)}</code>  (-{e['sl_pct']:.2f}%)\n"
    rr3_str = e.get("rr3_str", e.get("rr2_str", "?"))
    msg += f"🎯 <b>TP1   :</b> <code>{_fmt_price(t1)}</code>  (+{e['gain_t1_pct']:.1f}%)  RR:{e['rr_str']}x\n"
    msg += f"🎯 <b>TP2   :</b> <code>{_fmt_price(t2)}</code>  (+{e['gain_t2_pct']:.1f}%)  RR:{e.get('rr2_str','?')}x\n"
    msg += f"🎯 <b>TP3   :</b> <code>{_fmt_price(t3)}</code>  (+{e.get('gain_t3_pct', 0):.1f}%)  RR:{rr3_str}x\n"
    msg += f"⚖️ <b>ATR   :</b> {e.get('atr_pct', r.get('atr_pct',0)):.2f}%  |  SL: -{e['sl_pct']:.2f}%\n"
    msg += f"📌 <i>{e.get('trail_note','')}</i>\n"

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
        "BOS", "Funding", "HTF", "OI", "ATR", "BB"
    ]
    for s in r["signals"]:
        if any(kw in s for kw in keywords):
            priority_signals.append(s)
        if len(priority_signals) >= 7:
            break
    for s in priority_signals:
        s_short = s[:88] + "…" if len(s) > 88 else s
        msg += f"• {s_short}\n"

    msg += f"\n<i>Scanner v18 | ⚠️ Bukan financial advice</i>"
    return msg

def build_summary(results):
    msg = f"\U0001f4cb <b>TOP CANDIDATES Scanner v18 \u2014 {utc_now()}</b>\n{chr(9473)*28}\n"
    for i, r in enumerate(results, 1):
        vol_str    = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                      else f"${r['vol_24h']/1e3:.0f}K")
        lv18       = r.get("alert_level_v18", "ALERT")
        prob       = r.get("pump_prob", 0)
        level_icon = "\U0001f525" if lv18 == "STRONG ALERT" else ("\U0001f4e1" if lv18 == "ALERT" else "\U0001f441")
        vs_ratio   = r.get("vol_spike", {}).get("ratio", 0)
        bp_pct     = r.get("buy_press", {}).get("buy_pct", 0)
        whale_tag  = " \U0001f433" if r.get("whale_order", {}).get("is_whale") else ""
        fake_tag   = " \u26a0\ufe0f"  if r.get("fake_pump",   {}).get("is_fake")  else ""
        accel_tag  = " \u26a1"   if r.get("mom_accel",   {}).get("is_accelerating") else ""
        regime     = r.get("market_regime", "NEUTRAL")
        msg += (
            f"{i}. {level_icon} <b>{r['symbol']}</b> "
            f"[{lv18} | Score:{r['score']} | Prob:{prob}%]\n"
        )
        pt_r   = r.get("pump_timing", {})
        eta_r  = pt_r.get("eta", "?")
        eta_er = pt_r.get("eta_emoji", "")
        msg += (
            f"   Vol:{vs_ratio:.1f}x | Buy:{bp_pct:.0f}%{whale_tag}{accel_tag}{fake_tag} | "
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

    log.info(f"\n📊 SCAN SUMMARY Scanner v18:")
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
    log.info(f"=== QUANTITATIVE PUMP DETECTION SCANNER v18 — {utc_now()} ===")

    load_funding_snapshots()
    log.info(f"Funding snapshots loaded: {len(_funding_snapshots)} coins di memori")

    # FIX v18: load OI snapshots dari disk
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
                    f"  ✅ Score={res['score']} | {res['alert_level']} | "
                    f"{res['pump_type']} | pos:{res['price_pos_48']:.0%} | "
                    f"T1:+{res['entry']['gain_t1_pct']}%"
                )
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    save_all_funding_snapshots()
    log.info("Funding snapshots disimpan ke disk.")

    # FIX v18: simpan OI snapshots ke disk
    save_oi_snapshots()
    log.info("OI snapshots disimpan ke disk.")

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
            log.info(
                f"✅ Alert #{rank}: {r['symbol']} Score={r['score']} "
                f"Level={r['alert_level']}"
            )
        time.sleep(2)

    log.info(f"=== SELESAI Scanner v18 — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  QUANTITATIVE PUMP DETECTION SCANNER v18                                     ║")
    log.info("║  Focus: OI Persistence Fix + Funding Guard + Higher Lookback║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
