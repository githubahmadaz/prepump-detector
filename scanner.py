"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v4.0 — DATA-DRIVEN VARIABLE UPDATE                        ║
║                                                                              ║
║  FILOSOFI CORE:                                                              ║
║  Deteksi SEBELUM breakout. Bukan setelah. Bukan saat.                        ║
║  Noise lebih tinggi diterima — miss lebih mahal dari false positive.         ║
║                                                                              ║
║  PERUBAHAN v4.0 — VARIABEL BARU DARI ANALISIS 84 COIN × 60 HARI:           ║
║  Variabel berikut terbukti diskriminatif dari data historis nyata:          ║
║                                                                              ║
║  [VAR-1] dist_from_low_96 (AUROC=0.696, CohenD=0.387) — #1 terkuat        ║
║    Pump median +77% lebih tinggi dari non-pump.                             ║
║    Ditambahkan ke score_structure sebagai bonus positional.                 ║
║                                                                              ║
║  [VAR-2] dist_from_low_32 (AUROC=0.672, CohenD=0.209)                      ║
║    Konsisten di 80%+ coin dengan pump events.                               ║
║    Dipakai sebagai filter di score_structure dan PBB.                       ║
║                                                                              ║
║  [VAR-3] range_pct_4 & range_pct_8 (AUROC=0.685/0.688)                     ║
║    Volatility 1-2 jam sebelum pump lebih tinggi dari baseline.              ║
║    Pump vs ranging: +17% (range_4) dan +19% (range_8).                     ║
║    Dipakai di PBB untuk detect volatility expansion early.                  ║
║                                                                              ║
║  [VAR-4] atr_pct_32 (AUROC=0.695) — ATR RELATIF terhadap harga            ║
║    Lebih akurat dari ATR absolut karena normalisasi per-coin.               ║
║    Dipakai di tension scoring menggantikan ATR absolut.                     ║
║                                                                              ║
║  [VAR-5] vol_upday_ratio_16 (AUROC=0.572)                                   ║
║    Volume di candle naik vs total volume dalam 16 candle.                   ║
║    Pump: 0.516 vs ranging: 0.456 (+13.3%).                                  ║
║    Dipakai di volume intelligence.                                           ║
║                                                                              ║
║  [VAR-6] bb_width expansion (AUROC=0.672)                                   ║
║    BB width pump +54% lebih tinggi dari ranging.                            ║
║    Dipakai di stealth & PBB sebagai volatility expansion signal.            ║
║                                                                              ║
║  [VAR-7] lower_wick_avg_16 (AUROC=0.565)                                    ║
║    Lower wick dominance: buyer mempertahankan support secara konsisten.     ║
║    Pump: 0.228 vs ranging: 0.208 (+9.6%).                                   ║
║    Dipakai di candle structure analyzer.                                    ║
║                                                                              ║
║  [VAR-8] pos_in_range_96 (AUROC=0.557)                                      ║
║    Posisi harga dalam range 24-jam: pump cenderung di 50-55% range.        ║
║    Dipakai sebagai positional bonus di structure.                           ║
║                                                                              ║
║  KOMPONEN SCORING:                                                           ║
║  [A] Compression + Tension      — HIGH (max 40)                             ║
║  [B] Volume Intelligence        — MEDIUM (max 25)                            ║
║  [C] Structure + Positional     — MEDIUM (max 22, +2 dari VAR-1/2/8)       ║
║  [D] Continuation               — MEDIUM (max 15)                            ║
║  [E] Stealth Accumulation       — BONUS (max 15)                             ║
║  [F] Pre-Breakout Bias          — BONUS (max 15, +3 dari VAR-3/6)           ║
║  [G] Distribution Penalty       — capped at −25                              ║
║  [H] Slow-trend Penalty         — capped at −10                              ║
║  [I] Late-entry Penalty         — capped at −15                              ║
║                                                                              ║
║  Confidence:                                                                 ║
║    < 42  → ignore                                                            ║
║    42–60 → early (sinyal awal, noise lebih tinggi)                          ║
║    60–78 → strong pre-pump candidate (PRIORITAS)                            ║
║    > 78  → possibly late stage                                               ║
║                                                                              ║
║  INTERVAL : Setiap 1 jam | EXCHANGE : Bitget USDT-Futures                  ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
import logging.handlers as _lh
from datetime import datetime, timezone
from collections import defaultdict

# ─── env ──────────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ─── logging ──────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch = logging.StreamHandler();  _ch.setFormatter(_fmt); _root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v3.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── VOLUME SAFETY ─────────────────────────────────────────────────────────
    # Hanya pre-filter kasar — bukan hard filter berbasis sinyal
    "pre_filter_vol":          100_000,   # $100K minimum (noise filter)
    "min_vol_24h":             500_000,   # $500K minimum untuk trading
    "max_vol_24h":         800_000_000,   # $800M ceiling (sudah terlalu ramai)
    "gate_chg_24h_max":          50.0,   # coin sudah naik >50% 24h = definitely late

    # ── CANDLE CONFIG ─────────────────────────────────────────────────────────
    "candle_limit_1h":           720,    # 30 hari data 1H

    # ── REFERENCE WINDOWS (konsisten di semua modul) ──────────────────────────
    "win_short":                  10,    # short-term reference
    "win_mid":                    20,    # mid-term reference
    "win_structure":              50,    # structure window
    "win_compression":           100,    # compression history window (percentile)

    # ── COMPRESSION DETECTION ─────────────────────────────────────────────────
    # v3.1: min_candles 30→20, range 0.12→0.15 — catch zones earlier & wider
    "compression_min_candles":    20,    # 20 jam minimum (dari 30) — deteksi lebih awal
    "compression_max_candles":   720,
    "compression_range_pct":    0.15,   # 15% range (dari 12%) — toleransi lebih lebar
    "compression_lookback":      720,

    # ── ZONE PURITY (anti false compression) ──────────────────────────────────
    # v3.1: spike_max 2→3, choppy_max 0.025→0.03 — lebih toleran
    "zone_purity_vol_mult":       3.0,
    "zone_purity_spike_max":        3,   # toleransi 3 spike (dari 2)
    "compression_choppy_max":    0.030,  # avg candle range < 3% (dari 2.5%)

    # ── ZONE AGE DECAY ────────────────────────────────────────────────────────
    # v3.1: decay diperlambat — zona lama tetap valid lebih lama
    # formula: quality = length * exp(-age / decay_tau)
    "zone_age_decay_tau":         120,   # dari 72 → 120 (zona bertahan lebih lama)

    # ── TENSION SCORING ───────────────────────────────────────────────────────
    # v3.1: rasio ATR lebih longgar + toleransi micro-breakout lebih besar
    "tension_volatility_ratio_max": 0.90,  # dari 0.75 — tension lebih mudah terpenuhi
    "tension_range_percentile_max": 50,    # dari 40 — range < median historis sudah cukup
    "tension_vol_acc_max_ratio":    4.0,   # dari 3.0 — toleransi volume naik lebih tinggi
    "tension_breakout_pct":         0.05,  # dari 0.03 — toleransi micro-breakout 5%
    "tension_breakout_lookback":      6,

    # ── DISTRIBUTION FILTER ────────────────────────────────────────────────────
    # v3.1: penalty di-halving — distribusi masih bisa lolos jika sinyal lain kuat
    "dist_upper_wick_ratio":       0.65,   # dari 0.60 — sedikit lebih toleran
    "dist_vol_spike_mult":         3.0,    # dari 2.5 — butuh spike lebih besar untuk penalty
    "dist_high_vol_no_prog_pct":   0.010,  # dari 0.015 — lebih ketat cek no-progress
    "dist_penalty_cap":             25,    # v3.2: raised to 25 (was 20). Track A alone can hit 25.

    # ── ANTI-LATE-ENTRY ────────────────────────────────────────────────────────
    # v3.1: penalty dikurangi 50% — lebih toleran terhadap sinyal yang sedikit terlambat
    "anti_late_move_pct":          0.10,   # dari 0.08 — harus naik >10% baru penalty
    "anti_late_lookback":          10,
    "anti_late_penalty_cap":        15,    # BARU: cap penalty late di 15 (dari 30)

    # ── CONTINUATION LOGIC ────────────────────────────────────────────────────
    "continuation_move_min":       0.08,   # dari 0.10 — deteksi leg 2 lebih awal
    "continuation_pullback_max":   0.618,  # dari 0.50 — toleransi pullback Fibonacci
    "continuation_lookback":       48,

    # ── VOLUME INTELLIGENCE WINDOWS ──────────────────────────────────────────
    "vol_avg_short":               10,
    "vol_avg_long":                20,
    "vol_acc_window":               5,

    # ── PRE-BREAKOUT BIAS MODULE (NEW v3.1) ───────────────────────────────────
    # Deteksi micro-accumulation patterns SEBELUM volume spike terjadi
    # Window: 5-8 candle terakhir (lebih pendek = lebih early)
    "pbb_window":                   6,    # 6 candle terakhir untuk analisa micro
    "pbb_squeeze_atr_ratio":       0.60,  # ATR6/ATR30 < 0.60 = squeeze kuat
    "pbb_vol_creep_ratio":         0.15,  # volume naik perlahan: avg_last3/avg_prev3 > 1.15
    "pbb_wick_reject_min":         0.40,  # lower wick > 40% range = buyer di bawah
    "pbb_close_top_half":          0.55,  # close di atas 55% range candle = bullish bias

    # ── SCORING THRESHOLDS (OUTPUT FILTER) ────────────────────────────────────
    # v3.1: turunkan floor dari 50 → 42 — lebih banyak early signals keluar
    "score_min_output":            42,    # dari 50 — lebih agresif
    "score_target_low":            60,    # dari 65 — early zona mulai dari 60
    "score_target_high":           78,    # dari 80

    # ── STEALTH ACCUMULATION ──────────────────────────────────────────────────
    # v3.1: threshold 70→60 — stealth terdeteksi lebih mudah
    "stealth_window_candles":      30,
    "stealth_range_pct_max":     0.05,   # dari 0.04 — lebih longgar
    "stealth_atr_period":          14,
    "stealth_atr_ma_period":       20,
    "stealth_std_threshold":    0.013,   # dari 0.010 — lebih toleran
    "stealth_ema_period":          20,
    "stealth_ma_dist_max":       0.008,  # dari 0.005 — lebih longgar
    "stealth_slope_window":         5,
    "stealth_slope_max":         0.005,  # dari 0.003 — lebih longgar
    "stealth_wick_ratio_max":      3.0,  # dari 2.5
    "stealth_vol_spike_reject":    3.5,  # dari 3.0
    "stealth_upper_wick_max":     0.65,  # dari 0.60
    "stealth_vol_cv_max":         0.70,  # dari 0.60
    "stealth_breakdown_tolerance":0.007, # dari 0.005
    "stealth_score_threshold":     60,   # dari 70 — KUNCI: stealth lebih mudah terdeteksi

    # ── DATA-DRIVEN VARIABLES v4.0 (dari analisis 84 coin × 60 hari) ────────────
    # VAR-1: dist_from_low_96 — AUROC=0.696, pump median +77% vs non-pump
    # Threshold dari data: pump median=0.0529, non-pump median=0.0237
    "dfl96_pump_threshold":      0.035,   # (pump_med+nonpump_med)/2 → sinyal valid
    "dfl96_strong_threshold":    0.060,   # >= ini → bonus kuat
    "dfl96_bonus_strong":           8,    # bonus jika >= strong_threshold
    "dfl96_bonus_mild":             4,    # bonus jika >= pump_threshold
    # VAR-2: dist_from_low_32 — AUROC=0.672, pump +77% vs non-pump
    "dfl32_pump_threshold":      0.018,   # pump median=0.0248, non-pump=0.0139
    "dfl32_strong_threshold":    0.040,
    "dfl32_bonus_strong":           5,
    "dfl32_bonus_mild":             3,
    # VAR-3: range_pct_4 & range_pct_8 — volatility expansion sebelum pump
    # Pump: range_4=0.0175 vs ranging=0.0107 (+64%), range_8=0.0256 vs 0.0156 (+64%)
    "range4_expansion_threshold":  1.30,  # range_4 harus > 1.3x baseline (rolling 32c)
    "range8_expansion_threshold":  1.25,
    "range_expansion_bonus":         4,   # bonus di PBB jika terpenuhi
    # VAR-4: atr_pct_32 — ATR relatif, AUROC=0.695
    # Pump: atr_pct=0.0076 (0.76% dari harga) vs non-pump: 0.0052
    "atr_pct_pump_low":          0.006,   # ATR/price >= ini = volatility cukup
    "atr_pct_pump_high":         0.020,   # ATR/price >= ini = sudah terlalu volatile
    # VAR-5: vol_upday_ratio_16 — volume di candle naik, AUROC=0.572
    # Pump: 0.516 vs ranging: 0.456 (threshold: 0.50)
    "vol_upday_ratio_threshold": 0.500,   # >= 50% volume ada di candle bullish
    "vol_upday_bonus":               4,   # bonus di volume intelligence
    # VAR-6: bb_width expansion — AUROC=0.672
    # Pump: bb_width=0.0354 vs ranging=0.0229 (+54%)
    # BB width saat ini vs rolling mean 32c — expansion = pre-pump setup
    "bb_width_exp_threshold":      1.30,  # bb_width > 1.3x rolling_32c mean
    "bb_width_exp_bonus":            3,   # bonus di stealth & PBB
    # VAR-7: lower_wick_avg_16 — buyer defense at support, AUROC=0.565
    # Pump: 0.228 vs ranging: 0.208 (threshold: 0.22)
    "lower_wick_avg_threshold":   0.220,  # avg lower wick ratio >= ini
    "lower_wick_avg_bonus":           4,  # bonus di candle structure
    # VAR-8: pos_in_range_96 — posisi dalam 24-jam range, AUROC=0.557
    # Pump median: 0.51 (tengah range), non-pump: 0.42
    "pos_in_range96_low":         0.35,   # di bawah ini = terlalu di bawah
    "pos_in_range96_high":        0.75,   # di atas ini = overbought dalam range
    "pos_in_range96_bonus":           3,  # bonus jika dalam zone optimal (0.35-0.75)
    "min_target_pct":              7.0,  # dari 8.0 — target minimum lebih rendah
    "min_sl_pct":                  2.0,  # dari 2.5 — SL minimum lebih longgar
    "min_rr":                      1.1,  # dari 1.3 — R/R minimum lebih rendah

    # ── FUNDING ───────────────────────────────────────────────────────────────
    "funding_gate":             -0.005,  # dari -0.003 — lebih toleran terhadap funding negatif

    # ── LIQUIDITY SWEEP BONUS ──────────────────────────────────────────────────
    "liq_sweep_lookback":          16,   # dari 12 — window lebih panjang
    "liq_sweep_recover_bars":       6,   # dari 4 — recovery lebih longgar

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    "max_alerts_per_run":          10,   # dari 8 — lebih banyak coin dikirim
    "alert_cooldown_sec":        3600,
    "sleep_coins":                 0.7,
    "sleep_error":                 3.0,
    "cooldown_file":   "/tmp/v3_cooldown.json",
}

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
      "4USDT", "0GUSDT", "1000BONKUSDT", "1000PEPEUSDT", "1000RATSUSDT",
    "1000SHIBUSDT", "1000XECUSDT", "1INCHUSDT", "1MBABYDOGEUSDT", "2ZUSDT",
    "AAVEUSDT", "ACEUSDT", "ACHUSDT", "ACTUSDT", "ADAUSDT", "AEROUSDT",
    "AGLDUSDT", "AINUSDT", "AIOUSDT", "AIXBTUSDT", "AKTUSDT", "ALCHUSDT",
    "ALGOUSDT", "ALICEUSDT", "ALLOUSDT", "ALTUSDT", "ANIMEUSDT",
    "ANKRUSDT", "APEUSDT", "APEXUSDT", "API3USDT", "APRUSDT", "APTUSDT",
    "ARUSDT", "ARBUSDT", "ARCUSDT", "ARIAUSDT", "ARKUSDT", "ARKMUSDT",
    "ARPAUSDT", "ASTERUSDT", "ATUSDT", "ATHUSDT", "ATOMUSDT", "AUCTIONUSDT",
    "AVAXUSDT", "AVNTUSDT", "AWEUSDT", "AXLUSDT", "AXSUSDT", "AZTECUSDT",
    "BUSDT", "B2USDT", "BABYUSDT", "BANUSDT", "BANANAUSDT",
    "BANANAS31USDT", "BANKUSDT", "BARDUSDT", "BATUSDT", "BCHUSDT", "BEATUSDT",
    "BERAUSDT", "BGBUSDT", "BIGTIMEUSDT", "BIOUSDT", "BIRBUSDT", "BLASTUSDT",
    "BLESSUSDT", "BLURUSDT", "BNBUSDT", "BOMEUSDT", "BRETTUSDT", "BREVUSDT",
    "BROCCOLIUSDT", "BSVUSDT", "BTCUSDT", "BULLAUSDT", "C98USDT", "CAKEUSDT",
    "CCUSDT", "CELOUSDT", "CFXUSDT", "CHILLGUYUSDT", "CHZUSDT", "CLUSDT",
    "CLANKERUSDT", "CLOUSDT", "COAIUSDT", "COMPUSDT", "COOKIEUSDT",
    "COWUSDT", "CRCLUSDT", "CROUSDT", "CROSSUSDT", "CRVUSDT", "CTKUSDT",
    "CVCUSDT", "CVXUSDT", "CYBERUSDT", "CYSUSDT", "DASHUSDT", "DEEPUSDT",
    "DENTUSDT", "DEXEUSDT", "DOGEUSDT", "DOLOUSDT", "DOODUSDT", "DOTUSDT",
    "DRIFTUSDT", "DYDXUSDT", "DYMUSDT", "EGLDUSDT", "EIGENUSDT", "ENAUSDT",
    "ENJUSDT", "ENSUSDT", "ENSOUSDT", "EPICUSDT", "ESPUSDT", "ETCUSDT",
    "ETHUSDT", "ETHFIUSDT", "FUSDT", "FARTCOINUSDT", "FETUSDT",
    "FFUSDT", "FIDAUSDT", "FILUSDT", "FLOKIUSDT", "FLUIDUSDT", "FOGOUSDT",
    "FOLKSUSDT", "FORMUSDT", "GALAUSDT", "GASUSDT", "GIGGLEUSDT",
    "GLMUSDT", "GMTUSDT", "GMXUSDT", "GOATUSDT", "GPSUSDT", "GRASSUSDT", "GUSDT",
    "GRIFFAINUSDT", "GRTUSDT", "GUNUSDT", "GWEIUSDT", "HUSDT", "HBARUSDT",
    "HEIUSDT", "HEMIUSDT", "HMSTRUSDT", "HOLOUSDT", "HOMEUSDT", "HYPEUSDT", "HYPERUSDT",
    "ICNTUSDT", "ICPUSDT", "IDOLUSDT", "ILVUSDT",
    "IMXUSDT", "INITUSDT", "INJUSDT", "INXUSDT", "IOUSDT",
    "IOTAUSDT", "IOTXUSDT", "IPUSDT", "JASMYUSDT", "JCTUSDT", "JSTUSDT",
    "JTOUSDT", "JUPUSDT", "KAIAUSDT", "KAITOUSDT", "KASUSDT", "KAVAUSDT",
    "kBONKUSDT", "KERNELUSDT", "KGENUSDT", "KITEUSDT", "kPEPEUSDT", "kSHIBUSDT",
    "LAUSDT", "LABUSDT", "LAYERUSDT", "LDOUSDT", "LIGHTUSDT", "LINEAUSDT",
    "LINKUSDT", "LITUSDT", "LPTUSDT", "LSKUSDT", "LTCUSDT", "LUNAUSDT",
    "LUNCUSDT", "LYNUSDT", "MUSDT", "MAGICUSDT", "MAGMAUSDT", "MANAUSDT",
    "MANTAUSDT", "MANTRAUSDT", "MASKUSDT", "MAVUSDT", "MAVIAUSDT", "MBOXUSDT",
    "MEUSDT", "MEGAUSDT", "MELANIAUSDT", "MEMEUSDT", "MERLUSDT", "METUSDT",
    "METAUSDT", "MEWUSDT", "MINAUSDT", "MMTUSDT", "MNTUSDT", "MONUSDT",
    "MOODENGUSDT", "MORPHOUSDT", "MOVEUSDT", "MOVRUSDT", "MUUSDT", "MUBARAKUSDT",
    "MYXUSDT", "NAORISUSDT", "NEARUSDT", "NEIROCTOUSDT",
    "NEOUSDT", "NEWTUSDT", "NILUSDT", "NMRUSDT", "NOMUSDT", "NOTUSDT",
    "NXPCUSDT", "ONDOUSDT", "ONGUSDT", "ONTUSDT", "OPUSDT", "OPENUSDT",
    "OPNUSDT", "ORCAUSDT", "ORDIUSDT", "OXTUSDT", "PARTIUSDT",
    "PENDLEUSDT", "PENGUUSDT", "PEOPLEUSDT", "PEPEUSDT", "PHAUSDT", "PIEVERSEUSDT",
    "PIPPINUSDT", "PLUMEUSDT", "PNUTUSDT", "POLUSDT", "POLYXUSDT",
    "POPCATUSDT", "POWERUSDT", "PROMPTUSDT", "PROVEUSDT", "PUMPUSDT", "PURRUSDT",
    "PYTHUSDT", "QUSDT", "QNTUSDT", "RAVEUSDT", "RAYUSDT",
    "RECALLUSDT", "RENDERUSDT", "RESOLVUSDT", "REZUSDT", "RIVERUSDT", "ROBOUSDT",
    "ROSEUSDT", "RPLUSDT", "RSRUSDT", "RUNEUSDT", "SUSDT", "SAGAUSDT", "SAHARAUSDT",
    "SANDUSDT", "SAPIENUSDT", "SEIUSDT", "SENTUSDT", "SHIBUSDT", "SIGNUSDT",
    "SIRENUSDT", "SKHYNIXUSDT", "SKRUSDT", "SKYUSDT", "SKYAIUSDT", "SLPUSDT",
    "SNXUSDT", "SOLUSDT", "SOMIUSDT", "SONICUSDT", "SOONUSDT", "SOPHUSDT",
    "SPACEUSDT", "SPKUSDT", "SPXUSDT", "SQDUSDT", "SSVUSDT",
    "STBLUSDT", "STEEMUSDT", "STOUSDT", "STRKUSDT", "STXUSDT",
    "SUIUSDT", "SUNUSDT", "SUPERUSDT", "SUSHIUSDT", "SYRUPUSDT", "TUSDT",
    "TACUSDT", "TAGUSDT", "TAIKOUSDT", "TAOUSDT", "THEUSDT", "THETAUSDT",
    "TIAUSDT", "TNSRUSDT", "TONUSDT", "TOSHIUSDT", "TOWNSUSDT", "TRBUSDT",
    "TRIAUSDT", "TRUMPUSDT", "TRXUSDT", "TURBOUSDT", "UAIUSDT", "UBUSDT",
    "UMAUSDT", "UNIUSDT", "USUSDT", "USDKRWUSDT", "USELESSUSDT",
    "USUALUSDT", "VANAUSDT", "VANRYUSDT", "VETUSDT", "VINEUSDT", "VIRTUALUSDT",
    "VTHOUSDT", "VVVUSDT", "WUSDT", "WALUSDT", "WAXPUSDT", "WCTUSDT", "WETUSDT",
    "WIFUSDT", "WLDUSDT", "WLFIUSDT", "WOOUSDT", "WTIUSDT", "XAIUSDT",
    "XCUUSDT", "XDCUSDT", "XLMUSDT", "XMRUSDT", "XPDUSDT", "XPINUSDT",
    "XPLUSDT", "XRPUSDT", "XTZUSDT", "XVGUSDT", "YGGUSDT", "YZYUSDT", "ZAMAUSDT",
    "ZBTUSDT", "ZECUSDT", "ZENUSDT", "ZEREBROUSDT", "ZETAUSDT", "ZILUSDT",
    "ZKUSDT", "ZKCUSDT", "ZKJUSDT", "ZKPUSDT", "ZORAUSDT", "ZROUSDT",
}

MANUAL_EXCLUDE = set()

SECTOR_MAP = {
    "DEFI":      ["SNXUSDT","CRVUSDT","CVXUSDT","COMPUSDT","AAVEUSDT","UNIUSDT","DYDXUSDT",
                  "COWUSDT","PENDLEUSDT","MORPHOUSDT","FLUIDUSDT","SSVUSDT","LDOUSDT","ENSUSDT"],
    "AI_CRYPTO": ["FETUSDT","RENDERUSDT","TAOUSDT","GRASSUSDT","AKTUSDT","VANAUSDT",
                  "COAIUSDT","UAIUSDT","GRTUSDT"],
    "SOLANA_ECO":["ORCAUSDT","RAYUSDT","JTOUSDT","DRIFTUSDT","WIFUSDT","JUPUSDT",
                  "1000BONKUSDT","PYTHUSDT"],
    "LAYER1":    ["APTUSDT","SUIUSDT","SEIUSDT","INJUSDT","KASUSDT","BERAUSDT","MOVEUSDT",
                  "KAIAUSDT","TIAUSDT","EGLDUSDT","NEARUSDT","TONUSDT","ALGOUSDT","HBARUSDT"],
    "LAYER2":    ["ARBUSDT","OPUSDT","CELOUSDT","STRKUSDT","POLUSDT","LINEAUSDT"],
    "GAMING":    ["AXSUSDT","GALAUSDT","IMXUSDT","SANDUSDT","APEUSDT","SUPERUSDT","CHZUSDT","ENJUSDT"],
    "MEME":      ["PEPEUSDT","SHIBUSDT","FLOKIUSDT","BRETTUSDT","FARTCOINUSDT","MEMEUSDT",
                  "TURBOUSDT","PNUTUSDT","POPCATUSDT","MOODENGUSDT","1000BONKUSDT","TRUMPUSDT","WIFUSDT"],
}
SECTOR_LOOKUP = {coin: sec for sec, coins in SECTOR_MAP.items() for coin in coins}

BITGET_BASE = "https://api.bitget.com"
GRAN_MAP    = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
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
    except:
        pass
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except:
        pass

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):  return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]
def set_cooldown(sym): _cooldown[sym] = time.time(); save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("Rate limit — tunggu 20s")
                time.sleep(20)
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=15,
        )
        return r.status_code == 200
    except:
        return False

def utc_now(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════
def get_all_tickers():
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/tickers",
                    params={"productType": "usdt-futures"})
    if data and data.get("code") == "00000":
        return {t["symbol"]: t for t in data.get("data", [])}
    return {}

def get_candles(symbol, gran="1h", limit=504):
    g   = GRAN_MAP.get(gran, "1H")
    key = f"c_{symbol}_{g}_{limit}"
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < 90:
            return val
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        params={"symbol": symbol, "granularity": g,
                "limit": str(limit), "productType": "usdt-futures"},
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({
                "ts":         int(c[0]),
                "open":     float(c[1]),
                "high":     float(c[2]),
                "low":      float(c[3]),
                "close":    float(c[4]),
                "volume":   float(c[5]),
                "volume_usd": vol_usd,
            })
        except:
            continue
    candles.sort(key=lambda x: x["ts"])
    _cache[key] = (time.time(), candles)
    return candles

def get_funding(symbol):
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
                    params={"symbol": symbol, "productType": "usdt-futures"})
    if data and data.get("code") == "00000":
        try:
            return float(data["data"][0].get("fundingRate", 0))
        except:
            pass
    return 0.0

# ══════════════════════════════════════════════════════════════════════════════
#  📐  MATH HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _mean(arr):
    return sum(arr) / len(arr) if arr else 0.0

def _std(arr):
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / len(arr))

def _percentile(arr, p):
    """Return p-th percentile (0-100) of arr."""
    if not arr:
        return 0.0
    s = sorted(arr)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[-1]
    return s[f] + (k - f) * (s[c] - s[f])

def _median(arr):
    if not arr:
        return 0.0
    s = sorted(arr)
    n = len(s)
    return (s[n//2] + s[~(n//2)]) / 2

def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr

def get_rsi(candles, period=14):
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period-1) + gains[i]) / period
        avg_l = (avg_l * (period-1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))

def calc_poc(candles):
    if not candles:
        return None
    pmin = min(c["low"]  for c in candles)
    pmax = max(c["high"] for c in candles)
    if pmax == pmin:
        return candles[-1]["close"]
    bsize   = (pmax - pmin) / 40
    vol_bkt = defaultdict(float)
    for c in candles:
        lo = int((c["low"]  - pmin) / bsize)
        hi = int((c["high"] - pmin) / bsize)
        nb = max(hi - lo + 1, 1)
        for b in range(lo, hi + 1):
            vol_bkt[b] += c["volume_usd"] / nb
    poc_b = max(vol_bkt, key=vol_bkt.get) if vol_bkt else 20
    return pmin + (poc_b + 0.5) * bsize

def calc_ema(values, period):
    """Calculate EMA of a list of values."""
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  COMPRESSION ZONE DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def find_compression_zone(candles):
    """
    Cari zona compression terbaik dari candle history.
    Return zona atau None jika tidak ada yang cukup valid.
    Lebih toleran dari v2.8 — boleh range sedikit lebih lebar,
    karena hard filter sudah digantikan scoring.
    """
    cfg        = CONFIG
    min_len    = cfg["compression_min_candles"]
    max_len    = cfg["compression_max_candles"]
    range_pct  = cfg["compression_range_pct"]
    lookback   = min(cfg["compression_lookback"], len(candles))
    scan_slice = candles[-lookback:]
    n          = len(scan_slice)

    best = None

    for end in range(n - 1, min_len - 2, -1):
        zone_high = scan_slice[end]["high"]
        zone_low  = scan_slice[end]["low"]
        start     = end

        for start in range(end - 1, max(end - max_len, -1), -1):
            c = scan_slice[start]
            new_high = max(zone_high, c["high"])
            new_low  = min(zone_low,  c["low"])
            rng      = (new_high - new_low) / new_low if new_low > 0 else 999

            if rng > range_pct:
                start += 1
                break
            zone_high = new_high
            zone_low  = new_low

        length = end - start + 1
        if length < min_len:
            continue

        zone_candles = scan_slice[start:end+1]
        vols_zone    = sorted(c["volume_usd"] for c in zone_candles)
        mid          = length // 2
        median_vol   = (vols_zone[mid] + vols_zone[~mid]) / 2 if length > 1 else vols_zone[0]
        avg_vol      = sum(vols_zone) / length

        choppy_max = cfg.get("compression_choppy_max", 0.025)
        avg_candle_range = sum(
            (c["high"] - c["low"]) / c["low"]
            for c in zone_candles if c["low"] > 0
        ) / length
        if avg_candle_range > choppy_max:
            continue

        purity_mult = cfg.get("zone_purity_vol_mult", 3.0)
        purity_max  = cfg.get("zone_purity_spike_max", 2)
        spike_count = sum(
            1 for c in zone_candles
            if median_vol > 0 and c["volume_usd"] > purity_mult * median_vol
        )
        if spike_count > purity_max:
            continue

        age     = (n - 1) - end
        tau     = CONFIG.get("zone_age_decay_tau", 120)   # v3.1: slower decay
        quality = length * math.exp(-age / tau)

        if best is None or quality > best["quality"]:
            best = {
                "start_idx":        start,
                "end_idx":          end,
                "low":              zone_low,
                "high":             zone_high,
                "length":           length,
                "avg_vol":          avg_vol,
                "median_vol":       median_vol,
                "age_candles":      age,
                "quality":          quality,
                "range_pct":        (zone_high - zone_low) / zone_low,
                "avg_candle_range": avg_candle_range,
                "spike_count":      spike_count,
            }

        end = start

    return best

# ══════════════════════════════════════════════════════════════════════════════
#  💧  LIQUIDITY SWEEP DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def detect_liquidity_sweep(candles, support_low):
    lookback    = CONFIG["liq_sweep_lookback"]
    recover_bars = CONFIG["liq_sweep_recover_bars"]
    recent      = candles[-lookback:]

    for i in range(len(recent) - 1):
        c = recent[i]
        if c["low"] < support_low * 0.99:
            for j in range(i + 1, min(i + recover_bars + 1, len(recent))):
                if recent[j]["close"] > support_low:
                    return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
#  📊  CANDLE STRUCTURE ANALYZER
# ══════════════════════════════════════════════════════════════════════════════
def analyze_candle_structure(candle, recent_candles=None):
    """
    v4.0: tambah lower_wick_avg_16 dari data historis (VAR-7, AUROC=0.565).
    Pump: lower_wick_avg=0.228 vs ranging=0.208 (threshold: 0.22).
    """
    body   = abs(candle["close"] - candle["open"])
    rng    = candle["high"] - candle["low"]
    if rng == 0:
        return 0, "doji"

    lower_wick = min(candle["open"], candle["close"]) - candle["low"]
    body_pct   = body / rng
    lwick_pct  = lower_wick / rng

    if lwick_pct > 0.50 and body_pct < 0.35:
        base_score, label = 15, "bullish rejection wick"
    elif lwick_pct > 0.40:
        base_score, label = 12, "hammer/pin bar"
    elif body_pct < 0.15:
        base_score, label = 8, "doji (indecision)"
    elif candle["close"] > candle["open"]:
        base_score, label = 5, "green candle"
    else:
        base_score, label = 2, "red candle"

    # VAR-7: lower_wick_avg_16 — buyer defense di support (AUROC=0.565)
    # Pump: avg=0.228 vs ranging=0.208; threshold=0.220
    if recent_candles and len(recent_candles) >= 8:
        lwick_vals = []
        for c in recent_candles[-16:]:
            r_ = c["high"] - c["low"]
            if r_ > 0:
                lw = (min(c["open"], c["close"]) - c["low"]) / r_
                lwick_vals.append(lw)
        if lwick_vals:
            avg_lwick = sum(lwick_vals) / len(lwick_vals)
            lwick_thr = CONFIG.get("lower_wick_avg_threshold", 0.220)
            lwick_bon = CONFIG.get("lower_wick_avg_bonus", 4)
            if avg_lwick >= lwick_thr * 1.2:
                base_score += lwick_bon
                label += f"+lwick_avg({avg_lwick:.3f})"
            elif avg_lwick >= lwick_thr:
                base_score += lwick_bon // 2
                label += f"+lwick_mild({avg_lwick:.3f})"

    return base_score, label

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(candles, compression_zone):
    cur  = candles[-1]["close"]
    atr  = calc_atr(candles[-48:], 14) or cur * 0.025

    comp_mid = (compression_zone["high"] + compression_zone["low"]) / 2
    entry    = min(cur * 0.999, compression_zone["high"] * 1.005)

    sl = compression_zone["low"] - atr * CONFIG["atr_sl_mult"]
    sl = max(sl, entry * 0.85)

    min_sl_dist = entry * (CONFIG["min_sl_pct"] / 100)
    if (entry - sl) < min_sl_dist:
        sl = entry - min_sl_dist

    sl_pct = round((entry - sl) / entry * 100, 1)

    recent     = candles[-240:]
    res_levels = []
    min_target = cur * (1 + CONFIG["min_target_pct"] / 100)

    for i in range(3, len(recent) - 3):
        h = recent[i]["high"]
        if h <= min_target:
            continue
        touches = sum(
            1 for c in recent
            if abs(c["high"] - h) / h < 0.02 or abs(c["low"] - h) / h < 0.02
        )
        if touches >= 2:
            res_levels.append(h)

    if res_levels:
        res_levels.sort()
        t1 = res_levels[0]
        t2 = res_levels[1] if len(res_levels) > 1 else t1 * 1.15
    else:
        comp_len = compression_zone["length"]
        atr_mult = min(4.0 + comp_len / 48, 10.0)
        t1_atr   = entry + atr * atr_mult
        t1_min   = cur * 1.10
        t1       = max(t1_atr, t1_min)
        t2       = max(t1 * 1.20, cur * 1.22)

    if abs(t2 - t1) / t1 < 0.03:
        t2 = t1 * 1.15

    t1_pct = round((t1 - cur) / cur * 100, 1)
    t2_pct = round((t2 - cur) / cur * 100, 1)
    rr     = round((t1 - entry) / (entry - sl), 1) if (entry - sl) > 0 else 0

    poc    = calc_poc(candles[-168:])

    return {
        "cur":    cur,
        "entry":  round(entry, 8),
        "sl":     round(sl, 8),
        "sl_pct": sl_pct,
        "t1":     round(t1, 8),
        "t2":     round(t2, 8),
        "t1_pct": t1_pct,
        "t2_pct": t2_pct,
        "rr":     rr,
        "atr":    round(atr, 8),
        "z2":     round(poc, 8) if poc else None,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🔬  FORENSIC PATTERN DETECTORS
# ══════════════════════════════════════════════════════════════════════════════
def detect_higher_lows(candles, lookback=4):
    if len(candles) < lookback:
        return False
    recent = candles[-lookback:]
    lows   = [c["low"] for c in recent]
    for i in range(1, len(lows)):
        if lows[i] < lows[i-1] * 0.985:
            return False
    return True

def detect_price_acceleration(candles, lookback=6):
    if len(candles) < lookback:
        return False
    window = candles[-lookback:]
    half   = len(window) // 2
    if half < 1:
        return False
    late_start = window[half]["close"]
    late_end   = window[-1]["close"]
    if late_start <= 0:
        return False
    return (late_end - late_start) / late_start * 100 > 0.3

def detect_pre_pump_candle(candles):
    if len(candles) < 2:
        return 0, "insufficient data"
    c   = candles[-2]
    rng = c["high"] - c["low"]
    if rng == 0:
        return 0, "doji pre-spike"
    body     = c["close"] - c["open"]
    body_pct = abs(body) / rng
    if body > 0 and body_pct >= 0.30:
        return 5, f"pre-spike bull body {body_pct*100:.0f}%"
    elif c["close"] > (c["high"] + c["low"]) / 2:
        return 3, "pre-spike close above midpoint"
    else:
        return 0, "pre-spike bearish"

def detect_failed_breakdown(candles, comp_low, lookback=20):
    """
    Cek apakah ada failed breakdown (break support lalu recover).
    Lebih longgar dari liq_sweep: cek dalam 20 candle terakhir.
    """
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    for i in range(len(recent) - 2):
        c = recent[i]
        if c["low"] < comp_low * 0.98:
            for j in range(i + 1, min(i + 6, len(recent))):
                if recent[j]["close"] > comp_low:
                    return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
#  🟦  STEALTH ACCUMULATION DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def detect_stealth_accumulation(candles):
    cfg = CONFIG
    N   = cfg.get("stealth_window_candles", 30)

    _null = {
        "detected": False, "score": 0.0, "status": "NONE",
        "range_pct": 0.0, "atr_trend": "unknown",
        "ma_distance": 0.0, "volume_stability": 1.0,
        "structure_quality": "insufficient data", "entry_trigger": False,
        "_score_vol_suppress": 0, "_score_price_compress": 0,
        "_score_ma_hug": 0, "_score_low_noise": 0,
        "_score_no_dist": 0, "_score_vol_stable": 0,
        "_structure_penalty": 0, "_distribution_flags": 0,
    }

    atr_ma_p    = cfg.get("stealth_atr_ma_period", 20)
    atr_period  = cfg.get("stealth_atr_period", 14)
    min_candles = N + atr_ma_p + atr_period + 5

    if not candles or len(candles) < min_candles:
        return _null

    confirmed = candles[:-1]
    current   = candles[-1]
    window    = confirmed[-N:]

    if len(window) < N:
        return _null

    highs  = [c["high"]  for c in window]
    lows   = [c["low"]   for c in window]
    closes = [c["close"] for c in window]
    opens  = [c["open"]  for c in window]
    vols   = [c.get("volume_usd", c.get("volume", 0)) for c in window]
    n_win  = len(window)

    avg_price      = _mean(closes)
    avg_vol_window = _mean(vols)

    # [1] Volatility suppression (+25)
    max_h     = max(highs)
    min_l     = min(lows)
    range_pct = (max_h - min_l) / min_l if min_l > 0 else 1.0

    score_vol_suppress = 0.0
    rng_limit = cfg.get("stealth_range_pct_max", 0.04)
    if range_pct < rng_limit:
        score_vol_suppress = 25.0
    elif range_pct < rng_limit * 1.5:
        score_vol_suppress = 15.0
    elif range_pct < rng_limit * 2.0:
        score_vol_suppress = 7.0

    # ATR trend
    atr_p     = cfg.get("stealth_atr_period", 14)
    atr_ma_p_ = cfg.get("stealth_atr_ma_period", 20)
    all_cl    = [c["close"] for c in confirmed]
    all_hi    = [c["high"]  for c in confirmed]
    all_lo    = [c["low"]   for c in confirmed]

    atr_values = []
    if len(confirmed) > atr_p + atr_ma_p_ + 5:
        for i in range(atr_p, len(confirmed)):
            chunk = confirmed[max(0, i - atr_p): i + 1]
            v = calc_atr(chunk, min(atr_p, len(chunk) - 1))
            if v is not None:
                atr_values.append(v)

    atr_trend_label = "unknown"
    if len(atr_values) >= atr_ma_p_:
        recent_atr = _mean(atr_values[-5:])
        older_atr  = _mean(atr_values[-atr_ma_p_: -5]) if atr_ma_p_ > 10 else _mean(atr_values[:5])
        if older_atr > 0:
            ratio = recent_atr / older_atr
            if ratio < 0.85:
                atr_trend_label = "decreasing"
                score_vol_suppress = min(25.0, score_vol_suppress + 5.0)
            elif ratio < 1.05:
                atr_trend_label = "flat"
            else:
                atr_trend_label = "increasing"
                score_vol_suppress = max(0.0, score_vol_suppress - 5.0)

    score_vol_suppress = round(max(0.0, min(25.0, score_vol_suppress)), 2)

    # [2] Price compression (+20)
    std_close = _std(closes)
    score_price_compress = 0.0
    std_limit = avg_price * cfg.get("stealth_std_threshold", 0.010)
    if avg_price > 0 and std_close < std_limit:
        score_price_compress = 20.0
    elif avg_price > 0 and std_close < std_limit * 1.5:
        score_price_compress = 12.0
    elif avg_price > 0 and std_close < std_limit * 2.0:
        score_price_compress = 5.0
    score_price_compress = round(max(0.0, min(20.0, score_price_compress)), 2)

    # [3] MA hugging (+20)
    ema_p          = cfg.get("stealth_ema_period", 20)
    slope_win      = cfg.get("stealth_slope_window", 5)
    ma_dist_max    = cfg.get("stealth_ma_dist_max", 0.005)
    slope_max      = cfg.get("stealth_slope_max", 0.003)
    score_ma_hug   = 0.0
    ma_distance    = 0.0

    all_closes_ext = [c["close"] for c in confirmed]
    if len(all_closes_ext) >= ema_p:
        ema_now = calc_ema(all_closes_ext, ema_p)
        if ema_now and ema_now > 0:
            ma_distance = abs(closes[-1] - ema_now) / ema_now

            if ma_distance < ma_dist_max:
                score_ma_hug += 15.0
            elif ma_distance < ma_dist_max * 2:
                score_ma_hug += 8.0
            elif ma_distance < ma_dist_max * 3:
                score_ma_hug += 3.0

            if len(all_closes_ext) >= ema_p + slope_win:
                ema_prev = calc_ema(all_closes_ext[:-slope_win], ema_p)
                if ema_prev and ema_prev > 0:
                    slope = abs(ema_now - ema_prev) / ema_prev
                    if slope < slope_max:
                        score_ma_hug += 5.0
                    elif slope < slope_max * 2:
                        score_ma_hug += 2.0

    ma_distance_pct = ma_distance
    score_ma_hug = round(max(0.0, min(20.0, score_ma_hug)), 2)

    # [4] Low noise structure (+15)
    score_low_noise = 0.0
    wick_ratios = []
    for i in range(n_win):
        o, c_, h, l = opens[i], closes[i], highs[i], lows[i]
        body_ = abs(c_ - o)
        if body_ > 0:
            upper = h - max(o, c_)
            lower = min(o, c_) - l
            wick_ratios.append((upper + lower) / body_)

    if wick_ratios:
        avg_wick_ratio = _mean(wick_ratios)
        wick_limit     = cfg.get("stealth_wick_ratio_max", 2.5)
        if avg_wick_ratio < wick_limit * 0.5:
            score_low_noise = 15.0
        elif avg_wick_ratio < wick_limit:
            score_low_noise = 10.0
        elif avg_wick_ratio < wick_limit * 1.5:
            score_low_noise = 4.0
    score_low_noise = round(max(0.0, min(15.0, score_low_noise)), 2)

    # [5] No distribution (+10)
    score_no_dist      = 10.0
    distribution_flags = 0
    vol_spike_limit    = cfg.get("stealth_vol_spike_reject", 3.0)
    upper_wick_limit   = cfg.get("stealth_upper_wick_max", 0.60)
    vol_median         = _median(vols) if vols else 0

    for i in range(n_win):
        o, c_, h, l = opens[i], closes[i], highs[i], lows[i]
        v   = vols[i]
        rng = h - l
        is_red    = c_ < o
        vol_spike = vol_median > 0 and v > vol_spike_limit * vol_median

        if is_red and vol_spike:
            distribution_flags += 1
        if rng > 0:
            upper_wick_pct = (h - max(o, c_)) / rng
            if upper_wick_pct > upper_wick_limit:
                distribution_flags += 1
        if vol_spike:
            distribution_flags += 1

    if distribution_flags >= 5:
        score_no_dist = 0.0
    elif distribution_flags >= 3:
        score_no_dist = 3.0
    elif distribution_flags >= 1:
        score_no_dist = 7.0
    score_no_dist = round(max(0.0, min(10.0, score_no_dist)), 2)

    # [6] Volume stability (+10)
    score_vol_stable = 0.0
    volume_cv        = 1.0
    if vols and avg_vol_window > 0:
        std_vol   = _std(vols)
        volume_cv = std_vol / avg_vol_window
        cv_max    = cfg.get("stealth_vol_cv_max", 0.60)
        if volume_cv < cv_max:
            ratio = volume_cv / cv_max
            score_vol_stable = 10.0 * (1.0 - ratio)
            half       = n_win // 2
            avg_first  = _mean(vols[:half])  if half > 0 else 0.0
            avg_second = _mean(vols[half:])  if half > 0 else 0.0
            if avg_first > 0 and 1.0 <= avg_second / avg_first <= 1.5:
                score_vol_stable = min(10.0, score_vol_stable + 2.0)
    score_vol_stable = round(max(0.0, min(10.0, score_vol_stable)), 2)

    # [7] Structure stability (penalty)
    structure_quality = "stable"
    structure_penalty = 0.0
    breakdown_tol     = cfg.get("stealth_breakdown_tolerance", 0.005)
    lower_low_count   = 0
    for i in range(1, n_win):
        if lows[i] < lows[i-1] * (1.0 - breakdown_tol):
            lower_low_count += 1
    selloff_count = 0
    for i in range(n_win):
        rng_ = highs[i] - lows[i]
        if rng_ > 0:
            body_ = opens[i] - closes[i]
            if body_ > 0 and body_ / rng_ > 0.70:
                selloff_count += 1

    if lower_low_count > n_win * 0.30:
        structure_quality = "unstable (lower lows)"
        structure_penalty = 10.0
    elif selloff_count > n_win * 0.20:
        structure_quality = "unstable (sell-off)"
        structure_penalty = 8.0
    elif lower_low_count > n_win * 0.15:
        structure_quality = "weak (minor lower lows)"
        structure_penalty = 4.0

    stealth_score = (
        score_vol_suppress + score_price_compress + score_ma_hug +
        score_low_noise    + score_no_dist        + score_vol_stable
    )
    stealth_score = max(0.0, stealth_score - structure_penalty)

    # ── VAR-6: bb_width expansion dalam stealth context ───────────────────────
    # BB width pump +54% vs ranging (AUROC=0.672)
    # Dalam stealth: jika bb_width mulai EXPAND setelah compression panjang
    # ini adalah early signal bahwa compression akan segera break
    if len(confirmed) >= 52:
        cl_all    = [c["close"] for c in confirmed]
        win20_now = cl_all[-20:]
        bb_m      = _mean(win20_now)
        bb_s      = _std(win20_now)
        bb_w_now  = (4 * bb_s / bb_m) if bb_m > 0 else 0

        # Baseline: 32 periode sebelumnya
        bb_w_hist = []
        for k in range(10, 42):
            w20k = cl_all[-(20 + k):(-k)]
            if len(w20k) < 10:
                continue
            m_k = _mean(w20k)
            s_k = _std(w20k)
            bb_w_hist.append((4 * s_k / m_k) if m_k > 0 else 0)

        if bb_w_hist:
            bb_w_bl  = _mean(bb_w_hist)
            bb_exp_r = bb_w_now / bb_w_bl if bb_w_bl > 0 else 1.0
            thr_bb   = CONFIG.get("bb_width_exp_threshold", 1.30)
            bon_bb   = CONFIG.get("bb_width_exp_bonus", 3)
            if bb_exp_r >= thr_bb:
                # BB expansion dalam stealth = compression akan break
                stealth_score = min(100.0, stealth_score + bon_bb)

    stealth_score = round(min(100.0, stealth_score), 2)

    threshold = cfg.get("stealth_score_threshold", 70)
    detected  = stealth_score >= threshold
    status    = "STEALTH_ACCUMULATION_DETECTED" if detected else "NONE"

    # Entry trigger
    breakout_vol_mult = cfg.get("stealth_breakout_vol_mult", 1.5) if hasattr(cfg, "get") else 1.5
    breakout_vol_mult = 1.5
    recent_high       = max(highs)
    cur_close         = current["close"]
    cur_vol           = current.get("volume_usd", current.get("volume", 0))
    entry_trigger     = (
        cur_close > recent_high and avg_vol_window > 0
        and cur_vol > avg_vol_window * breakout_vol_mult
    )

    return {
        "detected": detected, "score": stealth_score, "status": status,
        "range_pct": round(range_pct, 6), "atr_trend": atr_trend_label,
        "ma_distance": round(ma_distance_pct, 6),
        "volume_stability": round(volume_cv, 4),
        "structure_quality": structure_quality,
        "entry_trigger": entry_trigger,
        "_score_vol_suppress":   score_vol_suppress,
        "_score_price_compress": score_price_compress,
        "_score_ma_hug":         score_ma_hug,
        "_score_low_noise":      score_low_noise,
        "_score_no_dist":        score_no_dist,
        "_score_vol_stable":     score_vol_stable,
        "_structure_penalty":    structure_penalty,
        "_lower_low_count":      lower_low_count,
        "_distribution_flags":   distribution_flags,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📊  ADAPTIVE VOLUME INTELLIGENCE
#  Relative scoring — semua vs rolling history coin itu sendiri
# ══════════════════════════════════════════════════════════════════════════════
def score_volume_intelligence(candles):
    """
    Adaptive volume scoring: SEMUA metrics relatif terhadap rolling history.
    Tidak ada fixed threshold.
    Return (score 0-25, breakdown_dict)
    """
    if len(candles) < 25:
        return 0, {"reason": "insufficient data"}

    cur_vol    = candles[-1]["volume_usd"]
    vols_short = [c["volume_usd"] for c in candles[-CONFIG["vol_avg_short"]:]]
    vols_long  = [c["volume_usd"] for c in candles[-CONFIG["vol_avg_long"]:]]

    avg_short  = _mean(vols_short)
    avg_long   = _mean(vols_long)

    if avg_long <= 0:
        return 0, {"reason": "no volume history"}

    vol_ratio = cur_vol / avg_long

    # Volume acceleration (vs avg_short to detect recent uptick)
    vol_acceleration = (cur_vol - avg_short) / avg_short if avg_short > 0 else 0

    # Scoring based on vol_ratio — adaptive classification
    # v3.1: early range widened, low-vol gets meaningful score (not penalized)
    if vol_ratio < 0.3:
        # Very suppressed — deep stealth accumulation, smart money not showing hand
        vol_score = 12
        vol_label = f"deep stealth ({vol_ratio:.2f}x) — possible smart money"
    elif 0.3 <= vol_ratio < 0.7:
        # Low but building — early accumulation, NOT a negative
        vol_score = 16
        vol_label = f"quiet accum ({vol_ratio:.2f}x)"
    elif 0.7 <= vol_ratio <= 1.5:
        # Normal-to-slightly-elevated — classic pre-pump zone
        vol_score = 20
        vol_label = f"pre-pump zone ({vol_ratio:.2f}x)"
    elif 1.5 < vol_ratio <= 3.0:
        # Clear volume expansion — strong early signal
        vol_score = 25
        vol_label = f"vol expansion ({vol_ratio:.2f}x)"
    elif 3.0 < vol_ratio <= 5.0:
        # Large spike — still useful but may be later stage
        vol_score = 15
        vol_label = f"large spike ({vol_ratio:.2f}x)"
    else:
        # Extreme — distribution or already-pumped territory
        vol_score = 5
        vol_label = f"extreme ({vol_ratio:.2f}x) — caution"

    # Volume acceleration bonus/penalty — v3.1: reward positive more, penalize less
    if vol_acceleration > 0.3:
        vol_score = min(25, vol_score + 10)
        vol_label += " +acc↑↑"
    elif vol_acceleration > 0.05:
        # Even tiny positive acceleration is a pre-pump signal
        vol_score = min(25, vol_score + 5)
        vol_label += " +acc↑"
    elif vol_acceleration < -0.5:
        # Only penalize if strongly declining
        vol_score = max(0, vol_score - 3)
        vol_label += " -acc"

    # RVOL vs same hour
    rvol = 1.0
    if len(candles) >= 25:
        last_vol       = candles[-2]["volume_usd"]
        target_hour    = (candles[-2]["ts"] // 3_600_000) % 24
        same_hour_vols = [c["volume_usd"] for c in candles[:-2]
                          if (c["ts"] // 3_600_000) % 24 == target_hour]
        if same_hour_vols:
            avg_sh = _mean(same_hour_vols)
            rvol   = last_vol / avg_sh if avg_sh > 0 else 1.0

    # ── RVOL scoring — dedicated component (v3.2) ────────────────────────────
    # RVOL vs same-hour is more honest than vol_ratio alone.
    # Elevated RVOL = real abnormal activity, not just a high-volume coin.
    rvol_score = 0
    if rvol >= 3.0:
        rvol_score = 10   # strong abnormal activity
        vol_label += f" RVOL:{rvol:.1f}x↑↑"
    elif rvol >= 2.0:
        rvol_score = 7
        vol_label += f" RVOL:{rvol:.1f}x↑"
    elif rvol >= 1.5:
        rvol_score = 4
        vol_label += f" RVOL:{rvol:.1f}x"
    elif rvol >= 1.0:
        rvol_score = 2
    # RVOL < 1.0 = no bonus, no penalty (could be accumulation before RVOL rises)

    vol_score = min(vol_score + rvol_score, 30)   # v3.2: ceiling raised 25→30

    # ── VAR-5: vol_upday_ratio_16 — AUROC=0.572 (dari data historis) ─────────
    # Volume di candle naik vs total volume dalam 16 candle.
    # Pump: 0.516 vs ranging: 0.456 — threshold: 0.500
    # Interpretasi: buyer lebih agresif → volume lebih banyak di candle hijau
    if len(candles) >= 16:
        win16    = candles[-16:]
        vol_up   = sum(c["volume_usd"] for c in win16 if c["close"] > c["open"])
        vol_tot  = sum(c["volume_usd"] for c in win16)
        upday_r  = vol_up / vol_tot if vol_tot > 0 else 0.5
        thr_up   = CONFIG.get("vol_upday_ratio_threshold", 0.500)
        bon_up   = CONFIG.get("vol_upday_bonus", 4)
        if upday_r >= thr_up * 1.15:
            vol_score = min(vol_score + bon_up, 30)
            vol_label += f" +upday({upday_r:.2f}↑↑)"
        elif upday_r >= thr_up:
            vol_score = min(vol_score + bon_up // 2, 30)
            vol_label += f" +upday({upday_r:.2f})"
    else:
        upday_r = 0.5

    return vol_score, {
        "vol_ratio":        round(vol_ratio, 3),
        "vol_acceleration": round(vol_acceleration, 3),
        "rvol":             round(rvol, 2),
        "rvol_score":       rvol_score,
        "vol_upday_ratio":  round(upday_r, 3),
        "label":            vol_label,
        "avg_long":         avg_long,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📦  ADAPTIVE COMPRESSION + TENSION SCORE
#  HIGH PRIORITY — core of pre-pump detection
# ══════════════════════════════════════════════════════════════════════════════
def score_compression_tension(candles, compression):
    """
    Adaptive compression + tension scoring.
    Uses percentile of range_pct vs rolling 100-candle history.
    Tension exists when ALL 4 conditions met (numerically defined).
    Return (score 0-35, breakdown_dict)
    """
    if compression is None:
        return 0, {"reason": "no compression zone"}

    comp_length = compression["length"]
    range_pct   = compression["range_pct"]

    # ── Compression score based on length ─────────────────────────────────────
    comp_score = 0
    if comp_length >= 30:  comp_score += 8
    if comp_length >= 72:  comp_score += 7   # 3+ hari
    if comp_length >= 168: comp_score += 6   # 7+ hari
    if comp_length >= 336: comp_score += 5   # 14+ hari

    # Range quality bonus — tighter is better
    if range_pct < 0.04:   comp_score += 5   # sangat ketat
    elif range_pct < 0.07: comp_score += 3
    elif range_pct < 0.10: comp_score += 1

    comp_score = min(comp_score, 22)   # v3.2: raised from 20

    # ── Tension detection (numerik, bukan binary) ─────────────────────────────
    tension_score = 0
    tension_details = []

    # ── VAR-4: atr_pct_32 — ATR relatif terhadap harga (AUROC=0.695) ─────────
    # Pump: atr_pct=0.0076 vs non-pump: 0.0052
    # ATR relatif lebih akurat dari ATR absolut — normalisasi per-harga per-coin
    # Range optimal pump: 0.006 – 0.020 (cukup volatil tapi belum ekstrem)
    price_ref_atr = candles[-1]["close"] if candles[-1]["close"] > 0 else 1.0
    atr_32_abs    = calc_atr(candles[-35:], 32)
    atr_pct_32    = (atr_32_abs / price_ref_atr) if atr_32_abs and price_ref_atr > 0 else 0
    atr_pct_low   = CONFIG.get("atr_pct_pump_low",  0.006)
    atr_pct_high  = CONFIG.get("atr_pct_pump_high", 0.020)
    if atr_pct_low <= atr_pct_32 <= atr_pct_high:
        tension_score += 4
        tension_details.append(f"atr_pct_ok({atr_pct_32:.4f})")
    elif atr_pct_32 > atr_pct_high:
        # Terlalu volatile = sudah dalam gerakan, bukan pre-pump setup
        tension_score -= 2
        tension_details.append(f"atr_pct_high({atr_pct_32:.4f})")
    # ATR < low = terlalu tenang, tidak ada bonus (bukan penalty — bisa stealth)

    # Condition 1: volatility_ratio (ATR short vs ATR long)
    win_short = CONFIG["win_short"]
    win_long  = CONFIG["win_structure"]
    atr_short = calc_atr(candles[-(win_short + 5):], win_short)
    atr_long  = calc_atr(candles[-(win_long + 5):],  win_long)

    vol_ratio = (atr_short / atr_long) if (atr_short and atr_long and atr_long > 0) else 1.0
    tension_max_ratio = CONFIG["tension_volatility_ratio_max"]
    if vol_ratio < tension_max_ratio:
        r = max(0, tension_max_ratio - vol_ratio) / tension_max_ratio
        tension_score += int(r * 12)   # v3.2: raised from 10 → 12
        tension_details.append(f"ATR_ratio={vol_ratio:.2f}")

    # Condition 2: range_pct < percentile_40 of last 100 candles
    history_ranges = []
    win_history = min(CONFIG["win_compression"], len(candles) - comp_length)
    if win_history > 20:
        # compute rolling range for chunks of comp_length size
        step = max(1, comp_length // 4)
        for i in range(0, win_history - comp_length, step):
            chunk = candles[-(win_history + comp_length - i): -(comp_length - i) or None]
            if len(chunk) >= comp_length:
                sub = chunk[:comp_length]
                h   = max(c["high"] for c in sub)
                l   = min(c["low"]  for c in sub)
                if l > 0:
                    history_ranges.append((h - l) / l)

    if history_ranges:
        p40 = _percentile(history_ranges, CONFIG["tension_range_percentile_max"])
        if range_pct < p40:
            tension_score += 10
            tension_details.append(f"range<p40({p40*100:.1f}%)")
        elif range_pct < _percentile(history_ranges, 60):
            tension_score += 5
            tension_details.append(f"range<p60")

    # Condition 3: volume acceleration > 0 but not extreme
    vols_recent = [c["volume_usd"] for c in candles[-10:]]
    vols_older  = [c["volume_usd"] for c in candles[-20:-10]]
    avg_recent  = _mean(vols_recent)
    avg_older   = _mean(vols_older)
    vol_acc_ratio = (avg_recent / avg_older) if avg_older > 0 else 1.0
    if 1.0 < vol_acc_ratio < CONFIG["tension_vol_acc_max_ratio"]:
        tension_score += 5
        tension_details.append(f"vol_acc={vol_acc_ratio:.2f}x")

    # Condition 4: no breakout >3% in last 6 candles
    lookback_bt = CONFIG["tension_breakout_lookback"]
    recent_moves = [
        abs(candles[-i]["close"] - candles[-i]["open"]) / candles[-i]["open"]
        for i in range(1, min(lookback_bt + 1, len(candles)))
        if candles[-i]["open"] > 0
    ]
    max_recent_move = max(recent_moves) if recent_moves else 0
    if max_recent_move < CONFIG["tension_breakout_pct"]:
        tension_score += 5
        tension_details.append("no_breakout")

    tension_score = min(tension_score, 18)   # v3.2: raised from 15 → 18

    total = comp_score + tension_score
    total = min(total, 40)   # v4.0: raised from 35 → 40 (match header)

    return total, {
        "comp_score":    comp_score,
        "tension_score": tension_score,
        "vol_ratio":     round(vol_ratio, 3),
        "tension_details": tension_details,
        "comp_length":   comp_length,
        "range_pct":     round(range_pct, 4),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🏗  STRUCTURE SCORE (Smart Money Footprint)
# ══════════════════════════════════════════════════════════════════════════════
def score_structure(candles, compression):
    """
    v4.0: tambah positional bonuses dari data historis (VAR-1, VAR-2, VAR-8).
    - dist_from_low_96: AUROC=0.696, pump +77% vs non-pump
    - dist_from_low_32: AUROC=0.672, pump +78% vs non-pump
    - pos_in_range_96:  AUROC=0.557, pump di zona 0.35-0.75
    Max score naik dari 20 → 22 untuk accommodate bonus baru.
    """
    score = 0
    breakdown = []

    # Higher lows
    higher_lows = detect_higher_lows(candles, lookback=4)
    if higher_lows:
        score += 10
        breakdown.append("higher_lows +10")

    if detect_price_acceleration(candles, lookback=6):
        score += 5
        breakdown.append("price_accel +5")

    if compression:
        failed_bd = detect_failed_breakdown(candles, compression["low"], lookback=20)
        if failed_bd:
            score += 18
            breakdown.append("failed_breakdown +18")

    # Pass recent_candles for lower_wick_avg_16 (VAR-7)
    candle_score, candle_label = analyze_candle_structure(candles[-1], candles)
    candle_score = min(candle_score, 10)   # v4.0: raised cap 8→10 karena include VAR-7
    score += candle_score
    breakdown.append(f"candle:{candle_label} +{candle_score}")

    pre_spike_sc, pre_spike_label = detect_pre_pump_candle(candles)
    score += pre_spike_sc
    if pre_spike_sc > 0:
        breakdown.append(f"pre_spike:{pre_spike_label} +{pre_spike_sc}")

    if len(candles) >= 6:
        highs = [candles[-i]["high"] for i in range(1, 7)]
        lower_high_count = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1] * 0.98)
        if lower_high_count >= 4:
            score -= 10
            breakdown.append("lower_highs_pattern -10")

    # ── VAR-1: dist_from_low_96 — AUROC=0.696 (terkuat dari data) ────────────
    # Pump median: 0.0529 vs non-pump: 0.0237
    # Threshold optimal: 0.035 (mild), 0.060 (strong)
    price_now = candles[-1]["close"]
    if price_now > 0 and len(candles) >= 96:
        low_96 = min(c["low"] for c in candles[-96:])
        dfl96  = (price_now - low_96) / price_now if price_now > 0 else 0
        thr_s  = CONFIG.get("dfl96_strong_threshold", 0.060)
        thr_m  = CONFIG.get("dfl96_pump_threshold",   0.035)
        bon_s  = CONFIG.get("dfl96_bonus_strong", 8)
        bon_m  = CONFIG.get("dfl96_bonus_mild",   4)
        if dfl96 >= thr_s:
            score += bon_s
            breakdown.append(f"dfl96_strong({dfl96:.4f}) +{bon_s}")
        elif dfl96 >= thr_m:
            score += bon_m
            breakdown.append(f"dfl96_mild({dfl96:.4f}) +{bon_m}")
        elif dfl96 < 0.008:
            # Terlalu dekat low 96c = bearish position, beri penalty ringan
            score -= 3
            breakdown.append(f"dfl96_low({dfl96:.4f}) -3")
    else:
        dfl96 = 0.0

    # ── VAR-2: dist_from_low_32 — AUROC=0.672 ────────────────────────────────
    # Pump median: 0.0248 vs non-pump: 0.0139
    if price_now > 0 and len(candles) >= 32:
        low_32 = min(c["low"] for c in candles[-32:])
        dfl32  = (price_now - low_32) / price_now if price_now > 0 else 0
        thr_s2 = CONFIG.get("dfl32_strong_threshold", 0.040)
        thr_m2 = CONFIG.get("dfl32_pump_threshold",   0.018)
        bon_s2 = CONFIG.get("dfl32_bonus_strong", 5)
        bon_m2 = CONFIG.get("dfl32_bonus_mild",   3)
        if dfl32 >= thr_s2:
            score += bon_s2
            breakdown.append(f"dfl32_strong({dfl32:.4f}) +{bon_s2}")
        elif dfl32 >= thr_m2:
            score += bon_m2
            breakdown.append(f"dfl32_mild({dfl32:.4f}) +{bon_m2}")
    else:
        dfl32 = 0.0

    # ── VAR-8: pos_in_range_96 — AUROC=0.557 ─────────────────────────────────
    # Pump di zona 0.35-0.75 dari 24-jam range
    # Pump median: 0.51 vs non-pump: 0.42
    if price_now > 0 and len(candles) >= 96:
        high_96 = max(c["high"] for c in candles[-96:])
        low_96_pos = min(c["low"] for c in candles[-96:])
        rng96 = high_96 - low_96_pos
        pos96 = (price_now - low_96_pos) / rng96 if rng96 > 0 else 0.5
        low_zone  = CONFIG.get("pos_in_range96_low",   0.35)
        high_zone = CONFIG.get("pos_in_range96_high",  0.75)
        bon96     = CONFIG.get("pos_in_range96_bonus",  3)
        if low_zone <= pos96 <= high_zone:
            score += bon96
            breakdown.append(f"pos_rng96_optimal({pos96:.2f}) +{bon96}")
        elif pos96 > high_zone:
            score -= 2
            breakdown.append(f"pos_rng96_high({pos96:.2f}) -2")

    score = max(-20, min(score, 22))   # v4.0: cap naik 20→22

    return score, {
        "score":           score,
        "breakdown":       breakdown,
        "higher_lows":     higher_lows,
        "candle_label":    candle_label,
        "pre_spike_label": pre_spike_label,
        "dfl96":           round(dfl96, 4) if 'dfl96' in dir() else 0,
        "dfl32":           round(dfl32, 4) if 'dfl32' in dir() else 0,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🔄  CONTINUATION SCORE (Second Leg Logic)
# ══════════════════════════════════════════════════════════════════════════════
def score_continuation(candles):
    """
    If coin moved >10% recently, check for high-quality pullback and tight base.
    Return (score 0-15, details)
    """
    if len(candles) < CONFIG["continuation_lookback"]:
        return 0, {}

    lb = CONFIG["continuation_lookback"]
    recent = candles[-lb:]

    price_start = recent[0]["close"]
    price_max   = max(c["high"] for c in recent)
    price_now   = recent[-1]["close"]

    if price_start <= 0:
        return 0, {}

    move_pct = (price_max - price_start) / price_start

    if move_pct < CONFIG["continuation_move_min"]:
        return 0, {"reason": "no prior move"}

    # Pullback from peak
    pullback_pct = (price_max - price_now) / price_max if price_max > 0 else 1.0

    if pullback_pct > CONFIG["continuation_pullback_max"]:
        return 0, {"reason": f"pullback too deep ({pullback_pct*100:.1f}%)"}

    # Check tight consolidation post-pullback (last 10 candles)
    last10 = candles[-10:]
    h10    = max(c["high"] for c in last10)
    l10    = min(c["low"]  for c in last10)
    rng10  = (h10 - l10) / l10 if l10 > 0 else 1.0

    # Higher low in post-pullback zone
    is_higher_low = price_now > price_start * 1.02

    score = 0
    details = {"move_pct": round(move_pct, 3), "pullback_pct": round(pullback_pct, 3),
               "rng10": round(rng10, 4)}

    if pullback_pct < 0.382:
        score += 10
        details["pullback_quality"] = "shallow (<38.2%)"
    elif pullback_pct < 0.50:
        score += 6
        details["pullback_quality"] = "moderate (38-50%)"

    if rng10 < 0.04:
        score += 5
        details["consolidation"] = "tight"
    elif rng10 < 0.07:
        score += 2
        details["consolidation"] = "moderate"

    if is_higher_low:
        score += 3
        details["higher_low"] = True

    return min(score, 15), details

# ══════════════════════════════════════════════════════════════════════════════
#  🚨  DISTRIBUTION PENALTY
#  Anti-trap: aggressively penalize fake strength
# ══════════════════════════════════════════════════════════════════════════════
def calc_distribution_penalty(candles):
    """
    v3.2 — Two-track distribution filter:
      Track A (heavy): high volume + no price progress — strongest false-positive signal.
        Max 25pts. This is the dominant distribution pattern before dumps.
      Track B (soft): upper wick rejection + lower highs structure.
        Max 15pts. Meaningful but less reliable alone.
    Total capped at CONFIG["dist_penalty_cap"] (default 25).
    """
    if len(candles) < 10:
        return 0, {}

    recent     = candles[-10:]
    price_now  = candles[-1]["close"]
    price_10a  = candles[-10]["close"]
    vols       = [c["volume_usd"] for c in recent]
    vol_median = _median(vols)
    dist_thresh = CONFIG["dist_vol_spike_mult"]      # 3.0x
    uw_thresh   = CONFIG["dist_upper_wick_ratio"]    # 0.65
    prog_thresh = CONFIG["dist_high_vol_no_prog_pct"] # 0.010

    penalty = 0
    flags   = []

    # ── TRACK A: High volume + no price progress (heaviest signal) ─────────────
    # This is what distribution looks like: smart money dumps into volume
    # while retail sees the volume and thinks it's a buy signal.
    high_vol_no_prog  = 0
    high_vol_red      = 0   # v3.2: high-vol red candle = additional confirmation
    for c in recent:
        v        = c["volume_usd"]
        high_vol = vol_median > 0 and v > dist_thresh * vol_median
        if high_vol:
            progress = abs(c["close"] - c["open"]) / max(c["open"], 1e-10)
            if progress < prog_thresh:
                high_vol_no_prog += 1
            if c["close"] < c["open"]:   # red + high volume
                high_vol_red += 1

    if high_vol_no_prog >= 3:
        penalty += 25
        flags.append(f"hvnp:{high_vol_no_prog}x (severe)")
    elif high_vol_no_prog >= 2:
        penalty += 18
        flags.append(f"hvnp:{high_vol_no_prog}x")
    elif high_vol_no_prog >= 1:
        penalty += 8
        flags.append(f"hvnp:1x")

    # High volume + red candles compound the penalty
    if high_vol_red >= 2 and high_vol_no_prog >= 1:
        penalty += 7
        flags.append(f"hvred:{high_vol_red}x")

    # ── TRACK B: Structural distribution signals (softer) ─────────────────────
    track_b = 0

    # Upper wick rejections (price pushed up then sold down = supply overhead)
    upper_wick_count = sum(
        1 for c in recent
        if (c["high"] - c["low"]) > 0 and
        (c["high"] - max(c["open"], c["close"])) / (c["high"] - c["low"]) > uw_thresh
    )
    if upper_wick_count >= 4:
        track_b += 12
        flags.append(f"upper_wicks:{upper_wick_count}")
    elif upper_wick_count >= 3:
        track_b += 7
        flags.append(f"upper_wicks:{upper_wick_count}")
    elif upper_wick_count >= 2:
        track_b += 3

    # Lower highs (bearish structure)
    highs = [c["high"] for c in recent]
    lower_high_seq = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1] * 0.99)
    if lower_high_seq >= 6:
        track_b += 12
        flags.append(f"lower_highs:{lower_high_seq}")
    elif lower_high_seq >= 4:
        track_b += 5

    # Price down on elevated volume (overall window)
    avg_vol      = _mean(vols)
    comp_vol_ref = candles[-20:-10]
    avg_comp_vol = _mean([c["volume_usd"] for c in comp_vol_ref]) if comp_vol_ref else avg_vol
    overall_move = (price_now - price_10a) / price_10a if price_10a > 0 else 0
    if avg_comp_vol > 0 and avg_vol > 1.5 * avg_comp_vol and overall_move < -0.02:
        track_b += 8
        flags.append("vol_up_price_down")

    penalty += min(track_b, 15)   # cap track B contribution

    cap = CONFIG.get("dist_penalty_cap", 25)   # v3.2: raised cap 20→25
    penalty = min(penalty, cap)
    return penalty, {"penalty": penalty, "flags": flags,
                     "high_vol_no_prog": high_vol_no_prog, "upper_wick_count": upper_wick_count}

# ══════════════════════════════════════════════════════════════════════════════
#  🐢  SLOW TREND PENALTY
#  Anti-drift: avoid smooth trends without compression
# ══════════════════════════════════════════════════════════════════════════════
def calc_slow_trend_penalty(candles, compression):
    """
    Penalize coins that are just drifting up/down without tension.
    Return (penalty 0-20, details)
    """
    if len(candles) < 30:
        return 0, {}

    closes = [c["close"] for c in candles[-30:]]
    ma5    = _mean(closes[-5:])
    ma20   = _mean(closes[-20:])
    ma30   = _mean(closes)

    # Smooth uptrend without compression = slow trend
    trending = (ma5 > ma20 > ma30 * 1.005) or (ma5 < ma20 < ma30 * 0.995)

    # ATR compression check
    atr_s = calc_atr(candles[-12:], 10)
    atr_l = calc_atr(candles[-32:], 30)
    no_compression = (atr_s is not None and atr_l is not None and atr_l > 0
                      and atr_s / atr_l > 0.90)

    # Compression zone exists — if yes, no slow-trend penalty
    has_compression = compression is not None

    if trending and no_compression and not has_compression:
        return min(10, 10), {"reason": "smooth trend without compression"}   # v3.1: capped at 10
    elif trending and no_compression and has_compression and compression["length"] < 30:
        return min(5, 10), {"reason": "short compression + trending"}
    return 0, {}

# ══════════════════════════════════════════════════════════════════════════════
#  ⏰  ANTI-LATE-ENTRY PENALTY
# ══════════════════════════════════════════════════════════════════════════════
def calc_late_entry_penalty(candles):
    """
    If price moved >8% in last 6-12 candles without consolidation → heavy penalty.
    Return (penalty 0-30, details)
    """
    lb = CONFIG["anti_late_lookback"]
    if len(candles) < lb + 2:
        return 0, {}

    price_now   = candles[-1]["close"]
    price_start = candles[-(lb + 1)]["close"]

    if price_start <= 0:
        return 0, {}

    move_pct = (price_now - price_start) / price_start

    if abs(move_pct) < CONFIG["anti_late_move_pct"]:
        return 0, {}

    # Check if there's consolidation after the move
    recent5 = candles[-5:]
    h5 = max(c["high"] for c in recent5)
    l5 = min(c["low"]  for c in recent5)
    rng5 = (h5 - l5) / l5 if l5 > 0 else 1.0

    has_consolidation = rng5 < 0.04   # tight last 5 = consolidating

    if not has_consolidation:
        cap     = CONFIG.get("anti_late_penalty_cap", 15)   # v3.1: capped at 15
        penalty = cap if abs(move_pct) > 0.15 else cap // 2
        return penalty, {
            "move_pct": round(move_pct, 3),
            "rng5": round(rng5, 4),
            "reason": "late_no_consolidation",
        }
    else:
        # Move happened but now consolidating = maybe second leg (handled in continuation)
        return 0, {"move_pct": round(move_pct, 3), "consolidating": True}


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PRE-BREAKOUT BIAS MODULE (NEW v3.1)
#
#  Mendeteksi micro-accumulation dalam 6 candle terakhir SEBELUM volume spike.
#  Fokus pada 4 sinyal yang konsisten terjadi 1-3 candle SEBELUM pump:
#    [1] ATR squeeze — volatility menyempit tajam dalam window pendek
#    [2] Volume creep — volume naik perlahan tanpa spike (smart money masuk diam)
#    [3] Wick rejection bawah — buyer mempertahankan harga di support
#    [4] Close bias atas — candle-candle terakhir tutup di bagian atas range
#
#  CRITICAL: Modul ini sengaja tidak bergantung pada compression zone.
#  Bisa detect sinyal bahkan jika compression belum teridentifikasi.
#  Max bonus: +12 pts
# ══════════════════════════════════════════════════════════════════════════════
def score_pre_breakout_bias(candles):
    """
    Deteksi micro-accumulation pattern dalam window pendek (6 candle).
    Return (score 0-12, details_dict)
    """
    win  = CONFIG.get("pbb_window", 6)
    if len(candles) < win + 15:   # butuh context untuk ATR long
        return 0, {}

    recent = candles[-win:]
    score  = 0
    flags  = []

    # ── [1] ATR SQUEEZE — volatility collapse in short window ─────────────────
    atr_short = calc_atr(candles[-(win + 2):], win)
    atr_long  = calc_atr(candles[-32:], 30)
    squeeze_ratio = CONFIG.get("pbb_squeeze_atr_ratio", 0.60)

    if atr_short and atr_long and atr_long > 0:
        ratio = atr_short / atr_long
        if ratio < squeeze_ratio:
            score += 4
            flags.append(f"squeeze({ratio:.2f})")
        elif ratio < squeeze_ratio * 1.2:
            score += 2
            flags.append(f"mild_squeeze({ratio:.2f})")

    # ── [2] VOLUME CREEP — slow silent increase, no spike ─────────────────────
    vols     = [c["volume_usd"] for c in recent]
    half     = max(1, win // 2)
    avg_prev = _mean(vols[:half])
    avg_last = _mean(vols[half:])
    creep_ratio = CONFIG.get("pbb_vol_creep_ratio", 0.15)

    if avg_prev > 0:
        vol_change = (avg_last - avg_prev) / avg_prev
        vol_max    = max(vols)
        vol_median = _median(vols)
        no_spike   = vol_median > 0 and vol_max < vol_median * 2.5   # no single spike

        if vol_change > creep_ratio and no_spike:
            score += 4
            flags.append(f"vol_creep(+{vol_change*100:.0f}%)")
        elif vol_change > 0.05 and no_spike:
            score += 2
            flags.append(f"vol_uptick(+{vol_change*100:.0f}%)")

    # ── [3] WICK REJECTION — consistent lower wick buying pressure ─────────────
    wick_min   = CONFIG.get("pbb_wick_reject_min", 0.40)
    wick_count = 0
    for c in recent:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        lower_wick = (min(c["open"], c["close"]) - c["low"]) / rng
        if lower_wick > wick_min:
            wick_count += 1

    if wick_count >= win * 0.5:      # ≥50% candle punya rejection wick bawah
        score += 3
        flags.append(f"wick_reject({wick_count}/{win})")
    elif wick_count >= win * 0.33:
        score += 1
        flags.append(f"wick_reject({wick_count}/{win})")

    # ── [4] CLOSE BIAS TOP — candles consistently closing in upper half ────────
    top_close_thresh = CONFIG.get("pbb_close_top_half", 0.55)
    top_count = 0
    for c in recent:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        close_pos = (c["close"] - c["low"]) / rng
        if close_pos > top_close_thresh:
            top_count += 1

    if top_count >= win * 0.67:
        score += 3
        flags.append(f"close_bias_top({top_count}/{win})")
    elif top_count >= win * 0.50:
        score += 1
        flags.append(f"close_bias_top({top_count}/{win})")

    # ── [5] VAR-3: range_pct_4 & range_pct_8 EXPANSION — AUROC=0.685/0.688 ───
    # Pump vs ranging: range_4 +64%, range_8 +64%
    # Volatility 1-2 jam sebelum pump lebih tinggi dari baseline 8-jam
    # Threshold: range_4 harus > 1.30x baseline rolling 32c
    if len(candles) >= 36:
        price_ref   = candles[-1]["close"] if candles[-1]["close"] > 0 else 1.0
        range_4     = (max(c["high"] for c in candles[-4:]) -
                       min(c["low"]  for c in candles[-4:])) / price_ref
        range_8     = (max(c["high"] for c in candles[-8:]) -
                       min(c["low"]  for c in candles[-8:])) / price_ref
        # Baseline: rolling range 32c sebagai referensi
        range_32    = (max(c["high"] for c in candles[-32:]) -
                       min(c["low"]  for c in candles[-32:])) / price_ref
        range_96_bl = (max(c["high"] for c in candles[-96:]) -
                       min(c["low"]  for c in candles[-96:])) / price_ref if len(candles) >= 96 else range_32

        # Normalize range_4 & range_8 terhadap baseline (range_32)
        r4_ratio = range_4 / (range_32 / 8)  if range_32 > 0 else 1.0   # per-candle normalized
        r8_ratio = range_8 / (range_32 / 4)  if range_32 > 0 else 1.0

        thr_exp  = CONFIG.get("range4_expansion_threshold", 1.30)
        thr_exp8 = CONFIG.get("range8_expansion_threshold", 1.25)
        bon_exp  = CONFIG.get("range_expansion_bonus", 4)

        if r4_ratio >= thr_exp * 1.20 or r8_ratio >= thr_exp8 * 1.20:
            score += bon_exp
            flags.append(f"rng_exp_strong(r4={r4_ratio:.2f} r8={r8_ratio:.2f})")
        elif r4_ratio >= thr_exp or r8_ratio >= thr_exp8:
            score += bon_exp // 2
            flags.append(f"rng_exp(r4={r4_ratio:.2f} r8={r8_ratio:.2f})")

    # ── [6] VAR-6: bb_width EXPANSION — AUROC=0.672 ──────────────────────────
    # BB width pump +54% vs ranging; expansion = pre-pump volatility setup
    # bb_width sekarang vs rolling mean 32c
    if len(candles) >= 52:
        cl_series = [c["close"] for c in candles]
        # BB width 20-period
        win20     = cl_series[-20:]
        bb_mid    = _mean(win20)
        bb_std    = _std(win20)
        bb_w_now  = (4 * bb_std / bb_mid) if bb_mid > 0 else 0  # normalized bb_width

        # Rolling baseline bb_width: avg bb_width over last 32 periods
        bb_widths_hist = []
        for k in range(32, 0, -1):
            w20k  = cl_series[-(20 + k):(-k)]
            if len(w20k) < 10:
                continue
            m_k   = _mean(w20k)
            s_k   = _std(w20k)
            bb_widths_hist.append((4 * s_k / m_k) if m_k > 0 else 0)

        if bb_widths_hist:
            bb_w_baseline = _mean(bb_widths_hist)
            bb_exp_ratio  = bb_w_now / bb_w_baseline if bb_w_baseline > 0 else 1.0
            thr_bb = CONFIG.get("bb_width_exp_threshold", 1.30)
            bon_bb = CONFIG.get("bb_width_exp_bonus", 3)
            if bb_exp_ratio >= thr_bb * 1.20:
                score += bon_bb
                flags.append(f"bb_exp_strong({bb_exp_ratio:.2f}x)")
            elif bb_exp_ratio >= thr_bb:
                score += bon_bb // 2
                flags.append(f"bb_exp({bb_exp_ratio:.2f}x)")

    score = min(score, 15)   # v4.0: raised cap 12→15 (accommodate VAR-3 & VAR-6)
    return score, {"pbb_score": score, "flags": flags, "window": win}




# ══════════════════════════════════════════════════════════════════════════════
#  🔢  v3.4 — SHARED NORMALIZATION UTILITY
#  Global normalisation rule (mandatory per spec):
#    z  = clamp((value - rolling_mean) / rolling_std, -3, 3)
#    n  = (z + 3) / 6   →  final range 0–1
#  rolling_mean / rolling_std use EXISTING system windows (win_mid=20 default).
# ══════════════════════════════════════════════════════════════════════════════
def _norm01(value, series, clamp_lo=-3.0, clamp_hi=3.0):
    """
    Normalise a single value against a list of reference values.
    Uses existing data series — no new windows created.
    Returns float in [0, 1].
    """
    if not series or len(series) < 2:
        return 0.5   # neutral when insufficient history
    mu  = _mean(series)
    sig = _std(series)
    if sig == 0:
        return 0.5
    z = (value - mu) / sig
    z = max(clamp_lo, min(clamp_hi, z))
    return (z + 3.0) / 6.0


# ══════════════════════════════════════════════════════════════════════════════
#  💧  v3.4 — LIQUIDITY NORMALISER (SECTION 1)
#  Replaces hard liquidity_penalty subtraction with a multiplicative scale.
#  Uses existing vol_avg_long (20-candle) window as rolling reference.
# ══════════════════════════════════════════════════════════════════════════════
def compute_liquidity_norm(vol_24h, candles):
    """
    Returns liquidity_norm in [0.4, 1.0].
    Normalises vol_24h against rolling volume history (win_mid=20 candles).
    Clamped to [0.4, 1.0] so even illiquid coins keep 40% of their score.
    """
    win = CONFIG.get("vol_avg_long", 20)
    ref_vols = [c["volume_usd"] for c in candles[-win:]]
    # Scale daily volume down to per-candle equivalent for comparison
    per_candle_equiv = vol_24h / 24.0
    z = _norm01(per_candle_equiv, ref_vols)
    return max(0.4, min(1.0, z))


# ══════════════════════════════════════════════════════════════════════════════
#  🧮  v3.4 — COMPONENT NORMALISER
#  Converts existing 0-100 sub-scores to 0-1 using rolling z-score.
#  Uses win_compression=100 candle history window as rolling reference.
# ══════════════════════════════════════════════════════════════════════════════
def _norm_score_to_01(score, max_possible):
    """
    Simple min-max normalisation for a score already bounded at max_possible.
    Returns float in [0, 1].
    """
    if max_possible <= 0:
        return 0.0
    return max(0.0, min(1.0, score / max_possible))


# ══════════════════════════════════════════════════════════════════════════════
#  ⚡  v3.4 — TENSION ENGINE (SECTION 3 — MULTIPLICATIVE)
#  Four sub-components multiplied together.
#  All sub-components normalised to [0,1] using rolling history.
# ══════════════════════════════════════════════════════════════════════════════
def compute_tension_engine(candles, compression, comp_tension_details):
    """
    tension_score =
        compression_norm
        * volume_stability_norm
        * breakout_suppression_norm
        * time_in_range_norm

    All components use rolling stats from existing windows.
    Returns float in [0, 1].
    """
    if compression is None:
        return 0.0, {}

    win_mid  = CONFIG.get("vol_avg_long", 20)
    win_long = CONFIG.get("win_structure", 50)

    # ── compression_norm: ATR ratio already computed in comp_tension_details ──
    atr_ratio = comp_tension_details.get("vol_ratio", 1.0)   # atr_short/atr_long
    # Lower ATR ratio = tighter compression = higher norm
    # Build reference series of ATR ratios over the past win_long candles
    win_s = CONFIG.get("win_short", 10)
    atr_ratio_samples = []
    step = max(1, win_s // 2)
    for i in range(win_long, min(len(candles) - win_s - 3, win_long * 4), step):
        as_ = calc_atr(candles[-(i + win_s + 3): -(i)], win_s)
        al_ = calc_atr(candles[-(i + win_long + 3): -(i)], win_long)
        if as_ and al_ and al_ > 0:
            atr_ratio_samples.append(as_ / al_)

    if atr_ratio_samples:
        # Invert: lower atr_ratio → higher normalised value
        inv_ratio = 1.0 / atr_ratio if atr_ratio > 0 else 2.0
        inv_samples = [1.0 / s if s > 0 else 2.0 for s in atr_ratio_samples]
        compression_norm = _norm01(inv_ratio, inv_samples)
    else:
        compression_norm = _norm_score_to_01(comp_tension_details.get("comp_score", 0), 22)

    # ── volume_stability_norm: inverse of rolling std(volume) ─────────────────
    vols = [c["volume_usd"] for c in candles[-win_mid:]]
    avg_v = _mean(vols)
    std_v = _std(vols)
    cv = (std_v / avg_v) if avg_v > 0 else 1.0
    # Low CV = stable volume = high stability_norm
    # Build CV samples over rolling windows
    cv_samples = []
    for i in range(win_long, min(len(candles), win_long * 3), win_mid):
        sl = [c["volume_usd"] for c in candles[-i - win_mid: -i]]
        if len(sl) >= 4:
            a = _mean(sl); s = _std(sl)
            if a > 0:
                cv_samples.append(s / a)
    if cv_samples:
        inv_cv = 1.0 / (cv + 1e-9)
        inv_cv_samples = [1.0 / (x + 1e-9) for x in cv_samples]
        volume_stability_norm = _norm01(inv_cv, inv_cv_samples)
    else:
        volume_stability_norm = max(0.0, min(1.0, 1.0 - cv))

    # ── breakout_suppression_norm: normalised count of failed breakouts ────────
    comp_low  = compression["low"]
    comp_high = compression["high"]
    atr_val   = calc_atr(candles[-25:], 14) or (price_now_ref := candles[-1]["close"]) * 0.02
    if atr_val is None:
        atr_val = candles[-1]["close"] * 0.02

    # Count failed breakdowns (spec: break < 1 ATR below support, recover ≤ 3 candles)
    lookback20 = candles[-20:] if len(candles) >= 20 else candles
    fbd_count  = 0
    for i in range(len(lookback20) - 1):
        c = lookback20[i]
        # Break below support
        if c["low"] < comp_low and (comp_low - c["low"]) < atr_val:
            for j in range(i + 1, min(i + 4, len(lookback20))):
                if lookback20[j]["close"] > comp_low:
                    fbd_count += 1
                    break
        # Break above resistance
        if c["high"] > comp_high and (c["high"] - comp_high) < atr_val:
            for j in range(i + 1, min(i + 4, len(lookback20))):
                if lookback20[j]["close"] < comp_high:
                    fbd_count += 1
                    break

    # Normalise against possible maximum (up to 5 events in 20 candles)
    breakout_suppression_norm = min(1.0, fbd_count / 4.0)

    # ── time_in_range_norm: fraction of last 50 candles inside compression ─────
    win_struct = min(win_long, len(candles))
    in_range_count = sum(
        1 for c in candles[-win_struct:]
        if comp_low <= c["close"] <= comp_high
    )
    time_in_range_raw = in_range_count / win_struct if win_struct > 0 else 0.0
    # Normalise against rolling reference (50 candles history)
    tir_samples = []
    block = win_struct // 3
    if block >= 5:
        for k in range(2):
            sl  = candles[-(win_struct + block * k): -(block * k) or None]
            if len(sl) >= win_struct:
                cnt = sum(1 for c in sl[-win_struct:] if comp_low <= c["close"] <= comp_high)
                tir_samples.append(cnt / win_struct)
    if tir_samples:
        time_in_range_norm = _norm01(time_in_range_raw, tir_samples)
    else:
        time_in_range_norm = time_in_range_raw

    # ── Multiplicative combination ────────────────────────────────────────────
    tension_engine_score = (
        compression_norm
        * volume_stability_norm
        * breakout_suppression_norm
        * time_in_range_norm
    )
    # Rescale: product of 4 terms [0,1] is small; raise to maintain signal strength
    tension_engine_score = min(1.0, tension_engine_score ** 0.5)

    return tension_engine_score, {
        "compression_norm":         round(compression_norm, 4),
        "volume_stability_norm":    round(volume_stability_norm, 4),
        "breakout_suppression_norm":round(breakout_suppression_norm, 4),
        "time_in_range_norm":       round(time_in_range_norm, 4),
        "fbd_count":                fbd_count,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🧲  v3.5 — ABSORPTION ENGINE (SECTION 2 — REAL IMPLEMENTATION)
# ══════════════════════════════════════════════════════════════════════════════
def compute_absorption_engine(candles, compression):
    """
    v3.5 — Four-component continuous absorption score.

    Components (all normalised 0-1):
      fbd_count          — number of failed breakdowns in last 20 candles
      fbd_depth          — avg penetration below support in ATR units (inverted)
      fbd_recovery_speed — normalised inverse of candles to return to range
      rejection_strength — avg lower wick / candle range

    Final formula (spec-exact):
      fbd_strength = clamp(
          (fbd_count / 3) * 0.30
          + (1 - fbd_depth) * 0.25
          + fbd_recovery_speed * 0.25
          + rejection_strength * 0.20,
          0, 1)

    absorption_score = fbd_strength
    """
    win = CONFIG.get("vol_avg_long", 20)
    n   = len(candles)
    if n < win + 5:
        return 0.0, {"reason": "insufficient data"}

    comp_low = compression["low"] if compression else candles[-1]["close"] * 0.97
    atr_val  = calc_atr(candles[-25:], 14)
    if not atr_val or atr_val <= 0:
        atr_val = candles[-1]["close"] * 0.02

    lookback20 = candles[-20:] if n >= 20 else candles

    # ── fbd_count, fbd_depth, fbd_recovery_speed ──────────────────────────────
    fbd_count         = 0
    total_depth_atr   = 0.0   # sum of penetration depths in ATR units
    total_recovery    = 0.0   # sum of candles to recover (lower = faster)
    fbd_events        = []    # (depth_atr, recovery_candles)

    for i in range(len(lookback20) - 1):
        c = lookback20[i]
        # Failed breakdown: break below support by less than 1 ATR, recover ≤ 3 candles
        if c["low"] < comp_low and (comp_low - c["low"]) < atr_val:
            depth_atr = (comp_low - c["low"]) / atr_val   # 0 to ~1
            for j in range(i + 1, min(i + 4, len(lookback20))):
                if lookback20[j]["close"] > comp_low:
                    recovery_candles = j - i   # 1, 2, or 3
                    fbd_count += 1
                    fbd_events.append((depth_atr, recovery_candles))
                    total_depth_atr += depth_atr
                    total_recovery  += recovery_candles
                    break

    # fbd_depth: avg ATR penetration, normalised to [0,1] — 0 = surface touch, 1 = deep
    if fbd_events:
        avg_depth_atr = total_depth_atr / len(fbd_events)
        fbd_depth = min(1.0, avg_depth_atr)   # already in [0,1] since depth < 1 ATR
    else:
        fbd_depth = 0.0   # no events = no depth = use 1-fbd_depth=1.0 in formula

    # fbd_recovery_speed: normalised inverse of avg recovery candles
    # faster recovery (1 candle) → higher score
    if fbd_events:
        avg_recovery = total_recovery / len(fbd_events)   # 1.0 to 3.0
        # Map: 1 candle → 1.0, 3 candles → 0.33
        fbd_recovery_speed = 1.0 / avg_recovery
    else:
        fbd_recovery_speed = 0.0

    # ── rejection_strength: avg lower wick / candle range (last N candles) ────
    recent = candles[-win:]
    wick_ratios = []
    for c in recent:
        rng = c["high"] - c["low"]
        if rng > 0:
            lower_wick = min(c["open"], c["close"]) - c["low"]
            wick_ratios.append(max(0.0, lower_wick / rng))
    rejection_strength = _mean(wick_ratios) if wick_ratios else 0.0
    # rejection_strength is already in [0,1] — lower wick fraction of range

    # ── Spec-exact final formula ──────────────────────────────────────────────
    fbd_count_norm = min(1.0, fbd_count / 3.0)   # normalised: 3+ events → 1.0
    fbd_depth_term = 1.0 - fbd_depth              # inverted: shallow = good

    fbd_strength = max(0.0, min(1.0,
        fbd_count_norm       * 0.30
        + fbd_depth_term     * 0.25
        + fbd_recovery_speed * 0.25
        + rejection_strength * 0.20
    ))

    absorption_score = fbd_strength

    return absorption_score, {
        "fbd_count":           fbd_count,
        "fbd_count_norm":      round(fbd_count_norm, 4),
        "fbd_depth":           round(fbd_depth, 4),
        "fbd_recovery_speed":  round(fbd_recovery_speed, 4),
        "rejection_strength":  round(rejection_strength, 4),
        "fbd_strength":        round(fbd_strength, 4),
        "absorption_score":    round(absorption_score, 4),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📡  v3.5 — OI + CVD DATA FETCHER
def compute_flow_engine(oi_series=None, cvd_series=None, price_series=None):
    """
    v3.5 — Continuous flow scoring (replaces binary logic).
    All three scores are continuous clamp functions, not binary 0/1.

    Validation:
      - If oi_series or cvd_series is None → flow_score = 0, log FLOW DATA MISSING
      - If len(oi_series) != len(cvd_series) → flow_score = 0, log FLOW LENGTH MISMATCH
    """
    # ── Validation (mandatory per spec Section 1) ─────────────────────────────
    if oi_series is None or cvd_series is None:
        log.debug("FLOW DATA MISSING — flow_score=0")
        return 0.0, {"reason": "FLOW DATA MISSING", "flow_score": 0.0,
                     "buildup_score": 0.0, "flow_absorption": 0.0, "squeeze_score": 0.0}

    # Align lengths to the shorter series
    min_len = min(len(oi_series), len(cvd_series))
    if min_len < 5:
        log.debug("FLOW DATA TOO SHORT — flow_score=0")
        return 0.0, {"reason": "FLOW DATA TOO SHORT", "flow_score": 0.0,
                     "buildup_score": 0.0, "flow_absorption": 0.0, "squeeze_score": 0.0}

    if len(oi_series) != len(cvd_series):
        log.debug(f"FLOW LENGTH MISMATCH (oi={len(oi_series)} cvd={len(cvd_series)}) — trimming to {min_len}")
        oi_series  = oi_series[-min_len:]
        cvd_series = cvd_series[-min_len:]

    # ── OI change z-score (continuous, normalised to 0-1) ────────────────────
    oi_changes  = [oi_series[i] - oi_series[i - 1] for i in range(1, len(oi_series))]
    cvd_changes = [cvd_series[i] - cvd_series[i - 1] for i in range(1, len(cvd_series))]

    cur_oi_change  = oi_changes[-1]  if oi_changes  else 0.0
    cur_cvd_change = cvd_changes[-1] if cvd_changes else 0.0

    # Raw z-scores (not normalised to 0-1 here — used directly in clamp formulas)
    def _raw_zscore(value, series):
        if len(series) < 2:
            return 0.0
        mu  = _mean(series)
        sig = _std(series)
        return (value - mu) / sig if sig > 0 else 0.0

    oi_z_raw  = _raw_zscore(cur_oi_change,  oi_changes)
    cvd_z_raw = _raw_zscore(cur_cvd_change, cvd_changes)

    # ── Continuous scoring (spec-exact clamp formulas) ────────────────────────
    # buildup_score = clamp((oi_z - 0.5) / 2.0, 0, 1)
    buildup_score    = max(0.0, min(1.0, (oi_z_raw - 0.5) / 2.0))

    # flow_absorption = clamp((cvd_z - 0.3) / 2.0, 0, 1)
    flow_absorption  = max(0.0, min(1.0, (cvd_z_raw - 0.3) / 2.0))

    # squeeze_score = clamp((oi_z - abs(cvd_z)) / 2.0, 0, 1)
    squeeze_score    = max(0.0, min(1.0, (oi_z_raw - abs(cvd_z_raw)) / 2.0))

    flow_score = (
        0.4 * buildup_score
        + 0.3 * flow_absorption
        + 0.3 * squeeze_score
    )
    return flow_score, {
        "oi_z_raw":        round(oi_z_raw, 4),
        "cvd_z_raw":       round(cvd_z_raw, 4),
        "buildup_score":   round(buildup_score, 4),
        "flow_absorption": round(flow_absorption, 4),
        "squeeze_score":   round(squeeze_score, 4),
        "flow_score":      round(flow_score, 4),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📖  v3.4 — ORDERBOOK VACUUM (SECTION 6)
#  Requires live orderbook ask volume near price.
#  Per spec: vacuum_score = 0 when data unavailable.
#  Architecture complete for future integration.
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  📡  v3.5 — OI + CVD DATA FETCHER (Section 1 — Flow Engine Activation)
#  Fetches real Open Interest series from Bitget.
#  CVD is derived from signed candle volume (no tick data needed).
# ══════════════════════════════════════════════════════════════════════════════
def get_oi_and_cvd(symbol, candles):
    """
    Fetch OI series from Bitget /api/v2/mix/market/open-interest.
    Derive CVD proxy from signed candle volume (close > open = buy pressure).

    Returns (oi_series list[float] | None, cvd_series list[float] | None).
    Returns (None, None) on any failure — caller handles gracefully.
    """
    # ── OI series from Bitget ─────────────────────────────────────────────────
    oi_series = None
    try:
        data = safe_get(
            f"{BITGET_BASE}/api/v2/mix/market/open-interest",
            params={"symbol": symbol, "productType": "usdt-futures"},
        )
        if data and data.get("code") == "00000" and data.get("data"):
            raw = data["data"]
            # API may return a single value or a list
            if isinstance(raw, list):
                oi_series = [float(item.get("openInterestList", item.get("openInterest", 0)))
                             for item in raw if item]
            elif isinstance(raw, dict):
                val = float(raw.get("openInterestList", raw.get("openInterest", 0)))
                if val > 0:
                    # Single point — replicate as flat series so downstream doesn't crash
                    oi_series = [val] * min(len(candles), 20)
    except Exception as e:
        log.debug(f"OI fetch failed for {symbol}: {e}")
        oi_series = None

    # ── CVD proxy derived from candles ────────────────────────────────────────
    # signed_vol = volume * body_direction * body_pct_of_range
    # This is a candle-level CVD proxy; real CVD requires tick data.
    cvd_series = None
    if candles and len(candles) >= 5:
        try:
            signed_vols = []
            for c in candles[-min(len(candles), 50):]:
                rng = c["high"] - c["low"]
                if rng > 0:
                    body_dir = 1.0 if c["close"] >= c["open"] else -1.0
                    body_pct = abs(c["close"] - c["open"]) / rng
                    signed_vols.append(c["volume_usd"] * body_dir * body_pct)
                else:
                    signed_vols.append(0.0)
            # Cumulative sum = CVD series
            cvd_series = []
            running = 0.0
            for sv in signed_vols:
                running += sv
                cvd_series.append(running)
        except Exception as e:
            log.debug(f"CVD proxy build failed for {symbol}: {e}")
            cvd_series = None

    return oi_series, cvd_series


def compute_vacuum_score(ask_volume_near=None, rolling_mean_ask=None):
    """
    Orderbook vacuum: low ask-side liquidity near price = price can move up easily.

    CURRENT STATUS: Orderbook data not fetched by this system.
    Per spec Section 6: vacuum_score = 0 when data unavailable.
    """
    if ask_volume_near is None or rolling_mean_ask is None or rolling_mean_ask <= 0:
        return 0.0, {"reason": "orderbook data not available — vacuum_score=0 per spec §6"}

    ask_liquidity_ratio = ask_volume_near / rolling_mean_ask
    vacuum_score = 1.0 if ask_liquidity_ratio < 0.5 else 0.0
    return vacuum_score, {"ask_liquidity_ratio": round(ask_liquidity_ratio, 4)}


# ══════════════════════════════════════════════════════════════════════════════
#  🔮  v3.4 — NON-LINEAR SCORE COMPOSER (SECTIONS 2, 7, 8, 9, 10)
#  Takes normalised sub-scores, applies multiplicative formula,
#  synergy bonuses, late-entry scaling, phase reclassification,
#  amplification and final clamp.
# ══════════════════════════════════════════════════════════════════════════════
def compose_v34_score(
    ct_norm,          # compression+tension 0-1
    v_norm,           # volume 0-1
    s_norm,           # structure 0-1
    tension_engine,   # tension engine 0-1
    absorption_score, # absorption 0-1
    flow_score,       # flow 0-1
    vacuum_score,     # vacuum 0-1
    liquidity_norm,   # liquidity multiplier 0.4-1.0
    failed_breakdown_strength,   # fbd_strength 0-1
    pbb_norm,         # pre-breakout bias 0-1
    candles,          # raw candles for return_z / distance_from_range
    compression,      # compression zone dict or None
    phase_v33,        # phase string from detect_phase
):
    """
    v3.5 — Unified non-linear score composer.
    raw_score_additive is NOT used here (kept for logging only, per Section 3).

    Sections applied:
      S3 — no blending with additive score
      S4 — base_score = clamp(CT*(1+V)*(1+S), 0, 2)
      S5 — synergy 0.25 / 0.35
      S6 — anomaly_score
      S7 — vacuum safe mode
      S8 — final composition formula
    """
    n = len(candles)

    # ── Section 4: Non-linear base score — clamp(CT*(1+V)*(1+S), 0, 2) ──────
    # REMOVED: prior version divided by 4.0 to flatten the score range.
    # NEW (spec-exact): clamp to [0, 2] without flattening.
    base_score = max(0.0, min(2.0,
        ct_norm * (1.0 + v_norm) * (1.0 + s_norm)
    ))

    # ── Section 5: Synergy bonuses (raised from 0.10/0.15 to 0.25/0.35) ────
    synergy_bonus = 0.0

    if ct_norm > 0.7 and v_norm > 0.6:
        synergy_bonus += 0.25   # was 0.10

    if failed_breakdown_strength > 0.7 and absorption_score > 0.6:
        synergy_bonus += 0.35   # was 0.15 with different condition

    if pbb_norm > 0.6:
        synergy_bonus += 0.10   # unchanged

    # ── return_z for late-entry scaling and phase (computed once here) ────────
    win_s = min(12, n - 1)
    win_r = min(CONFIG.get("win_structure", 50), n - 1)
    recent_ret = (
        (candles[-1]["close"] - candles[-win_s]["close"]) / candles[-win_s]["close"]
        if win_s > 0 and candles[-win_s]["close"] > 0 else 0.0
    )
    ret_history = [
        (candles[-i]["close"] - candles[-i - win_s]["close"]) / candles[-i - win_s]["close"]
        for i in range(1, min(win_r, n - win_s))
        if candles[-i - win_s]["close"] > 0
    ]
    return_z = _norm01(recent_ret, ret_history) if ret_history else 0.5

    # price_change_z (raw, not 0-1): used in anomaly_score
    if ret_history:
        mu  = _mean(ret_history)
        sig = _std(ret_history)
        price_change_z_raw = (recent_ret - mu) / sig if sig > 0 else 0.0
    else:
        price_change_z_raw = 0.0

    # ── Section 6: Anomaly detection (critical for ZETA/PHA-type) ─────────────
    anomaly_score = 0.0

    # Hidden accumulation: absorption present but structure weak and price flat
    if (absorption_score > 0.6
            and s_norm < 0.5
            and abs(price_change_z_raw) < 0.3):
        anomaly_score += 0.4

    # Flow divergence: flow active but price not moving
    if (flow_score > 0.5
            and abs(price_change_z_raw) < 0.3):
        anomaly_score += 0.3

    anomaly_score = max(0.0, min(1.0, anomaly_score))

    # ── Section 7: Vacuum (safe mode — already handled at call site, kept explicit)
    # vacuum_score is already 0.0 when data unavailable; no change needed here

    # ── Late entry control ─────────────────────────────────────────────────────
    distance_from_range = 0.0
    if compression:
        comp_high = compression["high"]
        price_now = candles[-1]["close"]
        atr_val   = calc_atr(candles[-25:], 14) or price_now * 0.02
        if atr_val > 0 and price_now > comp_high:
            distance_from_range = (price_now - comp_high) / atr_val

    late_scale = 1.0
    if return_z > 0.75 or distance_from_range > 1.2:
        late_scale = 0.65

    # ── Phase reclassification ─────────────────────────────────────────────────
    if ct_norm > 0.6 and return_z < 0.58:
        phase_v34 = "early"
    elif return_z > 0.75:
        phase_v34 = "late"
    else:
        phase_v34 = "mid"

    recent_pump_detected = return_z > 0.75 and distance_from_range > 0.5
    if recent_pump_detected and phase_v34 == "early":
        phase_v34 = "mid"

    # ── Section 8: Final score composition (spec-exact, no blending) ──────────
    # final_score = base_score + synergy_bonus + absorption*0.25
    #             + flow*0.25 + anomaly_score + vacuum*0.15
    pre_late_score = (
        base_score
        + synergy_bonus
        + tension_engine * 0.20
        + absorption_score * 0.25
        + flow_score * 0.25
        + anomaly_score
        + vacuum_score * 0.15
    )

    score = pre_late_score * late_scale * liquidity_norm

    # Amplification (Section 9 preserved)
    score = score ** 1.25

    # Final clamp 0-1 (Section 10)
    score = max(0.0, min(1.0, score))

    return score, {
        "base_score_nonlinear":  round(base_score, 4),
        "synergy_bonus":         round(synergy_bonus, 4),
        "tension_engine_contrib":round(tension_engine * 0.20, 4),
        "absorption_contrib":    round(absorption_score * 0.25, 4),
        "flow_contrib":          round(flow_score * 0.25, 4),
        "anomaly_score":         round(anomaly_score, 4),
        "vacuum_contrib":        round(vacuum_score * 0.15, 4),
        "return_z_norm":         round(return_z, 4),
        "price_change_z_raw":    round(price_change_z_raw, 4),
        "distance_from_range":   round(distance_from_range, 4),
        "liquidity_norm":        round(liquidity_norm, 4),
        "late_scale":            round(late_scale, 4),
        "phase_v34":             phase_v34,
        "recent_pump_detected":  recent_pump_detected,
    }



# ══════════════════════════════════════════════════════════════════════════════
#  📐  STANDARDIZED LOOKBACK CONSTANTS (v3.3)
#  All new modules use these exclusively — no mixing with other windows.
# ══════════════════════════════════════════════════════════════════════════════
LOOKBACK_SHORT = 12
LOOKBACK_MID   = 24
LOOKBACK_LONG  = 48


# ══════════════════════════════════════════════════════════════════════════════
#  🔬  TASK 1 — PHASE DETECTION
# ══════════════════════════════════════════════════════════════════════════════
def detect_phase(candles) -> str:
    n = len(candles)
    if n < LOOKBACK_LONG + 5:
        return "early"

    price_now = candles[-1]["close"]
    price_s   = candles[-LOOKBACK_SHORT]["close"]
    price_m   = candles[-LOOKBACK_MID]["close"]
    pc_short  = (price_now - price_s) / price_s if price_s > 0 else 0
    pc_mid    = (price_now - price_m) / price_m if price_m > 0 else 0

    atr_short = calc_atr(candles[-(LOOKBACK_SHORT + 3):], LOOKBACK_SHORT)
    atr_long  = calc_atr(candles[-(LOOKBACK_LONG  + 3):], LOOKBACK_LONG)
    atr_ratio = (atr_short / atr_long) if (atr_short and atr_long and atr_long > 0) else 1.0

    atr_ratio_history = []
    step = max(1, LOOKBACK_SHORT // 3)
    for i in range(LOOKBACK_LONG, min(n - LOOKBACK_SHORT - 3, LOOKBACK_LONG * 3), step):
        as_ = calc_atr(candles[-(i + LOOKBACK_SHORT + 3): -(i)], LOOKBACK_SHORT)
        al_ = calc_atr(candles[-(i + LOOKBACK_LONG  + 3): -(i)], LOOKBACK_LONG)
        if as_ and al_ and al_ > 0:
            atr_ratio_history.append(as_ / al_)

    if atr_ratio_history:
        atr_p33 = _percentile(atr_ratio_history, 33)
        atr_p66 = _percentile(atr_ratio_history, 66)
    else:
        atr_p33, atr_p66 = 0.7, 1.1

    recent_ranges  = [(c["high"] - c["low"]) / c["low"] for c in candles[-LOOKBACK_SHORT:] if c["low"] > 0]
    history_ranges = [(c["high"] - c["low"]) / c["low"] for c in candles[-LOOKBACK_LONG:-LOOKBACK_SHORT] if c["low"] > 0]
    avg_recent_range  = _mean(recent_ranges)
    avg_history_range = _mean(history_ranges) if history_ranges else avg_recent_range
    range_expansion_ratio = (avg_recent_range / avg_history_range) if avg_history_range > 0 else 1.0

    vols_recent = [c["volume_usd"] for c in candles[-LOOKBACK_SHORT:]]
    vols_prior  = [c["volume_usd"] for c in candles[-LOOKBACK_MID:-LOOKBACK_SHORT]]
    avg_vol_recent = _mean(vols_recent)
    avg_vol_prior  = _mean(vols_prior) if vols_prior else avg_vol_recent
    vol_acc_ratio  = (avg_vol_recent / avg_vol_prior) if avg_vol_prior > 0 else 1.0

    is_atr_low       = atr_ratio < atr_p33
    is_atr_expanding = atr_ratio > atr_p66
    is_range_wide    = range_expansion_ratio > 1.4
    is_vol_creeping  = 1.05 < vol_acc_ratio < 2.5
    is_vol_spiking   = vol_acc_ratio >= 2.5

    if abs(pc_mid) < 0.05 and is_atr_low and is_vol_creeping:
        return "early"
    if abs(pc_short) < 0.02 and not is_range_wide and not is_vol_spiking:
        return "early"
    if abs(pc_mid) > 0.10 and (is_atr_expanding or is_range_wide):
        return "late"
    if abs(pc_short) > 0.08 and is_vol_spiking and is_range_wide:
        return "late"
    return "mid"


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  TASK 2 — PRIORITY SCORE
# ══════════════════════════════════════════════════════════════════════════════
def compute_priority_score(score: float, phase: str,
                           structure_score: float,
                           distribution_flag: bool) -> float:
    STRUCTURE_STRONG_THRESHOLD = 10
    priority = float(score)
    if phase == "early":
        priority += 10
    elif phase == "late":
        priority -= 12
    if structure_score >= STRUCTURE_STRONG_THRESHOLD:
        priority += 5
    if distribution_flag:
        priority -= 10
    return round(max(0.0, priority), 2)


# ══════════════════════════════════════════════════════════════════════════════
#  🔀  TASK 3 — OVERLAP CONTROL PENALTY
# ══════════════════════════════════════════════════════════════════════════════
def overlap_penalty(compression_active: bool,
                    stealth_active: bool,
                    pre_breakout_active: bool) -> int:
    active_count = sum([compression_active, stealth_active, pre_breakout_active])
    if active_count == 3:
        return -8
    if active_count == 2:
        return -5
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  ⏰  TASK 4 — ENHANCED ANTI-LATE DETECTION
# ══════════════════════════════════════════════════════════════════════════════
def enhanced_late_penalty(candles) -> int:
    n = len(candles)
    if n < LOOKBACK_LONG + 3:
        return 0

    price_now = candles[-1]["close"]
    price_lb  = candles[-LOOKBACK_SHORT]["close"]
    pc_short  = abs((price_now - price_lb) / price_lb) if price_lb > 0 else 0

    atr_s = calc_atr(candles[-(LOOKBACK_SHORT + 3):], LOOKBACK_SHORT)
    atr_l = calc_atr(candles[-(LOOKBACK_LONG  + 3):], LOOKBACK_LONG)
    atr_expanded = (atr_s is not None and atr_l is not None and atr_l > 0
                    and atr_s > atr_l * 1.3)

    def avg_body(sl):
        b = [abs(c["close"] - c["open"]) / c["open"] for c in sl if c["open"] > 0]
        return _mean(b) if b else 0.0

    body_recent  = avg_body(candles[-LOOKBACK_SHORT:])
    body_history = avg_body(candles[-LOOKBACK_LONG:-LOOKBACK_SHORT])
    body_expanded = (body_history > 0 and body_recent > body_history * 1.5)

    late_signals = sum([pc_short > 0.06, atr_expanded, body_expanded])

    if late_signals >= 3:
        return -10
    if late_signals == 2:
        return -7
    if late_signals == 1 and pc_short > 0.06:
        return -5
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  💀  TASK 5 — DEAD MARKET PENALTY
# ══════════════════════════════════════════════════════════════════════════════
def dead_market_penalty(candles) -> int:
    n = len(candles)
    if n < LOOKBACK_LONG + 3:
        return 0

    vols = [c["volume_usd"] for c in candles[-LOOKBACK_MID:]]
    avg_v = _mean(vols)
    std_v = _std(vols)
    vol_cv = (std_v / avg_v) if avg_v > 0 else 0

    vols_long  = [c["volume_usd"] for c in candles[-LOOKBACK_LONG:]]
    avg_v_long = _mean(vols_long)
    std_v_long = _std(vols_long)
    vol_cv_long = (std_v_long / avg_v_long) if avg_v_long > 0 else 0
    vol_flat = vol_cv < vol_cv_long * 0.6

    atr_s = calc_atr(candles[-(LOOKBACK_SHORT + 3):], LOOKBACK_SHORT)
    atr_l = calc_atr(candles[-(LOOKBACK_LONG  + 3):], LOOKBACK_LONG)
    if atr_s is not None and atr_l is not None and atr_l > 0:
        atr_flat = 0.85 < (atr_s / atr_l) < 1.15
    else:
        atr_flat = False

    lows = [c["low"] for c in candles[-LOOKBACK_MID:]]
    higher_low_count = sum(1 for i in range(1, len(lows)) if lows[i] > lows[i - 1] * 1.001)
    no_higher_lows = higher_low_count < len(lows) * 0.30

    dead_signals = sum([vol_flat, atr_flat, no_higher_lows])
    if dead_signals >= 3:
        return -8
    if dead_signals == 2:
        return -4
    return 0



# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE v3.3
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    """
    v3.3 — Adds phase detection, priority score, overlap control,
    enhanced late penalty, dead market penalty.
    Original scoring logic untouched. Safety gates (data quality /
    liquidity) preserved. Signal-based return None replaced with penalties.
    """
    # ── Ambil candle 1H ──────────────────────────────────────────────────────
    c1h = get_candles(symbol, "1h", CONFIG["candle_limit_1h"])
    if len(c1h) < 72:
        return None   # DATA QUALITY — not a signal gate, cannot score without data

    try:
        chg_24h = float(ticker.get("change24h", 0)) * 100
        vol_24h = float(ticker.get("quoteVolume", 0))
        price   = float(ticker.get("lastPr", 0))
    except:
        return None   # DATA QUALITY — unparseable ticker

    if price <= 0:
        return None   # DATA QUALITY — zero price is invalid data

    # ── Liquidity gates → converted to heavy penalties (Task 6) ──────────────
    # Coins below min_vol are illiquid — penalise but don't skip entirely
    # so the scoring pipeline can still categorise them if needed.
    liquidity_penalty = 0
    if vol_24h < CONFIG["min_vol_24h"]:
        liquidity_penalty = 30   # very illiquid: heavy penalty
        log.info(f"  {symbol}: liquidity_penalty=-30 (vol=${vol_24h/1e3:.0f}K < min)")
    elif vol_24h > CONFIG["max_vol_24h"]:
        liquidity_penalty = 20   # mega-cap: likely not explosive mover
        log.info(f"  {symbol}: liquidity_penalty=-20 (vol=${vol_24h/1e6:.1f}M > max)")

    # ── Already-pumped gate → penalty (Task 6) ────────────────────────────────
    already_pumped_penalty = 0
    if chg_24h > CONFIG["gate_chg_24h_max"]:
        already_pumped_penalty = 25
        log.info(f"  {symbol}: already_pumped_penalty=-25 (chg={chg_24h:.1f}%)")

    # ── Funding gate → penalty (Task 6) ──────────────────────────────────────
    funding = get_funding(symbol)
    funding_gate_penalty = 0
    if funding < CONFIG["funding_gate"]:
        funding_gate_penalty = 15
        log.info(f"  {symbol}: funding_gate_penalty=-15 (funding={funding:.5f})")

    price_now = c1h[-1]["close"]

    # ── Cari compression zone ─────────────────────────────────────────────────
    compression = find_compression_zone(c1h)

    # ── [A] COMPRESSION + TENSION SCORE (HIGH PRIORITY, max 35) ──────────────
    comp_tension_score, comp_tension_details = score_compression_tension(c1h, compression)

    # ── [B] VOLUME INTELLIGENCE (MEDIUM, max 25) ──────────────────────────────
    vol_score, vol_details = score_volume_intelligence(c1h)
    rvol = vol_details.get("rvol", 1.0)

    # ── [C] STRUCTURE (MEDIUM, max 20) ───────────────────────────────────────
    struct_score, struct_details = score_structure(c1h, compression)

    # ── [D] CONTINUATION (MEDIUM, max 15) ─────────────────────────────────────
    cont_score, cont_details = score_continuation(c1h)

    # ── [E] STEALTH ACCUMULATION (BONUS, max 15) ──────────────────────────────
    stealth      = detect_stealth_accumulation(c1h)
    stealth_score_bonus = 0
    if stealth["detected"]:
        st = stealth["score"]
        # Interpolate: 70 → +10, 100 → +15
        t    = CONFIG["stealth_score_threshold"]
        ratio = max(0.0, min(1.0, (st - t) / (100.0 - t))) if (100.0 - t) > 0 else 1.0
        stealth_score_bonus = round(10 + ratio * 5)
    elif stealth["score"] >= 55:
        stealth_score_bonus = 5   # partial stealth = small bonus

    # ── [F] PRE-BREAKOUT BIAS (BONUS, max 12) — NEW v3.1 ─────────────────────
    pbb_score, pbb_details = score_pre_breakout_bias(c1h)

    # ── [G] DISTRIBUTION PENALTY (capped at 20 in v3.1) ──────────────────────
    dist_penalty, dist_details = calc_distribution_penalty(c1h)

    # ── [H] SLOW TREND PENALTY (capped at 10 in v3.1) ───────────────────────
    slow_penalty, slow_details = calc_slow_trend_penalty(c1h, compression)

    # ── [I] ANTI-LATE-ENTRY PENALTY (capped at 15 in v3.1) ───────────────────
    late_penalty, late_details = calc_late_entry_penalty(c1h)

    # ── RSI (v3.2: reduced to 1pt max — informational only, weak predictor) ───
    rsi       = get_rsi(c1h[-50:], 14)
    rsi_bonus = 1 if rsi < 35 else 0   # v3.2: was 3/2/1, now max 1

    # ── Liquidity sweep bonus ─────────────────────────────────────────────────
    comp_low    = compression["low"] if compression else price_now * 0.95
    liq_sweep   = detect_liquidity_sweep(c1h, comp_low)
    liq_bonus   = 8 if liq_sweep else 0

    # ══════════════════════════════════════════════════════════════════════════
    # ── NEW MODULES v3.3 ─────────────────────────────────────────────────────
    # ══════════════════════════════════════════════════════════════════════════

    # ── [J] PHASE DETECTION (Task 1) ─────────────────────────────────────────
    phase = detect_phase(c1h)
    phase_bonus   = 10  if phase == "early" else 0
    phase_penalty = 12  if phase == "late"  else 0

    # ── [K] ENHANCED LATE PENALTY (Task 4) ───────────────────────────────────
    # Additive with existing calc_late_entry_penalty (Task 6 constraint respected)
    enh_late_pen = enhanced_late_penalty(c1h)   # 0 to -10

    # ── [L] DEAD MARKET PENALTY (Task 5) ─────────────────────────────────────
    dead_pen = dead_market_penalty(c1h)          # 0 to -8

    # ── [M] OVERLAP CONTROL (Task 3) ─────────────────────────────────────────
    compression_active   = compression is not None
    stealth_active       = stealth.get("detected", False)
    pre_breakout_active  = pbb_score >= 6   # threshold: PBB signals are meaningful
    ovlp_pen = overlap_penalty(compression_active, stealth_active, pre_breakout_active)

    # ══════════════════════════════════════════════════════════════════════════
    # ── NEW MODULES v3.4 — NON-LINEAR ENGINE ─────────────────────────────────
    # ══════════════════════════════════════════════════════════════════════════

    # ── Normalise existing sub-scores to 0-1 ─────────────────────────────────
    ct_norm   = _norm_score_to_01(comp_tension_score, 40)   # max 40
    v_norm    = _norm_score_to_01(vol_score, 30)            # max 30
    s_norm    = _norm_score_to_01(max(0, struct_score), 20) # max 20, clamp neg
    pbb_norm  = _norm_score_to_01(pbb_score, 12)            # max 12

    # ── Liquidity norm (Section 1 — multiplicative, replaces additive penalty) ─
    liquidity_norm_v34 = compute_liquidity_norm(vol_24h, c1h)

    # ── Tension engine (Section 3 — multiplicative four-component) ────────────
    tension_engine_score, tension_engine_details = compute_tension_engine(
        c1h, compression, comp_tension_details
    )
    compression_norm_v34       = tension_engine_details.get("compression_norm", ct_norm)
    volume_stability_norm_v34  = tension_engine_details.get("volume_stability_norm", v_norm)

    # ── Absorption engine (Section 4 — hidden accumulation detector) ──────────
    absorption_score, absorption_details = compute_absorption_engine(c1h, compression)
    fbd_strength = absorption_details.get("fbd_strength", 0.0)  # already normalised 0-1

    # ── Flow engine (Section 1 — fetch real OI + CVD, wire in) ──────────────
    oi_series, cvd_series = get_oi_and_cvd(symbol, c1h)
    flow_score_v34, flow_details = compute_flow_engine(
        oi_series=oi_series,
        cvd_series=cvd_series,
    )

    # ── Vacuum score (Section 6 — orderbook, returns 0 if data unavailable) ───
    vacuum_score_v34, vacuum_details = compute_vacuum_score(
        ask_volume_near=None, rolling_mean_ask=None
    )

    # ── Non-linear composition (Sections 2, 7, 8, 9, 10) ─────────────────────
    v34_score_01, v34_composition = compose_v34_score(
        ct_norm              = ct_norm,
        v_norm               = v_norm,
        s_norm               = s_norm,
        tension_engine       = tension_engine_score,
        absorption_score     = absorption_score,
        flow_score           = flow_score_v34,
        vacuum_score         = vacuum_score_v34,
        liquidity_norm       = liquidity_norm_v34,
        failed_breakdown_strength = fbd_strength,
        pbb_norm             = pbb_norm,
        candles              = c1h,
        compression          = compression,
        phase_v33            = phase,
    )
    # Phase may be updated by v3.4 (Section 8 reclassification)
    phase = v34_composition.get("phase_v34", phase)

    # ── FINAL SCORE (Section 3 — no blending, v3.4 score is authoritative) ─────
    # REMOVED: prior version blended additive and non-linear scores 50/50.
    # Per Section 3: raw_score_additive is kept ONLY for logging transparency.
    raw_score_additive = (
        comp_tension_score
        + vol_score
        + struct_score
        + cont_score
        + stealth_score_bonus
        + pbb_score
        + rsi_bonus
        + liq_bonus
        + phase_bonus
        - phase_penalty
        - dist_penalty
        - slow_penalty
        - late_penalty
        + enh_late_pen
        + dead_pen
        + ovlp_pen
        - already_pumped_penalty
        - funding_gate_penalty
    )
    if funding < -0.001:
        raw_score_additive -= 5
    score_additive_01 = max(0.0, min(1.0, raw_score_additive / 100.0))  # for log only

    # v3.4 non-linear score is the sole final score (Section 3 requirement)
    score_01 = v34_score_01
    score = max(0, min(100, round(score_01 * 100)))

    # ── Confidence band ───────────────────────────────────────────────────────
    if score < CONFIG["score_min_output"]:
        confidence = "ignore"
    elif score < CONFIG["score_target_low"]:
        confidence = "early"
    elif score <= CONFIG["score_target_high"]:
        confidence = "strong"
    else:
        confidence = "possibly_late"

    # ── PRIORITY SCORE (Task 2) — separate field for final ranking ────────────
    distribution_flag = dist_penalty >= 8   # meaningful distribution detected
    priority_score = compute_priority_score(
        score, phase, struct_score, distribution_flag
    )

    # ── Score breakdown ────────────────────────────────────────────────────────
    score_breakdown = [
        f"Compression+Tension: +{comp_tension_score} ({comp_tension_details.get('comp_length','?')}h, range={comp_tension_details.get('range_pct',0)*100:.1f}%, tension={comp_tension_details.get('tension_score',0)})",
        f"Volume: +{vol_score} ({vol_details.get('label','')})",
        f"Structure: {struct_score:+d} ({', '.join(struct_details.get('breakdown',[]))[:50]})",
        f"Continuation: +{cont_score} ({cont_details.get('pullback_quality', 'no_prior_move')})" if cont_score else "Continuation: 0",
        f"PreBreakout: +{pbb_score} ({', '.join(pbb_details.get('flags',[])[:3])})" if pbb_score else "PreBreakout: 0",
        f"Stealth: +{stealth_score_bonus} (stealth_score={stealth['score']:.0f})" if stealth_score_bonus else "Stealth: 0",
        f"Phase: {phase} (bonus={phase_bonus} pen={phase_penalty})",
        f"[v3.4] Tension engine: {tension_engine_score:.3f} "
        f"(comp={compression_norm_v34:.2f} vstab={volume_stability_norm_v34:.2f} "
        f"fbd={tension_engine_details.get('fbd_count',0)} "
        f"tir={tension_engine_details.get('time_in_range_norm',0):.2f})",
        f"[v3.4] Absorption: {absorption_score:.3f} "
        f"(pcz={absorption_details.get('price_change_z_norm',0):.2f} "
        f"vz={absorption_details.get('volume_z_norm',0):.2f} "
        f"cvdz={absorption_details.get('cvd_z_norm',0):.2f} "
        f"fbd={absorption_details.get('fbd_count',0)})",
        f"[v3.5] Non-linear base: {v34_composition.get('base_score_nonlinear',0):.3f} "
        f"synergy: +{v34_composition.get('synergy_bonus',0):.3f} "
        f"anomaly: +{v34_composition.get('anomaly_score',0):.3f} "
        f"liq_norm: {liquidity_norm_v34:.3f}",
        f"[v3.5] final v34={v34_score_01:.3f} "
        f"(additive_ref={score_additive_01:.3f} — logging only, not blended)",
        f"RSI: +{rsi_bonus} (RSI={rsi:.0f})",
        f"LiqSweep: +{liq_bonus}" if liq_bonus else "",
        f"DistPenalty: -{dist_penalty} ({', '.join(dist_details.get('flags',[])[:3])})" if dist_penalty else "",
        f"SlowTrend: -{slow_penalty}" if slow_penalty else "",
        f"LateEntry(orig): -{late_penalty}" if late_penalty else "",
        f"LateEntry(enh): {enh_late_pen}" if enh_late_pen else "",
        f"DeadMarket: {dead_pen}" if dead_pen else "",
        f"Overlap: {ovlp_pen}" if ovlp_pen else "",
    ]
    score_breakdown = [s for s in score_breakdown if s]

    log.info(
        f"  {symbol}: Score={score} priority={priority_score} phase={phase} ({confidence}) "
        f"| CT={comp_tension_score} V={vol_score} S={struct_score} PBB={pbb_score} "
        f"| D=-{dist_penalty} Late={enh_late_pen} Dead={dead_pen} Ovlp={ovlp_pen}"
    )
    # ── Section 9: Mandatory v3.5 diagnostic logging ─────────────────────────
    buildup_score_log = flow_details.get("buildup_score", 0.0)
    fbd_strength_log  = absorption_details.get("fbd_strength", 0.0)
    anomaly_score_log = v34_composition.get("anomaly_score", 0.0)
    log.info(
        f"  {symbol} [v3.5] "
        f"compression_norm={compression_norm_v34:.3f} "
        f"volume_norm={v_norm:.3f} "
        f"structure_norm={s_norm:.3f} "
        f"tension_score={tension_engine_score:.3f} "
        f"absorption_score={absorption_score:.3f} "
        f"fbd_strength={fbd_strength_log:.3f} "
        f"flow_score={flow_score_v34:.3f} "
        f"buildup_score={buildup_score_log:.3f} "
        f"anomaly_score={anomaly_score_log:.3f} "
        f"vacuum_score={vacuum_score_v34:.3f} "
        f"liquidity_norm={liquidity_norm_v34:.3f} "
        f"v34_score={v34_score_01:.3f} "
        f"additive_ref={score_additive_01:.3f}"
    )

    # ── Output filter (Task 6: convert to penalty, not hard skip) ────────────
    # Coins below score_min_output get a low-score penalty applied rather than
    # being dropped, so they still appear in logs for diagnostics.
    if score < CONFIG["score_min_output"]:
        # Return with score recorded — run_scan will filter on priority_score
        # This keeps the full pipeline running with no hard skips on signals
        log.info(f"  {symbol}: score={score} below min_output — low priority")
        # Still build a minimal result so run_scan can track it
        return {
            "symbol":          symbol,
            "score":           score,
            "priority_score":  priority_score,
            "phase":           phase,
            "confidence":      "ignore",
            "compression":     compression,
            "vol_details":     vol_details,
            "struct_details":  struct_details,
            "cont_details":    cont_details,
            "stealth":         stealth,
            "entry":           None,
            "liq_sweep":       liq_sweep,
            "rsi":             rsi,
            "funding":         funding,
            "rvol":            round(rvol, 1),
            "price":           price_now,
            "chg_24h":         chg_24h,
            "vol_24h":         vol_24h,
            "rise_from_low":   0.0,
            "sector":          SECTOR_LOOKUP.get(symbol, "OTHER"),
            "urgency":         "⚫ BELOW THRESHOLD",
            "score_breakdown": score_breakdown,
            "dist_penalty":    dist_penalty, "slow_penalty": slow_penalty,
            "late_penalty":    late_penalty,
            "comp_tension_score": comp_tension_score,
            "vol_score":          vol_score,
            "struct_score":       struct_score,
            "cont_score":         cont_score,
            "pbb_score":          pbb_score,
            "pbb_details":        pbb_details,
            "stealth_bonus":      stealth_score_bonus,
            # v3.4 placeholders (not yet computed at this early-return point)
            "v34_score": 0.0, "v34_composition": {}, "tension_engine": 0.0,
            "tension_engine_details": {}, "absorption_score": 0.0,
            "absorption_details": {}, "flow_score": 0.0,
            "vacuum_score": 0.0, "liquidity_norm": 0.5,
            "compression_norm": 0.0, "volume_norm": 0.0, "structure_norm": 0.0,
            "flags": {
                "compression":  compression_active,
                "stealth":      stealth_active,
                "distribution": distribution_flag,
            },
        }

    # ── Entry & targets ────────────────────────────────────────────────────────
    comp_zone_for_entry = compression if compression else {
        "low": price_now * 0.95,
        "high": price_now,
        "length": 0,
        "avg_vol": 0,
    }
    entry_data = calc_entry_targets(c1h, comp_zone_for_entry)
    if not entry_data:
        # Entry calc failed: penalise score instead of dropping
        score = max(0, score - 15)
        entry_data = {
            "cur": price_now, "entry": price_now, "sl": price_now * 0.95,
            "sl_pct": 5.0, "t1": price_now * 1.10, "t2": price_now * 1.25,
            "t1_pct": 10.0, "t2_pct": 25.0, "rr": 1.0, "atr": price_now * 0.02,
            "z2": None,
        }
    if entry_data["t1_pct"] < CONFIG["min_target_pct"]:
        log.info(f"  {symbol}: T1={entry_data['t1_pct']:.1f}% < min — still included at lower score")
        score = max(0, score - 10)
    if entry_data["rr"] < CONFIG["min_rr"]:
        score = max(0, score - 8)

    # ── Urgency label ──────────────────────────────────────────────────────────
    vol_ratio = vol_details.get("vol_ratio", 1.0)
    comp_len  = compression["length"] if compression else 0
    if phase == "early" and comp_len >= 72:
        urgency = "🔵 EARLY PHASE — akumulasi aktif sebelum breakout"
    elif vol_ratio >= 4.0 and comp_len >= 168:
        urgency = "🔴 SANGAT TINGGI — mega spike + coil panjang, pump bisa dalam 1-3 jam"
    elif vol_ratio >= 2.5 or comp_len >= 168:
        urgency = "🟠 TINGGI — potensi pump dalam 6-24 jam"
    elif vol_ratio >= 1.2 and comp_len >= 72:
        urgency = "🟡 SEDANG — potensi pump dalam 12-48 jam"
    elif stealth["detected"]:
        urgency = "🔵 STEALTH — fase akumulasi diam-diam, breakout belum terjadi"
    else:
        urgency = "⚪ WATCH — sinyal awal, perlu konfirmasi"

    rise_from_low = 0.0
    if compression:
        rise_from_low = (price_now - compression["low"]) / compression["low"] if compression["low"] > 0 else 0

    return {
        "symbol":          symbol,
        "score":           score,
        "priority_score":  priority_score,          # Task 2 — used for ranking
        "phase":           phase,                   # Task 1 — "early"|"mid"|"late"
        "confidence":      confidence,
        "compression":     compression,
        "vol_details":     vol_details,
        "struct_details":  struct_details,
        "cont_details":    cont_details,
        "stealth":         stealth,
        "entry":           entry_data,
        "liq_sweep":       liq_sweep,
        "rsi":             rsi,
        "funding":         funding,
        "rvol":            round(rvol, 1),
        "price":           price_now,
        "chg_24h":         chg_24h,
        "vol_24h":         vol_24h,
        "rise_from_low":   rise_from_low,
        "sector":          SECTOR_LOOKUP.get(symbol, "OTHER"),
        "urgency":         urgency,
        "score_breakdown": score_breakdown,
        # Penalty details
        "dist_penalty":    dist_penalty,
        "slow_penalty":    slow_penalty,
        "late_penalty":    late_penalty,
        "enh_late_pen":    enh_late_pen,            # Task 4
        "dead_pen":        dead_pen,                # Task 5
        "ovlp_pen":        ovlp_pen,               # Task 3
        # Sub-scores
        "comp_tension_score": comp_tension_score,
        "vol_score":          vol_score,
        "struct_score":       struct_score,
        "cont_score":         cont_score,
        "pbb_score":          pbb_score,
        "pbb_details":        pbb_details,
        "stealth_bonus":      stealth_score_bonus,
        # v3.4 non-linear engine fields
        "v34_score":           round(v34_score_01, 4),
        "v34_composition":     v34_composition,
        "tension_engine":      round(tension_engine_score, 4),
        "tension_engine_details": tension_engine_details,
        "absorption_score":    round(absorption_score, 4),
        "absorption_details":  absorption_details,
        "flow_score":          round(flow_score_v34, 4),
        "vacuum_score":        round(vacuum_score_v34, 4),
        "liquidity_norm":      round(liquidity_norm_v34, 4),
        "compression_norm":    round(compression_norm_v34, 4),
        "volume_norm":         round(v_norm, 4),
        "structure_norm":      round(s_norm, 4),
        # Task 6 — required output structure
        "flags": {
            "compression":  compression_active,
            "stealth":      stealth_active,
            "distribution": distribution_flag,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER v3.0
# ══════════════════════════════════════════════════════════════════════════════
def _confidence_emoji(conf):
    return {"ignore": "⚫", "early": "🟡", "strong": "🟢", "possibly_late": "🟠"}.get(conf, "⚪")

def _phase_emoji(phase):
    return {"early": "🔵", "mid": "🟡", "late": "🔴"}.get(phase, "⚪")

def build_alert(r, rank=None):
    sc    = r["score"]
    conf  = r["confidence"]
    phase = r.get("phase", "?")
    prio  = r.get("priority_score", sc)
    bar   = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    e     = r["entry"]
    comp  = r["compression"]
    rk    = f"#{rank} " if rank else ""
    vol   = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
             else f"${r['vol_24h']/1e3:.0f}K")

    comp_str = "N/A"
    if comp:
        comp_days = comp["length"] / 24
        comp_str  = f"{comp_days:.0f} hari" if comp_days >= 1 else f"{comp['length']} jam"

    # Sub-scores summary (v3.3: includes phase and new penalties)
    enh_l = r.get("enh_late_pen", 0)
    dead  = r.get("dead_pen", 0)
    ovlp  = r.get("ovlp_pen", 0)
    sub = (
        f"  CT:{r['comp_tension_score']} V:{r['vol_score']} "
        f"S:{r['struct_score']:+d} PBB:{r.get('pbb_score',0)} St:{r['stealth_bonus']}\n"
        f"  D:-{r['dist_penalty']} La:-{r['late_penalty']} EnhL:{enh_l} Dead:{dead} Ovlp:{ovlp}"
    )

    # Stealth section
    stealth     = r.get("stealth", {})
    stealth_str = ""
    if stealth.get("detected"):
        ss = stealth["score"]
        stealth_str = (
            f"\n🟦 <b>STEALTH MODE</b> (Blue Box)\n"
            f"  Score:{ss:.0f}  ATR:{stealth.get('atr_trend','?')}  "
            f"CV:{stealth.get('volume_stability',0):.2f}  "
            f"Struct:{stealth.get('structure_quality','?')}\n"
            f"  Trigger: {'✅ BREAKOUT' if stealth.get('entry_trigger') else '⏳ waiting'}\n"
        )
    elif stealth.get("score", 0) >= 55:
        stealth_str = f"\n🔷 <b>Stealth Watch</b> ({stealth.get('score',0):.0f}/100)\n"

    # Forensic
    forensic_items = []
    sd = r.get("struct_details", {})
    if sd.get("higher_lows"):   forensic_items.append("Higher lows ✅")
    if r.get("liq_sweep"):      forensic_items.append("Liq sweep ✅")
    pbb_flags = r.get("pbb_details", {}).get("flags", [])
    if pbb_flags:
        forensic_items.append(f"PreBreakout: {', '.join(pbb_flags[:3])} 🔵")
    cd = r.get("cont_details", {})
    if r.get("cont_score", 0) > 0:
        forensic_items.append(f"Continuation ✅ ({cd.get('pullback_quality','?')})")
    forensic_str = "  " + " · ".join(forensic_items) if forensic_items else "  — sinyal masih awal"

    # Distribution warning
    dist_warn = ""
    df = r.get("dist_details", {})
    if r.get("dist_penalty", 0) >= 20:
        dist_str = ", ".join(r.get("dist_details", {}).get("flags", ["?"])[:3])
        dist_warn = f"\n⚠️ <b>DISTRIBUSI TERDETEKSI:</b> {dist_str}\n"

    msg = (
        f"🚀 <b>PRE-PUMP SIGNAL {rk}— v4.0</b>\n\n"
        f"<b>Symbol    :</b> {r['symbol']} [{r['sector']}]\n"
        f"<b>Skor      :</b> {sc}/100  {_confidence_emoji(conf)} {conf.upper()}\n"
        f"<b>Phase     :</b> {_phase_emoji(phase)} {phase.upper()}  |  Priority: {prio:.0f}\n"
        f"<b>Bar       :</b> {bar}\n"
        f"<b>Urgency   :</b> {r['urgency']}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>SUB-SCORES</b>\n"
        f"{sub}\n"
        f"  CT=Compression+Tension  V=Volume  S=Structure\n"
        f"  PBB=Pre-Breakout  St=Stealth  D=Dist\n"
        f"  La=Late(orig)  EnhL=Late(enh)  Dead  Ovlp=Overlap\n"
        f"\n📐 <b>DATA-DRIVEN VARS (v4.0)</b>\n"
        f"  DFL96:{sd.get('dfl96', 0):.4f}  DFL32:{sd.get('dfl32', 0):.4f}\n"
        f"  Vol↑Day:{r.get('vol_details',{}).get('vol_upday_ratio', 0):.3f}\n"
        f"\n📦 <b>COMPRESSION ZONE</b>\n"
        f"  Durasi : {comp_str}\n"
    )
    if comp:
        msg += (
            f"  Range  : {comp['range_pct']*100:.1f}% "
            f"(${comp['low']:.6g}–${comp['high']:.6g})\n"
            f"  Harga  : ${r['price']:.6g} (+{r['rise_from_low']*100:.1f}% dari low)\n"
        )

    # Flags summary (required output structure — Task 6)
    flg = r.get("flags", {})
    flags_str = (
        f"  Comp:{('✅' if flg.get('compression') else '❌')}  "
        f"Stealth:{('✅' if flg.get('stealth') else '❌')}  "
        f"Dist:{('⚠️' if flg.get('distribution') else '✅')}\n"
    )

    vd = r.get("vol_details", {})
    msg += (
        f"\n⚡ <b>VOLUME</b>\n"
        f"  Ratio  : {vd.get('vol_ratio',0):.2f}x avg20  |  RVOL: {r['rvol']:.1f}x\n"
        f"  Acc    : {vd.get('vol_acceleration',0):+.2f}  |  Vol24H: {vol}\n"
        f"\n📊 <b>TEKNIKAL</b>\n"
        f"  RSI    : {r['rsi']:.0f}  |  Chg24H: {r['chg_24h']:+.1f}%\n"
        f"  Candle : {sd.get('candle_label','?')}\n"
        f"  Funding: {r['funding']:.5f}\n"
        f"\n🏷 <b>FLAGS</b>\n"
        f"{flags_str}"
        f"\n🔬 <b>SINYAL AKUMULASI</b>\n"
        f"{forensic_str}\n"
        f"{stealth_str}"
        f"{dist_warn}"
    )

    if e:
        poc_line  = f"  POC   : ${e['z2']}\n" if e.get("z2") else ""
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY &amp; TARGET</b>\n"
            f"{poc_line}"
            f"  Entry : ${e['entry']}\n"
            f"  SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n"
            f"  T1    : ${e['t1']}  (+{e['t1_pct']:.1f}%)\n"
            f"  T2    : ${e['t2']}  (+{e['t2_pct']:.1f}%)\n"
            f"  R/R   : 1:{e['rr']}  |  ATR: ${e['atr']}\n"
        )

    # Score breakdown
    msg += (
        f"\n🔢 <b>BREAKDOWN:</b>\n"
        + "\n".join(f"  {s}" for s in r["score_breakdown"][:8])
        + f"\n\n🕐 {utc_now()}\n<i>⚠️ Bukan financial advice. DYOR.</i>"
    )
    return msg


def build_summary(results):
    msg  = f"📋 <b>PRE-PUMP WATCHLIST v4.0 — {utc_now()}</b>\n{'━'*30}\n"
    for i, r in enumerate(results, 1):
        comp  = r["compression"]
        vol   = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                 else f"${r['vol_24h']/1e3:.0f}K")
        comp_str   = f"{comp['length']}h" if comp else "?"
        conf_emoji = _confidence_emoji(r["confidence"])
        phase_emoji = _phase_emoji(r.get("phase", "mid"))
        prio = r.get("priority_score", r["score"])
        vd = r.get("vol_details", {})
        entry = r.get("entry")
        t1_str = f"+{entry['t1_pct']:.0f}%" if entry else "?"
        msg += (
            f"{i}. <b>{r['symbol']}</b> "
            f"[S:{r['score']} P:{prio:.0f} {phase_emoji}{r.get('phase','?')} {conf_emoji}]\n"
            f"   Coil:{comp_str} · Vol:{vd.get('vol_ratio',0):.1f}x · T1:{t1_str} · {vol}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    candidates = []
    not_found  = []
    stats      = defaultdict(int)

    log.info("=" * 70)
    log.info(f"🔍 SCANNING {len(WHITELIST_SYMBOLS)} coin — PRE-PUMP v4.0 (DATA-DRIVEN VARS)")
    log.info("=" * 70)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:
            stats["manual_exclude"] += 1
            continue
        if is_cooldown(sym):
            stats["cooldown"] += 1
            continue
        if sym not in tickers:
            not_found.append(sym)
            continue

        t = tickers[sym]
        try:
            vol   = float(t.get("quoteVolume", 0))
            chg   = float(t.get("change24h",   0)) * 100
            price = float(t.get("lastPr",       0))
        except:
            stats["parse_error"] += 1
            continue

        # Only hard pre-filter: noise coins and extreme already-moved coins
        if vol < CONFIG["pre_filter_vol"]:
            stats["vol_too_low"] += 1
            continue
        if vol > CONFIG["max_vol_24h"]:
            stats["vol_too_high"] += 1
            continue
        if chg > CONFIG["gate_chg_24h_max"]:
            stats["change_extreme"] += 1
            continue
        if price <= 0:
            stats["invalid_price"] += 1
            continue

        candidates.append((sym, t))

    log.info(f"\n📊 Pre-filter: {len(candidates)}/{len(WHITELIST_SYMBOLS)} coin akan di-scan")
    log.info(f"   Cooldown: {stats['cooldown']} | Vol rendah: {stats['vol_too_low']} | "
             f"Vol tinggi: {stats['vol_too_high']} | Chg ekstrem: {stats['change_extreme']}")
    if not_found:
        log.info(f"   Tidak di Bitget: {len(not_found)} coin")
    log.info(f"   ⏱️  Est. waktu: ~{len(candidates) * CONFIG['sleep_coins'] / 60:.1f} menit")
    log.info("=" * 70)

    return candidates


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v4.0 — {utc_now()} ===")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total ticker Bitget: {len(tickers)}")

    candidates = build_candidate_list(tickers)
    all_results = []   # semua coin yang punya score >= min_output (termasuk weak)

    for i, (sym, t) in enumerate(candidates):
        log.info(f"[{i+1}/{len(candidates)}] {sym} ...")
        try:
            res = master_score(sym, t)
            if res:
                all_results.append(res)
                log.info(
                    f"  ✅ SCORED! Score={res['score']} ({res['confidence']}) "
                    f"CT={res['comp_tension_score']} V={res['vol_score']} "
                    f"S={res['struct_score']} C={res['cont_score']}"
                )
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}", exc_info=True)

        time.sleep(CONFIG["sleep_coins"])

    # Sort by priority_score DESC (Task 2 — early phases float to top)
    all_results.sort(key=lambda x: x["priority_score"], reverse=True)

    log.info(f"\n{'='*70}")
    log.info(f"✅ Total coin scored: {len(all_results)}")
    conf_counts  = defaultdict(int)
    phase_counts = defaultdict(int)
    for r in all_results:
        conf_counts[r["confidence"]] += 1
        phase_counts[r["phase"]] += 1
    log.info(f"   Confidence — Strong: {conf_counts['strong']} | Early: {conf_counts['early']} | Late: {conf_counts['possibly_late']}")
    log.info(f"   Phase      — Early: {phase_counts['early']} | Mid: {phase_counts['mid']} | Late: {phase_counts['late']}")
    log.info(f"{'='*70}\n")

    if not all_results:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    # Filter: only send coins with non-ignore confidence AND valid entry
    sendable = [r for r in all_results if r["confidence"] != "ignore" and r.get("entry")]

    # Prioritize phase=="early" in the top_list (Task 2)
    early_ph = [r for r in sendable if r["phase"] == "early"]
    others_ph = [r for r in sendable if r["phase"] != "early"]
    top_list = (early_ph + others_ph)[:CONFIG["max_alerts_per_run"]]

    if len(top_list) >= 2:
        send_telegram(build_summary(top_list))
        time.sleep(2)

    for rank, r in enumerate(top_list, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"📤 Alert #{rank}: {r['symbol']} Score={r['score']} ({r['confidence']})")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top_list)} alert dikirim — {utc_now()} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v4.0 — DATA-DRIVEN UPDATE       ║")
    log.info("║  +VAR-1: dist_from_low_96 (AUROC=0.696) → struct  ║")
    log.info("║  +VAR-2: dist_from_low_32 (AUROC=0.672) → struct  ║")
    log.info("║  +VAR-3: range_pct_4/8 expansion → PBB            ║")
    log.info("║  +VAR-4: atr_pct_32 relatif → tension scoring     ║")
    log.info("║  +VAR-5: vol_upday_ratio_16 → volume intel        ║")
    log.info("║  +VAR-6: bb_width expansion → stealth + PBB       ║")
    log.info("║  +VAR-7: lower_wick_avg_16 → candle structure     ║")
    log.info("║  +VAR-8: pos_in_range_96 → structure positional   ║")
    log.info("╚════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)

    run_scan()
