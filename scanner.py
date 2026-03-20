"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v3.7 — AUDIT-CORRECTED                                    ║
║                                                                              ║
║  FILOSOFI CORE:                                                              ║
║  Deteksi SEBELUM breakout. Bukan setelah. Bukan saat.                        ║
║  Noise lebih tinggi diterima — miss lebih mahal dari false positive.         ║
║                                                                              ║
║  PERBAIKAN v3.7 (dari hasil audit v3.5):                                     ║
║                                                                              ║
║  BUG #1 FIXED — Flow Engine (OI):                                            ║
║    Sebelum: OI API single-point → flat series → all-zero changes →           ║
║    buildup_score & squeeze_score SELALU 0.                                   ║
║    Sesudah: Jika OI API tidak memberi series, gunakan OI proxy dari          ║
║    signed candle volume (realistic delta, bukan flat replika).               ║
║                                                                              ║
║  BUG #2 FIXED — Absorption Floor Phantom:                                   ║
║    Sebelum: fbd_depth=0 (no events) → fbd_depth_term=1.0 →                  ║
║    absorption_score min 0.27 bahkan tanpa satu pun failed breakdown.         ║
║    Sesudah: fbd_depth=0.5 (neutral) ketika tidak ada events.                ║
║                                                                              ║
║  BUG #3 FIXED — CRITICAL: Dead Penalty/Bonus Block:                         ║
║    Sebelum: dist_penalty, phase_bonus/penalty, late_penalty,                 ║
║    slow_penalty, stealth_bonus, cont_score, liq_bonus, overlap,              ║
║    funding_penalty, liquidity_penalty — SEMUA masuk raw_score_additive       ║
║    yang "logging only" → tidak ada efek ke final score.                      ║
║    Sesudah: Semua dikompilasi ke working_score sebelum final clamp.          ║
║    Distribution filter sekarang BENAR-BENAR mengurangi skor.                ║
║                                                                              ║
║  BUG #4 FIXED — Double Clamp Score Suppression:                             ║
║    Sebelum: compose_v34_score clamp(0,1) + score_with_pressure clamp(0,1)   ║
║    → pressure_score tidak bisa berkontribusi untuk coin kuat.                ║
║    Sesudah: Satu final clamp di akhir, di ruang 0-100.                      ║
║                                                                              ║
║  BUG #5 FIXED — v_norm ceiling mismatch:                                    ║
║    Sebelum: v_norm normalisasi dengan max=20 padahal vol_score max=30.      ║
║    Sesudah: max=30 sesuai score_volume_intelligence ceiling.                 ║
║                                                                              ║
║  KOMPONEN SCORING (v3.7):                                                    ║
║  Non-linear base (compose_v34_score) → 0–1 scale (×100 ke working space)   ║
║  [+] Pressure layer                   — up to +35 pts                       ║
║  [+] Stealth Accumulation             — up to +15 pts                       ║
║  [+] Liquidity Sweep bonus            — +8 pts                              ║
║  [+] Phase early bonus                — +10 pts                             ║
║  [+] Continuation (partial)           — up to +7.5 pts                      ║
║  [-] Phase late penalty               — −12 pts                             ║
║  [-] Distribution penalty (ACTIVE)   — up to −25 pts                       ║
║  [-] Slow trend penalty               — up to −10 pts                       ║
║  [-] Late entry original              — up to −15 pts                       ║
║  [-] Enhanced late penalty            — up to −10 pts                       ║
║  [-] Dead market penalty              — up to −8 pts                        ║
║  [-] Overlap penalty                  — up to −8 pts                        ║
║  [-] Funding penalty                  — −15 / −5 pts                        ║
║  [-] Liquidity penalty                — −20 / −30 pts                      ║
║  [-] Already pumped penalty           — −25 pts                             ║
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

    # ── ENTRY / TARGET ────────────────────────────────────────────────────────
    "atr_sl_mult":                 1.2,
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
def analyze_candle_structure(candle):
    body   = abs(candle["close"] - candle["open"])
    rng    = candle["high"] - candle["low"]
    if rng == 0:
        return 0, "doji"

    lower_wick = min(candle["open"], candle["close"]) - candle["low"]
    body_pct   = body / rng
    lwick_pct  = lower_wick / rng

    if lwick_pct > 0.50 and body_pct < 0.35:
        return 15, "bullish rejection wick"
    if lwick_pct > 0.40:
        return 12, "hammer/pin bar"
    if body_pct < 0.15:
        return 8, "doji (indecision)"
    if candle["close"] > candle["open"]:
        return 5, "green candle"
    return 2, "red candle"

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(candles, compression_zone):
    cur  = candles[-1]["close"]
    atr  = calc_atr(candles[-48:], 14) or cur * 0.025

    comp_mid = (compression_zone["high"] + compression_zone["low"]) / 2
    comp_low  = compression_zone["low"]
    comp_high = compression_zone["high"]

    # v3.9 MODULE 7: PRE-BREAKOUT ENTRY LOGIC
    # Old logic: entry = min(cur, comp_high * 1.005) — waits near top of range
    # New logic: if price is INSIDE compression, enter at current price (or slight discount)
    #            This enables positioning BEFORE the breakout, not after confirmation.
    price_in_compression = comp_low <= cur <= comp_high
    if price_in_compression:
        # Inside zone: enter at current price with small discount for limit order
        entry = cur * 0.998
    else:
        # Above zone (already breaking out): revert to confirmation entry
        entry = min(cur * 0.999, comp_high * 1.005)

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
    """
    v3.9 FIX: Original used 4 candles with 1.5% tolerance.
    With ATR=1%, random walk creates 'higher lows' 75% of the time on ANY coin.
    
    Fixed requirements:
    1. Use 8-candle lookback (not 4) — reduces noise sensitivity
    2. Require net upward slope: last low > first low by >0.3% (trend, not noise)
    3. Price POSITION check: higher lows are only bullish when price is at/below
       the compression zone midpoint — NOT when price is already elevated (distribution)
    """
    # Use wider lookback for noise resistance
    effective_lookback = max(lookback, 8)
    if len(candles) < effective_lookback:
        return False

    recent = candles[-effective_lookback:]
    lows   = [c["low"] for c in recent]

    # Requirement 1: No lower low (original check, now over 8 candles)
    for i in range(1, len(lows)):
        if lows[i] < lows[i-1] * 0.985:
            return False

    # Requirement 2: Net upward trend in lows (not just flat/noise)
    if lows[0] > 0:
        net_slope = (lows[-1] - lows[0]) / lows[0]
        if net_slope < 0.003:   # must rise at least 0.3% over the window
            return False

    # Requirement 3: Price position — higher lows are bullish ONLY near lows.
    # If current price is >15% above the 8-candle low range, it's distribution.
    low_range_floor = min(lows)
    cur_price = candles[-1]["close"]
    if low_range_floor > 0:
        price_above_range = (cur_price - low_range_floor) / low_range_floor
        if price_above_range > 0.15:
            # Price too far above the lows — higher lows are from OLD compression,
            # current price is at distribution level
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
    stealth_score = round(min(100.0, stealth_score), 2)

    threshold = cfg.get("stealth_score_threshold", 70)
    detected  = stealth_score >= threshold
    status    = "STEALTH_ACCUMULATION_DETECTED" if detected else "NONE"

    # BUG FIX v3.9: Stealth fires on distribution coins (flat price + stable high vol
    # = high score_vol_suppress + score_price_compress). Add distribution context check:
    # Stealth is NOT valid when volume is ELEVATED (>2x historical baseline).
    # Real stealth accumulation has LOW/DECLINING volume, not steady high volume.
    if detected:
        # Check: is the current vol level elevated vs 60-candle baseline?
        baseline_window = confirmed[-60:] if len(confirmed) >= 60 else confirmed
        baseline_vols = [c.get("volume_usd", c.get("volume", 0)) for c in baseline_window[:-len(window)]] if len(baseline_window) > len(window) else []
        if baseline_vols:
            baseline_avg = _mean(baseline_vols)
            current_avg  = avg_vol_window
            vol_elevation = current_avg / baseline_avg if baseline_avg > 0 else 1.0
            if vol_elevation > 2.0:
                # Volume is elevated 2x+ — this is NOT stealth, it's active trading
                detected = False
                status   = f"STEALTH_INVALIDATED(vol_elevation={vol_elevation:.1f}x)"
                stealth_score = max(0.0, stealth_score - 25.0)
            elif vol_elevation > 1.5:
                # Elevated but not extreme — reduce score, don't fully invalidate
                stealth_score = max(0.0, stealth_score - 10.0)
                detected = stealth_score >= threshold
                if not detected:
                    status = "NONE"

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

    # ── v3.9 FIX: PRICE POSITION CONTEXT ─────────────────────────────────────
    # Low volume means different things at different price levels:
    #   Low vol at LOWS  = accumulation (smart money quietly buying) → BULLISH
    #   Low vol at HIGHS = distribution exhaustion (supply absorbed, less selling)
    #                      OR dead top (no buyers left) → BEARISH/NEUTRAL
    #
    # Check: is current price near the 100-candle HIGH or LOW?
    # If near high → discount low-vol score heavily
    if len(candles) >= 50:
        highs100 = [c["high"] for c in candles[-100:]] if len(candles) >= 100 else [c["high"] for c in candles]
        lows100  = [c["low"]  for c in candles[-100:]] if len(candles) >= 100 else [c["low"]  for c in candles]
        h100 = max(highs100)
        l100 = min(lows100)
        price_range = h100 - l100
        cur_close = candles[-1]["close"]

        if price_range > 0:
            # Position within 100-candle range: 0.0 = at low, 1.0 = at high
            price_position = (cur_close - l100) / price_range

            if price_position > 0.80 and vol_ratio < 0.8:
                # Price at top 20% of range + declining vol = distribution exhaustion
                # NOT accumulation. Reduce score significantly.
                vol_score = max(0, vol_score - 12)
                vol_label += f" [TOP_DIST_zone:{price_position:.2f}]"
            elif price_position > 0.65 and vol_ratio < 0.5:
                # Price in upper third + low vol = likely post-pump
                vol_score = max(0, vol_score - 6)
                vol_label += f" [upper_zone:{price_position:.2f}]"
            elif price_position < 0.30 and vol_ratio < 0.7:
                # Price near lows + low vol = genuine accumulation zone → boost
                vol_score = min(30, vol_score + 3)
                vol_label += f" [accum_zone:{price_position:.2f}]"

    return vol_score, {
        "vol_ratio":        round(vol_ratio, 3),
        "vol_acceleration": round(vol_acceleration, 3),
        "rvol":             round(rvol, 2),
        "rvol_score":       rvol_score,
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
    total = min(total, 40)   # v3.2: raised from 35 → 40

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
    Score structure signals. Max 20 pts.
    """
    score = 0
    breakdown = []

    # Higher lows
    higher_lows = detect_higher_lows(candles, lookback=4)
    if higher_lows:
        score += 10
        breakdown.append("higher_lows +10")

    # Flat base (compression stability already in compression score)
    # Check if price acceleration is present
    if detect_price_acceleration(candles, lookback=6):
        score += 5
        breakdown.append("price_accel +5")

    # Failed breakdown bonus — v3.2: raised 12→18 (strongest smart-money signal)
    if compression:
        failed_bd = detect_failed_breakdown(candles, compression["low"], lookback=20)
        if failed_bd:
            score += 18
            breakdown.append("failed_breakdown +18")

    # Candle structure — v3.2: ceiling reduced 15→8 (noisy signal, easily faked)
    candle_score, candle_label = analyze_candle_structure(candles[-1])
    candle_score = min(candle_score, 8)   # hard cap
    score += candle_score
    breakdown.append(f"candle:{candle_label} +{candle_score}")

    # Pre-spike candle
    pre_spike_sc, pre_spike_label = detect_pre_pump_candle(candles)
    score += pre_spike_sc
    if pre_spike_sc > 0:
        breakdown.append(f"pre_spike:{pre_spike_label} +{pre_spike_sc}")

    # Lower highs penalty
    if len(candles) >= 6:
        highs = [candles[-i]["high"] for i in range(1, 7)]
        lower_high_count = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1] * 0.98)
        if lower_high_count >= 4:
            score -= 10
            breakdown.append("lower_highs_pattern -10")

    score = max(-20, min(score, 20))

    return score, {
        "score":          score,
        "breakdown":      breakdown,
        "higher_lows":    higher_lows,
        "candle_label":   candle_label,
        "pre_spike_label": pre_spike_label,
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

    # ── MINIMUM LIQUIDITY GATE ────────────────────────────────────────────────
    # v3.7 FIX: Distribution signal is meaningless on ultra-illiquid coins.
    # If median vol < $10K/candle, signals are statistical noise not real distribution.
    MIN_DIST_VOL_USD = 10_000   # $10K minimum median candle volume for dist to fire
    if vol_median < MIN_DIST_VOL_USD:
        return 0, {"reason": f"illiquid_vol_median=${vol_median:.0f} < ${MIN_DIST_VOL_USD}"}

    # ── CONTEXT CHECK: tight price range = accumulation zone, not distribution ──
    # If overall price hasn't moved much in the window, "high vol no progress" 
    # candles are likely accumulation noise, not smart money distribution.
    price_range_pct = abs(max(c["high"] for c in recent) - min(c["low"] for c in recent)) / min(c["low"] for c in recent) if min(c["low"] for c in recent) > 0 else 1.0
    is_tight_range = price_range_pct < 0.08   # less than 8% range = compression zone

    penalty = 0
    flags   = []

    # ── OVERALL WINDOW PRICE DIRECTION ────────────────────────────────────────
    # v3.7 FIX: If price is clearly UP over the 10-candle window, this is NOT
    # distribution — it's a breakout/pump. Distribution = high vol + price flat or DOWN.
    # Skip Track A entirely if overall move is positive and significant.
    overall_move = (price_now - price_10a) / price_10a if price_10a > 0 else 0
    is_breakout_up = overall_move > 0.04   # price up >4% in 10 candles = pump, not dist

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

    # v3.7 FIX: Skip Track A if price clearly broke out upward (pump, not distribution)
    # Also: reduce Track A significantly in tight range zones (could be accumulation)
    if not is_breakout_up:
        if high_vol_no_prog >= 3:
            base_pen = 25 if not is_tight_range else 10
            penalty += base_pen
            flags.append(f"hvnp:{high_vol_no_prog}x (severe)")
        elif high_vol_no_prog >= 2:
            base_pen = 18 if not is_tight_range else 6
            penalty += base_pen
            flags.append(f"hvnp:{high_vol_no_prog}x")
        elif high_vol_no_prog >= 1:
            base_pen = 8 if not is_tight_range else 2
            penalty += base_pen
            flags.append(f"hvnp:1x")

        # High volume + red candles compound the penalty (only outside tight range)
        if high_vol_red >= 2 and high_vol_no_prog >= 1 and not is_tight_range:
            penalty += 7
            flags.append(f"hvred:{high_vol_red}x")
    else:
        flags.append(f"track_a_skipped(breakout_up={overall_move*100:.1f}%)")

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

    # ── TRACK C: Stable elevated volume distribution (BUG FIX v3.9) ───────────
    # Track A only fires on vol SPIKES (>3x median). But real distribution often
    # has STEADILY HIGH volume without individual spikes — smart money distributing
    # over many candles. This was the core gap causing false pre-pump signals.
    track_c = 0
    if not is_breakout_up:
        # Compare current 10-candle avg vol vs baseline 30-candle avg
        baseline_vol = _mean([c["volume_usd"] for c in candles[-30:-10]]) if len(candles) >= 30 else avg_vol
        elevated_vol_ratio = avg_vol / baseline_vol if baseline_vol > 0 else 1.0

        if elevated_vol_ratio >= 2.0 and overall_move <= 0.01:
            # Vol is 2x+ elevated but price going nowhere or down = distribution
            # Scale: 2x=8pts, 3x=14pts, 4x+=20pts (capped at 20)
            track_c = min(20, int((elevated_vol_ratio - 1.0) * 8))
            flags.append(f"stable_elevated_vol({elevated_vol_ratio:.1f}x_no_progress)")
        elif elevated_vol_ratio >= 1.5 and overall_move < -0.02:
            # Elevated vol + price declining = clear distribution
            track_c = 10
            flags.append(f"elevated_vol_price_down({elevated_vol_ratio:.1f}x)")

    penalty += min(track_c, 20)   # cap track C at 20

    cap = CONFIG.get("dist_penalty_cap", 25)   # v3.2: raised cap 20→25
    # v3.9: raise total cap to 35 — stable distribution must be properly penalized
    cap = 35
    penalty = min(penalty, cap)
    return penalty, {"penalty": penalty, "flags": flags,
                     "high_vol_no_prog": high_vol_no_prog, "upper_wick_count": upper_wick_count,
                     "track_c": track_c}

# ══════════════════════════════════════════════════════════════════════════════
#  🐢  SLOW TREND PENALTY
#  Anti-drift: avoid smooth trends without compression
# ══════════════════════════════════════════════════════════════════════════════
def calc_slow_trend_penalty(candles, compression):
    """
    Penalize coins that are just drifting up/down without tension.
    v3.7: also penalizes trending compression (compression window itself is sloping).
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

    # Compression zone exists — if yes, no slow-trend penalty UNLESS the
    # compression itself is trending (price moved significantly within the zone window)
    has_compression = compression is not None

    # v3.7 FIX: Check if price moved significantly WITHIN the compression window.
    # A real compression zone has price go NOWHERE (tight base).
    # A slow trend has price drifting consistently upward even inside the "zone".
    trending_compression = False
    if has_compression and compression["length"] >= 20:
        comp_len = min(compression["length"], len(candles) - 1)
        price_at_comp_start = candles[-comp_len]["close"]
        price_now_comp      = candles[-1]["close"]
        if price_at_comp_start > 0:
            comp_price_move = abs(price_now_comp - price_at_comp_start) / price_at_comp_start
            # If price moved >8% during the compression window, it's a trend, not a base
            if comp_price_move > 0.08 and trending:
                trending_compression = True

    if trending and no_compression and not has_compression:
        return 10, {"reason": "smooth trend without compression"}
    elif trending and no_compression and has_compression and compression["length"] < 30:
        return 5, {"reason": "short compression + trending"}
    elif trending_compression:
        # v3.7: trending within compression = not a real base = penalize
        comp_move_pct = abs(price_now_comp - price_at_comp_start) / price_at_comp_start
        return 8, {"reason": f"trending_compression (moved {comp_move_pct*100:.1f}% in zone)"}
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

    if top_count >= win * 0.67:      # ≥67% candle tutup di bagian atas
        score += 3
        flags.append(f"close_bias_top({top_count}/{win})")
    elif top_count >= win * 0.50:
        score += 1
        flags.append(f"close_bias_top({top_count}/{win})")

    score = min(score, 12)
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
    # BUG FIX v3.9: ONLY count failed breaks BELOW support (buyers defending lows).
    # Previously also counted failed breaks ABOVE resistance — that is SELLING pressure
    # (distribution), not absorption. Removing it stops false inflation of fbd_count.
    lookback20 = candles[-20:] if len(candles) >= 20 else candles
    fbd_count  = 0
    for i in range(len(lookback20) - 1):
        c = lookback20[i]
        # Break below support → buyer absorbs sell pressure → recovery = bullish
        if c["low"] < comp_low and (comp_low - c["low"]) < atr_val:
            for j in range(i + 1, min(i + 4, len(lookback20))):
                if lookback20[j]["close"] > comp_low:
                    fbd_count += 1
                    break
        # REMOVED: "Break above resistance" was counting DISTRIBUTION as absorption
        # A high that pierces resistance and gets sold back = supply overhead, not demand

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
    # AUDIT FIX: when no events exist, set fbd_depth=0.5 so fbd_depth_term=0.5 (not 1.0).
    # This prevents a phantom absorption_score of 0.25+ on coins with ZERO real absorption.
    if fbd_events:
        avg_depth_atr = total_depth_atr / len(fbd_events)
        fbd_depth = min(1.0, avg_depth_atr)   # already in [0,1] since depth < 1 ATR
    else:
        fbd_depth = 0.5   # FIXED: neutral value when no events — was 0.0 which gave 1.0 term

    # fbd_recovery_speed: normalised inverse of avg recovery candles
    # faster recovery (1 candle) → higher score
    # AUDIT FIX: neutral 0.5 when no events (was 0.0 but fbd_depth_term=0.5 now)
    if fbd_events:
        avg_recovery = total_recovery / len(fbd_events)   # 1.0 to 3.0
        # Map: 1 candle → 1.0, 3 candles → 0.33
        fbd_recovery_speed = 1.0 / avg_recovery
    else:
        fbd_recovery_speed = 0.0   # genuinely 0 — no recovery events occurred

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
            if isinstance(raw, list) and len(raw) > 1:
                oi_series = [float(item.get("openInterestList", item.get("openInterest", 0)))
                             for item in raw if item]
            elif isinstance(raw, list) and len(raw) == 1:
                # Single-element list — fall through to proxy below
                pass
            elif isinstance(raw, dict):
                # Single snapshot — DO NOT replicate as flat series (causes all-zero changes)
                # oi_series stays None → will use proxy below
                pass
    except Exception as e:
        log.debug(f"OI fetch failed for {symbol}: {e}")
        oi_series = None

    # AUDIT FIX: If OI series is unavailable or a single point (flat replication produces
    # all-zero changes → buildup_score=0 always), derive OI proxy from candle volume.
    # Logic: large green candles with expanding volume = OI buildup proxy.
    if oi_series is None or len(oi_series) < 5:
        if candles and len(candles) >= 10:
            try:
                oi_proxy = []
                running_oi = 1000.0   # arbitrary starting unit
                for c in candles[-min(len(candles), 50):]:
                    rng = c["high"] - c["low"]
                    if rng > 0:
                        # Estimate OI change: buy candles add OI, sell candles reduce
                        body_dir = 1.0 if c["close"] >= c["open"] else -0.5
                        vol_factor = c["volume_usd"] / max(1.0, c["volume_usd"])  # normalised ~1
                        running_oi += body_dir * (c["volume_usd"] * 0.00001)
                        running_oi = max(0.1, running_oi)
                    oi_proxy.append(running_oi)
                if len(oi_proxy) >= 5:
                    oi_series = oi_proxy
                    log.debug(f"OI proxy built for {symbol} ({len(oi_proxy)} points)")
            except Exception as e:
                log.debug(f"OI proxy build failed for {symbol}: {e}")
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
# ══════════════════════════════════════════════════════════════════════════════
#  🔴  v3.6 — PRESSURE DETECTION LAYER
#  Pure additive overlay — zero changes to existing scoring logic.
#  Window N = CONFIG["vol_avg_long"] (20) — identical to volume normalization.
# ══════════════════════════════════════════════════════════════════════════════
def compute_pressure_layer(candles, absorption_score, flow_score):
    """
    Detects hidden accumulation, slow buildup, and volume/price divergence.

    Window N uses vol_avg_long (existing system window) — no new windows.
    All z-scores use the same rolling N, same formula:
        z = (x - rolling_mean(x, N)) / rolling_std(x, N)
    All outputs normalised 0–1. No binary logic.

    Returns (pressure_score float [0,1], details_dict).
    """
    N = CONFIG.get("vol_avg_long", 20)   # SAME N as volume normalization
    n = len(candles)

    if n < N + 3:
        return 0.0, {
            "pressure_score": 0.0, "vol_price_divergence": 0.0,
            "micro_accumulation": 0.0, "compression_tightness": 0.0,
            "volume_z": 0.0, "price_change_z": 0.0,
        }

    # ── Shared rolling helpers (same N, same formula everywhere) ──────────────
    def _rolling_zscore(value, series_n):
        """z = (x - mean(series_N)) / std(series_N)"""
        if len(series_n) < 2:
            return 0.0
        mu  = _mean(series_n)
        sig = _std(series_n)
        return (value - mu) / sig if sig > 0 else 0.0

    # ── Section 1: Volume vs Price Divergence ─────────────────────────────────
    # volume_z = zscore(volume) using same N
    vols    = [c["volume_usd"] for c in candles[-N:]]
    cur_vol = candles[-1]["volume_usd"]
    volume_z = _rolling_zscore(cur_vol, vols)

    # price_change_z = zscore(price_return) using same N
    returns_n = [
        (candles[-i]["close"] - candles[-i - 1]["close"]) / candles[-i - 1]["close"]
        if candles[-i - 1]["close"] > 0 else 0.0
        for i in range(1, N + 1)
        if i + 1 <= n
    ]
    cur_return = returns_n[0] if returns_n else 0.0
    price_change_z = _rolling_zscore(cur_return, returns_n)

    # vol_price_divergence = clamp((volume_z - abs(price_change_z)) / 2.0, 0, 1)
    vol_price_divergence = max(0.0, min(1.0,
        (volume_z - abs(price_change_z)) / 2.0
    ))

    # ── Section 2: Micro Accumulation ─────────────────────────────────────────
    # Higher low: current_low > previous_low (candle LOW values only)
    recent_lows = [candles[-i]["low"] for i in range(1, min(N + 1, n + 1))]
    recent_lows.reverse()   # chronological order: oldest first

    hl_count = sum(
        1 for i in range(1, len(recent_lows))
        if recent_lows[i] > recent_lows[i - 1]
    )
    hl_ratio = hl_count / max(len(recent_lows) - 1, 1)
    micro_accumulation = max(0.0, min(1.0, hl_ratio))

    # ── Section 3: Compression Tightness ──────────────────────────────────────
    # range = high - low (absolute); rolling mean of range over N candles
    ranges_n = [c["high"] - c["low"] for c in candles[-N:]]
    cur_range = candles[-1]["high"] - candles[-1]["low"]
    range_avg = _mean(ranges_n) if ranges_n else cur_range

    # compression_tightness = clamp(1 - (range / range_avg), 0, 1)
    if range_avg > 0:
        compression_tightness = max(0.0, min(1.0, 1.0 - (cur_range / range_avg)))
    else:
        compression_tightness = 0.0

    # ── Section 4: Base Pressure Score ────────────────────────────────────────
    pressure_score = (
        0.4 * vol_price_divergence
        + 0.3 * micro_accumulation
        + 0.3 * compression_tightness
    )

    # ── Section 5: Absorption Confirmation ────────────────────────────────────
    if pressure_score > 0.5 and absorption_score > 0.5:
        pressure_score += 0.3

    # ── Section 6: Flow Confirmation ──────────────────────────────────────────
    if flow_score > 0.4 and pressure_score > 0.5:
        pressure_score += 0.2

    # ── Section 7: Noise Control ──────────────────────────────────────────────
    if volume_z < -0.5:
        pressure_score *= 0.6

    if compression_tightness < 0.2:
        pressure_score *= 0.7

    # ── Section 8: Integration Priority Rule (anti false positive) ────────────
    if pressure_score > 0.6 and absorption_score < 0.3:
        pressure_score *= 0.7

    # ── Section 9: Final Normalisation ────────────────────────────────────────
    pressure_score = max(0.0, min(1.0, pressure_score))

    return pressure_score, {
        "pressure_score":        round(pressure_score, 4),
        "vol_price_divergence":  round(vol_price_divergence, 4),
        "micro_accumulation":    round(micro_accumulation, 4),
        "compression_tightness": round(compression_tightness, 4),
        "volume_z":              round(volume_z, 4),
        "price_change_z":        round(price_change_z, 4),
        "hl_count":              hl_count,
    }


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

    # ── v3.9 FIX: Price position scale — prevents v34=1.0 on distribution coins ──
    # If price is at the top of its 100-candle range, all the compression/absorption
    # signals are from OLD accumulation, not current. Scale down proportionally.
    # This fixes the core bug: v34 amplifies old signals equally for pre-pump AND
    # post-pump/distribution coins.
    if n >= 50:
        price_highs = [c["high"] for c in candles[-min(n, 100):]]
        price_lows  = [c["low"]  for c in candles[-min(n, 100):]]
        h100 = max(price_highs)
        l100 = min(price_lows)
        rng100 = h100 - l100
        cur_price = candles[-1]["close"]
        if rng100 > 0:
            price_pos = (cur_price - l100) / rng100   # 0=at lows, 1=at highs
            # Scale: at lows (pos<0.25) = full score. At highs (pos>0.85) = 40% score.
            # Linear interpolation between 0.25→1.0 and 0.85→0.40
            if price_pos > 0.85:
                price_pos_scale = 0.40
            elif price_pos > 0.25:
                # Linearly interpolate: 0.25→1.0, 0.85→0.40
                price_pos_scale = 1.0 - (price_pos - 0.25) / (0.85 - 0.25) * 0.60
            else:
                price_pos_scale = 1.0   # at lows — full score
            score = score * price_pos_scale

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
#  🔍  v3.7 — VOLUME TREND QUALIFIER
#  Differentiates dead market (flat vol) from real accumulation (vol trend UP).
#  Returns multiplier 0.3–1.0 applied to working_score.
# ══════════════════════════════════════════════════════════════════════════════
def compute_volume_trend_qualifier(candles):
    """
    Real accumulation has volume trending UP (smart money quietly entering).
    Dead market has flat or declining volume.
    Slow trend has random/flat volume.

    Returns (multiplier float [0.3, 1.0], details dict).
    multiplier=1.0 → volume trend confirms signal
    multiplier=0.3 → dead/declining volume, heavy suppression
    """
    N = CONFIG.get("vol_avg_long", 20)
    n = len(candles)
    if n < N * 3:
        return 1.0, {"reason": "insufficient history", "vol_trend": "unknown"}

    # Split into 3 equal windows: old → mid → recent
    third = N
    vols_old    = [c["volume_usd"] for c in candles[-(third*3):-(third*2)]]
    vols_mid    = [c["volume_usd"] for c in candles[-(third*2):-third]]
    vols_recent = [c["volume_usd"] for c in candles[-third:]]

    avg_old    = _mean(vols_old)    if vols_old    else 0.0
    avg_mid    = _mean(vols_mid)    if vols_mid    else 0.0
    avg_recent = _mean(vols_recent) if vols_recent else 0.0

    if avg_old <= 0:
        return 1.0, {"reason": "no vol history"}

    # Trend ratio: recent vs old
    trend_ratio = avg_recent / avg_old

    # Coefficient of variation across full window — dead market = very low CV
    all_vols = [c["volume_usd"] for c in candles[-N*3:]]
    cv = _std(all_vols) / _mean(all_vols) if _mean(all_vols) > 0 else 0.0

    # Classify
    if trend_ratio >= 1.30 and cv > 0.15:
        # Clearly rising volume with variance — real accumulation
        multiplier = 1.0
        trend_label = f"rising_strong(x{trend_ratio:.2f})"
    elif trend_ratio >= 1.10:
        multiplier = 0.90
        trend_label = f"rising(x{trend_ratio:.2f})"
    elif trend_ratio >= 0.90 and cv > 0.20:
        # Flat but with variance — could be pre-pump stealth
        multiplier = 0.75
        trend_label = f"flat_with_variance(cv={cv:.2f})"
    elif trend_ratio >= 0.90 and cv <= 0.10:
        # Flat volume + low variance = dead market
        multiplier = 0.35
        trend_label = f"DEAD_FLAT(x{trend_ratio:.2f},cv={cv:.2f})"
    elif trend_ratio < 0.80:
        # Declining volume — no pump coming
        multiplier = 0.45
        trend_label = f"declining(x{trend_ratio:.2f})"
    else:
        multiplier = 0.65
        trend_label = f"weak(x{trend_ratio:.2f})"

    return multiplier, {
        "multiplier": round(multiplier, 3),
        "trend_ratio": round(trend_ratio, 3),
        "vol_cv": round(cv, 4),
        "vol_trend": trend_label,
        "avg_old": round(avg_old),
        "avg_recent": round(avg_recent),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ⚡  v3.7 — VOLATILITY RANK SCORE
#  Pre-pump coins have HIGH historical ATR relative to current compression.
#  Dead market has uniformly low ATR (never expanded).
#  Returns (score 0–20, details dict).
# ══════════════════════════════════════════════════════════════════════════════
def compute_volatility_rank_score(candles):
    """
    Measures ATR expansion POTENTIAL by comparing:
      - Current ATR (short window) vs historical ATR distribution
      - If current ATR is in bottom quartile of history → coiled = bullish
      - If historical max ATR >> current ATR → coin CAN move fast
      - If ATR always flat → dead coin, no pump potential

    Returns (score 0-20, details dict).
    Score 15-20 = coiled high-volatility coin ready to expand
    Score 5-10  = moderate, possible
    Score 0-3   = dead, flat ATR history = no pump potential
    """
    win_s = CONFIG.get("win_short", 10)
    win_l = CONFIG.get("win_structure", 50)
    n = len(candles)

    if n < win_l + win_s + 5:
        # v3.7 FIX: Don't return blind 5 — compute range CV from available candles.
        # Dead micro-caps have near-zero range CV even with few candles.
        if n >= 10:
            ranges = [c["high"] - c["low"] for c in candles if c["low"] > 0]
            quick_cv = _std(ranges) / _mean(ranges) if _mean(ranges) > 0 else 0
            if quick_cv < 0.05:
                return 0, {"reason": "insufficient_data+dead_range", "atr_cv": quick_cv}
        return 3, {"reason": "insufficient data"}

    atr_short = calc_atr(candles[-(win_s + 5):], win_s)
    atr_long  = calc_atr(candles[-(win_l + 5):], win_l)

    if not atr_short or not atr_long or atr_long <= 0:
        return 0, {"reason": "atr_calc_failed"}

    # Build ATR history: rolling short ATRs over past 200 candles
    atr_history = []
    step = max(1, win_s // 2)
    for i in range(win_s + 5, min(n, 200), step):
        a = calc_atr(candles[-(i + win_s + 5): -(i)], win_s)
        if a and a > 0:
            atr_history.append(a)

    if len(atr_history) < 5:
        # v3.7 FIX: use available atr_history or range cv to detect dead coin
        if len(atr_history) >= 2:
            quick_cv = _std(atr_history) / _mean(atr_history) if _mean(atr_history) > 0 else 0
            if quick_cv < 0.05:
                return 0, {"reason": "short_history+dead_atr", "atr_cv": quick_cv}
        return 3, {"reason": "insufficient atr history"}

    # What percentile is the current ATR in historical distribution?
    atr_p10  = _percentile(atr_history, 10)
    atr_p25  = _percentile(atr_history, 25)
    atr_p50  = _percentile(atr_history, 50)
    atr_p90  = _percentile(atr_history, 90)
    atr_max  = max(atr_history)
    atr_cv   = _std(atr_history) / _mean(atr_history) if _mean(atr_history) > 0 else 0

    # Expansion potential = max ATR / current ATR
    # High ratio = coin has moved violently before = can do it again
    expansion_potential = atr_max / atr_short if atr_short > 0 else 1.0

    score = 0

    # [1] Current ATR in low percentile → coiled (0-12 pts)
    if atr_short <= atr_p10:
        score += 12
    elif atr_short <= atr_p25:
        score += 8
    elif atr_short <= atr_p50:
        score += 4

    # [2] Expansion potential (0-5 pts)
    if expansion_potential >= 5.0:
        score += 5
    elif expansion_potential >= 3.0:
        score += 3
    elif expansion_potential >= 2.0:
        score += 1

    # [3] v3.7: ATR actively expanding (cv high + recent ATR > older ATR)
    # A pump in progress expands ATR from low → high. This IS a valid signal.
    if atr_cv >= 0.25 and len(atr_history) >= 10:
        recent_atr_avg = _mean(atr_history[-5:])
        older_atr_avg  = _mean(atr_history[:5])
        if older_atr_avg > 0 and recent_atr_avg / older_atr_avg >= 1.5:
            # ATR expanding = volatility regime change = pump in progress
            score = max(score, 8)   # floor at 8 for expanding volatile coins

    # [4] ATR CV — dead coins have near-zero CV (flat ATR always)
    # PENALTY: if CV < 0.05 → monotone ATR → dead market → hard suppress
    if atr_cv < 0.05:
        score = max(0, score - 15)   # dead coin: total suppression
    elif atr_cv < 0.10:
        score = max(0, score - 5)

    score = max(0, min(20, score))

    return score, {
        "atr_short": round(atr_short, 8),
        "atr_long":  round(atr_long, 8),
        "atr_p25":   round(atr_p25, 8),
        "atr_p50":   round(atr_p50, 8),
        "atr_max":   round(atr_max, 8),
        "atr_cv":    round(atr_cv, 4),
        "expansion_potential": round(expansion_potential, 2),
        "vol_rank_score": score,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ⏰  v3.9 — SESSION ALPHA BONUS
#  Asia session transition (~02:00 WIB = 19:00 UTC prev day) is historically
#  the highest-probability window for altcoin ignition by market makers.
#  Pre-Asia buildup: 17:00–19:00 UTC (00:00–02:00 WIB)
#  Asia expansion:   19:00–22:00 UTC (02:00–05:00 WIB)
#  Europe open:      06:00–08:00 UTC (13:00–15:00 WIB) — secondary window
# ══════════════════════════════════════════════════════════════════════════════
def session_alpha_bonus(current_utc_hour: int) -> float:
    """
    Returns a score bonus based on current UTC hour aligned to WIB (UTC+7).
    Only applies conditional bonus — never a flat constant.

    Window mapping:
      UTC 17-18 (WIB 00-01): pre-Asia buildup        → +6  (accumulation phase)
      UTC 19-21 (WIB 02-04): Asia ignition window    → +10 (highest probability)
      UTC 22-23 (WIB 05-06): Asia continuation       → +5
      UTC 06-07 (WIB 13-14): Europe open window      → +4
      UTC 08-11 (WIB 15-18): US pre-market           → +3
      All other hours                                 → 0
    """
    h = current_utc_hour % 24
    if h in (17, 18):      return 6.0    # pre-Asia accumulation
    if h in (19, 20, 21):  return 10.0   # Asia ignition — peak window
    if h in (22, 23):      return 5.0    # Asia continuation
    if h in (6, 7):        return 4.0    # Europe open
    if h in (8, 9, 10, 11): return 3.0  # US pre-market
    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  🔥  v3.9 — IGNITION TRIGGER DETECTOR
#  Detects the transition: compression+accumulation → expansion.
#  Uses only internal signals — no price confirmation needed.
#
#  Logic:
#    - pressure_score > 0.55: hidden demand is elevated (buyers absorbing supply)
#    - flow_score > 0.30: OI/CVD confirms directional intent (not just noise)
#    - cqi < 0.022: compression is structurally sound (not just random chop)
#    - compression_length >= 48: coil has aged enough to release (2+ days)
#
#  ALL four conditions must align — prevents false triggers on single-factor spikes.
# ══════════════════════════════════════════════════════════════════════════════
def detect_ignition(pressure_score: float, flow_score: float,
                    cqi: float, compression_length: int) -> tuple:
    """
    Returns (is_ignition: bool, ignition_score: float, reason: str).
    ignition_score is graded 0.0-1.0 — partial credit for near-ignition.
    """
    conditions_met = 0
    details = []

    # Condition 1: pressure elevated — demand absorbing supply
    if pressure_score > 0.55:
        conditions_met += 1
        details.append(f"press={pressure_score:.2f}")
    elif pressure_score > 0.40:
        conditions_met += 0.5   # partial

    # Condition 2: flow directional intent confirmed
    if flow_score > 0.30:
        conditions_met += 1
        details.append(f"flow={flow_score:.2f}")
    elif flow_score > 0.15:
        conditions_met += 0.5

    # Condition 3: compression quality structural (not noise)
    if 0 < cqi < 0.022:
        conditions_met += 1
        details.append(f"cqi={cqi:.4f}")
    elif 0 < cqi < 0.035:
        conditions_met += 0.5

    # Condition 4: coil has aged — energy stored (2+ days minimum)
    if compression_length >= 48:
        conditions_met += 1
        details.append(f"coil={compression_length}h")
    elif compression_length >= 24:
        conditions_met += 0.5

    # Full ignition: all 4 conditions met
    is_ignition = conditions_met >= 3.5
    ignition_score = min(1.0, conditions_met / 4.0)

    reason = "+".join(details) if details else "none"
    return is_ignition, ignition_score, reason


# ══════════════════════════════════════════════════════════════════════════════
#  💧  v3.9 — LIQUIDITY VACUUM PROXY
#  Simulates thin ask-side liquidity WITHOUT orderbook data.
#
#  Market maker pattern before a pump:
#    1. Range narrows (they stop providing wide spreads)
#    2. Volume declines (not distributing, just holding)
#    3. Volatility compresses (ATR squeezes relative to history)
#    4. Then: sudden micro-expansion candle with volume (ignition test)
#
#  Proxy score = probability that ask-side is thin above price.
# ══════════════════════════════════════════════════════════════════════════════
def liquidity_vacuum_proxy(candles) -> tuple:
    """
    Returns (vacuum_score float [0,1], details dict).
    Higher score = thinner liquidity above = easier breakout.

    Uses last 6 candles (micro-window) vs 30-candle baseline.
    """
    n = len(candles)
    if n < 35:
        return 0.0, {"reason": "insufficient data"}

    recent6  = candles[-6:]
    base30   = candles[-30:]

    # [1] Range compression — last 6 candles significantly tighter than baseline
    ranges6  = [(c["high"] - c["low"]) / c["low"] for c in recent6  if c["low"] > 0]
    ranges30 = [(c["high"] - c["low"]) / c["low"] for c in base30   if c["low"] > 0]
    avg_range6  = _mean(ranges6)  if ranges6  else 0.01
    avg_range30 = _mean(ranges30) if ranges30 else 0.01
    range_compression = 1.0 - min(1.0, avg_range6 / avg_range30) if avg_range30 > 0 else 0.0

    # [2] Volume declining into compression — supply exhaustion signal
    vols6  = [c["volume_usd"] for c in recent6]
    vols30 = [c["volume_usd"] for c in base30]
    avg_vol6  = _mean(vols6)  if vols6  else 0
    avg_vol30 = _mean(vols30) if vols30 else 1
    vol_decline = max(0.0, 1.0 - (avg_vol6 / avg_vol30)) if avg_vol30 > 0 else 0.0
    vol_decline = min(vol_decline, 0.9)   # cap: complete silence is suspect

    # [3] Micro-expansion on last candle (ignition test candle)
    # Breakout attempt: last candle range > 1.5× avg of 5 before it
    last_c = candles[-1]
    prev5_ranges = [(c["high"] - c["low"]) / c["low"] for c in candles[-6:-1] if c["low"] > 0]
    avg_prev5 = _mean(prev5_ranges) if prev5_ranges else 0.01
    last_range = (last_c["high"] - last_c["low"]) / last_c["low"] if last_c["low"] > 0 else 0
    micro_expansion = max(0.0, min(1.0, (last_range / avg_prev5 - 1.0) / 2.0)) if avg_prev5 > 0 else 0.0

    # [4] Close position bias — consistent closing in upper half = buyers in control
    close_bias = _mean([
        (c["close"] - c["low"]) / (c["high"] - c["low"])
        for c in recent6 if (c["high"] - c["low"]) > 0
    ]) if recent6 else 0.5
    close_bias_score = max(0.0, (close_bias - 0.5) * 2.0)   # 0.5→0, 1.0→1.0

    # Weighted combination
    vacuum_score = (
        0.35 * range_compression    # most important: supply wall collapsing
        + 0.25 * vol_decline        # volume drying up = less supply
        + 0.25 * micro_expansion    # ignition test candle
        + 0.15 * close_bias_score   # directional intent
    )
    vacuum_score = max(0.0, min(1.0, vacuum_score))

    return vacuum_score, {
        "vacuum_score":      round(vacuum_score, 4),
        "range_compression": round(range_compression, 4),
        "vol_decline":       round(vol_decline, 4),
        "micro_expansion":   round(micro_expansion, 4),
        "close_bias":        round(close_bias, 4),
    }


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

    # ── [v3.9] POST-PUMP ZONE INVALIDATION ────────────────────────────────────
    # Critical bug fix: scanner finds OLD compression zones and scores them as
    # pre-pump signals even when the coin already pumped above the zone.
    # Two checks:
    #   [A] Price > comp_high * 1.10 = coin already broke out (>10% above range)
    #   [B] Zone age > zone length = breakout happened before current scan window
    # Both indicate a STALE zone — should not score as pre-pump setup.
    stale_zone_penalty = 0
    if compression:
        comp_high = compression["high"]
        comp_low  = compression["low"]
        age       = compression["age_candles"]
        length    = compression["length"]

        rise_above_zone = (price_now - comp_high) / comp_high if comp_high > 0 else 0.0
        age_ratio       = age / length if length > 0 else 0.0

        if rise_above_zone > 0.15:
            # Price is >15% above the compression zone top = ALREADY PUMPED
            # Nullify the compression — it's a post-pump distribution zone now
            stale_zone_penalty = 35
            log.info(f"  {symbol}: stale_zone_penalty=-35 (price {rise_above_zone*100:.0f}% above comp_high)")
            # Invalidate compression so downstream modules don't use it
            compression = None
        elif rise_above_zone > 0.08:
            # 8-15% above = borderline — penalize but keep zone
            stale_zone_penalty = 20
            log.info(f"  {symbol}: stale_zone_penalty=-20 (price {rise_above_zone*100:.0f}% above comp_high)")
        elif age_ratio > 1.5:
            # Zone age >> length = breakout happened long ago, zone is expired
            stale_zone_penalty = 15
            log.info(f"  {symbol}: stale_zone_penalty=-15 (stale zone: age={age}h length={length}h ratio={age_ratio:.1f}x)")

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
    v_norm    = _norm_score_to_01(vol_score, 30)            # max 30 (score_volume_intelligence ceiling is 30)
    s_norm    = _norm_score_to_01(max(0, struct_score), 41) # max 20 base + 18 fbd + 8 candle + 5 = ~41 real max
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

    # ══════════════════════════════════════════════════════════════════════════
    # ── v3.6 PRESSURE DETECTION LAYER ────────────────────────────────────────
    # ══════════════════════════════════════════════════════════════════════════
    pressure_score, pressure_details = compute_pressure_layer(
        candles        = c1h,
        absorption_score = absorption_score,
        flow_score     = flow_score_v34,
    )

    # ══════════════════════════════════════════════════════════════════════════
    # ── v3.7 NEW FILTERS — differentiates dead market vs accumulation ─────────
    # ══════════════════════════════════════════════════════════════════════════

    # [N] Volume trend qualifier — dead market = flat/declining vol, accum = rising
    vol_trend_mult, vol_trend_details = compute_volume_trend_qualifier(c1h)

    # [O] Volatility rank score — pre-pump coins have coiled ATR in low percentile
    #     AND have expansion potential (high historical ATR vs current ATR)
    vol_rank_score, vol_rank_details = compute_volatility_rank_score(c1h)

    log.info(
        f"  {symbol} [v3.7-filters] "
        f"vol_trend_mult={vol_trend_mult:.3f} ({vol_trend_details.get('vol_trend','?')}) "
        f"vol_rank={vol_rank_score} (cv={vol_rank_details.get('atr_cv',0):.4f} "
        f"exp_pot={vol_rank_details.get('expansion_potential',0):.2f}x)"
    )

    # ── FINAL SCORE (v3.7 — all signal components properly wired) ───────────────
    #
    # AUDIT FIX: Previously, ALL of the following went to raw_score_additive
    # which was explicitly "logging only" — none of them affected the real output.
    # This has been corrected: non-linear v34 score is the base, and meaningful
    # additive corrections are applied on top of it before the single final clamp.
    #
    # Components now properly wired to final score:
    #   [+] stealth_score_bonus  — real accumulation signal
    #   [+] pbb_score (already in v34 via pbb_norm, but cap ensures correct weight)
    #   [+] liq_bonus            — liquidity sweep is high-quality signal
    #   [+] phase_bonus/penalty  — early/late phase has real impact on pump timing
    #   [+] cont_score           — second-leg continuation is valid pump signal
    #   [+] vol_rank_score       — [NEW v3.7] high-volatility coiled coin bonus
    #   [-] dist_penalty         — distribution MUST reduce score (was completely dead)
    #   [-] slow_penalty         — trending without compression = false signal
    #   [-] late_penalty         — late entry without consolidation = trap
    #   [-] enh_late_pen         — enhanced late detection
    #   [-] dead_pen             — dead market = no pump
    #   [-] ovlp_pen             — overlap noise reduction
    #   [-] funding_gate_penalty — negative funding = bearish sentiment
    #   [-] liquidity_penalty    — illiquid coins are dangerous
    #   [-] already_pumped_penalty — already moved = late entry
    # [×] vol_trend_mult         — [NEW v3.7] multiplicative: dead vol → suppresses

    # ── Safe defaults for v3.8/v3.9 fields (before any conditional logic) ──────
    cqi              = 0.0
    cqi_bonus        = 0
    pump_started     = False
    pump_move_12h    = 0.0
    price_24h_move   = 0.0
    ignition_fired   = False
    ignition_score_v = 0.0
    ignition_reason  = "none"
    vacuum_score_v39 = 0.0
    vacuum_det_v39   = {}
    session_bonus    = 0.0

    # Step 1: convert v34 score (0-1) to 0-100 working space
    working_score = v34_score_01 * 100.0

    # ── [v3.9 MODULE 1] SESSION ALPHA BONUS ───────────────────────────────────
    # Asia session transition (02:00 WIB = 19:00 UTC) is the peak pump window.
    # BUG FIX v3.9: Only apply if price has NOT already moved (not post-pump coin).
    _utc_hour = datetime.now(timezone.utc).hour
    _raw_session_bonus = session_alpha_bonus(_utc_hour)
    # Gate: only apply session bonus if price is flat (<8% in 24h)
    # This prevents post-pump distribution coins from getting the time bonus
    _price_24h_check = 0.0
    if len(c1h) >= 24:
        _p24 = c1h[-24]["close"]
        _price_24h_check = abs(c1h[-1]["close"] - _p24) / _p24 if _p24 > 0 else 0.0
    session_bonus = _raw_session_bonus if _price_24h_check < 0.08 else 0.0
    working_score += session_bonus

    # ── [v3.9 MODULE 6] LIQUIDITY VACUUM PROXY ────────────────────────────────
    # Simulates thin ask-side liquidity: range compression + vol decline + micro-expansion
    vacuum_score_v39, vacuum_det_v39 = liquidity_vacuum_proxy(c1h)
    vacuum_contrib_v39 = vacuum_score_v39 * 18.0   # max +18 pts (thin liquidity = easy breakout)
    working_score += vacuum_contrib_v39

    # Step 2: pressure overlay
    pressure_contribution = pressure_score * 35.0
    working_score += pressure_contribution

    # Step 3: Standard additive bonuses
    working_score += stealth_score_bonus        # 0, 5, 10-15
    working_score += liq_bonus                  # 0 or +8
    working_score += cont_score * 0.5           # 0-7.5
    working_score += rsi_bonus                  # 0 or +1
    working_score += vol_rank_score             # 0-20 pts

    # ── [v3.9 MODULE 5] UPGRADED CQI — compute and wire ───────────────────────
    # CQI threshold validated from 8 pump coins: CQI 0.009-0.026
    # Tight coil = more stored energy = sharper pump
    cqi_bonus = 0
    cqi = 0.0
    if compression:
        cqi = compression["range_pct"] / max(compression["length"] ** 0.5, 1)
        if cqi < 0.010:
            cqi_bonus = 15   # ultra-tight (GUSDT/ZETA level) — raised from 12
        elif cqi < 0.015:
            cqi_bonus = 10   # tight coil — raised from 8
        elif cqi < 0.022:
            cqi_bonus = 5    # moderate coil — raised from 4, threshold from 0.020
        elif cqi > 0.040:
            cqi_bonus = -10  # loose — raised penalty from -8
        working_score += cqi_bonus

        # ── [v3.9 MODULE 2] IGNITION TRIGGER (needs real cqi) ─────────────────
        # Re-run ignition with real cqi now that compression is confirmed
        ignition_fired, ignition_score_v, ignition_reason = detect_ignition(
            pressure_score     = pressure_score,
            flow_score         = flow_score_v34,
            cqi                = cqi,
            compression_length = compression["length"],
        )
        if ignition_fired:
            working_score += 15   # full ignition: all 4 conditions aligned
            log.info(f"  {symbol} [v3.9] 🔥 IGNITION FIRED: {ignition_reason}")
        elif ignition_score_v >= 0.5:
            working_score += round(ignition_score_v * 10)  # partial credit 5-9 pts
    else:
        # No compression → no ignition possible
        ignition_fired = False
        ignition_score_v = 0.0

    # ── [v3.9 MODULE 3] FLOW SIGNAL NON-LINEAR AMPLIFICATION ──────────────────
    # flow_score in linear average loses impact. Non-linear gate restores signal strength.
    # flow_score > 0.7: OI buildup + CVD absorption + squeeze all aligned = major signal
    # flow_score < 0.15: flow negative/absent = mild suppression
    if flow_score_v34 > 0.7:
        working_score += 15   # strong directional flow — market maker accumulating
    elif flow_score_v34 > 0.5:
        working_score += 8    # moderate flow — meaningful
    elif flow_score_v34 > 0.3:
        working_score += 4    # weak but present
    elif flow_score_v34 < 0.15:
        working_score -= 5    # flow absent/negative — caution

    if phase == "early":
        working_score += phase_bonus            # +10
    elif phase == "late":
        working_score -= phase_penalty          # -12

    # Step 4: Apply ALL penalties (all were previously dead — now active)
    # Distribution penalty is most critical — MUST suppress pump false positives
    working_score -= dist_penalty               # 0 to -35 (Track A+B+C)
    working_score -= slow_penalty               # 0 to -10
    working_score -= late_penalty               # 0 to -15
    working_score -= abs(enh_late_pen)          # 0 to -10
    working_score -= abs(dead_pen)              # 0 to -8
    working_score -= abs(ovlp_pen)              # 0 to -8
    working_score -= funding_gate_penalty       # 0 or -15
    working_score -= liquidity_penalty          # 0, -20, or -30
    working_score -= already_pumped_penalty     # 0 or -25
    working_score -= stale_zone_penalty         # [NEW v3.9] 0, -15, -20, or -35
    if funding < -0.001:
        working_score -= 5                      # additional funding penalty

    # ── [v3.9 MODULE 4] vol_trend_mult — FIX STEALTH SUPPRESSION ─────────────
    # BUG FIXED: previously vol_trend_mult suppressed stealth accumulation coins.
    # Low volume DURING stealth is the signal, not a failure.
    # Rule: only apply suppression if stealth NOT detected AND phase NOT early.
    stealth_detected_flag = stealth.get("detected", False)
    _apply_vol_suppression = (
        vol_trend_mult < 1.0
        and not stealth_detected_flag   # stealth = intentional low vol, don't punish
        and phase != "early"            # early phase by definition has low vol
    )
    if _apply_vol_suppression:
        working_score *= vol_trend_mult

    # Step 6: Single final clamp to 0-100
    score = max(0, min(100, round(working_score)))

    # For logging transparency, keep raw_score_additive as before
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

    # Derive score_01 from final score for logging/compatibility
    score_01 = score / 100.0

    # ── Confidence band ───────────────────────────────────────────────────────
    # v3.8 FIX: 'possibly_late' requires price to have ACTUALLY MOVED,
    # not just score > 78. High score on flat-price coin = compression, not late entry.
    # Validated from data: PEPE, ZETA, ARIA all labeled LATE incorrectly
    # despite price being flat. Only label LATE if price moved >12% in 24h.
    price_24h_move = 0.0
    if len(c1h) >= 24:
        p_now   = c1h[-1]["close"]
        p_24h   = c1h[-24]["close"]
        price_24h_move = abs(p_now - p_24h) / p_24h if p_24h > 0 else 0.0

    if score < CONFIG["score_min_output"]:
        confidence = "ignore"
    elif score < CONFIG["score_target_low"]:
        confidence = "early"
    elif score <= CONFIG["score_target_high"]:
        confidence = "strong"
    elif price_24h_move > 0.12:
        # Price truly moved >12% in last 24h AND score > 78 → genuinely late
        confidence = "possibly_late"
    else:
        # Score > 78 but price flat → all signals maxed on compressed coin = STRONG, not late
        confidence = "strong"

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
        f"(additive_ref={score_additive_01:.3f} — logging only)",
        f"[v3.7] pressure={pressure_score:.3f} contribution=+{pressure_score*35:.1f}pts "
        f"(vpd={pressure_details.get('vol_price_divergence',0):.3f} "
        f"ma={pressure_details.get('micro_accumulation',0):.3f} "
        f"ct={pressure_details.get('compression_tightness',0):.3f} "
        f"vz={pressure_details.get('volume_z',0):.3f} "
        f"pcz={pressure_details.get('price_change_z',0):.3f})",
        f"[v3.7] vol_trend_mult={vol_trend_mult:.3f} ({vol_trend_details.get('vol_trend','?')}) "
        f"vol_rank={vol_rank_score} (exp_pot={vol_rank_details.get('expansion_potential',0):.2f}x "
        f"atr_cv={vol_rank_details.get('atr_cv',0):.4f})",
        f"[v3.8] CQI={cqi:.4f} bonus={cqi_bonus:+d} | "
        f"pump_started={'YES ⚠️' if pump_started else 'no'} ({pump_move_12h*100:.1f}% in 12h) | "
        f"price_24h_move={price_24h_move*100:.1f}% conf={confidence}",
        f"[v3.9] session=+{session_bonus:.0f}pts(UTC{_utc_hour}h) | "
        f"ignition={'🔥FIRED' if ignition_fired else f'score={ignition_score_v:.2f}'}({ignition_reason}) | "
        f"vacuum={vacuum_score_v39:.3f}(+{vacuum_score_v39*18:.1f}pts) | "
        f"flow_nl={'strong' if flow_score_v34>0.7 else 'mod' if flow_score_v34>0.3 else 'weak'}({flow_score_v34:.3f}) | "
        f"stealth_vol_bypass={stealth_detected_flag} | "
        f"stale_zone_pen=-{stale_zone_penalty}",
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
        f"| D=-{dist_penalty} Late={enh_late_pen} Dead={dead_pen} Ovlp={ovlp_pen} "
        f"| v34_base={v34_score_01:.3f} press={pressure_score:.3f}"
    )
    # ── Diagnostic logging ────────────────────────────────────────────────────
    buildup_score_log = flow_details.get("buildup_score", 0.0)
    fbd_strength_log  = absorption_details.get("fbd_strength", 0.0)
    anomaly_score_log = v34_composition.get("anomaly_score", 0.0)
    log.info(
        f"  {symbol} [v3.7] "
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
        f"pressure_score={pressure_score:.3f} "
        f"pressure_contrib=+{pressure_score*35:.2f}pts "
        f"dist_penalty=-{dist_penalty} "
        f"final_score={score}"
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

    # v3.8: Detect if pump has already started (price moved >15% in last 12h)
    # Validated from GUSDT data: score correctly collapses when pump is active
    # This flag helps users understand WHY score is low despite showing compression
    pump_started = False
    pump_move_12h = 0.0
    if len(c1h) >= 12:
        p_12h = c1h[-12]["close"]
        if p_12h > 0:
            pump_move_12h = (price_now - p_12h) / p_12h
            pump_started = pump_move_12h > 0.15   # >15% in 12h = pump active

    if pump_started:
        urgency = f"🚀 PUMP AKTIF — harga sudah naik {pump_move_12h*100:.0f}% dalam 12 jam — high risk entry"
    elif phase == "early" and comp_len >= 72:
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
        # v3.6 pressure layer
        "pressure_score":      round(pressure_score, 4),
        "pressure_details":    pressure_details,
        # v3.7 new filters
        "vol_trend_mult":      round(vol_trend_mult, 4),
        "vol_trend_details":   vol_trend_details,
        "vol_rank_score":      vol_rank_score,
        "vol_rank_details":    vol_rank_details,
        "stale_zone_penalty":  stale_zone_penalty,
        # v3.8 new fields
        "cqi":                 round(cqi, 5),
        "cqi_bonus":           cqi_bonus,
        "pump_started":        pump_started,
        "pump_move_12h":       round(pump_move_12h * 100, 1),
        "price_24h_move_pct":  round(price_24h_move * 100, 1),
        # v3.9 new fields
        "session_bonus":       session_bonus,
        "ignition_fired":      ignition_fired,
        "ignition_score":      round(ignition_score_v, 3),
        "ignition_reason":     ignition_reason,
        "vacuum_score_v39":    round(vacuum_score_v39, 4),
        "vacuum_details_v39":  vacuum_det_v39,
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
        f"🚀 <b>PRE-PUMP SIGNAL {rk}— v3.3</b>\n\n"
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

    # Pressure layer display (v3.6)
    pd_ = r.get("pressure_details", {})
    ps  = r.get("pressure_score", 0.0)
    pressure_str = ""
    if ps > 0.2:
        pressure_str = (
            f"\n🔴 <b>PRESSURE LAYER</b> ({ps:.2f})\n"
            f"  VPD:{pd_.get('vol_price_divergence',0):.2f}  "
            f"MA:{pd_.get('micro_accumulation',0):.2f}  "
            f"CT:{pd_.get('compression_tightness',0):.2f}  "
            f"Vz:{pd_.get('volume_z',0):.2f}  "
            f"PCz:{pd_.get('price_change_z',0):.2f}\n"
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
        f"{pressure_str}"
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
    msg  = f"📋 <b>PRE-PUMP WATCHLIST v3.3 — {utc_now()}</b>\n{'━'*30}\n"
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
    log.info(f"🔍 SCANNING {len(WHITELIST_SYMBOLS)} coin — PRE-PUMP v3.0 (PROBABILISTIC)")
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
    log.info(f"=== PRE-PUMP SCANNER v3.0 — {utc_now()} ===")

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
    log.info("║  PRE-PUMP SCANNER v3.6 — PRESSURE DETECTION LAYER ║")
    log.info("║  +VPD: volume vs price divergence                 ║")
    log.info("║  +Micro accumulation (higher lows ratio)          ║")
    log.info("║  +Compression tightness overlay                   ║")
    log.info("║  pressure_score * 0.35 → final score             ║")
    log.info("╚════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)

    run_scan()
