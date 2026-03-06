"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v15.3                                                      ║
║                                                                              ║
║  PERUBAHAN DARI v15.0 — 3 Perbaikan Kritis:                                 ║
║                                                                              ║
║  #1 Funding Gate DILONGGARKAN (PALING PENTING):                              ║
║     Dari: avg < -0.0002 ATAU cumul < -0.001                                 ║
║     Ke  : avg < -0.00005 ATAU cumul < -0.0001                               ║
║     Alasan: gate lama terlalu ketat → hampir semua coin gagal funding gate  ║
║     bahkan yang punya setup bagus. -0.00005 = sekitar netral/sedikit negatif║
║     sehingga scanner tidak melewatkan fase early accumulation.              ║
║                                                                              ║
║  #2 VWAP Gate DILONGGARKAN:                                                  ║
║     Dari: price > vwap                                                       ║
║     Ke  : price > vwap * 0.97  (toleransi 3% di bawah VWAP)                ║
║     Alasan: fase pre-pump (accumulation + liquidity sweep) sering terjadi   ║
║     justru ketika harga sedikit di bawah VWAP. Gate ketat melewatkan setup  ║
║     paling bagus — "calm before storm" dengan harga ditahan di bawah VWAP.  ║
║                                                                              ║
║  #3 NO_MOMENTUM FILTER DIHAPUS, DIGANTI ENERGY BUILD-UP FILTER:             ║
║     Sebelumnya: skip coin jika chg_24h < -5% (no_momentum filter)           ║
║     Sekarang  : skip coin jika chg_24h < -15% (hanya skip dump besar)       ║
║     + Tambahkan Energy Build-Up detector:                                   ║
║       OI_change > 5% + volume > 1.5x avg + price_range_1h < 2.5%           ║
║       = "OI Build + Volume Build + Price Stuck" (pola inventory build)      ║
║     Alasan: pump besar sering muncul dari coin "membosankan" yang harganya  ║
║     sideways tetapi OI dan volume sedang dibangun (absorption oleh whale).  ║
║     Filter lama membuang exactly coin-coin ini.                              ║
║                                                                              ║
║  WARISAN dari v15.0:                                                         ║
║  - Volume Ratio threshold: 2.5 → 1.5 (accumulation detection)              ║
║  - EMA Gap score: 5 → 2 (indikator momentum, bukan accumulation)           ║
║  - HTF Accumulation Filter (4H)                                             ║
║  - Liquidity Sweep Detection                                                ║
║  - Deep Entry Model (VWAP - 0.5*ATR)                                        ║
║  - Whitelist dipangkas ke top pairs                                          ║
║                                                                              ║
║  BOBOT SKOR:                                                                 ║
║    energy_buildup      : +4 (OI+vol naik, harga stuck — pola terkuat)      ║
║    htf_accumulation    : +3 (4H build-up)                                   ║
║    liquidity_sweep     : +3 (stop hunt sebelum reversal)                    ║
║    accumulation+compress: +8 (kombinasi akumulasi + kompresi)               ║
║    oi_expansion_strong : +5                                                  ║
║    atr >= 1.5%         : +4                                                  ║
║    bbw >= 0.10         : +4                                                  ║
║    bos+vwap            : +4                                                  ║
║    bos_up saja         : +3                                                  ║
║    ema_gap >= 1.0      : +2 (diturunkan — momentum bukan pre-pump)          ║
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
    "/tmp/scanner_v15.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v15.3 — log aktif: /tmp/scanner_v15.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_alert":          10,
    "max_alerts_per_run":       15,

    # ── Volume 24h total (USD) ────────────────────────────────────────────────
    "min_vol_24h":          10_000,   # dinaikkan — fokus pada coin liquid
    "max_vol_24h":      50_000_000,
    "pre_filter_vol":        5_000,   # dinaikkan — hindari coin illiquid

    # ── Open Interest minimum filter (BARU) ───────────────────────────────────
    # Futures pump biasanya terjadi di coin dengan OI besar.
    # Filter ini menghindari coin illiquid tanpa posisi futures signifikan.
    "min_oi_usd":          100_000,   # minimal $100K OI — skip coin illiquid

    # ── Gate perubahan harga ──────────────────────────────────────────────────
    "gate_chg_24h_max":         12.0,
    # PERUBAHAN v15.3: -5% → -15% (longgarkan, hapus no_momentum filter)
    # Filter lama membuang coin yang sideways/sedikit merah tapi sedang
    # dalam fase accumulation (OI naik, volume naik, harga ditahan).
    # Pump besar sering muncul dari coin "membosankan" ini.
    "gate_chg_24h_min":        -15.0,   # DILONGGARKAN dari -5.0 → hanya skip dump besar

    # ── VWAP Gate Tolerance (BARU v15.3) ─────────────────────────────────────
    # Dari: price > vwap (strict)
    # Ke  : price > vwap * 0.97 (toleransi 3% di bawah VWAP)
    # Alasan: fase accumulation dan liquidity sweep sering terjadi justru
    # ketika harga SEDIKIT DI BAWAH VWAP. "Calm before storm" dengan harga
    # ditahan oleh market maker di bawah VWAP sambil OI + volume dibangun.
    # Gate lama membuang exactly setup terbaik ini.
    "vwap_gate_tolerance":      0.97,   # BARU: price > vwap * 0.97

    # ── Energy Build-Up Detector (BARU v15.3) ────────────────────────────────
    # Pola: OI Build + Volume Build + Price Stuck
    # = Market maker/whale sedang membangun inventori (absorption)
    # Ketika kompresi ini dilepas → expansion sangat cepat
    #
    # Kondisi:
    #   1. OI naik > 5% dalam periode scan
    #   2. Volume 1h > 1.5x rata-rata
    #   3. Price range 1h < 2.5% (harga tidak bergerak meski ada aktivitas)
    # Versi kuat: tambahkan funding <= 0 (mayoritas trader short/netral)
    "energy_oi_change_min":     5.0,   # OI change % minimum untuk konfirmasi
    "energy_vol_ratio_min":     1.5,   # volume > N x rata-rata
    "energy_range_max_pct":     2.5,   # price range 1h < N% (harga stuck)
    "score_energy_buildup":     4,     # skor tertinggi — ini sinyal terkuat

    # ── Gate uptrend usia ─────────────────────────────────────────────────────
    "gate_uptrend_max_hours":   10,

    # ── Gate RSI overbought ───────────────────────────────────────────────────
    "gate_rsi_max":             75.0,

    # ── Gate BB Position ──────────────────────────────────────────────────────
    "gate_bb_pos_max":          1.05,

    # ── Funding — SCORING ONLY (v15.3, hard gate DINONAKTIFKAN) ──────────────
    # LOG ANALYSIS: dari 104 coin, mayoritas gagal funding gate meski funding
    # sudah negatif (IOTXUSDT -0.000041, KASUSDT -0.000012, UNIUSDT -0.000005).
    # Di market bearish/sideways, rata-rata funding mendekati 0, bukan -0.0005.
    # Hard gate funding = scanner tidak berguna di kondisi pasar saat ini.
    #
    # SOLUSI v15.3: funding jadi SCORING, bukan gate.
    #   Funding sangat negatif  → bonus skor (short squeeze setup)
    #   Funding sangat positif  → penalti skor (coin sudah overbought)
    #   Funding netral/sedikit  → tidak ada efek (normal di early accumulation)
    "funding_penalty_avg":     0.0003,   # funding > +0.03% → penalti -2 (overbought)
    "funding_bonus_avg":      -0.0002,   # funding < -0.02% → bonus +2 (squeeze setup)
    "funding_bonus_cumul":    -0.001,    # funding cumul < -0.1% → bonus +1

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h":                168,
    "candle_15m":                96,
    "candle_4h":                 48,   # BARU: untuk HTF accumulation filter

    # ── Entry / SL — DEEP ENTRY MODEL (BARU) ────────────────────────────────
    # Model sebelumnya: breakout-oriented (mudah dimanipulasi market maker)
    # Model baru: masuk lebih dalam (tahan manipulasi)
    #   entry = VWAP - 0.5 * ATR
    #   SL    = entry - ATR
    #   TP    = entry + 2 * ATR
    "deep_entry_vwap_atr_mult":  0.5,  # entry = VWAP - N * ATR
    "deep_entry_sl_atr_mult":    1.0,  # SL    = entry - N * ATR
    "deep_entry_tp_atr_mult":    2.0,  # TP    = entry + N * ATR
    # Flag: True = pakai deep entry model; False = pakai model breakout lama
    "use_deep_entry":           True,

    # Entry lama (breakout) — tetap dipakai jika use_deep_entry=False
    "entry_bos_buffer":         0.001,
    "entry_vwap_buffer":        0.001,
    "sl_swing_lookback":        12,
    "sl_swing_buffer":          0.003,
    "sl_atr_multiplier_min":    0.5,
    "sl_atr_multiplier_max":    2.5,
    "max_sl_pct":               8.0,
    "min_sl_pct":               0.5,

    # ── Operasional ───────────────────────────────────────────────────────────
    "alert_cooldown_sec":      1800,
    "sleep_coins":              0.8,
    "sleep_error":              3.0,
    "cooldown_file":           "./cooldown.json",
    "funding_snapshot_file":   "./funding.json",

    # ── Bobot skor — disesuaikan (fokus pre-pump accumulation) ───────────────
    #
    # PERUBAHAN DARI v14.3:
    # - score_ema_gap: 5 → 2 (EMA gap = momentum, bukan accumulation)
    # - score_vol_compression: 3 → 4 (kompensasi EMA gap yang dikurangi)
    # - vol_ratio_threshold: 2.5 → 1.5 (accumulation detection lebih awal)
    # - Tambahan: htf_accumulation (+3) dan liquidity_sweep (+3)
    "score_ema_gap":            2,   # DITURUNKAN dari 5 — momentum, bukan pre-pump
    "score_atr_15":             4,
    "score_bbw_10":             4,
    "score_above_vwap_bos":     4,
    "score_bos_up":             3,
    "score_atr_10":             3,
    "score_bbw_6":              3,
    "score_funding_neg_pct":    3,
    "score_funding_streak":     3,
    "score_higher_low":         2,
    "score_bb_squeeze":         2,
    "score_rsi_65":             2,
    "score_funding_cumul":      2,
    "score_vol_ratio":          2,
    "score_vol_accel":          2,
    "score_rsi_55":             1,
    "score_price_chg":          1,

    # ── Accumulation + Compression (dinaikkan kompensasi EMA gap) ─────────────
    "score_accumulation":       4,
    "score_vol_compression":    4,   # DINAIKKAN dari 3 → 4

    # ── HTF Accumulation Filter 4H (BARU) ────────────────────────────────────
    # Banyak pump dimulai dari 4H accumulation yang tidak terdeteksi 1H/15m
    # Kondisi: 4H ATR menyempit + 4H volume naik + 4H range sempit
    "htf_atr_contract_ratio":   0.85,  # 4H ATR terkini < 85% dari rata-rata
    "htf_vol_ratio_min":        1.3,   # 4H volume > 1.3x rata-rata 4H
    "htf_range_max_pct":        3.0,   # 4H range < 3% (sideways/konsolidasi)
    "score_htf_accumulation":   3,     # skor jika 4H accumulation terdeteksi

    # ── Liquidity Sweep Detection (BARU) ─────────────────────────────────────
    # Pola: harga turun ke bawah support (ambil stop loss), lalu bounce kembali
    # Ini adalah tanda stop hunt sebelum reversal/pump besar
    "liq_sweep_lookback":       20,    # berapa candle ke belakang untuk cari support
    "liq_sweep_wick_min_pct":   0.3,   # minimal wick bawah 0.3% dari range candle
    "score_liquidity_sweep":    3,     # skor jika sweep terdeteksi

    # ── OI Expansion ─────────────────────────────────────────────────────────
    "oi_change_min_pct":        3.0,
    "oi_strong_pct":           10.0,
    "score_oi_expansion":       3,
    "score_oi_strong":          5,

    # ── BTC Regime + Outperformance ───────────────────────────────────────────
    "btc_bearish_threshold":   -3.0,
    "btc_bullish_threshold":    3.0,
    "outperform_min_delta":     2.0,
    "score_outperform":         3,

    # ── Threshold indikator ───────────────────────────────────────────────────
    "above_vwap_rate_min":      0.6,
    "ema_gap_threshold":        1.0,
    "bbw_threshold_high":       0.10,
    "bbw_threshold_mid":        0.06,
    "bb_squeeze_threshold":     0.04,
    # DIUBAH: 2.5 → 1.5 untuk menangkap fase accumulation lebih awal
    "vol_ratio_threshold":      1.5,   # DITURUNKAN dari 2.5 — accumulation detection
    "vol_accel_threshold":      0.5,
    "funding_streak_min":       5,

    # ── Smart Money Accumulation ──────────────────────────────────────────────
    "accum_vol_ratio":          1.5,
    "accum_price_range_max":    2.0,
    "accum_atr_lookback_long":  24,
    "accum_atr_lookback_short": 6,
    "accum_atr_contract_ratio": 0.75,
}

MANUAL_EXCLUDE = set()

EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST — dipangkas ke top ~100 pairs (OI & volume tinggi)
#  Whitelist terlalu besar = memonitor coin dengan liquidity rendah,
#  manipulasi tinggi, spread besar. Dibatasi ke pair dengan OI & volume kuat.
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

GRAN_MAP    = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
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
#  💾  FUNDING SNAPSHOTS — batch I/O
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
    except Exception as e:
        log.warning(f"Gagal simpan funding snapshot: {e}")

def add_funding_snapshot(symbol, funding_rate):
    now = time.time()
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({"ts": now, "funding": funding_rate})
    _funding_snapshots[symbol] = sorted(
        _funding_snapshots[symbol], key=lambda x: x["ts"]
    )[-20:]

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit — tunggu 15s")
                time.sleep(15)
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg):
    """
    Kirim pesan ke Telegram dengan error logging yang proper.
    v15.3: tambah log error detail agar masalah pengiriman terdeteksi.
    Telegram membatasi pesan maksimum 4096 karakter — potong jika perlu.
    """
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN atau CHAT_ID tidak ada!")
        return False
    # Telegram max 4096 chars — potong dan tambah marker jika terlalu panjang
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
            "symbol": symbol,
            "granularity": g,
            "limit": str(limit),
            "productType": "usdt-futures",
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
                "open":     float(c[1]),
                "high":     float(c[2]),
                "low":      float(c[3]),
                "close":    float(c[4]),
                "volume":   float(c[5]),
                "volume_usd": vol_usd,
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    _cache[key] = (time.time(), candles)
    return candles

def get_funding(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            return float(data["data"][0].get("fundingRate", 0))
        except Exception:
            pass
    return 0.0

def get_btc_candles_cached(limit=48):
    """
    Cache candle BTCUSDT 1h selama 5 menit — hemat ~100 API call per scan.
    """
    global _btc_candles_cache
    if time.time() - _btc_candles_cache["ts"] < 300 and _btc_candles_cache["data"]:
        return _btc_candles_cache["data"]
    candles = get_candles("BTCUSDT", "1h", limit)
    if candles:
        _btc_candles_cache = {"ts": time.time(), "data": candles}
    return candles

def get_funding_stats(symbol):
    """
    Hitung statistik funding dari snapshot in-memory.
    """
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None

    all_rates = [s["funding"] for s in snaps]
    last6     = all_rates[-6:]
    avg6      = sum(last6) / len(last6)
    cumul     = sum(last6)
    neg_pct   = sum(1 for f in last6 if f < 0) / len(last6) * 100

    streak = 0
    for f in reversed(all_rates):
        if f < 0:
            streak += 1
        else:
            break

    basis = all_rates[-1] * 100

    return {
        "avg":          avg6,
        "cumulative":   cumul,
        "neg_pct":      neg_pct,
        "streak":       streak,
        "basis":        basis,
        "current":      all_rates[-1],
        "sample_count": len(all_rates),
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
    """
    ema_gap = close_terakhir / EMA(period)
    CATATAN v15: bobot diturunkan (5→2) karena ini indikator momentum,
    bukan accumulation. Scanner harus deteksi sebelum EMA gap terjadi.
    """
    if len(candles) < period + 1:
        return 0.0
    closes  = [c["close"] for c in candles]
    ema_val = _calc_ema_series(closes, period)
    if ema_val is None or ema_val == 0:
        return 0.0
    return candles[-1]["close"] / ema_val

def calc_bbw(candles, period=20):
    """BB Width dalam format desimal."""
    if len(candles) < period:
        return 0.0, 0.5
    closes   = [c["close"] for c in candles[-period:]]
    mean     = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std      = math.sqrt(variance)
    bb_upper = mean + 2 * std
    bb_lower = mean - 2 * std
    bbw      = (bb_upper - bb_lower) / mean if mean > 0 else 0.0
    last     = candles[-1]["close"]
    if bb_upper - bb_lower == 0:
        bb_pct = 0.5
    else:
        bb_pct = (last - bb_lower) / (bb_upper - bb_lower)
    return bbw, bb_pct

def calc_bb_squeeze(candles, period=20):
    """BB Squeeze: band sangat sempit (< 4%) → konsolidasi sebelum breakout."""
    bbw, _ = calc_bbw(candles, period)
    return bbw < CONFIG["bb_squeeze_threshold"]

def calc_atr_pct(candles, period=14):
    """ATR sebagai % dari harga close terakhir."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h  = candles[idx]["high"]
        l  = candles[idx]["low"]
        pc = candles[idx - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
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
        h  = candles[idx]["high"]
        l  = candles[idx]["low"]
        pc = candles[idx - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else candles[-1]["close"] * 0.01

def calc_vwap(candles, lookback=24):
    """VWAP rolling: rata-rata harga tertimbang volume."""
    n = min(lookback, len(candles))
    if n == 0:
        return candles[-1]["close"] if candles else 0.0
    recent = candles[-n:]
    cum_tv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in recent)
    cum_v  = sum(c["volume"] for c in recent)
    return (cum_tv / cum_v) if cum_v > 0 else candles[-1]["close"]

def get_rsi(candles, period=14):
    """RSI Wilder (smoothed moving average)."""
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))

def detect_bos_up(candles, lookback=3):
    """Break of Structure ke atas."""
    if len(candles) < lookback + 1:
        return False, 0.0
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level  = max(prev_highs)
    is_bos     = candles[-1]["close"] > bos_level
    return is_bos, bos_level

def higher_low_detected(candles):
    """Higher Low: low candle terakhir > semua low dalam 5 candle sebelumnya."""
    if len(candles) < 6:
        return False
    lows = [c["low"] for c in candles[-6:]]
    return lows[-1] > min(lows[:-1])

def calc_accumulation_phase(candles):
    """
    Deteksi fase akumulasi smart money sebelum pump.
    Vol naik + harga sideways = smart money accumulation
    ATR + BB menyempit = volatility compression
    """
    if len(candles) < 36:
        return {"is_accumulating": False, "is_vol_compress": False,
                "vol_ratio_4h": 0.0, "price_range_pct": 0.0,
                "atr_contract": 1.0, "bbw_contracting": False,
                "phase_label": "Data kurang", "phase_score": 0}

    vol_4h  = sum(c["volume_usd"] for c in candles[-4:]) / 4
    vol_24h = sum(c["volume_usd"] for c in candles[-28:-4]) / 24
    vol_ratio_4h = (vol_4h / vol_24h) if vol_24h > 0 else 1.0

    r12 = candles[-12:]
    hi12 = max(c["high"] for c in r12)
    lo12 = min(c["low"]  for c in r12)
    mid12 = (hi12 + lo12) / 2
    price_range_pct = ((hi12 - lo12) / mid12 * 100) if mid12 > 0 else 99.0

    def _atr(cl, n):
        trs = []
        for i in range(1, min(n + 1, len(cl))):
            ix = len(cl) - i
            if ix < 1:
                break
            h = cl[ix]["high"]; l = cl[ix]["low"]; pc = cl[ix-1]["close"]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return sum(trs) / len(trs) if trs else 0.0

    atr_s = _atr(candles, CONFIG["accum_atr_lookback_short"])
    atr_l = _atr(candles, CONFIG["accum_atr_lookback_long"])
    atr_contract = (atr_s / atr_l) if atr_l > 0 else 1.0

    bbw_now, _ = calc_bbw(candles)
    bbw_12h, _ = calc_bbw(candles[:-12]) if len(candles) > 32 else (bbw_now, 0.0)
    bbw_contracting = (bbw_now < bbw_12h * 0.85) if bbw_12h > 0 else False

    vol_rising     = vol_ratio_4h    >= CONFIG["accum_vol_ratio"]
    price_sideways = price_range_pct <= CONFIG["accum_price_range_max"]
    atr_shrinking  = atr_contract    <= CONFIG["accum_atr_contract_ratio"]
    is_vol_compress = atr_shrinking and bbw_contracting
    is_accumulating = vol_rising and price_sideways

    if is_accumulating and is_vol_compress:
        ps, pl = 2, "🏦 AKUMULASI + COMPRESSION — setup pra-pump kuat"
    elif is_accumulating:
        ps, pl = 1, "📦 AKUMULASI — volume naik, harga sideways"
    elif is_vol_compress:
        ps, pl = 1, "🗜️ VOLATILITY COMPRESSION — energi menumpuk"
    else:
        ps, pl = 0, "—"

    return {
        "is_accumulating":  is_accumulating,
        "is_vol_compress":  is_vol_compress,
        "vol_ratio_4h":     round(vol_ratio_4h, 2),
        "price_range_pct":  round(price_range_pct, 2),
        "atr_contract":     round(atr_contract, 3),
        "bbw_contracting":  bbw_contracting,
        "phase_label":      pl,
        "phase_score":      ps,
    }

def calc_htf_accumulation(candles_4h):
    """
    HTF Accumulation Filter — deteksi akumulasi di timeframe 4H.

    Banyak pump besar dimulai dari konsolidasi 4H yang tidak terlihat di 1H.
    Scanner sebelumnya hanya melihat 1H + 15m sehingga melewatkan build-up ini.

    Kondisi HTF accumulation:
      1. 4H ATR < rata-rata ATR 4H (volatility compression di TF besar)
      2. 4H volume > 1.3x rata-rata (ada buying interest tersembunyi)
      3. 4H range 8 candle terakhir < 3% (harga sideways di TF besar)

    Return:
      is_htf_accum : True jika semua kondisi terpenuhi
      detail       : dict berisi nilai masing-masing kondisi
    """
    if len(candles_4h) < 16:
        return {
            "is_htf_accum": False,
            "atr_ratio": 1.0,
            "vol_ratio": 1.0,
            "range_pct": 99.0,
            "label": "Data 4H tidak cukup",
        }

    # 4H ATR — 4 candle terkini vs rata-rata 12 candle sebelumnya
    def _atr4(cl, n):
        trs = []
        for i in range(1, min(n + 1, len(cl))):
            ix = len(cl) - i
            if ix < 1:
                break
            h  = cl[ix]["high"]
            l  = cl[ix]["low"]
            pc = cl[ix - 1]["close"]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return sum(trs) / len(trs) if trs else 0.0

    atr_recent = _atr4(candles_4h, 4)
    atr_avg    = _atr4(candles_4h, 12)
    atr_ratio  = (atr_recent / atr_avg) if atr_avg > 0 else 1.0

    # 4H volume — 2 candle terkini vs rata-rata 8 candle sebelumnya
    vol_recent = sum(c["volume_usd"] for c in candles_4h[-2:]) / 2
    vol_avg    = sum(c["volume_usd"] for c in candles_4h[-10:-2]) / 8 if len(candles_4h) >= 10 else vol_recent
    vol_ratio  = (vol_recent / vol_avg) if vol_avg > 0 else 1.0

    # 4H price range — 8 candle terakhir
    r8   = candles_4h[-8:]
    hi8  = max(c["high"] for c in r8)
    lo8  = min(c["low"]  for c in r8)
    mid8 = (hi8 + lo8) / 2
    range_pct = ((hi8 - lo8) / mid8 * 100) if mid8 > 0 else 99.0

    # Evaluasi kondisi
    atr_compressed = atr_ratio  <= CONFIG["htf_atr_contract_ratio"]
    vol_building   = vol_ratio  >= CONFIG["htf_vol_ratio_min"]
    price_sideways = range_pct  <= CONFIG["htf_range_max_pct"]

    is_htf_accum = atr_compressed and vol_building and price_sideways

    if is_htf_accum:
        label = (
            f"🕯️ 4H HTF Akumulasi — ATR ratio {atr_ratio:.2f}, "
            f"vol {vol_ratio:.1f}x, range {range_pct:.1f}%"
        )
    elif atr_compressed and price_sideways:
        label = f"🕯️ 4H Konsolidasi (vol belum naik) — range {range_pct:.1f}%"
    else:
        label = "—"

    return {
        "is_htf_accum": is_htf_accum,
        "atr_ratio":    round(atr_ratio, 3),
        "vol_ratio":    round(vol_ratio, 2),
        "range_pct":    round(range_pct, 2),
        "label":        label,
    }

def detect_liquidity_sweep(candles, lookback=None):
    """
    Liquidity Sweep Detection — identifikasi stop hunt sebelum reversal/pump.

    Pola stop hunt (liquidity sweep):
      1. Harga menembus ke bawah level support terdahulu (ambil stop loss)
      2. Candle memiliki wick bawah panjang (tanda penolakan harga rendah)
      3. Close kembali di atas support (market maker selesai mengumpulkan)

    Urutan pump yang ideal:
      Accumulation → Liquidity Sweep → Leverage Build Up →
      Volatility Compression → Expansion

    Fase Liquidity Sweep adalah konfirmasi kuat bahwa expansion segera terjadi.

    Return:
      is_sweep   : True jika pola terdeteksi
      sweep_low  : level terendah yang di-sweep
      support    : level support yang dilanggar
      detail     : dict berisi nilai detail
    """
    if lookback is None:
        lookback = CONFIG["liq_sweep_lookback"]

    if len(candles) < lookback + 3:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "Data kurang"}

    # Cari support level dari candle sebelumnya (lookback candle, kecualikan 3 terbaru)
    reference_candles = candles[-(lookback + 3):-3]
    if not reference_candles:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "—"}

    # Support = rata-rata dari 3 low terendah dalam periode referensi
    lows_sorted = sorted(c["low"] for c in reference_candles)
    support_level = sum(lows_sorted[:3]) / 3

    # Analisis 3 candle terbaru untuk pola sweep
    recent = candles[-3:]
    sweep_detected = False
    sweep_candle   = None
    sweep_low_val  = 0.0

    for candle in recent:
        candle_range = candle["high"] - candle["low"]
        if candle_range <= 0:
            continue

        wick_bottom = candle["open"] - candle["low"] if candle["close"] > candle["open"] else candle["close"] - candle["low"]
        wick_pct    = wick_bottom / candle_range if candle_range > 0 else 0

        # Kondisi sweep:
        # 1. Low candle menembus ke bawah support
        # 2. Close candle kembali di atas support
        # 3. Wick bawah signifikan (penolakan harga rendah)
        went_below  = candle["low"] < support_level
        closed_above = candle["close"] > support_level
        has_wick    = wick_pct >= CONFIG["liq_sweep_wick_min_pct"]

        if went_below and closed_above and has_wick:
            sweep_detected = True
            sweep_candle   = candle
            sweep_low_val  = candle["low"]
            break

    if sweep_detected and sweep_candle is not None:
        depth_pct = (support_level - sweep_low_val) / support_level * 100
        label = (
            f"🎯 Liquidity Sweep — low ${sweep_low_val:.6g} menembus support "
            f"${support_level:.6g} ({depth_pct:.2f}%), close kembali di atas"
        )
    else:
        label = "—"

    return {
        "is_sweep":   sweep_detected,
        "sweep_low":  round(sweep_low_val, 8),
        "support":    round(support_level, 8),
        "label":      label,
    }

def get_open_interest(symbol):
    """Ambil Open Interest dari Bitget Futures API. Return float USD."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d = data["data"]
            if isinstance(d, list) and d:
                d = d[0]
            oi = float(
                d.get("openInterestList", [{}])[0].get("openInterest", 0)
                if "openInterestList" in d
                else d.get("openInterest", d.get("holdingAmount", 0))
            )
            price = float(d.get("indexPrice", d.get("lastPr", 0)) or 0)
            if 0 < oi < 1e9 and price > 0:
                return oi * price
            return oi
        except Exception:
            pass
    return 0.0

_oi_snapshot = {}

def get_oi_change(symbol):
    """Hitung % perubahan OI sejak snapshot terakhir."""
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
        "oi_now":      round(oi_now, 2),
        "oi_prev":     round(oi_prev, 2),
        "change_pct":  round(change_pct, 2),
        "is_new":      False,
    }

def calc_btc_correlation(coin_candles, btc_candles, lookback=24):
    """
    Pearson correlation antara pct_change coin dan pct_change BTC.
    Interpretasi:
      corr >= 0.75 → CORRELATED   : coin mengikuti BTC kuat
      corr  0.4-0.74 → MODERATE   : sebagian mengikuti BTC
      corr < 0.40  → INDEPENDENT  : coin bergerak sendiri (lebih ideal untuk pump)
    """
    if not coin_candles or not btc_candles or len(coin_candles) < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": 0, "risk_note": "Data tidak cukup"}

    n   = min(lookback, len(coin_candles), len(btc_candles))
    c_c = coin_candles[-n:]
    c_b = btc_candles[-n:]

    def pct_changes(candles):
        changes = []
        for i in range(1, len(candles)):
            prev = candles[i-1]["close"]
            if prev > 0:
                changes.append((candles[i]["close"] - prev) / prev)
        return changes

    cc = pct_changes(c_c)
    cb = pct_changes(c_b)

    mn = min(len(cc), len(cb))
    if mn < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": mn, "risk_note": "Data tidak cukup"}

    cc, cb = cc[-mn:], cb[-mn:]

    mc   = sum(cc) / mn
    mb   = sum(cb) / mn
    num  = sum((x - mc) * (y - mb) for x, y in zip(cc, cb))
    sd_c = (sum((x - mc)**2 for x in cc)) ** 0.5
    sd_b = (sum((y - mb)**2 for y in cb)) ** 0.5

    if sd_c < 1e-10 or sd_b < 1e-10:
        corr = 0.0
    else:
        corr = num / (sd_c * sd_b)
        corr = max(-1.0, min(1.0, corr))

    if corr >= 0.75:
        label     = "CORRELATED"
        emoji     = "🔗"
        risk_note = "⚠️ Ikuti BTC! Jika BTC dump → exit cepat"
    elif corr >= 0.40:
        label     = "MODERATE"
        emoji     = "〰️"
        risk_note = "🔶 Sebagian ikuti BTC — pantau jika BTC turun"
    else:
        label     = "INDEPENDENT"
        emoji     = "🚀"
        risk_note = "✅ Pergerakan independen — lebih tahan dump BTC"

    btc_chg = 0.0
    if len(c_b) >= 2 and c_b[0]["close"] > 0:
        btc_chg = (c_b[-1]["close"] - c_b[0]["close"]) / c_b[0]["close"] * 100

    if btc_chg <= CONFIG["btc_bearish_threshold"]:
        btc_regime, btc_re = "BEARISH", "🔻"
        btc_rn = f"⚠️ BTC bearish ({btc_chg:+.1f}%/{mn}h) — risiko tinggi"
    elif btc_chg >= CONFIG["btc_bullish_threshold"]:
        btc_regime, btc_re = "BULLISH", "🟢"
        btc_rn = f"✅ BTC bullish ({btc_chg:+.1f}%/{mn}h) — kondisi favorable"
    else:
        btc_regime, btc_re = "SIDEWAYS", "⬜"
        btc_rn = f"BTC sideways ({btc_chg:+.1f}%/{mn}h) — altcoin bisa gerak independen"

    coin_chg = 0.0
    if len(c_c) >= 2 and c_c[0]["close"] > 0:
        coin_chg = (c_c[-1]["close"] - c_c[0]["close"]) / c_c[0]["close"] * 100

    delta = coin_chg - btc_chg
    if delta >= CONFIG["outperform_min_delta"] and coin_chg > 0:
        op_label, op_emoji = "OUTPERFORM",   "🚀"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% (+{delta:.1f}% lebih kuat)"
    elif delta <= -CONFIG["outperform_min_delta"]:
        op_label, op_emoji = "UNDERPERFORM", "📉"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% ({delta:.1f}% lebih lemah)"
    else:
        op_label, op_emoji = "IN-LINE",      "〰️"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% — pergerakan sejalan"

    return {
        "correlation":       round(corr, 3),
        "label":             label,
        "emoji":             emoji,
        "lookback":          mn,
        "risk_note":         risk_note,
        "btc_regime":        btc_regime,
        "btc_regime_emoji":  btc_re,
        "btc_regime_note":   btc_rn,
        "btc_period_chg":    round(btc_chg, 2),
        "coin_period_chg":   round(coin_chg, 2),
        "outperform_label":  op_label,
        "outperform_emoji":  op_emoji,
        "outperform_note":   op_note,
        "delta_vs_btc":      round(delta, 2),
    }

def calc_uptrend_age(candles):
    """
    Ukur sudah berapa candle (jam) harga berada dalam tren naik berturut-turut.
    Pre-pump ideal: harga baru saja mulai naik (1-8 jam terakhir).
    """
    if len(candles) < 4:
        return {"age_hours": 0, "is_fresh": False, "is_late": False}

    streak = 0
    for i in range(len(candles) - 1, 0, -1):
        if candles[i]["close"] > candles[i - 1]["close"]:
            streak += 1
        else:
            break

    return {
        "age_hours": streak,
        "is_fresh":  1 <= streak <= 8,
        "is_late":   streak > 12,
    }

def calc_support_resistance(candles, lookback=48, n_levels=3):
    """
    Level support & resistance dari pivot point 48 candle terakhir.
    """
    if len(candles) < 10:
        return {"resistance": [], "support": [], "nearest_res": None, "nearest_sup": None}

    n      = min(lookback, len(candles))
    recent = candles[-n:]
    price  = candles[-1]["close"]

    pivots_high = []
    pivots_low  = []
    for i in range(1, len(recent) - 1):
        h = recent[i]["high"]
        l = recent[i]["low"]
        if h > recent[i-1]["high"] and h > recent[i+1]["high"]:
            pivots_high.append(h)
        if l < recent[i-1]["low"] and l < recent[i+1]["low"]:
            pivots_low.append(l)

    def cluster_levels(levels, cluster_pct=0.005):
        if not levels:
            return []
        levels = sorted(levels)
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

    resistance_all = cluster_levels(pivots_high)
    support_all    = cluster_levels(pivots_low)

    resistance = sorted([r for r in resistance_all if r > price * 1.001])[:n_levels]
    support    = sorted([s for s in support_all    if s < price * 0.999], reverse=True)[:n_levels]

    def fmt_level(lv, ref_price):
        gap = (lv - ref_price) / ref_price * 100
        return {"level": round(lv, 8), "gap_pct": round(gap, 1)}

    return {
        "resistance":   [fmt_level(r, price) for r in resistance],
        "support":      [fmt_level(s, price) for s in support],
        "nearest_res":  fmt_level(resistance[0], price) if resistance else None,
        "nearest_sup":  fmt_level(support[0], price) if support else None,
    }

def calc_volume_ratio(candles, lookback=24):
    """
    Rasio volume candle terakhir vs rata-rata lookback candle sebelumnya.
    PERUBAHAN v15: threshold turun dari 2.5 → 1.5 untuk deteksi accumulation lebih awal.
    """
    if len(candles) < lookback + 1:
        return 0.0
    avg_vol = sum(c["volume_usd"] for c in candles[-(lookback + 1):-1]) / lookback
    if avg_vol <= 0:
        return 0.0
    return candles[-1]["volume_usd"] / avg_vol

def calc_volume_acceleration(candles):
    """Volume acceleration: volume 1h terakhir vs rata-rata 3h sebelumnya."""
    if len(candles) < 4:
        return 0.0
    vol_1h = candles[-1]["volume_usd"]
    vol_3h = sum(c["volume_usd"] for c in candles[-4:-1]) / 3
    if vol_3h <= 0:
        return 0.0
    return (vol_1h - vol_3h) / vol_3h

def check_volume_consistent(candles, lookback=3, min_ratio=1.5):
    """
    Anti-manipulasi: volume tinggi harus konsisten di >= 2 candle terakhir,
    bukan hanya 1 spike.
    """
    if len(candles) < 24:
        return False
    avg_vol = sum(c["volume_usd"] for c in candles[-24:]) / 24
    if avg_vol <= 0:
        return False
    recent    = candles[-lookback:]
    above_avg = sum(1 for c in recent if c["volume_usd"] > avg_vol * min_ratio)
    return above_avg >= max(1, lookback // 2)

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def calc_fib_targets(entry, candles):
    """
    Target Fibonacci extension 1.272 dan 1.618.
    Fallback ke +8% dan +15% jika pola tidak valid.
    """
    lookback = min(48, len(candles))
    recent   = candles[-lookback:]

    lows  = [(i, c["low"])  for i, c in enumerate(recent)]
    highs = [(i, c["high"]) for i, c in enumerate(recent)]

    low_idx,  swing_low  = min(lows,  key=lambda x: x[1])
    high_idx, swing_high = max(highs, key=lambda x: x[1])

    if low_idx >= high_idx or (swing_high - swing_low) <= 0:
        return round(entry * 1.08, 8), round(entry * 1.15, 8)

    fib_range = swing_high - swing_low
    t1 = swing_low + fib_range * 1.272
    t2 = swing_low + fib_range * 1.618

    if t1 <= entry:
        t1 = entry * 1.08
    if t2 <= t1:
        t2 = t1 * 1.08

    return round(t1, 8), round(t2, 8)

def find_swing_low_sl(candles, lookback=12):
    """Cari swing low terbaru dalam lookback candle sebagai dasar SL."""
    n = min(lookback, len(candles) - 1)
    if n < 2:
        return None
    recent_lows = [c["low"] for c in candles[-(n + 1):-1]]
    swing_low   = min(recent_lows)
    return swing_low * (1.0 - CONFIG["sl_swing_buffer"])

def calc_entry(candles, bos_level, alert_level, vwap, price_now, atr_abs_val=None, sr=None):
    """
    Entry & SL & Target — v15.3

    Entry:
      HIGH  → tepat di atas BOS level (konfirmasi breakout)
      MEDIUM → pullback VWAP (harga dekat VWAP = optimal entry)
               Jika harga sudah jauh di atas VWAP (>2%), pakai market price.

    SL:
      Dari swing low struktur 12 candle terakhir.
      Clamp ke 1.5x–3x ATR agar tidak terlalu sempit atau terlalu lebar.

    Target (per-coin, dinamis — bukan flat persentase):
      1. Prioritas: level resistance terdekat dari pivot 48–168 candle
         → T1 = R1 (resistance terdekat di atas entry)
         → T2 = R2 (resistance kedua) atau R1 * 1.272 (fib ext)

      2. Jika tidak ada pivot resistance:
         → Gunakan swing high range sebagai proyeksi
         → T1 = entry + range * 0.618 (fib 61.8% extension)
         → T2 = entry + range * 1.000 (fib 100% extension)

      3. Minimum floor: T1 ≥ entry * 1.08, T2 ≥ entry * 1.15
         (ini minimum pump yang wajar, bukan target flat 15%)

    Semua angka desimal diformat str agar tidak muncul sebagai tag HTML di Telegram.
    """
    if atr_abs_val is None:
        atr_abs_val = calc_atr_abs(candles)

    # ── Entry ─────────────────────────────────────────────────────────────────
    gap_to_vwap_pct = (price_now - vwap) / vwap * 100 if vwap > 0 else 0

    if alert_level == "HIGH" and bos_level > 0 and bos_level < price_now * 1.05:
        # HIGH: beli dekat BOS level (konfirmasi)
        entry = bos_level * 1.0005
        entry_reason = "BOS breakout"
    elif gap_to_vwap_pct <= 2.0:
        # Harga dekat VWAP → entry di VWAP (pullback optimal)
        entry = max(vwap, price_now)
        entry_reason = "VWAP pullback"
    else:
        # Harga sudah jauh di atas VWAP → entry market price
        entry = price_now * 1.001
        entry_reason = "market price"

    # Pastikan entry tidak di bawah harga sekarang
    if entry < price_now:
        entry = price_now * 1.001

    # ── SL: swing low 12 candle ───────────────────────────────────────────────
    sl_swing = find_swing_low_sl(candles, lookback=12)
    if sl_swing is None or sl_swing >= entry:
        sl_swing = entry - atr_abs_val * 2.0

    sl_floor = entry - atr_abs_val * 3.0   # max loss = 3x ATR
    sl_ceil  = entry - atr_abs_val * 1.0   # min loss = 1x ATR

    sl = sl_swing
    sl = max(sl, sl_floor)
    sl = min(sl, sl_ceil)

    # Hard absolute limits
    sl = max(sl, entry * 0.92)   # max SL 8%
    sl = min(sl, entry * 0.995)  # min SL 0.5%

    if sl >= entry:
        sl = entry * 0.98

    # ── Target: resistance-based, per-coin ───────────────────────────────────
    # Kumpulkan semua resistance yang valid di atas entry
    res_levels = []

    # 1. Dari sr (pivot resistance) yang sudah dihitung sebelumnya
    if sr and sr.get("resistance"):
        for rv in sr["resistance"]:
            if rv["level"] > entry * 1.005:
                res_levels.append(rv["level"])

    # 2. Scan ulang pivot dari candle untuk resistance lebih jauh (168 candle)
    lookback_long = min(168, len(candles))
    recent_long   = candles[-lookback_long:]
    pivot_highs   = []
    for i in range(2, len(recent_long) - 2):
        h = recent_long[i]["high"]
        if (h > recent_long[i-1]["high"] and h > recent_long[i-2]["high"] and
                h > recent_long[i+1]["high"] and h > recent_long[i+2]["high"]):
            pivot_highs.append(h)

    # Cluster pivot highs
    if pivot_highs:
        pivot_highs = sorted(set(pivot_highs))
        clusters = []
        cur = [pivot_highs[0]]
        for ph in pivot_highs[1:]:
            if (ph - cur[-1]) / cur[-1] < 0.015:
                cur.append(ph)
            else:
                clusters.append(sum(cur) / len(cur))
                cur = [ph]
        clusters.append(sum(cur) / len(cur))
        for c in clusters:
            if c > entry * 1.005 and c not in res_levels:
                res_levels.append(c)

    res_levels = sorted(set(res_levels))

    # 3. Swing range fallback
    swing_low_val  = min(c["low"]  for c in recent_long)
    swing_high_val = max(c["high"] for c in recent_long)
    swing_range    = swing_high_val - swing_low_val

    if res_levels:
        # Ada resistance → gunakan sebagai target
        t1 = res_levels[0]   # R1 = resistance terdekat
        if len(res_levels) >= 2:
            t2 = res_levels[1]   # R2 = resistance kedua
        else:
            # Hanya 1 level → proyeksikan dari R1
            t2 = t1 * 1.272  # 27.2% di atas R1 (fib ext)
    else:
        # Tidak ada resistance → proyeksi dari swing range
        t1 = entry + swing_range * 0.618
        t2 = entry + swing_range * 1.000

    # Floor minimum (bukan flat % — ini hanya batas bawah logis)
    t1 = max(t1, entry * 1.07)   # minimal 7% di atas entry
    t2 = max(t2, entry * 1.12)   # minimal 12% di atas entry
    if t2 <= t1:
        t2 = t1 * 1.10

    # ── R/R ───────────────────────────────────────────────────────────────────
    risk   = entry - sl
    reward = t1 - entry
    rr_val = round(reward / risk, 1) if risk > 0 else 0.0
    sl_pct = round((entry - sl) / entry * 100, 2)

    return {
        "entry":        round(entry, 8),
        "sl":           round(sl, 8),
        "sl_pct":       sl_pct,
        "t1":           round(t1, 8),
        "t2":           round(t2, 8),
        "rr":           rr_val,
        "rr_str":       f"{rr_val:.1f}",   # string — hindari HTML tag bug Telegram
        "vwap":         round(vwap, 8),
        "bos_level":    round(bos_level, 8),
        "alert_level":  alert_level,
        "gain_t1_pct":  round((t1 - entry) / entry * 100, 1),
        "gain_t2_pct":  round((t2 - entry) / entry * 100, 1),
        "atr_abs":      round(atr_abs_val, 8),
        "sl_method":    entry_reason,
        "used_resistance": len(res_levels) > 0,
        "n_res_levels": len(res_levels),
    }

def detect_energy_buildup(candles_1h, oi_data):
    """
    Energy Build-Up Detector — "OI Build + Volume Build + Price Stuck"

    Ini adalah pola paling penting yang sering diabaikan scanner biasa.
    Sebagian besar scanner mencari: volume spike, momentum, breakout.
    Padahal sebelum pump besar sering terjadi:
      - volume naik perlahan
      - OI naik (posisi dibangun)
      - price sideways (harga DITAHAN oleh market maker/whale)

    Ini disebut absorption: market maker menyerap order agar harga stabil
    sambil mengumpulkan posisi besar. Ketika selesai → harga dilepas cepat.

    Struktur pola:
      Time  →
      Price : ──────────── (flat)
      Volume: ▁▂▃▄▅ (naik)
      OI    : ▁▂▃▄▅ (naik)
      ATR   : ▁▁▁▁▁ (rendah)

    Kondisi deteksi:
      1. OI naik > 5% (ada posisi baru dibangun)
      2. Volume 1h > 1.5x rata-rata (aktivitas trading tinggi)
      3. Price range 1h < 2.5% (harga tidak bergerak meski ada aktivitas)

    Versi kuat: + funding <= 0 (majoritas trader short/netral → potensi squeeze)

    Return:
      is_buildup  : True jika pola terdeteksi
      is_strong   : True jika + funding netral/negatif (squeeze potential)
      detail      : dict berisi nilai masing-masing kondisi
    """
    if len(candles_1h) < 24:
        return {
            "is_buildup": False, "is_strong": False,
            "oi_change": 0.0, "vol_ratio": 0.0, "range_pct": 0.0,
            "label": "Data tidak cukup",
        }

    # Kondisi 1: OI naik
    oi_change = oi_data.get("change_pct", 0.0)
    oi_rising = (not oi_data.get("is_new", True)) and oi_change >= CONFIG["energy_oi_change_min"]

    # Kondisi 2: Volume naik
    vol_1h    = candles_1h[-1]["volume_usd"]
    avg_vol   = sum(c["volume_usd"] for c in candles_1h[-24:-1]) / 23 if len(candles_1h) >= 24 else vol_1h
    vol_ratio = (vol_1h / avg_vol) if avg_vol > 0 else 1.0
    vol_rising = vol_ratio >= CONFIG["energy_vol_ratio_min"]

    # Kondisi 3: Harga tidak bergerak (price stuck)
    recent_3h  = candles_1h[-3:]
    hi3  = max(c["high"]  for c in recent_3h)
    lo3  = min(c["low"]   for c in recent_3h)
    mid3 = (hi3 + lo3) / 2
    range_pct = ((hi3 - lo3) / mid3 * 100) if mid3 > 0 else 99.0
    price_stuck = range_pct <= CONFIG["energy_range_max_pct"]

    is_buildup = oi_rising and vol_rising and price_stuck

    # Versi kuat: + funding netral atau negatif
    # (Funding dikirim dari luar fungsi ini karena sudah dihitung di master_score)
    is_strong = False  # akan di-set dari master_score jika funding <= 0

    if is_buildup:
        label = (
            f"⚡ ENERGY BUILD-UP — OI +{oi_change:.1f}%, vol {vol_ratio:.1f}x, "
            f"range {range_pct:.1f}% (harga ditahan, posisi dibangun)"
        )
    else:
        conditions_met = sum([oi_rising, vol_rising, price_stuck])
        label = f"— ({conditions_met}/3 kondisi terpenuhi: OI={oi_rising}, vol={vol_rising}, stuck={price_stuck})"

    return {
        "is_buildup":  is_buildup,
        "is_strong":   is_strong,
        "oi_change":   round(oi_change, 2),
        "vol_ratio":   round(vol_ratio, 2),
        "range_pct":   round(range_pct, 2),
        "oi_rising":   oi_rising,
        "vol_rising":  vol_rising,
        "price_stuck": price_stuck,
        "label":       label,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candle_4h"])   # BARU: HTF candles

    if len(c1h) < 48:
        log.info(f"  {symbol}: Candle 1h tidak cukup ({len(c1h)} < 48)")
        return None

    # Parse data ticker
    try:
        vol_24h   = float(ticker.get("quoteVolume", 0))
        chg_24h   = float(ticker.get("change24h",  0)) * 100
        price_now = float(ticker.get("lastPr",      0)) or c1h[-1]["close"]
    except Exception:
        return None

    if vol_24h <= 0 or price_now <= 0:
        return None

    # ── GATE 0: Open Interest minimum (BARU) ─────────────────────────────────
    # Futures pump biasanya terjadi di coin dengan OI signifikan.
    # Coin dengan OI sangat rendah = illiquid, mudah dimanipulasi, spread besar.
    oi_data = get_oi_change(symbol)
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]:
        log.info(
            f"  {symbol}: OI terlalu kecil ${oi_data['oi_now']:,.0f} "
            f"< ${CONFIG['min_oi_usd']:,} — GATE GAGAL (coin illiquid)"
        )
        return None

    # ── GATE 1: Funding (WAJIB) ──────────────────────────────────────────────
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)
    fstats  = get_funding_stats(symbol)

    # v15.3: Funding snapshot belum cukup → lanjut dengan fstats kosong
    # (dulu: return None → melewatkan coin bagus di run pertama)
    if fstats is None:
        fstats = {
            "avg": funding, "cumulative": funding, "neg_pct": 0.0,
            "streak": 0, "basis": funding * 100, "current": funding,
            "sample_count": 1,
        }
        log.info(f"  {symbol}: Funding snapshot baru (1 data) — lanjut scan")

    # v15.3: Funding BUKAN lagi hard gate — hanya penalti/bonus skor
    # Log menunjukkan bahwa funding gate memblokir hampir semua coin di pasar saat ini
    # (mayoritas funding netral ≈ 0, bukan -0.0005 seperti era bull market)

    # ── GATE 2: above_vwap dengan toleransi (DIPERBARUI v15.3) ──────────────
    # Dari: price > vwap  (terlalu ketat — melewatkan accumulation di bawah VWAP)
    # Ke  : price > vwap * 0.97  (toleransi 3% di bawah VWAP)
    # Fase accumulation dan liquidity sweep sering terjadi di bawah VWAP.
    vwap = calc_vwap(c1h, lookback=24)
    vwap_gate_level = vwap * CONFIG["vwap_gate_tolerance"]
    if price_now < vwap_gate_level:
        log.info(
            f"  {symbol}: Harga terlalu jauh di bawah VWAP — GATE GAGAL "
            f"(${price_now:.6g} < ${vwap_gate_level:.6g} = VWAP*{CONFIG['vwap_gate_tolerance']})"
        )
        return None

    # ── Indikator teknikal ───────────────────────────────────────────────────
    ema_gap           = calc_ema_gap(c1h, period=20)
    bbw, bb_pct       = calc_bbw(c1h)
    bb_squeeze        = calc_bb_squeeze(c1h)
    atr_pct           = calc_atr_pct(c1h)
    rsi               = get_rsi(c1h[-48:])
    bos_up, bos_level = detect_bos_up(c1h)
    higher_low        = higher_low_detected(c1h)
    vol_ratio         = calc_volume_ratio(c1h)
    vol_accel         = calc_volume_acceleration(c1h)
    vol_consistent    = check_volume_consistent(c1h)
    uptrend           = calc_uptrend_age(c1h)
    sr                = calc_support_resistance(c1h)
    btc_candles       = get_btc_candles_cached(48)
    btc_corr          = calc_btc_correlation(c1h, btc_candles, lookback=24)
    accum             = calc_accumulation_phase(c1h)

    # BARU: HTF Accumulation + Liquidity Sweep
    htf_accum         = calc_htf_accumulation(c4h)
    liq_sweep         = detect_liquidity_sweep(c1h)

    # BARU v15.3: Energy Build-Up (OI+vol naik, harga stuck)
    # Deteksi dilakukan setelah oi_data tersedia
    energy            = detect_energy_buildup(c1h, oi_data)
    # Set is_strong jika funding netral/negatif (squeeze potential)
    if energy["is_buildup"] and fstats and fstats.get("current", 1) <= 0:
        energy["is_strong"] = True
        energy["label"] = energy["label"] + " 🔥 + funding negatif (squeeze)"

    atr_abs_val       = calc_atr_abs(c1h)

    # ── GATE 3: Uptrend tidak terlalu tua (anti-late-pump) ───────────────────
    if uptrend["is_late"]:
        log.info(
            f"  {symbol}: Uptrend sudah {uptrend['age_hours']}h berturut — "
            f"terlalu tua, kemungkinan distribusi (GATE GAGAL)"
        )
        return None

    # ── GATE 4: RSI tidak overbought ─────────────────────────────────────────
    if rsi >= CONFIG["gate_rsi_max"]:
        log.info(
            f"  {symbol}: RSI {rsi:.1f} ≥ {CONFIG['gate_rsi_max']} — "
            f"overbought/distribusi (GATE GAGAL)"
        )
        return None

    # ── GATE 5: BB Position tidak di puncak ──────────────────────────────────
    if bb_pct >= CONFIG["gate_bb_pos_max"]:
        log.info(
            f"  {symbol}: BB Pos {bb_pct*100:.0f}% ≥ {CONFIG['gate_bb_pos_max']*100:.0f}% — "
            f"overbought BB (GATE GAGAL)"
        )
        return None

    # Rate above VWAP dalam 6 candle terakhir
    above_vwap_rate = 0.0
    if len(c1h) >= 6:
        recent_6        = c1h[-6:]
        above           = sum(1 for c in recent_6 if c["close"] > vwap)
        above_vwap_rate = above / len(recent_6)

    # Price change 1h
    price_chg = 0.0
    if len(c1h) >= 2 and c1h[-2]["close"] > 0:
        price_chg = (c1h[-1]["close"] - c1h[-2]["close"]) / c1h[-2]["close"] * 100

    # ── SCORING ──────────────────────────────────────────────────────────────
    score   = 0
    signals = []

    # 1. EMA Gap (DITURUNKAN: 5→2 — ini indikator momentum, bukan pre-pump)
    if ema_gap >= CONFIG["ema_gap_threshold"]:
        score += CONFIG["score_ema_gap"]
        signals.append(f"EMA Gap {ema_gap:.3f} ≥ 1.0 — harga di atas EMA20")

    # 2. ATR
    if atr_pct >= 1.5:
        score += CONFIG["score_atr_15"]
        signals.append(f"ATR {atr_pct:.2f}% ≥ 1.5% — volatilitas tinggi")
    elif atr_pct >= 1.0:
        score += CONFIG["score_atr_10"]
        signals.append(f"ATR {atr_pct:.2f}% ≥ 1.0% — volatilitas sedang")

    # 3. BB Width
    if bbw >= CONFIG["bbw_threshold_high"]:
        score += CONFIG["score_bbw_10"]
        signals.append(f"BB Width {bbw*100:.2f}% ≥ 10% — band lebar kuat")
    elif bbw >= CONFIG["bbw_threshold_mid"]:
        score += CONFIG["score_bbw_6"]
        signals.append(f"BB Width {bbw*100:.2f}% ≥ 6% — band mulai lebar")

    # 4. BOS Up + Above VWAP
    if bos_up and above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        score += CONFIG["score_above_vwap_bos"]
        signals.append(
            f"BOS Up + Above VWAP {above_vwap_rate*100:.0f}% — kombinasi terkuat (lift 3.72x)"
        )
    elif bos_up:
        score += CONFIG["score_bos_up"]
        signals.append(f"Break of Structure ke atas (lift 2.81x)")

    # 5. Higher Low
    if higher_low:
        score += CONFIG["score_higher_low"]
        signals.append("Higher Low terdeteksi — struktur bullish")

    # 6. BB Squeeze
    if bb_squeeze:
        score += CONFIG["score_bb_squeeze"]
        signals.append(f"BB Squeeze aktif (band < 4%) — konsolidasi pre-breakout")

    # 7. RSI
    if rsi >= 65:
        score += CONFIG["score_rsi_65"]
        signals.append(f"RSI {rsi:.1f} ≥ 65 — momentum kuat (lift 2.46x)")
    elif rsi >= 55:
        score += CONFIG["score_rsi_55"]
        signals.append(f"RSI {rsi:.1f} ≥ 55 — bullish")

    # 8. Funding — scoring system (v15.3: bukan gate, tapi bonus/penalti)
    f_avg = fstats["avg"]
    f_cur = fstats["current"]

    if f_avg <= CONFIG["funding_bonus_avg"]:
        # Funding sangat negatif → short squeeze setup → bonus kuat
        score += CONFIG["score_funding_cumul"]
        signals.append(f"⭐ Funding avg {f_avg:.6f} — sangat negatif (short squeeze setup)")
    elif fstats["cumulative"] <= CONFIG["funding_bonus_cumul"]:
        score += 1
        signals.append(f"Funding kumulatif {fstats['cumulative']:.5f} — akumulasi negatif")
    elif f_avg < 0:
        # Funding negatif ringan — tetap catat sebagai sinyal positif
        signals.append(f"Funding avg {f_avg:.6f} — negatif (favorable untuk long)")
    elif f_avg >= CONFIG["funding_penalty_avg"]:
        # Funding sangat positif → coin sudah overbought, short banyak dibuka → penalti
        score -= 2
        signals.append(f"⚠️ Funding avg {f_avg:.6f} — sangat positif (penalti: overbought/longs banyak)")
    else:
        # Funding netral — normal di fase early accumulation
        signals.append(f"Funding avg {f_avg:.6f} — netral (normal di fase accumulation)")

    if fstats["neg_pct"] >= 70 and fstats["sample_count"] >= 3:
        score += CONFIG["score_funding_neg_pct"]
        signals.append(f"Funding negatif {fstats['neg_pct']:.0f}% dari {fstats['sample_count']} periode")

    if fstats["streak"] >= CONFIG["funding_streak_min"]:
        score += CONFIG["score_funding_streak"]
        signals.append(
            f"Funding streak negatif {fstats['streak']}x berturut "
            f"(dari {fstats['sample_count']} total data)"
        )

    # 9. Volume — hanya jika konsisten (anti-manipulasi)
    # PERUBAHAN v15: threshold 2.5 → 1.5 untuk deteksi accumulation lebih awal
    if vol_ratio > CONFIG["vol_ratio_threshold"]:
        if vol_consistent:
            score += CONFIG["score_vol_ratio"]
            signals.append(
                f"Volume {vol_ratio:.1f}x rata-rata (konsisten ≥ 2 candle) "
                f"[threshold diturunkan 2.5→1.5: accumulation detection]"
            )
        else:
            signals.append(
                f"⚠️ Volume spike {vol_ratio:.1f}x tapi tidak konsisten "
                f"— kemungkinan manipulasi, skor TIDAK ditambah"
            )

    if vol_accel > CONFIG["vol_accel_threshold"] and vol_consistent:
        score += CONFIG["score_vol_accel"]
        signals.append(f"Volume acceleration {vol_accel*100:.0f}% dalam 1h terakhir")

    # 10. Price change
    if price_chg >= 0.5:
        score += CONFIG["score_price_chg"]
        signals.append(f"Price +{price_chg:.2f}% dalam 1h terakhir")

    # 11. Smart Money Accumulation + Volatility Compression
    # PERUBAHAN v15: score_vol_compression dinaikkan 3→4 (kompensasi EMA gap turun)
    if accum["is_accumulating"] and accum["is_vol_compress"]:
        score += CONFIG["score_accumulation"] + CONFIG["score_vol_compression"]
        signals.append(
            f"🏦 AKUMULASI + VOL COMPRESSION — vol {accum['vol_ratio_4h']:.1f}x, "
            f"range {accum['price_range_pct']:.1f}%, ATR {accum['atr_contract']:.2f}x"
        )
    elif accum["is_accumulating"]:
        score += CONFIG["score_accumulation"]
        signals.append(
            f"📦 Smart Money Accumulation — vol {accum['vol_ratio_4h']:.1f}x, "
            f"sideways {accum['price_range_pct']:.1f}%"
        )
    elif accum["is_vol_compress"]:
        score += CONFIG["score_vol_compression"]
        signals.append(f"🗜️ Volatility Compression — ATR {accum['atr_contract']:.2f}x dari baseline")

    # 12. HTF Accumulation Filter 4H (BARU)
    # Deteksi build-up besar di timeframe 4H sebelum breakout terjadi
    if htf_accum["is_htf_accum"]:
        score += CONFIG["score_htf_accumulation"]
        signals.append(htf_accum["label"])

    # 13. Liquidity Sweep (BARU)
    # Stop hunt sebelum reversal = konfirmasi kuat pump akan terjadi
    if liq_sweep["is_sweep"]:
        score += CONFIG["score_liquidity_sweep"]
        signals.append(liq_sweep["label"])

    # 14. Energy Build-Up (BARU v15.3) — OI Build + Volume Build + Price Stuck
    # Ini adalah pola "calm before storm" yang sering diabaikan scanner biasa.
    # Pump besar hampir selalu butuh liquidity, dan liquidity dibangun via OI.
    # Jika OI + volume naik tapi harga tidak bergerak = absorption sedang terjadi.
    if energy["is_buildup"]:
        score += CONFIG["score_energy_buildup"]
        signals.append(energy["label"])
        if energy["is_strong"]:
            score += 2   # bonus ekstra untuk kombinasi energy + funding negatif
            signals.append("⭐ Energy Build-Up + Funding Negatif = squeeze probability tinggi")

    # 14. Open Interest Expansion
    if not oi_data["is_new"] and oi_data["oi_now"] > 0:
        chg = oi_data["change_pct"]
        if chg >= CONFIG["oi_strong_pct"]:
            score += CONFIG["score_oi_strong"]
            signals.append(f"📈 OI Expansion KUAT +{chg:.1f}% — posisi leverage besar dibangun")
        elif chg >= CONFIG["oi_change_min_pct"]:
            score += CONFIG["score_oi_expansion"]
            signals.append(f"📊 OI Expansion +{chg:.1f}% — akumulasi posisi futures")
    elif oi_data["is_new"] and oi_data["oi_now"] > 0:
        signals.append(f"📊 OI baseline: ${oi_data['oi_now']/1e6:.2f}M (snapshot pertama)")

    # 15. BTC Outperformance
    if btc_corr.get("outperform_label") == "OUTPERFORM":
        score += CONFIG["score_outperform"]
        signals.append(
            f"🚀 OUTPERFORM BTC — coin {btc_corr['coin_period_chg']:+.1f}% vs BTC "
            f"{btc_corr['btc_period_chg']:+.1f}% ({btc_corr['delta_vs_btc']:+.1f}%)"
        )

    # ── Alert Level ──────────────────────────────────────────────────────────
    alert_level = "MEDIUM"
    pump_type   = "VWAP Momentum"

    if bos_up and ema_gap >= CONFIG["ema_gap_threshold"] and above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        alert_level = "HIGH"
        pump_type   = "Momentum Breakout"
    elif bos_up and ema_gap >= CONFIG["ema_gap_threshold"]:
        alert_level = "HIGH"
        pump_type   = "BOS + EMA Breakout"
    elif liq_sweep["is_sweep"] and htf_accum["is_htf_accum"]:
        # BARU: kombinasi sweep + 4H accumulation = setup pre-pump sangat kuat
        alert_level = "HIGH"
        pump_type   = "Liquidity Sweep + HTF Accumulation"
    elif energy["is_buildup"] and energy["is_strong"]:
        # BARU v15.3: energy build-up kuat = absorption + funding negatif
        alert_level = "HIGH"
        pump_type   = "Energy Build-Up (OI+Vol+Stuck) + Short Squeeze"
    elif energy["is_buildup"]:
        alert_level = "MEDIUM"
        pump_type   = "Energy Build-Up (OI+Vol+Price Stuck)"
    elif liq_sweep["is_sweep"]:
        alert_level = "MEDIUM"
        pump_type   = "Liquidity Sweep Reversal"
    elif htf_accum["is_htf_accum"]:
        alert_level = "MEDIUM"
        pump_type   = "HTF Accumulation Build-Up"
    elif above_vwap_rate >= CONFIG["above_vwap_rate_min"] and fstats["cumulative"] <= -0.05 and higher_low:
        alert_level = "MEDIUM"
        pump_type   = "Short Squeeze Setup"
    elif above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        alert_level = "MEDIUM"
        pump_type   = "VWAP Momentum"

    # ── Entry & Target ────────────────────────────────────────────────────────
    entry_data = calc_entry(c1h, bos_level, alert_level, vwap, price_now, atr_abs_val=atr_abs_val, sr=sr)

    if score >= CONFIG["min_score_alert"]:
        return {
            "symbol":          symbol,
            "score":           score,
            "signals":         signals,
            "entry":           entry_data,
            "price":           price_now,
            "chg_24h":         chg_24h,
            "vol_24h":         vol_24h,
            "rsi":             round(rsi, 1),
            "ema_gap":         round(ema_gap, 3),
            "bbw":             round(bbw * 100, 2),
            "bb_pct":          round(bb_pct, 2),
            "bb_squeeze":      bb_squeeze,
            "atr_pct":         round(atr_pct, 2),
            "above_vwap_rate": round(above_vwap_rate * 100, 1),
            "vwap":            round(vwap, 8),
            "bos_up":          bos_up,
            "bos_level":       round(bos_level, 8),
            "higher_low":      higher_low,
            "funding_stats":   fstats,
            "pump_type":       pump_type,
            "alert_level":     alert_level,
            "vol_ratio":       round(vol_ratio, 2),
            "vol_accel":       round(vol_accel * 100, 1),
            "vol_consistent":  vol_consistent,
            "uptrend_age":     uptrend["age_hours"],
            "sr":              sr,
            "btc_corr":        btc_corr,
            "accum":           accum,
            "htf_accum":       htf_accum,
            "liq_sweep":       liq_sweep,
            "energy":          energy,
            "oi_data":         oi_data,
        }
    else:
        log.info(f"  {symbol}: Skor {score} < {CONFIG['min_score_alert']} — dilewati")
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def _fmt_price(p):
    """Format harga coin secara otomatis sesuai magnitudo."""
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
    Pesan Telegram ringkas — hanya data trading esensial.
    v15.3 fix:
    - Hapus semua kemungkinan angka di dalam '<>' yang Telegram baca sebagai HTML tag
    - R/R tidak ditulis "1:X" karena "1:1.0" → "<1.0>" → HTML parse error
    - Format angka desimal dengan _fmt_price()
    """
    level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
    e   = r["entry"]
    bc  = r.get("btc_corr", {})
    sr  = r.get("sr", {})
    en  = r.get("energy", {})
    htf = r.get("htf_accum", {})
    ls  = r.get("liq_sweep", {})
    oi  = r.get("oi_data", {})

    p     = r["price"]
    entry = e["entry"]
    sl    = e["sl"]
    t1    = e["t1"]
    t2    = e["t2"]

    # ── Header ────────────────────────────────────────────────────────────────
    msg  = f"{level_icon} <b>{r['symbol']} — {r['alert_level']}</b>  #{rank}\n"
    msg += f"<b>Score:</b> {r['score']}  |  {r['pump_type']}\n"
    msg += f"<b>Waktu scan:</b> {utc_now()}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    # ── Harga & VWAP ─────────────────────────────────────────────────────────
    msg += f"<b>Harga :</b> <code>{_fmt_price(p)}</code>  ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>VWAP  :</b> <code>{_fmt_price(r['vwap'])}</code>"
    gap_vwap = (p - r['vwap']) / r['vwap'] * 100 if r['vwap'] > 0 else 0
    msg += f"  ({gap_vwap:+.1f}% vs harga)\n"

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    # Gunakan <code> agar tidak ada risiko parsing HTML pada angka
    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(entry)}</code>  [{e['sl_method']}]\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(sl)}</code>  (-{e['sl_pct']:.2f}%)\n"

    t1_tag = "resistance" if e.get("used_resistance") else "proj"
    msg += f"🎯 <b>T1    :</b> <code>{_fmt_price(t1)}</code>  (+{e['gain_t1_pct']:.1f}%)  [{t1_tag}]\n"
    msg += f"🎯 <b>T2    :</b> <code>{_fmt_price(t2)}</code>  (+{e['gain_t2_pct']:.1f}%)\n"

    # R/R — ditulis sebagai "RR = X.X" bukan "1:X.X" untuk hindari HTML tag parse error
    msg += f"⚖️ <b>RR    :</b> {e['rr_str']}x  |  ATR: {r['atr_pct']:.2f}%\n"

    # ── Outperform BTC ────────────────────────────────────────────────────────
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    if bc.get("correlation") is not None:
        msg += (
            f"{bc.get('btc_regime_emoji','❓')} <b>BTC:</b> {bc.get('btc_regime','?')}"
            f"  ({bc.get('btc_period_chg',0):+.1f}%/{bc['lookback']}h)\n"
        )
        op_emoji = bc.get("outperform_emoji", "〰️")
        coin_chg = bc.get("coin_period_chg", 0)
        btc_chg  = bc.get("btc_period_chg", 0)
        msg += (
            f"{op_emoji} <b>vs BTC:</b> {bc.get('outperform_label','?')} "
            f"| Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}%\n"
        )
    else:
        msg += "📊 <b>vs BTC:</b> data tidak tersedia\n"

    # ── Support & Resistance ─────────────────────────────────────────────────
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    res_list = sr.get("resistance", [])
    sup_list = sr.get("support",    [])
    if res_list:
        for rv in res_list[:2]:
            msg += f"🔴 R <code>{_fmt_price(rv['level'])}</code>  ({rv['gap_pct']:+.1f}%)\n"
    msg += f"▶ NOW <code>{_fmt_price(p)}</code>\n"
    if sup_list:
        for sv in sup_list[:2]:
            msg += f"🟢 S <code>{_fmt_price(sv['level'])}</code>  ({sv['gap_pct']:+.1f}%)\n"

    # ── OI ───────────────────────────────────────────────────────────────────
    if oi.get("oi_now", 0) > 0:
        ov     = oi["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs     = f"({oi['change_pct']:+.1f}%)" if not oi.get("is_new") else "(baseline)"
        msg += f"📈 <b>OI:</b> {os_str} {cs}\n"

    # ── Sinyal aktif (ringkas) ────────────────────────────────────────────────
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>Sinyal:</b>\n"

    # Kumpulkan sinyal penting saja (maksimal 6)
    priority_signals = []
    for s in r["signals"]:
        # Skip sinyal informatif panjang, ambil sinyal teknikal kunci
        if any(kw in s for kw in [
            "AKUMULASI", "BUILD-UP", "Sweep", "BOS", "VWAP",
            "Funding", "squeeze", "HTF", "Higher Low", "Compression",
            "OI", "Volume", "ATR", "BB Width", "EMA Gap"
        ]):
            priority_signals.append(s)
        if len(priority_signals) >= 6:
            break

    for s in priority_signals:
        # Potong teks panjang agar tidak melewati limit
        s_short = s[:80] + "…" if len(s) > 80 else s
        msg += f"• {s_short}\n"

    msg += f"\n<i>v15.3 | ⚠️ Bukan financial advice</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v15.3 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        vol_str    = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                      else f"${r['vol_24h']/1e3:.0f}K")
        level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
        htf_tag    = " 🕯️" if r.get("htf_accum", {}).get("is_htf_accum") else ""
        sweep_tag  = " 🎯" if r.get("liq_sweep", {}).get("is_sweep") else ""
        energy_tag = " ⚡" if r.get("energy", {}).get("is_buildup") else ""
        msg += f"{i}. {level_icon} <b>{r['symbol']}</b> [Score:{r['score']} | {r['alert_level']}{htf_tag}{sweep_tag}{energy_tag}]\n"
        msg += (
            f"   {vol_str} | RSI:{r['rsi']} | EMAGap:{r['ema_gap']} | "
            f"T1:+{r['entry']['gain_t1_pct']}%\n"
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

        if chg > CONFIG["gate_chg_24h_max"]:
            filtered_stats["change_too_high"] += 1
            continue

        # PERUBAHAN v15.3: no_momentum filter DIHAPUS
        # Sebelumnya: skip coin jika chg < -5% (membuang coin sideways/accumulation)
        # Sekarang  : skip hanya jika dump besar (< -15%)
        # Pump besar sering muncul dari coin yang terlihat "membosankan" dengan
        # harga sideways — justru itulah "OI Build + Volume Build + Price Stuck"
        if chg < CONFIG["gate_chg_24h_min"]:
            filtered_stats["dump_too_deep"] += 1
            continue

        if price <= 0:
            filtered_stats["invalid_price"] += 1
            continue

        all_candidates.append((sym, ticker))

    total     = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    filtered  = total - will_scan - len(not_found)

    log.info(f"\n📊 SCAN SUMMARY:")
    log.info(f"   Whitelist total  : {total} coins")
    log.info(f"   ✅ Will scan     : {will_scan} ({will_scan/total*100:.1f}%)")
    log.info(f"   ❌ Filtered      : {filtered}")
    log.info(f"   ⚠️  Not in Bitget : {len(not_found)}")
    log.info(f"\n📋 Filter breakdown:")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"   {k:20s}: {v}")
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
    log.info(f"=== PRE-PUMP SCANNER v15.3 — {utc_now()} ===")

    load_funding_snapshots()
    log.info(f"Funding snapshots loaded: {len(_funding_snapshots)} coins di memori")

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

        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum, skip")
            continue

        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")

        try:
            res = master_score(sym, t)
            if res:
                log.info(
                    f"  ✅ Score={res['score']} | {res['alert_level']} | "
                    f"{res['pump_type']} | T1:+{res['entry']['gain_t1_pct']}%"
                )
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    save_all_funding_snapshots()
    log.info("Funding snapshots disimpan ke disk.")

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

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v15.3                             ║")
    log.info("║  Focus: Energy Build-Up + VWAP tolerance + Funding  ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
