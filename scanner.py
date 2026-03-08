"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v15.7                                                      ║
║                                                                              ║
║  ROMBAKAN BESAR v15.7 — Scoring system dirancang ulang dari nol:            ║
║                                                                              ║
║  MASALAH FUNDAMENTAL v15.6 yang diperbaiki:                                  ║
║                                                                              ║
║  1. ATR TINGGI BUKAN SINYAL BAGUS — dibalik                                 ║
║     Sebelumnya: ATR ≥ 1.5% → +4, ATR ≥ 1.0% → +3 (reward volatilitas)     ║
║     Sekarang  : ATR rendah & menyempit → +3 (reward kompresi pre-pump)      ║
║     Alasan: pump terjadi SETELAH kompresi, bukan SAAT volatilitas tinggi.   ║
║     ATR tinggi = volatilitas sudah meledak = distribusi atau pump berjalan.  ║
║                                                                              ║
║  2. BB WIDTH LEBAR BUKAN SINYAL BAGUS — dibalik                             ║
║     Sebelumnya: BBW ≥ 10% → +4, BBW ≥ 6% → +3 (reward ekspansi)           ║
║     Sekarang  : BB Squeeze (BBW < 4%) → +4 (reward kompresi)                ║
║     BBW lebar = breakout sudah terjadi sebelumnya = terlambat/distribusi.   ║
║                                                                              ║
║  3. VOLUME TINGGI TANPA KONTEKS ARAH = BUTA DISTRIBUSI — diperbaiki        ║
║     Sebelumnya: vol_ratio tinggi + konsisten → +2 (tanpa cek arah harga)   ║
║     Sekarang  : volume naik HANYA diberi skor jika candle mayoritas bullish  ║
║     Volume tinggi + candle merah = distribusi, BUKAN accumulation.          ║
║                                                                              ║
║  4. RSI 65–74 DIBERI SKOR POSITIF = REWARD MOMENTUM — dihapus              ║
║     Sebelumnya: RSI ≥ 65 → +2 (reward harga yang sudah naik banyak)        ║
║     Sekarang  : RSI ideal pre-pump = 40–60. RSI 60–74 = netral.            ║
║     Bonus hanya untuk RSI 40–60 (keluar oversold, belum overbought).        ║
║                                                                              ║
║  5. BOS UP ADALAH SINYAL POST-BREAKOUT, BUKAN PRE-PUMP — diturunkan        ║
║     Sebelumnya: BOS Up + VWAP → +4 (skor tertinggi untuk breakout)         ║
║     Sekarang  : BOS Up = konfirmasi minor +1. Pre-pump = SEBELUM BOS.      ║
║     BOS = breakout sudah terjadi. Pre-pump = mendeteksi sebelum breakout.   ║
║                                                                              ║
║  6. gate_chg_24h_max TERLALU LONGGAR — diperketat                          ║
║     Sebelumnya: 12% (coin yang sudah pump 10% masih lolos)                  ║
║     Sekarang  : 5% (pre-pump = harga belum banyak bergerak)                 ║
║                                                                              ║
║  7. calc_accumulation_phase TIDAK CEK POSISI HARGA — diperbaiki            ║
║     Sebelumnya: vol naik + harga sideways = accumulation (selalu)           ║
║     Sekarang  : tambah cek price_pos_in_range — harga harus di bawah 70%   ║
║     dari swing range. Konsolidasi di atas (70–100%) = distribusi.          ║
║                                                                              ║
║  8. detect_energy_buildup VOLUME 1 CANDLE — diperbaiki                      ║
║     Sebelumnya: vol_1h (satu candle) vs avg 23 candle                       ║
║     Sekarang  : vol_3h_avg (rata-rata 3 candle terkini) vs baseline 24h    ║
║     Spike 1 candle bisa wash trade/liquidation, bukan accumulation.         ║
║                                                                              ║
║  9. vol_accel TANPA CEK ARAH — diperbaiki                                   ║
║     Sebelumnya: vol_accel tinggi + konsisten → +2 (tanpa cek candle arah)  ║
║     Sekarang  : vol_accel hanya diberi skor jika candle terbaru bullish.    ║
║                                                                              ║
║  10. calc_htf_accumulation TIDAK CEK POSISI HARGA 4H — diperbaiki          ║
║      Sekarang: tambah cek harga 4H tidak sedang di zona distribusi          ║
║      (di atas 80% swing range 4H = distribusi, bukan accumulation).        ║
║                                                                              ║
║  WARISAN v15.6 (dipertahankan):                                              ║
║  - get_funding: guard IndexError data["data"] = []                          ║
║  - get_open_interest: guard IndexError openInterestList = []                ║
║  - double OI scoring guard (skip OI expansion jika energy_buildup aktif)   ║
║  - safe_get: retry setelah 429 rate limit                                   ║
║  - gate_uptrend_max_hours, funding_bonus_cumul dari CONFIG                  ║
║  - calc_entry: TP per-coin dinamis (ATR/Pivot/Fib)                          ║
║  - build_alert: HTML-safe formatting                                        ║
║                                                                              ║
║  BOBOT SKOR v15.7 — urutan dari yang paling kuat:                           ║
║    bb_squeeze (BBW < 4%)       : +4  (kompresi sebelum ekspansi)            ║
║    energy_buildup              : +4  (OI+vol naik, harga stuck)             ║
║    accumulation + compression  : +8  (vol naik + sideways + ATR sempit)     ║
║    htf_accumulation 4H         : +3  (konsolidasi di TF besar)              ║
║    liquidity_sweep             : +3  (stop hunt = reversal akan datang)     ║
║    oi_expansion_strong         : +5  (OI naik > 10%, posisi besar)          ║
║    atr_contracting             : +3  (ATR menyempit = kompresi energi)      ║
║    volume_bullish              : +2  (vol naik + candle bullish)             ║
║    rsi_ideal (40–60)           : +2  (belum overbought, momentum awal)      ║
║    higher_low                  : +2  (struktur bullish awal)                 ║
║    bos_up (konfirmasi minor)   : +1  (breakout sudah terjadi = terlambat)   ║
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
log.info("Scanner v15.7 — log aktif: /tmp/scanner_v15.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_alert":          7,
    "max_alerts_per_run":       15,

    # ── Volume 24h total (USD) ────────────────────────────────────────────────
    "min_vol_24h":          10_000,
    "max_vol_24h":      50_000_000,
    "pre_filter_vol":       10_000,

    # ── Open Interest minimum filter ──────────────────────────────────────────
    # Futures pump biasanya terjadi di coin dengan OI signifikan.
    "min_oi_usd":          100_000,   # minimal $100K OI

    # ── Gate perubahan harga 24h ──────────────────────────────────────────────
    # FIX v15.7: diperketat dari 12% → 5%.
    # Pre-pump = harga BELUM banyak bergerak. Coin yang sudah naik 10% bukan
    # pre-pump candidate — itu coin yang sudah pump. Fase distribusi sering
    # terjadi setelah pump besar, lalu harga konsolidasi tinggi + volume tinggi.
    "gate_chg_24h_max":          15.0,
    "gate_chg_24h_min":        -15.0,   # hanya skip dump besar

    # ── VWAP Gate Tolerance ───────────────────────────────────────────────────
    # Accumulation sering terjadi sedikit di bawah VWAP.
    "vwap_gate_tolerance":      0.97,   # price > vwap * 0.97

    # ── Gate uptrend usia ─────────────────────────────────────────────────────
    # Uptrend yang sudah terlalu lama = distribusi, bukan pre-pump.
    "gate_uptrend_max_hours":   10,

    # ── Gate RSI overbought ───────────────────────────────────────────────────
    "gate_rsi_max":             72.0,

    # ── Gate BB Position ──────────────────────────────────────────────────────
    # Harga di puncak BB = overbought/distribusi.
    "gate_bb_pos_max":          1.05,

    # ── Funding rate scoring ──────────────────────────────────────────────────
    # Funding = scoring only, bukan hard gate.
    # Sangat negatif → squeeze setup → bonus.
    # Sangat positif → overbought/longs berat → penalti.
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

    # ══════════════════════════════════════════════════════════════════════════
    #  BOBOT SKOR v15.7 — logika dirombak total
    #
    #  PRINSIP BARU:
    #  - Reward KOMPRESI (ATR sempit, BB squeeze, harga sideways)
    #  - Reward ACCUMULATION (OI naik + vol naik + harga tidak bergerak)
    #  - Reward STRUKTUR BULLISH AWAL (higher low, belum overbought)
    #  - PENALTI momentum yang sudah berjalan (jangan reward terlambat)
    # ══════════════════════════════════════════════════════════════════════════

    # ── BB Squeeze — sinyal kompresi terkuat (skor dinaikkan dari +2 ke +4) ──
    # BBW < threshold → energi terakumulasi, siap meledak.
    # BUKAN BBW lebar (itu sudah meledak = distribusi/pump berjalan).
    "bb_squeeze_threshold":    0.045,
    "score_bb_squeeze":        3,    # FIX: naik dari +2 ke +4 — ini sinyal terpenting

    # ── ATR Contracting — kompresi volatilitas ────────────────────────────────
    # ATR menyempit = energi menumpuk. BUKAN ATR tinggi (itu sudah meledak).
    # atr_short (6 candle) / atr_long (24 candle) < threshold = menyempit.
    "atr_contract_ratio":      0.80,  # ATR 6c < 75% dari ATR 24c = kompresi
    "score_atr_contracting":   2,     # FIX: ganti score_atr_15 dan score_atr_10

    # ── Energy Build-Up — OI naik + volume bullish + harga stuck ─────────────
    # Kondisi: OI +5%, vol 3h > 1.5x avg, range 3h < 2.5%, mayoritas candle hijau
    "energy_oi_change_min":    4.0,
    "energy_vol_ratio_min":    1.4,
    "energy_range_max_pct":    3.5,
    "score_energy_buildup":    4,

    # ── Smart Money Accumulation — vol naik + harga sideways + posisi rendah ─
    # FIX: tambah syarat price_pos_in_range < 0.70 (harga harus di bawah 70%
    # dari swing range). Sideways di atas = distribusi, bukan accumulation.
    "accum_vol_ratio":         1.4,
    "accum_price_range_max":   3.5,
    #"accum_atr_lookback_long": 24,
    #"accum_atr_lookback_short": 6,
    "accum_atr_contract_ratio": 0.75,
    "accum_max_pos_in_range":  0.70,  # FIX BARU: harga max 70% dari swing range
    "score_accumulation":      3,
    "score_vol_compression":   3,

    # ── HTF Accumulation 4H ───────────────────────────────────────────────────
    # FIX: tambah syarat 4H price position < 75% dari swing range 4H.
    "htf_atr_contract_ratio":  0.85,
    "htf_vol_ratio_min":       1.3,
    "htf_range_max_pct":       3.0,
    "htf_max_pos_in_range":    0.75,  # FIX BARU: harga 4H max 75% dari range 4H
    "score_htf_accumulation":  3,

    # ── Liquidity Sweep ───────────────────────────────────────────────────────
    "liq_sweep_lookback":      20,
    "liq_sweep_wick_min_pct":  0.3,
    "score_liquidity_sweep":   3,

    # ── OI Expansion ─────────────────────────────────────────────────────────
    "oi_change_min_pct":       2.0,
    "oi_strong_pct":          8.0,
    "score_oi_expansion":      3,
    "score_oi_strong":         5,

    # ── Volume dengan konteks arah harga ─────────────────────────────────────
    # FIX: volume tinggi hanya diberi skor jika mayoritas candle terbaru bullish.
    # Volume + candle merah = distribusi, TIDAK mendapat skor.
    "vol_ratio_threshold":     1.5,
    "vol_bullish_min_ratio":   0.6,   # min 60% candle terbaru harus bullish
    "score_vol_bullish":       2,     # FIX: ganti score_vol_ratio

    # ── Volume Acceleration dengan konteks arah ───────────────────────────────
    "vol_accel_threshold":     1.4,
    "score_vol_accel":         2,     # hanya jika candle terbaru bullish

    # ── RSI ideal pre-pump = 40–60 ────────────────────────────────────────────
    # FIX: score_rsi_65 DIHAPUS (RSI 65+ = sudah naik banyak, bukan pre-pump).
    # RSI 40–60 = keluar dari oversold, belum overbought = sweet spot pre-pump.
    "rsi_ideal_min":           40.0,
    "rsi_ideal_max":           65.0,
    "score_rsi_ideal":         2,     # FIX: ganti score_rsi_65 dan score_rsi_55

    # ── Higher Low ────────────────────────────────────────────────────────────
    "score_higher_low":        2,
  "score_bos_up": 2,
    # ── BOS Up — diturunkan drastis (ini post-breakout, bukan pre-pump) ───────
    # FIX: BOS Up bukan pre-pump. Tapi tetap diberi +1 sebagai konfirmasi minor
    # bahwa struktur sedang berbalik. Pre-pump yang ideal = deteksi SEBELUM BOS.
    "score_bos_up":            1,     # FIX: turun dari +3/+4 ke +1

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
    """
    Ambil funding rate terkini.
    Guard v15.6: cek data["data"] tidak kosong sebelum akses index 0.
    """
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
    """
    Ambil Open Interest dari Bitget Futures API.
    Guard v15.6: cek openInterestList tidak kosong sebelum akses index 0.
    """
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
        "oi_now":     round(oi_now, 2),
        "oi_prev":    round(oi_prev, 2),
        "change_pct": round(change_pct, 2),
        "is_new":     False,
    }

    # Fungsi Baru
    def detect_momentum_breakout(candles, oi_change):

    if len(candles) < 6:
        return {"is_breakout": False, "score": 0}

    last = candles[-1]
    prev = candles[-2]

    price_move = (last["close"] - prev["close"]) / prev["close"] * 100

    vol_recent = last["volume_usd"]
    vol_avg = sum(c["volume_usd"] for c in candles[-12:-1]) / 11

    vol_ratio = vol_recent / vol_avg if vol_avg > 0 else 1

    range_pct = (last["high"] - last["low"]) / last["close"] * 100

    score = 0

    if price_move > 1.2:
        score += 2

    if vol_ratio > 2:
        score += 2

    if oi_change > 6:
        score += 3

    if range_pct > 1.5:
        score += 1

    return {
        "is_breakout": score >= 4,
        "score": score,
        "vol_ratio": round(vol_ratio,2),
        "price_move": round(price_move,2)
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
    """EMA gap = close / EMA(period). Digunakan untuk cek posisi harga vs EMA."""
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

    ATR_short (6 candle) / ATR_long (24 candle) < threshold = sedang menyempit.
    Ini adalah sinyal terkuat bahwa energi menumpuk sebelum ekspansi.

    Penting: ini KEBALIKAN dari scoring v15.6 yang memberi bonus ATR tinggi.
    ATR tinggi = sudah meledak. ATR menyempit = akan meledak.
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

def detect_bos_up(candles, lookback=3):
    """Break of Structure ke atas (harga close > max high N candle sebelumnya)."""
    if len(candles) < lookback + 1:
        return False, 0.0
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level  = max(prev_highs)
    return candles[-1]["close"] > bos_level, bos_level

def higher_low_detected(candles):
    """Higher Low: low candle terakhir > min(low 5 candle sebelumnya)."""
    if len(candles) < 6:
        return False
    lows = [c["low"] for c in candles[-6:]]
    return lows[-1] > min(lows[:-1])

def calc_swing_range_position(candles, lookback=48):
    """
    Hitung posisi harga saat ini dalam swing range lookback candle.
    Return 0.0 (di bawah = accumulation zone) hingga 1.0 (di atas = distribusi zone).

    Ini adalah filter distribusi utama v15.7:
    - pos < 0.5 = harga di separuh bawah range = accumulation zone
    - pos 0.5–0.7 = zona tengah = netral
    - pos > 0.7 = harga di atas 70% dari range = kemungkinan distribusi
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
    Hitung rasio candle bullish dalam lookback candle terakhir.
    Candle bullish = close > open.
    Return 0.0–1.0. Digunakan untuk filter distribusi pada scoring volume.
    """
    if len(candles) < lookback:
        return 0.5
    recent   = candles[-lookback:]
    bullish  = sum(1 for c in recent if c["close"] >= c["open"])
    return bullish / len(recent)

def calc_accumulation_phase(candles):
    """
    Deteksi fase akumulasi smart money.

    Kondisi akumulasi VALID (v15.7):
      1. Volume 4 candle terbaru > 1.5x baseline 24 candle sebelumnya (vol naik)
      2. Price range 12 candle < 2% (harga sideways)
      3. ATR short < 75% ATR long (volatilitas menyempit)
      4. BB Width menyempit dibanding 12 candle yang lalu
      5. FIX v15.7: posisi harga dalam swing range < 70%
         (harga harus di zona bawah/tengah, bukan di atas setelah pump)

    Kondisi 5 adalah perbedaan terpenting dari v15.6:
    Tanpanya, distribusi pasca-pump (harga konsolidasi tinggi setelah pump)
    akan terdeteksi sebagai "akumulasi" — ini adalah false positive utama.
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

    # FIX v15.7: posisi harga dalam swing range 48 candle
    price_pos = calc_swing_range_position(candles, lookback=48)

    vol_rising      = vol_ratio_4h    >= CONFIG["accum_vol_ratio"]
    price_sideways  = price_range_pct <= CONFIG["accum_price_range_max"]
    atr_shrinking   = atr_contract    <= CONFIG["accum_atr_contract_ratio"]
    is_vol_compress = atr_shrinking and bbw_contracting
    # FIX v15.7: tambah syarat price_pos < accum_max_pos_in_range
    # Tanpa ini: distribusi pasca-pump (sideways tinggi) lolos sebagai "akumulasi"
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
      2. Volume 4H terbaru > 1.3x rata-rata (buying interest tersembunyi)
      3. Range 8 candle 4H < 3% (harga sideways di TF besar)
      4. FIX v15.7: posisi harga 4H < 75% swing range 4H
         (harga 4H tidak boleh di zona distribusi atas)
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

    # FIX v15.7: cek posisi harga 4H dalam swing range lebih panjang
    price_pos = calc_swing_range_position(candles_4h, lookback=32)

    atr_compressed = atr_ratio  <= CONFIG["htf_atr_contract_ratio"]
    vol_building   = vol_ratio  >= CONFIG["htf_vol_ratio_min"]
    price_sideways = range_pct  <= CONFIG["htf_range_max_pct"]
    # FIX v15.7: harga 4H tidak boleh di zona distribusi
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

    Pola: harga turun ke bawah support (mengambil stop loss retail),
    lalu candle close kembali di atas support dengan wick panjang.
    Ini adalah tanda market maker sudah selesai mengumpulkan likuiditas.
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
    Energy Build-Up Detector — "OI Build + Volume Bullish + Price Stuck"

    Pola absorption: market maker menyerap order sambil membangun posisi.
    Harga DITAHAN (sideways) meski volume dan OI naik = klasik pre-pump.

    FIX v15.7 dari v15.6:
      - Volume: rata-rata 3 candle terbaru (bukan 1 candle) vs baseline 24h
        Alasan: 1 candle spike bisa wash trade/liquidation, bukan accumulation.
        Rata-rata 3 candle = pola yang lebih konsisten dan reliabel.
      - Cek mayoritas candle 3h terakhir bullish (close >= open)
        Alasan: OI naik + volume naik + candle merah = distribusi/short building,
        BUKAN accumulation. Candle harus hijau untuk konfirmasi buying pressure.

    Kondisi deteksi:
      1. OI naik > 5% (posisi baru dibangun)
      2. Vol rata-rata 3h terbaru > 1.5x baseline 24h (aktivitas naik, konsisten)
      3. Price range 3h < 2.5% (harga tidak bergerak meski ada aktivitas)
      4. FIX: minimal 2 dari 3 candle terbaru bullish (buying pressure, bukan dump)
    """
    if len(candles_1h) < 24:
        return {
            "is_buildup": False, "is_strong": False,
            "oi_change": 0.0, "vol_ratio": 0.0, "range_pct": 0.0,
            "label": "Data tidak cukup",
        }

    # Kondisi 1: OI naik (membutuhkan snapshot sebelumnya)
    oi_change = oi_data.get("change_pct", 0.0)
    oi_rising = (not oi_data.get("is_new", True)) and oi_change >= CONFIG["energy_oi_change_min"]

    # FIX v15.7: volume rata-rata 3 candle terbaru vs baseline, bukan 1 candle
    # Ini mencegah false positive dari satu spike liquidation/wash trade
    recent_3 = candles_1h[-3:]
    vol_3h_avg = sum(c["volume_usd"] for c in recent_3) / 3
    baseline   = candles_1h[-24:-3]
    avg_vol    = sum(c["volume_usd"] for c in baseline) / len(baseline) if baseline else vol_3h_avg
    vol_ratio  = (vol_3h_avg / avg_vol) if avg_vol > 0 else 1.0
    vol_rising = vol_ratio >= CONFIG["energy_vol_ratio_min"]

    # Kondisi 3: harga tidak bergerak (price stuck)
    hi3       = max(c["high"]  for c in recent_3)
    lo3       = min(c["low"]   for c in recent_3)
    mid3      = (hi3 + lo3) / 2
    range_pct = ((hi3 - lo3) / mid3 * 100) if mid3 > 0 else 99.0
    price_stuck = range_pct <= CONFIG["energy_range_max_pct"]

    # FIX v15.7: mayoritas candle terbaru harus bullish (close >= open)
    # OI naik + vol naik + candle merah = short dibangun atau distribusi
    # OI naik + vol naik + candle hijau = long dibangun = accumulation
    bullish_count = sum(1 for c in recent_3 if c["close"] >= c["open"])
    candles_bullish = bullish_count >= 2   # min 2 dari 3 harus bullish

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
    Entry / SL / Target — v15.7

    Entry:
      HIGH  → di atas BOS level + buffer kecil
      MEDIUM → VWAP atau market price

    SL:
      Swing low 12 candle, clamp [1x–3x ATR] dan [0.5%–8%]

    Target (3-tier, per-coin dinamis):
      Tier 1: Resistance pivot 48–168 candle (paling akurat)
      Tier 2: ATR projection per-coin (adaptif volatilitas)
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
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    c4h = get_candles(symbol, "4h", CONFIG["candle_4h"])

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
    # FIX v15.7: coin yang harganya sudah di atas 85% dari swing range 48h
    # hampir pasti sedang distribusi atau pump sudah berjalan. Skip.
    if price_pos_48 > 0.85:
        log.info(
            f"  {symbol}: Posisi harga {price_pos_48:.0%} dari swing range — "
            f"zona distribusi atas (GATE GAGAL)"
        )
        return None

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v15.7 — logika baru
    # ══════════════════════════════════════════════════════════════════════════
    score   = 0
    signals = []

    # ── 1. BB Squeeze — kompresi terkuat sebelum ekspansi ────────────────────
    # FIX v15.7: skor dinaikkan +2 → +4. BB Squeeze = energi tersimpan.
    # BUKAN BB Width lebar (itu sudah meledak).
    bb_squeeze = bbw < CONFIG["bb_squeeze_threshold"]
    if bb_squeeze:
        score += CONFIG["score_bb_squeeze"]
        signals.append(
            f"🗜️ BB Squeeze aktif (BBW {bbw*100:.2f}% < {CONFIG['bb_squeeze_threshold']*100:.0f}%) "
            f"— kompresi energi sebelum breakout"
        )

    # ── 2. ATR Contracting — volatilitas menyempit ───────────────────────────
    # FIX v15.7: GANTI score_atr_15/score_atr_10.
    # ATR menyempit = akan meledak. ATR tinggi = sudah meledak = terlambat.
    if atr_contr["is_contracting"]:
        score += CONFIG["score_atr_contracting"]
        signals.append(
            f"📉 ATR Menyempit — rasio {atr_contr['ratio']:.2f} "
            f"(ATR 6c = {atr_contr['ratio']*100:.0f}% dari ATR 24c) — energi menumpuk"
        )

    # ── 3. Energy Build-Up — OI + volume bullish + harga stuck ───────────────
    # FIX v15.7: kondisi diperketat dengan cek candle bullish.
    if energy["is_buildup"]:
        score += CONFIG["score_energy_buildup"]
        signals.append(energy["label"])
        if energy["is_strong"]:
            score   += 2
            signals.append("⭐ Energy Build-Up + Funding Negatif = squeeze probability tinggi")

    # ── 4. Smart Money Accumulation + Volatility Compression ─────────────────
    # FIX v15.7: calc_accumulation_phase sekarang cek posisi harga dalam range.
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
    # FIX v15.7: calc_htf_accumulation sekarang cek posisi harga 4H dalam range.
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
            oi_str  = (f"${oi_data['oi_now']/1e6:.2f}M" if oi_data["oi_now"] >= 1e6
                       else f"${oi_data['oi_now']/1e3:.0f}K")
            chg_str = (f"+{oi_data['change_pct']:.1f}%" if not oi_data.get("is_new")
                       else "baseline")
            signals.append(f"📊 OI: {oi_str} ({chg_str}) — sudah termasuk dalam Energy Build-Up")

    # ── 8. Volume dengan konteks arah harga ──────────────────────────────────
    # FIX v15.7: volume tinggi hanya diberi skor jika mayoritas candle bullish.
    # Volume + candle merah = distribusi. Volume + candle hijau = accumulation.
    if vol_ratio > CONFIG["vol_ratio_threshold"] and vol_consistent:
        if candle_dir_ratio >= CONFIG["vol_bullish_min_ratio"]:
            # Volume naik + candle mayoritas hijau = accumulation
            score += CONFIG["score_vol_bullish"]
            signals.append(
                f"🟢 Volume {vol_ratio:.1f}x rata-rata + {candle_dir_ratio*100:.0f}% candle "
                f"bullish — buying pressure konsisten"
            )
        else:
            # Volume naik + candle mayoritas merah = distribusi = TIDAK diberi skor
            signals.append(
                f"⚠️ Volume {vol_ratio:.1f}x tapi {candle_dir_ratio*100:.0f}% candle "
                f"bullish — kemungkinan distribusi/short, skor TIDAK ditambah"
            )

    # ── 9. Volume Acceleration dengan konteks arah ────────────────────────────
    # FIX v15.7: hanya diberi skor jika candle terbaru bullish.
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
    # FIX v15.7: HAPUS score_rsi_65 (RSI 65+ = momentum sudah berjalan).
    # Pre-pump sweet spot: RSI keluar dari oversold tapi belum overbought.
    # RSI 40–60 = momentum awal yang ideal untuk entry sebelum pump.
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
        # RSI > 60 tapi < gate 72: informasi saja, tidak ada skor
        signals.append(f"📊 RSI {rsi:.1f} — di atas zona ideal, momentum sudah berjalan")

    # ── 11. Higher Low ────────────────────────────────────────────────────────
    if higher_low:
        score += CONFIG["score_higher_low"]
        signals.append("🔼 Higher Low terdeteksi — struktur bullish awal mulai terbentuk")

    # ── 12. BOS Up — dikonfirmasi tapi diberi skor rendah ────────────────────
    # FIX v15.7: turun dari +3/+4 ke +1.
    # BOS = breakout sudah terjadi. Pre-pump = SEBELUM BOS.
    # Tetap dicatat sebagai konfirmasi minor bahwa struktur sedang berbalik.
    if bos_up:
        score += CONFIG["score_bos_up"]
        signals.append(
            f"🔺 BOS Up (level {_fmt_price(bos_level)}) — breakout minor, "
            f"konfirmasi struktur berbalik (skor rendah: idealnya deteksi sebelum BOS)"
        )

    # ── 13. Funding rate ──────────────────────────────────────────────────────
    f_avg = fstats["avg"]

    if f_avg <= CONFIG["funding_bonus_avg"]:
        score += CONFIG["score_funding_avg_neg"]
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

    # ── 14. BTC Outperformance ────────────────────────────────────────────────
    if btc_corr.get("outperform_label") == "OUTPERFORM":
        score += CONFIG["score_outperform"]
        signals.append(
            f"🚀 OUTPERFORM BTC — coin {btc_corr['coin_period_chg']:+.1f}% vs BTC "
            f"{btc_corr['btc_period_chg']:+.1f}% ({btc_corr['delta_vs_btc']:+.1f}%)"
        )

        # MOMENTUM BREAKOUT DETECTOR
momentum = detect_momentum_breakout(candles_1h, oi_data["change_pct"])

if momentum["is_breakout"]:
    score += momentum["score"]
    reasons.append(
        f"🚀 Momentum breakout: price {momentum['price_move']}% "
        f"vol {momentum['vol_ratio']}x"
    )

    # ══════════════════════════════════════════════════════════════════════════
    #  ALERT LEVEL
    # ══════════════════════════════════════════════════════════════════════════
    alert_level = "MEDIUM"
    pump_type   = "Accumulation Setup"

    # HIGH: pola akumulasi terkuat — energy build-up + squeeze setup
    if energy["is_buildup"] and energy["is_strong"]:
        alert_level = "HIGH"
        pump_type   = "Energy Build-Up + Short Squeeze"
    # HIGH: sweep + HTF accumulation = konfirmasi ganda pre-pump
    elif liq_sweep["is_sweep"] and htf_accum["is_htf_accum"]:
        alert_level = "HIGH"
        pump_type   = "Liquidity Sweep + HTF Accumulation"
    # HIGH: akumulasi kuat + kompresi volatilitas = classic pre-pump
    elif accum["is_accumulating"] and accum["is_vol_compress"] and atr_contr["is_contracting"]:
        alert_level = "HIGH"
        pump_type   = "Smart Money Accumulation + ATR Compression"
    # HIGH: BB squeeze + energy build-up = dual compression signal
    elif bb_squeeze and energy["is_buildup"]:
        alert_level = "HIGH"
        pump_type   = "BB Squeeze + Energy Build-Up"
    # MEDIUM: energy build-up saja
    elif energy["is_buildup"]:
        alert_level = "MEDIUM"
        pump_type   = "Energy Build-Up (OI+Vol+Price Stuck)"
    # MEDIUM: akumulasi terdeteksi
    elif accum["is_accumulating"]:
        alert_level = "MEDIUM"
        pump_type   = "Smart Money Accumulation"
    # MEDIUM: HTF accumulation
    elif htf_accum["is_htf_accum"]:
        alert_level = "MEDIUM"
        pump_type   = "HTF Accumulation Build-Up"
    # MEDIUM: liquidity sweep saja (reversal akan datang)
    elif liq_sweep["is_sweep"]:
        alert_level = "MEDIUM"
        pump_type   = "Liquidity Sweep Reversal"
    # MEDIUM: BB squeeze + kompresi volatilitas
    elif bb_squeeze and atr_contr["is_contracting"]:
        alert_level = "MEDIUM"
        pump_type   = "BB Squeeze + ATR Compression"
    # MEDIUM: OI expansion + accumulation (posisi besar dibangun)
    elif (not oi_data["is_new"] and oi_data["change_pct"] >= CONFIG["oi_strong_pct"]
          and accum["is_vol_compress"]):
        alert_level = "MEDIUM"
        pump_type   = "OI Expansion Kuat + Vol Compression"

    # ── Entry & Target ────────────────────────────────────────────────────────
    entry_data = calc_entry(
        c1h, bos_level, alert_level, vwap, price_now,
        atr_abs_val=atr_abs_val, sr=sr
    )

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
        }
    else:
        log.info(f"  {symbol}: Skor {score} < {CONFIG['min_score_alert']} — dilewati")
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
    Pesan Telegram ringkas — hanya data trading esensial.
    HTML-safe: semua angka dalam <code> tag, R/R ditulis sebagai "X.Xx".
    """
    level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
    e   = r["entry"]
    bc  = r.get("btc_corr", {})
    sr  = r.get("sr", {})
    oi  = r.get("oi_data", {})
    en  = r.get("energy", {})

    p     = r["price"]
    entry = e["entry"]
    sl    = e["sl"]
    t1    = e["t1"]
    t2    = e["t2"]

    # Header
    msg  = f"{level_icon} <b>{r['symbol']} — {r['alert_level']}</b>  #{rank}\n"
    msg += f"<b>Score:</b> {r['score']}  |  {r['pump_type']}\n"
    msg += f"<b>Waktu scan:</b> {utc_now()}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    # Harga & kondisi pasar
    msg += f"<b>Harga :</b> <code>{_fmt_price(p)}</code>  ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>VWAP  :</b> <code>{_fmt_price(r['vwap'])}</code>"
    gap_vwap = (p - r['vwap']) / r['vwap'] * 100 if r['vwap'] > 0 else 0
    msg += f"  ({gap_vwap:+.1f}% vs harga)\n"
    msg += (
        f"<b>Posisi:</b> {r['price_pos_48']:.0%} dari range 48h  |  "
        f"RSI: {r['rsi']}  |  "
        f"Candle: {r['candle_dir_ratio']:.0f}% hijau\n"
    )

    # Entry / SL / TP
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(entry)}</code>  [{e['sl_method']}]\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(sl)}</code>  (-{e['sl_pct']:.2f}%)\n"
    t1_tag = e.get("t1_source", "proj")
    msg += f"🎯 <b>T1    :</b> <code>{_fmt_price(t1)}</code>  (+{e['gain_t1_pct']:.1f}%)  [{t1_tag}]\n"
    msg += f"🎯 <b>T2    :</b> <code>{_fmt_price(t2)}</code>  (+{e['gain_t2_pct']:.1f}%)\n"
    msg += f"⚖️ <b>RR    :</b> {e['rr_str']}x  |  ATR: {r['atr_pct']:.2f}%\n"

    # BTC correlation
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    if bc.get("correlation") is not None:
        msg += (
            f"{bc.get('btc_regime_emoji','❓')} <b>BTC:</b> {bc.get('btc_regime','?')}"
            f"  ({bc.get('btc_period_chg',0):+.1f}%/{bc['lookback']}h)\n"
        )
        msg += (
            f"{bc.get('outperform_emoji','〰️')} <b>vs BTC:</b> {bc.get('outperform_label','?')} "
            f"| Coin {bc.get('coin_period_chg',0):+.1f}% vs BTC {bc.get('btc_period_chg',0):+.1f}%\n"
        )
    else:
        msg += "📊 <b>vs BTC:</b> data tidak tersedia\n"

    # Support & Resistance
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

    # OI
    if oi.get("oi_now", 0) > 0:
        ov     = oi["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs     = f"({oi['change_pct']:+.1f}%)" if not oi.get("is_new") else "(baseline)"
        msg += f"📈 <b>OI:</b> {os_str} {cs}\n"

    # Sinyal teknikal — prioritas
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>Sinyal:</b>\n"
    priority_signals = []
    keywords = [
        "AKUMULASI", "BUILD-UP", "Squeeze", "Sweep", "BOS",
        "VWAP", "Funding", "HTF", "Higher Low", "Compression",
        "OI", "Volume", "ATR", "BB", "RSI", "Menyempit"
    ]
    for s in r["signals"]:
        if any(kw in s for kw in keywords):
            priority_signals.append(s)
        if len(priority_signals) >= 6:
            break
    for s in priority_signals:
        s_short = s[:85] + "…" if len(s) > 85 else s
        msg += f"• {s_short}\n"

    msg += f"\n<i>v15.7 | ⚠️ Bukan financial advice</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v15.7 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        vol_str    = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                      else f"${r['vol_24h']/1e3:.0f}K")
        level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
        htf_tag    = " 🕯️" if r.get("htf_accum", {}).get("is_htf_accum") else ""
        sweep_tag  = " 🎯" if r.get("liq_sweep", {}).get("is_sweep")     else ""
        energy_tag = " ⚡" if r.get("energy",    {}).get("is_buildup")   else ""
        squeeze_tag= " 🔥" if r.get("energy",    {}).get("is_strong")    else ""
        pos_str    = f"pos:{r['price_pos_48']:.0%}"
        msg += (
            f"{i}. {level_icon} <b>{r['symbol']}</b> "
            f"[Score:{r['score']} | {r['alert_level']}{htf_tag}{sweep_tag}{energy_tag}{squeeze_tag}]\n"
        )
        msg += (
            f"   {vol_str} | RSI:{r['rsi']} | {pos_str} | "
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

        # FIX v15.7: diperketat dari 12% → 5%
        # Pre-pump = harga belum banyak bergerak.
        # Coin naik 10% dalam 24h bukan pre-pump — itu sudah pump.
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

    log.info(f"\n📊 SCAN SUMMARY v15.7:")
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
    log.info(f"=== PRE-PUMP SCANNER v15.7 — {utc_now()} ===")

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

    log.info(f"=== SELESAI v15.7 — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v15.7                                     ║")
    log.info("║  Focus: Accumulation Detection, Anti-Distribution Filter    ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
