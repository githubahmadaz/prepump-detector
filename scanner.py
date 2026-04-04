#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NEXUS-SR v1.0 — Support & Resistance High Volume Zone Scanner               ║
║                                                                              ║
║  Port identik dari Pine Script:                                              ║
║  "Support and Resistance (High Volume Boxes) [ChartPrime]"                   ║
║                                                                              ║
║  LOGIKA INTI:                                                                ║
║  · Delta Volume   — Pine Script lines 23-38 (upAndDownVolume)               ║
║  · Pivot Low      — Pine Script line 67     (ta.pivotlow)                   ║
║  · Zone Confirm   — Pine Script line 74     (Vol > vol_hi)                  ║
║  · Zone Width     — Pine Script line 70     (ATR(200) × box_width)          ║
║  · Zone State     — Pine Script lines 105-120 (VALID/TESTING/BROKEN)        ║
║                                                                              ║
║  BOUNCE QUALITY SCORE (5 komponen, bobot default):                           ║
║  · Volume Ratio     30%   current_vol / avg_vol_20                           ║
║  · Distance to Bot  25%   proximity ke zone_bottom                          ║
║  · RSI              20%   RSI 25-40 = full, 40-50 = half                    ║
║  · Candle Pattern   15%   Hammer / Engulfing / Doji                         ║
║  · Touch Bonus      10%   +5 per clean touch, max +15                       ║
║                                                                              ║
║  MANDATORY ADJUSTMENTS:                                                      ║
║  · Coin below EMA200 daily   → score −25 (non-optional)                     ║
║  · BTC+ETH below EMA50 weekly → threshold 65 → 80 (CAUTION mode)           ║
║  · Tidak ada 4H confluence   → score −15                                    ║
║                                                                              ║
║  Exchange : Bitget USDT-Futures (direct REST, no ccxt)                       ║
║  Data     : Bitget REST API v2 (identical pattern to scanner_v5.py)          ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import logging.handlers as _lh
import math
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch   = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/nexus_sr.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── CREDENTIALS ────────────────────────────────────────────────────────
    "bot_token": os.getenv("BOT_TOKEN"),
    "chat_id":   os.getenv("CHAT_ID"),

    # ── UNIVERSE ───────────────────────────────────────────────────────────
    "min_vol_24h":        1_000_000,   # $1M/day — lebih inklusif di bear market
    "max_vol_24h":      800_000_000,   # $800M ceiling (too liquid = less move)
    "gate_chg_24h_max":        40.0,   # skip coin yang sudah pump >40%

    # ── SCANNER PARAMETERS (Pine Script equivalents) ────────────────────
    "lookback_period":           20,   # Pine: lookbackPeriod — pivot kiri & kanan
    "vol_len":                    2,   # Pine: vol_len — delta volume filter window
    "box_width_multiplier":     1.0,   # Pine: box_withd — ATR multiplier zone height
    "candle_limit_1h":         1000,   # 1000 candle 1H = ~41 hari history (pagination)
    "candle_limit_4h":          120,   # candle 4H (~20 hari)
    "candle_limit_1d":          210,   # candle 1D (>200 untuk EMA200)
    "candle_limit_1w":           60,   # candle 1W (>50 untuk EMA50)

    # ── INDICATORS ─────────────────────────────────────────────────────────
    "atr_period":               200,   # Pine: ta.atr(200)
    "rsi_period":                14,
    "ema_daily_period":         200,   # regime filter
    "ema_weekly_period":         50,   # BTC+ETH CAUTION check
    "vol_avg_period":            20,   # baseline volume ratio

    # ── BOUNCE SCORE WEIGHTS (default, kalibrasi ulang setelah 30+ sinyal) ─
    "w_vol_ratio":              0.30,
    "w_distance":               0.25,
    "w_rsi":                    0.20,
    "w_pattern":                0.15,
    "w_touch":                  0.10,

    "vol_ratio_full":            2.0,  # vol > 2x avg = score penuh
    "rsi_full_lo":              25.0,  # RSI range untuk full score
    "rsi_full_hi":              40.0,
    "rsi_half_lo":              40.0,  # RSI range untuk half score
    "rsi_half_hi":              50.0,
    "touch_bonus_per_touch":      5,
    "touch_bonus_max":           15,

    # ── CANDLE PATTERNS ─────────────────────────────────────────────────
    "hammer_shadow_ratio":       2.0,  # lower_shadow > 2 × body
    "hammer_upper_limit":        0.3,  # upper_shadow < 0.3 × body
    "hammer_body_upper":        0.70,  # body in top 30% of range
    "doji_body_ratio":          0.10,  # abs(close-open) < 10% of range

    # ── REGIME ────────────────────────────────────────────────────────────
    "regime_penalty":            25,   # score -25 jika di bawah EMA200 daily
    "caution_score_bump":        15,   # passing threshold +15 di CAUTION mode

    # ── THRESHOLDS ────────────────────────────────────────────────────────
    "score_threshold_normal":    65,   # minimum score
    "score_strong":              80,   # STRONG label
    "confluence_penalty":        15,   # -15 jika tidak ada 4H zone match
    "resistance_clearance_pct": 15.0,  # tolak sinyal jika ada resist <15% di atas
    "max_break_count":            3,   # zone invalid setelah 3x break
    "vol_outlier_mult":          5.0,  # candle vol > 5x avg = outlier
    "vol_outlier_lookback":      20,

    # ── OUTPUT ────────────────────────────────────────────────────────────
    "top_n":                     10,
    "max_alerts":                 5,   # max sinyal dikirim ke Telegram
    "alert_cooldown_sec":      3600,
    "cooldown_file":   "/tmp/nexus_sr_cooldown.json",
    "sleep_between_coins":      0.25,  # detik jeda antar coin saat fetch
    "approaching_pct":           3.0,  # tampilkan pre-alert jika harga dalam 3% di atas zone_top
}

# ── Derived threshold ─────────────────────────────────────────────────────────
SCORE_PASS_NORMAL  = CONFIG["score_threshold_normal"]
SCORE_PASS_CAUTION = SCORE_PASS_NORMAL + CONFIG["caution_score_bump"]


# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    "4USDT","0GUSDT","1000BONKUSDT","1000PEPEUSDT","1000RATSUSDT",
    "1000SHIBUSDT","1000XECUSDT","1INCHUSDT","1MBABYDOGEUSDT","2ZUSDT",
    "AAVEUSDT","ACEUSDT","ACHUSDT","ACTUSDT","ADAUSDT","AEROUSDT",
    "AGLDUSDT","AINUSDT","AIOUSDT","AIXBTUSDT","AKTUSDT","ALCHUSDT",
    "ALGOUSDT","ALICEUSDT","ALLOUSDT","ALTUSDT","ANIMEUSDT",
    "ANKRUSDT","APEUSDT","APEXUSDT","API3USDT","APRUSDT","APTUSDT",
    "ARUSDT","ARBUSDT","ARCUSDT","ARIAUSDT","ARKUSDT","ARKMUSDT",
    "ARPAUSDT","ASTERUSDT","ATUSDT","ATHUSDT","ATOMUSDT","AUCTIONUSDT",
    "AVAXUSDT","AVNTUSDT","AWEUSDT","AXLUSDT","AXSUSDT","AZTECUSDT",
    "BUSDT","B2USDT","BABYUSDT","BANUSDT","BANANAUSDT",
    "BANANAS31USDT","BANKUSDT","BARDUSDT","BATUSDT","BCHUSDT","BEATUSDT",
    "BERAUSDT","BGBUSDT","BIGTIMEUSDT","BIOUSDT","BIRBUSDT","BLASTUSDT",
    "BLESSUSDT","BLURUSDT","BNBUSDT","BOMEUSDT","BRETTUSDT","BREVUSDT",
    "BROCCOLIUSDT","BSVUSDT","BTCUSDT","BULLAUSDT","C98USDT","CAKEUSDT",
    "CCUSDT","CELOUSDT","CFXUSDT","CHILLGUYUSDT","CHZUSDT","CLUSDT",
    "CLANKERUSDT","CLOUSDT","COAIUSDT","COMPUSDT","COOKIEUSDT",
    "COWUSDT","CRCLUSDT","CROUSDT","CROSSUSDT","CRVUSDT","CTKUSDT",
    "CVCUSDT","CVXUSDT","CYBERUSDT","CYSUSDT","DASHUSDT","DEEPUSDT",
    "DENTUSDT","DEXEUSDT","DOGEUSDT","DOLOUSDT","DOODUSDT","DOTUSDT",
    "DRIFTUSDT","DYDXUSDT","DYMUSDT","EGLDUSDT","EIGENUSDT","ENAUSDT",
    "ENJUSDT","ENSUSDT","ENSOUSDT","EPICUSDT","ESPUSDT","ETCUSDT",
    "ETHUSDT","ETHFIUSDT","FUSDT","FARTCOINUSDT","FETUSDT",
    "FFUSDT","FIDAUSDT","FILUSDT","FLOKIUSDT","FLUIDUSDT","FOGOUSDT",
    "FOLKSUSDT","FORMUSDT","GALAUSDT","GASUSDT","GIGGLEUSDT",
    "GLMUSDT","GMTUSDT","GMXUSDT","GOATUSDT","GPSUSDT","GRASSUSDT","GUSDT",
    "GRIFFAINUSDT","GRTUSDT","GUNUSDT","GWEIUSDT","HUSDT","HBARUSDT",
    "HEIUSDT","HEMIUSDT","HMSTRUSDT","HOLOUSDT","HOMEUSDT","HYPEUSDT","HYPERUSDT",
    "ICNTUSDT","ICPUSDT","IDOLUSDT","ILVUSDT",
    "IMXUSDT","INITUSDT","INJUSDT","INXUSDT","IOUSDT",
    "IOTAUSDT","IOTXUSDT","IPUSDT","JASMYUSDT","JCTUSDT","JSTUSDT",
    "JTOUSDT","JUPUSDT","KAIAUSDT","KAITOUSDT","KASUSDT","KAVAUSDT",
    "kBONKUSDT","KERNELUSDT","KGENUSDT","KITEUSDT","kPEPEUSDT","kSHIBUSDT",
    "LAUSDT","LABUSDT","LAYERUSDT","LDOUSDT","LIGHTUSDT","LINEAUSDT",
    "LINKUSDT","LITUSDT","LPTUSDT","LSKUSDT","LTCUSDT","LUNAUSDT",
    "LUNCUSDT","LYNUSDT","MUSDT","MAGICUSDT","MAGMAUSDT","MANAUSDT",
    "MANTAUSDT","MANTRAUSDT","MASKUSDT","MAVUSDT","MAVIAUSDT","MBOXUSDT",
    "MEUSDT","MEGAUSDT","MELANIAUSDT","MEMEUSDT","MERLUSDT","METUSDT",
    "METAUSDT","MEWUSDT","MINAUSDT","MMTUSDT","MNTUSDT","MONUSDT",
    "MOODENGUSDT","MORPHOUSDT","MOVEUSDT","MOVRUSDT","MUUSDT","MUBARAKUSDT",
    "MYXUSDT","NAORISUSDT","NEARUSDT","NEIROCTOUSDT",
    "NEOUSDT","NEWTUSDT","NILUSDT","NMRUSDT","NOMUSDT","NOTUSDT",
    "NXPCUSDT","ONDOUSDT","ONGUSDT","ONTUSDT","OPUSDT","OPENUSDT",
    "OPNUSDT","ORCAUSDT","ORDIUSDT","OXTUSDT","PARTIUSDT",
    "PENDLEUSDT","PENGUUSDT","PEOPLEUSDT","PEPEUSDT","PHAUSDT","PIEVERSEUSDT",
    "PIPPINUSDT","PLUMEUSDT","PNUTUSDT","POLUSDT","POLYXUSDT",
    "POPCATUSDT","POWERUSDT","PROMPTUSDT","PROVEUSDT","PUMPUSDT","PURRUSDT",
    "PYTHUSDT","QUSDT","QNTUSDT","RAVEUSDT","RAYUSDT",
    "RECALLUSDT","RENDERUSDT","RESOLVUSDT","REZUSDT","RIVERUSDT","ROBOUSDT",
    "ROSEUSDT","RPLUSDT","RSRUSDT","RUNEUSDT","SUSDT","SAGAUSDT","SAHARAUSDT",
    "SANDUSDT","SAPIENUSDT","SEIUSDT","SENTUSDT","SHIBUSDT","SIGNUSDT",
    "SIRENUSDT","SKHYNIXUSDT","SKRUSDT","SKYUSDT","SKYAIUSDT","SLPUSDT",
    "SNXUSDT","SOLUSDT","SOMIUSDT","SONICUSDT","SOONUSDT","SOPHUSDT",
    "SPACEUSDT","SPKUSDT","SPXUSDT","SQDUSDT","SSVUSDT",
    "STBLUSDT","STEEMUSDT","STOUSDT","STRKUSDT","STXUSDT",
    "SUIUSDT","SUNUSDT","SUPERUSDT","SUSHIUSDT","SYRUPUSDT","TUSDT",
    "TACUSDT","TAGUSDT","TAIKOUSDT","TAOUSDT","THEUSDT","THETAUSDT",
    "TIAUSDT","TNSRUSDT","TONUSDT","TOSHIUSDT","TOWNSUSDT","TRBUSDT",
    "TRIAUSDT","TRUMPUSDT","TRXUSDT","TURBOUSDT","UAIUSDT","UBUSDT",
    "UMAUSDT","UNIUSDT","USUSDT","USDKRWUSDT","USELESSUSDT",
    "USUALUSDT","VANAUSDT","VANRYUSDT","VETUSDT","VINEUSDT","VIRTUALUSDT",
    "VTHOUSDT","VVVUSDT","WUSDT","WALUSDT","WAXPUSDT","WCTUSDT","WETUSDT",
    "WIFUSDT","WLDUSDT","WLFIUSDT","WOOUSDT","WTIUSDT","XAIUSDT",
    "XCUUSDT","XDCUSDT","XLMUSDT","XMRUSDT","XPDUSDT","XPINUSDT",
    "XPLUSDT","XRPUSDT","XTZUSDT","XVGUSDT","YGGUSDT","YZYUSDT","ZAMAUSDT",
    "ZBTUSDT","ZECUSDT","ZENUSDT","ZEREBROUSDT","ZETAUSDT","ZILUSDT",
    "ZKUSDT","ZKCUSDT","ZKJUSDT","ZKPUSDT","ZORAUSDT","ZROUSDT",
}

MANUAL_EXCLUDE: set = set()


# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN  (identik dengan scanner_v5)
# ══════════════════════════════════════════════════════════════════════════════
def _load_cooldown() -> dict:
    try:
        path = CONFIG["cooldown_file"]
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception:
        pass
    return {}

def _save_cooldown(state: dict) -> None:
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_cooldown_state = _load_cooldown()

def is_on_cooldown(sym: str) -> bool:
    return (time.time() - _cooldown_state.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym: str) -> None:
    _cooldown_state[sym] = time.time()
    _save_cooldown(_cooldown_state)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT  (identik pattern dengan scanner_v5)
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"
    _candle_cache: Dict = {}

    @staticmethod
    def _get(url: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=timeout)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    log.warning("Bitget rate limit — tunggu 30s")
                    time.sleep(30)
                    continue
                log.warning(f"Bitget HTTP {e.response.status_code} on {url}")
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        """GET /api/v2/mix/market/tickers — semua ticker USDT-Futures."""
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"}
        )
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, granularity: str = "1H",
                    limit: int = 200) -> List[dict]:
        """
        GET /api/v2/mix/market/candles
        Candle row: [ts, open, high, low, close, base_vol, quote_vol_usd]
        Mendukung limit > 200 via pagination otomatis (max 5 halaman = 1000 candle).
        Returns list sorted ascending by ts.
        """
        cache_key = f"{symbol}:{granularity}:{limit}"
        if cache_key in cls._candle_cache:
            return cls._candle_cache[cache_key]

        def _parse_rows(raw_rows: list) -> List[dict]:
            out = []
            for row in raw_rows:
                try:
                    vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                    out.append({
                        "ts":    int(row[0]),
                        "open":  float(row[1]),
                        "high":  float(row[2]),
                        "low":   float(row[3]),
                        "close": float(row[4]),
                        "vol":   vol_usd,
                    })
                except (IndexError, ValueError):
                    continue
            return out

        if limit <= 200:
            # ── single request (original v5 pattern) ─────────────────────────
            data = cls._get(
                f"{cls.BASE}/api/v2/mix/market/candles",
                params={
                    "symbol":      symbol,
                    "productType": "USDT-FUTURES",
                    "granularity": granularity,
                    "limit":       limit,
                }
            )
            if not data or data.get("code") != "00000":
                return []
            candles = _parse_rows(data.get("data", []))
            candles.sort(key=lambda x: x["ts"])
            cls._candle_cache[cache_key] = candles
            return candles

        # ── Paginated fetch (limit > 200) ─────────────────────────────────────
        # Bitget returns newest-first; we walk backwards using endTime.
        collected: Dict[int, dict] = {}
        end_time: Optional[int]    = None
        max_pages                  = math.ceil(limit / 200)

        for page in range(max_pages):
            params: dict = {
                "symbol":      symbol,
                "productType": "USDT-FUTURES",
                "granularity": granularity,
                "limit":       200,
            }
            if end_time is not None:
                params["endTime"] = str(end_time)

            data = cls._get(f"{cls.BASE}/api/v2/mix/market/candles", params=params)
            if not data or data.get("code") != "00000":
                break

            raw = data.get("data", [])
            if not raw:
                break

            batch = _parse_rows(raw)
            for c in batch:
                collected[c["ts"]] = c

            if len(batch) < 200:
                break                               # exchange has no more history

            # Oldest timestamp in this batch → next page goes earlier
            oldest_ts = min(c["ts"] for c in batch)
            end_time  = oldest_ts - 1

            if len(collected) >= limit:
                break

            time.sleep(0.15)                        # pagination rate-limit courtesy

        result = sorted(collected.values(), key=lambda x: x["ts"])
        result = result[-limit:]                    # keep most recent `limit` candles
        cls._candle_cache[cache_key] = result
        return result

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  📐  MATH UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def _mean(arr: list) -> float:
    return sum(arr) / len(arr) if arr else 0.0

def _std(arr: list) -> float:
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / len(arr))

def _wilder_ema(values: list, period: int) -> list:
    """Wilder's smoothing (RMA) — identik dengan Pine's ta.ema / ta.atr internals.
    alpha = 1/period, adjust=False."""
    if not values:
        return []
    result = [0.0] * len(values)
    alpha  = 1.0 / period
    # Warm up: SMA for first period values, then Wilder's EMA
    result[0] = values[0]
    for i in range(1, len(values)):
        result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1]
    return result

def _std_ema(values: list, period: int) -> list:
    """Standard EMA (Pine's ta.ema) — span=period, adjust=False."""
    if not values:
        return []
    alpha  = 2.0 / (period + 1)
    result = [0.0] * len(values)
    result[0] = values[0]
    for i in range(1, len(values)):
        result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1]
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DELTA VOLUME  (Pine Script lines 23–38: upAndDownVolume)
# ══════════════════════════════════════════════════════════════════════════════
def compute_delta_volume(candles: List[dict]) -> List[float]:
    """
    Pine Script: upAndDownVolume() — lines 23-38
    Returns signed volume per candle:
      +vol  if close > open  (bullish)
      -vol  if close < open  (bearish)
      ±vol  if close == open (doji: inherits previous direction via var isBuyVolume)
    """
    result    = []
    is_buy    = True   # Pine: var isBuyVolume = true
    for c in candles:
        if c["close"] > c["open"]:
            is_buy = True
        elif c["close"] < c["open"]:
            is_buy = False
        # doji: is_buy unchanged (Pine var persists)
        result.append(c["vol"] if is_buy else -c["vol"])
    return result


def compute_vol_thresholds(delta_vol: List[float],
                           vol_len: int) -> Tuple[List[float], List[float]]:
    """
    Pine Script: lines 48-49
      vol_hi = ta.highest(Vol/2.5, vol_len)
      vol_lo = ta.lowest (Vol/2.5, vol_len)
    Rolling window max/min of delta_vol/2.5.
    """
    n      = len(delta_vol)
    scaled = [v / 2.5 for v in delta_vol]
    vol_hi = [0.0] * n
    vol_lo = [0.0] * n
    for i in range(n):
        window = scaled[max(0, i - vol_len + 1): i + 1]
        vol_hi[i] = max(window)
        vol_lo[i] = min(window)
    return vol_hi, vol_lo


# ══════════════════════════════════════════════════════════════════════════════
#  📏  ATR  (Pine Script line 69: ta.atr(200))
# ══════════════════════════════════════════════════════════════════════════════
def compute_atr(candles: List[dict], period: int = 200) -> List[float]:
    """
    Pine Script: ta.atr(200) — line 69
    Wilder's smoothed ATR (RMA).
    """
    trs = []
    for i, c in enumerate(candles):
        prev_close = candles[i - 1]["close"] if i > 0 else c["close"]
        tr = max(
            c["high"] - c["low"],
            abs(c["high"] - prev_close),
            abs(c["low"]  - prev_close),
        )
        trs.append(tr)
    return _wilder_ema(trs, period)


# ══════════════════════════════════════════════════════════════════════════════
#  📉  RSI  (Wilder's smoothing, Pine's ta.rsi)
# ══════════════════════════════════════════════════════════════════════════════
def compute_rsi(candles: List[dict], period: int = 14) -> List[float]:
    closes = [c["close"] for c in candles]
    gains  = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]

    avg_g = _wilder_ema(gains,  period)
    avg_l = _wilder_ema(losses, period)

    rsi = [50.0]
    for i in range(len(avg_g)):
        if avg_l[i] == 0:
            rsi.append(100.0)
        else:
            rs = avg_g[i] / avg_l[i]
            rsi.append(100.0 - 100.0 / (1.0 + rs))
    return rsi   # len = len(candles)


# ══════════════════════════════════════════════════════════════════════════════
#  📈  EMA  (Standard, Pine's ta.ema)
# ══════════════════════════════════════════════════════════════════════════════
def compute_ema(candles: List[dict], period: int) -> List[float]:
    closes = [c["close"] for c in candles]
    return _std_ema(closes, period)


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  PIVOT LOW DETECTION  (Pine Script line 67: ta.pivotlow)
# ══════════════════════════════════════════════════════════════════════════════
def find_pivot_lows(lows: List[float], lookback: int) -> List[Optional[float]]:
    """
    Pine Script: ta.pivotlow(src, lookbackPeriod, lookbackPeriod) — line 67
    Pivot low at index i: low[i] <= all lows in [i-lookback .. i+lookback].
    Returns list same length as lows; None where no pivot.
    The most recent confirmable pivot is at index -(lookback+1).
    """
    n      = len(lows)
    result = [None] * n
    for i in range(lookback, n - lookback):
        val   = lows[i]
        left  = lows[i - lookback: i]
        right = lows[i + 1: i + lookback + 1]
        if val <= min(left) and val <= min(right):
            result[i] = val
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  🟩  SUPPORT ZONE DETECTION  (Pine Script lines 72–78)
# ══════════════════════════════════════════════════════════════════════════════
def detect_support_zones(
    candles:   List[dict],
    lookback:  int,
    vol_len:   int,
    box_width: float,
    atr_period: int = 200,
) -> List[dict]:
    """
    Pine Script: calcSupportResistance() — support branch, lines 72-78
    For each pivot low:
      CONFIRM: delta_vol[i] > vol_hi[i]    (Pine line 74)
      zone_top    = pivot_low              (Pine line 75)
      zone_bottom = pivot_low − ATR × box_width  (Pine line 70: withd)
    """
    if len(candles) < lookback * 2 + 5:
        return []

    dv      = compute_delta_volume(candles)
    vh, _   = compute_vol_thresholds(dv, vol_len)
    atr     = compute_atr(candles, atr_period)
    lows    = [c["low"] for c in candles]
    pivots  = find_pivot_lows(lows, lookback)

    zones = []
    for i, piv in enumerate(pivots):
        if piv is None:
            continue
        if dv[i] <= vh[i]:
            continue                  # Pine line 74: Vol > vol_hi
        if atr[i] <= 0:
            continue

        zone_top    = piv
        zone_bottom = piv - atr[i] * box_width

        zones.append({
            "bar_index":   i,
            "ts":          candles[i]["ts"],
            "zone_top":    zone_top,
            "zone_bottom": zone_bottom,
            "delta_vol":   dv[i],
            "atr":         atr[i],
            "break_count": 0,          # computed below
            "touch_count": 0,          # computed below
        })

    # ── Compute touch_count and break_count from remaining candles ────────────
    for z in zones:
        bi        = z["bar_index"]
        top       = z["zone_top"]
        bottom    = z["zone_bottom"]
        in_zone   = False
        touches   = 0
        breaks    = 0

        for j in range(bi + 1, len(candles)):
            c_low  = candles[j]["low"]
            c_high = candles[j]["high"]

            if not in_zone:
                if bottom <= c_low <= top:
                    in_zone  = True
                    touches += 1
            else:
                if c_high < bottom:       # broke below
                    breaks  += 1
                    in_zone  = False
                elif c_low > top:         # bounced back above
                    in_zone  = False

        z["touch_count"] = touches
        z["break_count"] = breaks

    return zones


# ══════════════════════════════════════════════════════════════════════════════
#  🟥  RESISTANCE ZONE DETECTION  (Pine Script lines 82–101)
# ══════════════════════════════════════════════════════════════════════════════
def detect_resistance_zones(
    candles:   List[dict],
    lookback:  int,
    vol_len:   int,
    box_width: float,
    atr_period: int = 200,
) -> List[dict]:
    """
    Pine Script: calcSupportResistance() — resistance branch, lines 82-101
    Pivot high + Vol < vol_lo → resistance zone.
    """
    if len(candles) < lookback * 2 + 5:
        return []

    dv    = compute_delta_volume(candles)
    _, vl = compute_vol_thresholds(dv, vol_len)
    atr   = compute_atr(candles, atr_period)
    highs = [c["high"] for c in candles]
    n     = len(candles)
    result = [None] * n

    for i in range(lookback, n - lookback):
        val   = highs[i]
        left  = highs[i - lookback: i]
        right = highs[i + 1: i + lookback + 1]
        if val >= max(left) and val >= max(right):
            result[i] = val

    zones = []
    for i, piv in enumerate(result):
        if piv is None:
            continue
        if dv[i] >= vl[i]:
            continue              # Pine line 82: Vol < vol_lo
        if atr[i] <= 0:
            continue

        zones.append({
            "zone_bottom": piv,
            "zone_top":    piv + atr[i] * box_width,
        })

    return zones


# ══════════════════════════════════════════════════════════════════════════════
#  🔄  ZONE STATE MACHINE  (Pine Script lines 105–120)
# ══════════════════════════════════════════════════════════════════════════════
def get_zone_state(zone: dict, current_low: float, current_high: float) -> str:
    """
    Pine Script: brekout_sup / sup_holds logic — lines 105-120
      VALID   : low > zone_top          (price above zone)
      TESTING : zone_bottom <= low <= zone_top   (price inside zone)
      BROKEN  : high < zone_bottom      (price below zone floor)
    """
    top    = zone["zone_top"]
    bottom = zone["zone_bottom"]
    if current_high < bottom:
        return "BROKEN"
    if bottom <= current_low <= top:
        return "TESTING"
    if current_low > top:
        return "VALID"
    return "VALID"


# ══════════════════════════════════════════════════════════════════════════════
#  🕯️  CANDLE PATTERN DETECTION
# ══════════════════════════════════════════════════════════════════════════════
def detect_pattern(candles: List[dict]) -> str:
    """
    Spec definitions (exact implementation):

    Hammer:
      lower_shadow > 2 × body
      upper_shadow < 0.3 × body
      body in upper 30% of candle range: min(o,c) > low + 0.7*(high-low)

    Bullish Engulfing:
      close[0] > open[0]               current bullish
      open[0]  < close[-1]             open below prev close
      close[0] > open[-1]              close above prev open
      |body[0]| > |body[-1]|           larger body

    Doji:
      abs(close - open) < 0.10 × (high - low)
    """
    if len(candles) < 2:
        return "NONE"

    cur  = candles[-1]
    prev = candles[-2]

    o, h, l, c = cur["open"], cur["high"], cur["low"], cur["close"]
    rng  = h - l
    body = abs(c - o)

    if rng <= 0:
        return "NONE"

    upper_shadow = h - max(o, c)
    lower_shadow = min(o, c) - l

    # Hammer
    cfg = CONFIG
    if (lower_shadow > cfg["hammer_shadow_ratio"] * body
            and upper_shadow < cfg["hammer_upper_limit"] * body
            and min(o, c) > l + cfg["hammer_body_upper"] * rng):
        return "HAMMER"

    # Bullish Engulfing
    po, pc = prev["open"], prev["close"]
    prev_body = abs(pc - po)
    if (c > o and o < pc and c > po and body > prev_body):
        return "ENGULFING"

    # Doji
    if body < cfg["doji_body_ratio"] * rng:
        return "DOJI"

    return "NONE"


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  BOUNCE SCORE  (5 komponen)
# ══════════════════════════════════════════════════════════════════════════════
def compute_bounce_score(
    candles_1h: List[dict],
    zone:       dict,
    rsi_series: List[float],
) -> Tuple[float, dict]:
    """
    Computes raw Bounce Score (0–100) before regime/confluence adjustments.
    Returns (raw_score, component_breakdown).
    """
    cfg = CONFIG
    cur = candles_1h[-1]
    current_price = cur["close"]

    # ── Component 1: Volume Ratio (30%) ──────────────────────────────────────
    avg_p = cfg["vol_avg_period"]
    recent_vols = [c["vol"] for c in candles_1h[-avg_p - 1: -1]]
    avg_vol     = _mean(recent_vols) if recent_vols else 0
    vol_ratio   = cur["vol"] / avg_vol if avg_vol > 0 else 0

    full_thr  = cfg["vol_ratio_full"]
    vol_score = cfg["w_vol_ratio"] * 100 * min(vol_ratio / full_thr, 1.0)

    # ── Component 2: Distance to Zone Bottom (25%) ───────────────────────────
    zone_range = zone["zone_top"] - zone["zone_bottom"]
    if zone_range > 0:
        dist_norm  = max(0, min(1, (zone["zone_top"] - current_price) / zone_range))
        dist_score = cfg["w_distance"] * 100 * dist_norm
    else:
        dist_norm  = 0.0
        dist_score = 0.0

    # ── Component 3: RSI (20%) ────────────────────────────────────────────────
    rsi_val = rsi_series[-1] if rsi_series else 50.0
    if cfg["rsi_full_lo"] <= rsi_val <= cfg["rsi_full_hi"]:
        rsi_score = cfg["w_rsi"] * 100
    elif cfg["rsi_half_lo"] < rsi_val <= cfg["rsi_half_hi"]:
        rsi_score = cfg["w_rsi"] * 100 * 0.5
    else:
        rsi_score = 0.0

    # ── Component 4: Candle Pattern (15%) ────────────────────────────────────
    pattern      = detect_pattern(candles_1h)
    candle_score = cfg["w_pattern"] * 100 if pattern != "NONE" else 0.0

    # ── Component 5: Zone Touch Bonus (10%) ──────────────────────────────────
    touch_count = zone.get("touch_count", 0)
    touch_bonus = min(touch_count * cfg["touch_bonus_per_touch"],
                      cfg["touch_bonus_max"])
    touch_score = (cfg["w_touch"] * 100 * touch_bonus / cfg["touch_bonus_max"]
                   if cfg["touch_bonus_max"] > 0 else 0.0)

    # ── Volume outlier check — if outlier, penalise (filter #5) ──────────────
    outlier_lookback = cfg["vol_outlier_lookback"]
    outlier_vols     = [c["vol"] for c in candles_1h[-outlier_lookback - 1: -1]]
    outlier_avg      = _mean(outlier_vols)
    is_outlier       = (cur["vol"] > cfg["vol_outlier_mult"] * outlier_avg
                        if outlier_avg > 0 else False)

    raw_score = vol_score + dist_score + rsi_score + candle_score + touch_score

    return raw_score, {
        "vol_ratio":    round(vol_ratio, 3),
        "vol_score":    round(vol_score, 2),
        "dist_norm":    round(dist_norm, 3),
        "dist_score":   round(dist_score, 2),
        "rsi_val":      round(rsi_val, 2),
        "rsi_score":    round(rsi_score, 2),
        "pattern":      pattern,
        "candle_score": round(candle_score, 2),
        "touch_count":  touch_count,
        "touch_score":  round(touch_score, 2),
        "raw_score":    round(raw_score, 2),
        "is_vol_outlier": is_outlier,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🌍  REGIME FILTER  (mandatory per spec)
# ══════════════════════════════════════════════════════════════════════════════
def is_below_ema200_daily(candles_1d: List[dict]) -> bool:
    """
    Spec: "Jika coin di bawah EMA200 daily: score −25 poin"
    Returns True jika current close < EMA200 pada daily chart.
    """
    if not candles_1d or len(candles_1d) < 10:
        return False
    ema = _std_ema([c["close"] for c in candles_1d], 200)
    return candles_1d[-1]["close"] < ema[-1]


def is_caution_mode(btc_1w: List[dict], eth_1w: List[dict]) -> bool:
    """
    Spec: "Jika BTC + ETH keduanya di bawah EMA50 weekly: min passing score 65→80"
    Returns True (CAUTION) hanya jika KEDUANYA BTC dan ETH di bawah EMA50 weekly.
    """
    if len(btc_1w) < 10 or len(eth_1w) < 10:
        return False
    ema_btc = _std_ema([c["close"] for c in btc_1w], 50)
    ema_eth = _std_ema([c["close"] for c in eth_1w], 50)
    btc_below = btc_1w[-1]["close"] < ema_btc[-1]
    eth_below = eth_1w[-1]["close"] < ema_eth[-1]
    return btc_below and eth_below


# ══════════════════════════════════════════════════════════════════════════════
#  🔗  MTF CONFLUENCE  (1H zone vs 4H zone dalam ±1 ATR(4H))
# ══════════════════════════════════════════════════════════════════════════════
def check_4h_confluence(
    zone_1h:    dict,
    candles_4h: List[dict],
    cfg:        dict,
) -> Tuple[bool, str]:
    """
    Spec: "Sinyal 1h valid sempurna jika center zone 1h berada dalam
           ±1 ATR(4h) dari support zone 4h"
    Returns (has_confluence, note_string).
    """
    if not candles_4h or len(candles_4h) < 50:
        return False, "no 4h data"

    atr_4h_list = compute_atr(candles_4h, cfg["atr_period"])
    atr_4h      = atr_4h_list[-1] if atr_4h_list else 0
    if atr_4h <= 0:
        return False, "atr4h=0"

    zones_4h = detect_support_zones(
        candles_4h,
        cfg["lookback_period"],
        cfg["vol_len"],
        cfg["box_width_multiplier"],
        cfg["atr_period"],
    )
    if not zones_4h:
        return False, "no 4h zones"

    center_1h = (zone_1h["zone_top"] + zone_1h["zone_bottom"]) / 2.0
    radius    = atr_4h  # ±1 ATR(4H)

    for z4 in zones_4h:
        center_4h = (z4["zone_top"] + z4["zone_bottom"]) / 2.0
        if abs(center_1h - center_4h) <= radius:
            return True, f"4h_zone@{z4['zone_top']:.4f}"

    return False, f"no match (radius={atr_4h:.4f})"


# ══════════════════════════════════════════════════════════════════════════════
#  🚧  RESISTANCE CLEARANCE  (filter #6)
# ══════════════════════════════════════════════════════════════════════════════
def has_nearby_resistance(
    zones_r:       List[dict],
    current_price: float,
    clearance_pct: float = 15.0,
) -> bool:
    """
    Spec: "Tidak ada resistance zone kuat dalam radius < 15% di atas harga saat ini"
    Returns True (reject) jika ada resistance zone dalam clearance_pct% di atas price.
    """
    threshold = current_price * (1.0 + clearance_pct / 100.0)
    for z in zones_r:
        if current_price < z["zone_bottom"] <= threshold:
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM  (identik dengan scanner_v5)
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(msg: str) -> bool:
    bot_token = CONFIG["bot_token"]
    chat_id   = CONFIG["chat_id"]
    if not bot_token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🖨️  OUTPUT FORMATTING
# ══════════════════════════════════════════════════════════════════════════════
def build_approaching_table(approaching: list) -> str:
    """Tabel coin yang mendekati zona support (dalam approaching_pct% di atas zone_top)."""
    if not approaching:
        return ""
    lines = [
        "",
        "  📍 APPROACHING ZONES  (harga dalam 3% di atas zone_top — pantau untuk entry)",
        "  " + "─" * 80,
        f"  {'#':>2}  {'Coin':<14} {'Price':>12} {'ZoneTop':>11} {'ZoneBot':>11} {'Dist%':>7}  Vol24h(M)",
        "  " + "─" * 80,
    ]
    for i, a in enumerate(approaching[:10], 1):
        raw = a["symbol"].replace("USDT", "")
        lines.append(
            f"  {i:>2}  {raw:<14} {a['price']:>12.6f} {a['zone_top']:>11.6f} "
            f"{a['zone_bot']:>11.6f} {a['dist_pct']:>6.2f}%  ${a['vol_24h_m']:.1f}M"
        )
    lines.append("  " + "─" * 80)
    return "\n".join(lines)
    """Tabel terminal untuk top-N sinyal."""
    mode_str = "⚠️  CAUTION (threshold=80)" if caution else "✅ NORMAL (threshold=65)"
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines    = [
        "",
        "═" * 115,
        f"  NEXUS-SR  |  Bitget Futures  |  {now_str}  |  {mode_str}",
        "═" * 115,
        f"  {'#':>2}  {'Coin':<13} {'Price':>13} {'ZoneTop':>11} {'ZoneBot':>11} "
        f"{'Score':>6} {'Target':>13} {'VolRat':>7} {'RSI':>6} {'Pat':<10} {'Conf':>5}  Strength",
        "─" * 115,
    ]
    for i, r in enumerate(results, 1):
        raw = r["symbol"].replace("USDT", "")
        conf_mark = "★" if r["confluence"] else " "
        ema_mark  = "↓" if r["below_ema"] else " "
        lines.append(
            f"  {i:>2}  {raw:<13} {r['price']:>13.6f} {r['zone_top']:>11.6f} "
            f"{r['zone_bot']:>11.6f} {r['score']:>6.1f} {r['target']:>13.6f} "
            f"{r['vol_ratio']:>6.2f}x {r['rsi']:>6.1f} {r['pattern']:<10} "
            f"{conf_mark}{ema_mark}{' ':>3}  {r['strength']}"
        )
    lines += [
        "─" * 115,
        "  ★ = 4H confluence  ↓ = coin below EMA200 daily",
        "═" * 115,
    ]
    return "\n".join(lines)


def build_telegram_summary(results: list, caution: bool) -> str:
    mode = "⚠️ CAUTION" if caution else "🟢 NORMAL"
    now  = datetime.now(timezone.utc).strftime("%H:%M UTC")
    txt  = (f"🎯 <b>NEXUS-SR — Bounce Signals</b> [{now}]\n"
            f"Mode: {mode} | {len(results)} sinyal\n{'─'*30}\n")
    for i, r in enumerate(results, 1):
        raw     = r["symbol"].replace("USDT", "")
        cf      = " ★4H" if r["confluence"] else ""
        ema_tag = " 📉EMA" if r["below_ema"] else ""
        txt += (
            f"{i}. <b>{raw}</b>{cf}{ema_tag}\n"
            f"   Price: <code>{r['price']:.6f}</code> | "
            f"Zone: <code>{r['zone_bot']:.4f}–{r['zone_top']:.4f}</code>\n"
            f"   🎯 Target: <code>{r['target']:.4f}</code> (+15%) | "
            f"🛑 Stop: <code>{r['stop']:.4f}</code>\n"
            f"   Score: <b>{r['score']:.1f}</b> ({r['strength']}) | "
            f"RSI:{r['rsi']:.1f} Vol:{r['vol_ratio']:.2f}x {r['pattern']}\n\n"
        )
    txt += "⚠️ <i>Bukan rekomendasi finansial. Selalu gunakan manajemen risiko.</i>"
    return txt


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    start_ts = time.time()
    cfg      = CONFIG

    log.info("=" * 70)
    log.info(f"  NEXUS-SR v1.0 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 70)

    # ── 1. Regime data (BTC + ETH weekly — fetched SEKALI di awal) ────────────
    log.info("Fetching BTC/ETH weekly data untuk regime check …")
    btc_1w = BitgetClient.get_candles("BTCUSDT", "1W", cfg["candle_limit_1w"])
    eth_1w = BitgetClient.get_candles("ETHUSDT", "1W", cfg["candle_limit_1w"])
    caution = is_caution_mode(btc_1w, eth_1w)
    score_threshold = SCORE_PASS_CAUTION if caution else SCORE_PASS_NORMAL
    log.info(f"Market regime: {'⚠️  CAUTION MODE (threshold=80)' if caution else '✅ NORMAL MODE (threshold=65)'}")

    # ── 2. Fetch tickers ────────────────────────────────────────────────────
    log.info("Fetching Bitget USDT-Futures tickers …")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        send_telegram("⚠️ NEXUS-SR: Gagal fetch Bitget tickers")
        return
    log.info(f"Tickers received: {len(tickers)}")

    # ── 3. Build candidates ─────────────────────────────────────────────────
    from collections import defaultdict
    skip_stats = defaultdict(int)
    candidates = []

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:            skip_stats["excluded"]   += 1; continue
        if is_on_cooldown(sym):              skip_stats["cooldown"]   += 1; continue
        if sym not in tickers:               skip_stats["not_found"]  += 1; continue
        t = tickers[sym]
        try:
            vol  = float(t.get("quoteVolume", 0))
            chg  = float(t.get("change24h",   0)) * 100
        except Exception:
            skip_stats["parse_error"] += 1; continue
        if vol < cfg["min_vol_24h"]:         skip_stats["vol_low"]    += 1; continue
        if vol > cfg["max_vol_24h"]:         skip_stats["vol_high"]   += 1; continue
        if chg > cfg["gate_chg_24h_max"]:    skip_stats["pumped"]     += 1; continue
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    # ── 4. Score each candidate ─────────────────────────────────────────────
    results         = []
    all_approaching = []   # coins mendekat zona tapi belum TESTING
    BitgetClient.clear_cache()

    for idx, (sym, ticker) in enumerate(candidates):
        log.info(f"[{idx+1}/{len(candidates)}] {sym}")

        try:
            price   = float(ticker.get("lastPr",      0))
            vol_24h = float(ticker.get("quoteVolume",  0))
            if price <= 0:
                skip_stats["no_price"] += 1
                continue

            # ── Fetch 1H candles ────────────────────────────────────────────
            candles_1h = BitgetClient.get_candles(sym, "1H", cfg["candle_limit_1h"])
            if len(candles_1h) < cfg["lookback_period"] * 2 + 20:
                log.debug(f"  Skip {sym}: candle 1H kurang ({len(candles_1h)})")
                skip_stats["candle_short"] += 1
                continue

            current_low  = candles_1h[-1]["low"]
            current_high = candles_1h[-1]["high"]
            current_close = candles_1h[-1]["close"]

            # ── Detect support zones ─────────────────────────────────────────
            sup_zones = detect_support_zones(
                candles_1h,
                cfg["lookback_period"],
                cfg["vol_len"],
                cfg["box_width_multiplier"],
                cfg["atr_period"],
            )
            if not sup_zones:
                log.debug(f"  {sym}: tidak ada support zone dari {len(candles_1h)} candle")
                skip_stats["no_zones"] += 1
                continue

            # ── Cari zone TESTING (cek 2 candle terakhir) ────────────────────
            # Harga mungkin masuk zona di candle sebelumnya dan belum keluar
            testing_zones = []
            for z in sup_zones:
                if z["break_count"] >= cfg["max_break_count"]:
                    continue
                for lookback_i in (1, 2):          # cek candle[-1] dan candle[-2]
                    if lookback_i >= len(candles_1h):
                        continue
                    c = candles_1h[-lookback_i]
                    if get_zone_state(z, c["low"], c["high"]) == "TESTING":
                        testing_zones.append(z)
                        break

            # ── APPROACHING: zone dalam 3% di atas harga (pre-alert) ─────────
            approaching_zones = []
            for z in sup_zones:
                if z["break_count"] >= cfg["max_break_count"]:
                    continue
                if z not in testing_zones:
                    dist_pct = (z["zone_top"] - current_close) / current_close * 100
                    if 0 < dist_pct <= cfg.get("approaching_pct", 3.0):
                        approaching_zones.append((z, round(dist_pct, 2)))

            if not testing_zones:
                n_broken = sum(1 for z in sup_zones
                               if get_zone_state(z, current_low, current_high) == "BROKEN")
                n_valid  = sum(1 for z in sup_zones
                               if get_zone_state(z, current_low, current_high) == "VALID")
                approach_str = (f" | approaching={len(approaching_zones)}" if approaching_zones else "")
                log.debug(
                    f"  {sym}: {len(sup_zones)} zones — "
                    f"broken={n_broken} valid={n_valid}{approach_str}"
                )
                if approaching_zones:
                    skip_stats["approaching"] = skip_stats.get("approaching", 0) + 1
                    all_approaching.append({
                        "symbol":   sym,
                        "price":    current_close,
                        "zone_top": approaching_zones[0][0]["zone_top"],
                        "zone_bot": approaching_zones[0][0]["zone_bottom"],
                        "dist_pct": approaching_zones[0][1],
                        "vol_24h_m": round(vol_24h / 1_000_000, 2),
                    })
                skip_stats["no_testing"] += 1
                continue

            # ── Filter #5: volume outlier check ──────────────────────────────
            recent_vols  = [c["vol"] for c in candles_1h[-cfg["vol_outlier_lookback"]-1:-1]]
            avg_vol_base = _mean(recent_vols)
            if (avg_vol_base > 0
                    and candles_1h[-1]["vol"] > cfg["vol_outlier_mult"] * avg_vol_base):
                log.debug(f"  {sym}: volume outlier — skip")
                skip_stats["vol_outlier"] += 1
                continue

            # ── Pre-compute indicators (shared across zones) ───────────────
            rsi_series = compute_rsi(candles_1h, cfg["rsi_period"])

            # ── Detect resistance zones for filter #6 ─────────────────────
            res_zones = detect_resistance_zones(
                candles_1h,
                cfg["lookback_period"],
                cfg["vol_len"],
                cfg["box_width_multiplier"],
                cfg["atr_period"],
            )

            # ── Check filter #6: resistance clearance ─────────────────────
            if has_nearby_resistance(res_zones, current_close,
                                     cfg["resistance_clearance_pct"]):
                log.debug(f"  {sym}: resistance clearance FAIL")
                skip_stats["resistance_block"] += 1
                continue

            # ── Fetch 4H (confluence) — hanya jika ada TESTING zone ────────
            candles_4h = BitgetClient.get_candles(sym, "4H", cfg["candle_limit_4h"])
            time.sleep(0.1)  # light rate-limit courtesy

            # ── Fetch 1D (regime per coin) ────────────────────────────────
            candles_1d = BitgetClient.get_candles(sym, "1D", cfg["candle_limit_1d"])
            time.sleep(0.1)

            below_ema = is_below_ema200_daily(candles_1d)

            # ── Score each TESTING zone, ambil yang terbaik ───────────────
            best_score  = -999
            best_result = None

            for zone in testing_zones:
                raw_score, breakdown = compute_bounce_score(
                    candles_1h, zone, rsi_series
                )

                # Mandatory regime penalty
                adj_score = raw_score
                if below_ema:
                    adj_score -= cfg["regime_penalty"]

                # MTF confluence check
                has_conf, conf_note = check_4h_confluence(zone, candles_4h, cfg)
                if not has_conf:
                    adj_score -= cfg["confluence_penalty"]

                # Filter #2: score threshold
                if adj_score < score_threshold:
                    continue

                if adj_score > best_score:
                    best_score = adj_score
                    target     = current_close * 1.15
                    stop       = zone["zone_bottom"] * 0.99
                    strength   = ("STRONG"   if adj_score >= 80  else
                                  "MODERATE" if adj_score >= 65  else
                                  "WEAK")

                    best_result = {
                        "symbol":      sym,
                        "price":       round(current_close, 8),
                        "zone_top":    round(zone["zone_top"], 8),
                        "zone_bot":    round(zone["zone_bottom"], 8),
                        "target":      round(target, 8),
                        "stop":        round(stop, 8),
                        "score":       round(adj_score, 2),
                        "raw_score":   round(raw_score, 2),
                        "vol_ratio":   breakdown["vol_ratio"],
                        "rsi":         breakdown["rsi_val"],
                        "pattern":     breakdown["pattern"],
                        "touch_count": breakdown["touch_count"],
                        "confluence":  has_conf,
                        "conf_note":   conf_note,
                        "below_ema":   below_ema,
                        "caution":     caution,
                        "strength":    strength,
                        "vol_24h_m":   round(vol_24h / 1_000_000, 2),
                    }

            if best_result:
                results.append(best_result)
                log.info(
                    f"  ✅ Score={best_result['score']} ({best_result['strength']}) | "
                    f"RSI={best_result['rsi']:.1f} Vol={best_result['vol_ratio']:.2f}x "
                    f"Pat={best_result['pattern']} Conf={'★' if best_result['confluence'] else '·'} "
                    f"EMA={'↓' if best_result['below_ema'] else '✓'}"
                )
            else:
                log.debug(f"  {sym}: no zone passed score filter")

        except Exception as exc:
            log.warning(f"  Error {sym}: {exc}", exc_info=False)

        time.sleep(cfg["sleep_between_coins"])

    # ── 5. Sort & output ─────────────────────────────────────────────────────
    results.sort(key=lambda x: (-x["score"], -x["vol_ratio"]))
    top = results[:cfg["top_n"]]

    elapsed = round(time.time() - start_ts, 1)
    log.info(
        f"\nTotal sinyal: {len(results)} | Approaching: {len(all_approaching)} "
        f"| Ditampilkan: {len(top)} | Waktu: {elapsed}s"
    )
    log.info(f"Skip stats: {dict(skip_stats)}")

    # ── Terminal table ────────────────────────────────────────────────────────
    print(build_terminal_table(top, caution))

    # ── Approaching zones (pre-alert, no Telegram) ────────────────────────────
    approaching_sorted = sorted(all_approaching, key=lambda x: x["dist_pct"])
    print(build_approaching_table(approaching_sorted))

    if not top:
        log.info("Tidak ada sinyal bounce saat ini.")
        return

    # ── Telegram ─────────────────────────────────────────────────────────────
    strong_top = [r for r in top if r["strength"] == "STRONG"]
    send_targets = strong_top if strong_top else top[:cfg["max_alerts"]]

    if send_targets:
        ok = send_telegram(build_telegram_summary(send_targets[:cfg["max_alerts"]], caution))
        if ok:
            log.info(f"📤 Telegram sent: {len(send_targets)} sinyal")
            for r in send_targets:
                set_cooldown(r["symbol"])
        else:
            log.warning("Telegram send FAILED")

    log.info(f"=== SELESAI — {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)
    run_scan()
