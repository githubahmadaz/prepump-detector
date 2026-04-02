#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v5.0 — COINALYZE INTEGRATION                          ║
║                                                                          ║
║  ARSITEKTUR:                                                             ║
║  ┌─────────────────┐   ┌────────────────────┐   ┌─────────────────┐    ║
║  │  BitgetClient   │   │ CoinalyzeClient     │   │  SymbolMapper   │    ║
║  │  · tickers      │   │ · ohlcv+btx+bv      │   │  Bitget ↔ CLZ  │    ║
║  │  · candles      │   │ · liquidations      │   │  auto-discover  │    ║
║  └────────┬────────┘   │ · open_interest     │   └────────┬────────┘    ║
║           │             │ · funding_rate      │            │             ║
║           └─────────────┴────────────────────┘            │             ║
║                         │                                  │             ║
║                   ┌─────▼──────────────────────────────────▼────┐       ║
║                   │              Scorer (5 komponen)              │       ║
║                   │  [A] Buy TX Z-score     — 30 pts (La Morgia) │       ║
║                   │  [B] Buy Volume Z-score — 30 pts (La Morgia) │       ║
║                   │  [C] Volume Z-score     — 20 pts (Fantazzini)│       ║
║                   │  [D] Short Liq Z-score  — 12 pts (squeeze)   │       ║
║                   │  [E] OI Change Z-score  —  8 pts (confirm)   │       ║
║                   └───────────────────────────────────────────────┘       ║
║                                                                          ║
║  DATA SOURCES:                                                           ║
║  · Bitget   : USDT-Futures tickers + 1H candles                          ║
║  · Coinalyze: btx, bv (La Morgia #1/#2) + liquidations + OI             ║
║                                                                          ║
║  RATE LIMIT:                                                             ║
║  · Coinalyze: 40 calls/min → 51 calls per scan → 1.3 menit              ║
║  · Budget hourly: 51 / 2400 = 2% — sangat aman untuk scan 1 jam sekali  ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import logging.handlers as _lh
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
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
_root = logging.getLogger(); _root.setLevel(logging.INFO)
_ch   = logging.StreamHandler(); _ch.setFormatter(_fmt); _root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/scanner_v5.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log   = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── ENVIRONMENT ────────────────────────────────────────────────────────
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":         os.getenv("BOT_TOKEN"),
    "chat_id":           os.getenv("CHAT_ID"),

    # ── VOLUME PRE-FILTER ──────────────────────────────────────────────────
    "pre_filter_vol":      100_000,    # $100K noise floor
    "min_vol_24h":         500_000,    # $500K minimum
    "max_vol_24h":     800_000_000,    # $800M ceiling
    "gate_chg_24h_max":       40.0,    # Coin naik >40% 24h = terlambat

    # ── DATA WINDOWS ───────────────────────────────────────────────────────
    "candle_limit_bitget":     200,    # Bitget: 200 candle 1H (~8 hari)
    "coinalyze_lookback_h":    168,    # Coinalyze: 7 hari history untuk baseline
    "coinalyze_interval":   "1hour",   # Interval Coinalyze

    # ── BASELINE & Z-SCORE WINDOWS ─────────────────────────────────────────
    "baseline_window":          24,    # 24 candle (1 hari) untuk rolling mean/std
    "baseline_min_samples":     15,    # Minimum data untuk Z-score valid

    # ── [A] BUY TRANSACTION Z-SCORE (La Morgia 2023 — feature #2) ─────────
    # btx = jumlah transaksi beli per candle (taker buy count)
    # Sumber: Coinalyze OHLCV field 'btx'
    "buy_tx_weight":            30,    # Max 30 poin
    "buy_tx_z_strong":         2.0,    # Z ≥ 2.0 → full score
    "buy_tx_z_medium":         1.0,    # Z ≥ 1.0 → partial score

    # ── [B] BUY VOLUME Z-SCORE (La Morgia 2023 — feature #1 proxy) ────────
    # bv/v = rasio taker buy volume (proxy rush orders)
    # Sumber: Coinalyze OHLCV fields 'bv', 'v'
    "buy_vol_weight":           30,    # Max 30 poin
    "buy_vol_z_strong":        2.0,
    "buy_vol_z_medium":        0.9,

    # ── [C] VOLUME Z-SCORE (Fantazzini 2023) ──────────────────────────────
    # volume 24h anomali vs rolling history
    # Sumber: Bitget candles (fallback jika Coinalyze unavailable)
    "volume_weight":            20,    # Max 20 poin
    "volume_z_strong":         2.5,
    "volume_z_medium":         1.5,

    # ── [D] SHORT LIQUIDATION Z-SCORE (short squeeze detector) ────────────
    # short_liq spike → posisi short di-force close → forced buy → harga naik
    # Sumber: Coinalyze liquidation history field 's'
    "short_liq_weight":         12,    # Max 12 poin
    "short_liq_z_strong":      2.0,
    "short_liq_z_medium":      1.0,

    # ── [E] OI CHANGE Z-SCORE (confirmation signal) ────────────────────────
    # OI naik + harga naik → posisi long baru dibuka → bullish
    # Sumber: Coinalyze open interest history
    "oi_change_weight":          8,    # Max 8 poin
    "oi_z_strong":             1.5,
    "oi_z_medium":             0.5,

    # ── MINIMUM ACTIVE COMPONENTS ──────────────────────────────────────────
    # Minimal 2 dari 5 komponen harus menghasilkan skor > 0
    # Mencegah sinyal dari satu anomali tunggal yang mungkin noise
    "min_active_components":     2,

    # ── SIGNAL THRESHOLDS ──────────────────────────────────────────────────
    "score_threshold":          55,    # Minimum skor untuk alert
    "score_strong":             72,    # "Strong" signal
    "score_very_strong":        88,    # "Very strong" signal

    # ── ENTRY CALCULATION ──────────────────────────────────────────────────
    "atr_period":               14,
    "atr_sl_mult":             1.5,
    "min_target_pct":          7.0,

    # ── OUTPUT ─────────────────────────────────────────────────────────────
    "max_alerts":                8,
    "alert_cooldown_sec":     3600,
    "sleep_between_coins":     0.3,    # Detik antara coin saat scoring
    "cooldown_file":  "/tmp/v5_cooldown.json",

    # ── COINALYZE RATE LIMIT ───────────────────────────────────────────────
    # 40 calls/minute → 1 call per 1.5 detik minimum
    "clz_min_interval_sec":    1.6,    # Sedikit lebih konservatif dari 1.5
    "clz_batch_size":           20,    # Max symbol per call (API limit)
    "clz_retry_attempts":        3,
    "clz_retry_wait_sec":        5,
}


# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST (324 coin)
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
#  📐  MATH UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def _mean(arr: list) -> float:
    return sum(arr) / len(arr) if arr else 0.0

def _std(arr: list) -> float:
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / len(arr))

def zscore(value: float, series: list, min_samples: int = 10) -> float:
    """Robust Z-score. Returns 0 if insufficient data or zero std."""
    if len(series) < min_samples:
        return 0.0
    sigma = _std(series)
    if sigma == 0:
        return 0.0
    return (value - _mean(series)) / sigma

def score_from_z(z: float, z_strong: float, z_medium: float, weight: int) -> int:
    """
    Linearly interpolate a score [0, weight] from a Z-score.
    z >= z_strong  → full weight
    z >= z_medium  → proportional between weight/2 and weight
    z >= 0         → proportional between 0 and weight/2
    z <  0         → 0
    """
    if z >= z_strong:
        return weight
    if z >= z_medium:
        ratio = (z - z_medium) / (z_strong - z_medium)
        return int(weight // 2 + ratio * (weight - weight // 2))
    if z >= 0:
        ratio = z / z_medium
        return int(ratio * weight // 2)
    return 0

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
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
log.info(f"Cooldown aktif: {len(_cooldown_state)} coin")

def is_on_cooldown(sym: str) -> bool:
    return (time.time() - _cooldown_state.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym: str) -> None:
    _cooldown_state[sym] = time.time()
    _save_cooldown(_cooldown_state)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT
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
                log.warning(f"Bitget HTTP error: {e.response.status_code}")
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        """Ambil semua ticker USDT-Futures dari Bitget."""
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 200) -> List[dict]:
        """Ambil candle 1H dari Bitget, cached per symbol."""
        cache_key = f"{symbol}:{limit}"
        if cache_key in cls._candle_cache:
            return cls._candle_cache[cache_key]

        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/candles",
            params={"symbol": symbol, "productType": "USDT-FUTURES",
                    "granularity": "1H", "limit": limit}
        )
        if not data or data.get("code") != "00000":
            return []

        candles = []
        for row in data.get("data", []):
            try:
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({
                    "ts":         int(row[0]),
                    "open":       float(row[1]),
                    "high":       float(row[2]),
                    "low":        float(row[3]),
                    "close":      float(row[4]),
                    "volume_usd": vol_usd,
                })
            except (IndexError, ValueError):
                continue
        candles.sort(key=lambda x: x["ts"])
        cls._candle_cache[cache_key] = candles
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"}
        )
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  📡  COINALYZE CLIENT (rate-limited, batched)
# ══════════════════════════════════════════════════════════════════════════════
class CoinalyzeClient:
    BASE     = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0
    _cache: Dict = {}

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _wait(self) -> None:
        """Enforce minimum interval between calls (rate limit compliance)."""
        elapsed = time.time() - CoinalyzeClient._last_call
        wait    = CONFIG["clz_min_interval_sec"] - elapsed
        if wait > 0:
            time.sleep(wait)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[list]:
        """Single API call with retry on 429."""
        params["api_key"] = self.api_key
        for attempt in range(CONFIG["clz_retry_attempts"]):
            self._wait()
            try:
                r = requests.get(f"{self.BASE}/{endpoint}", params=params, timeout=15)
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 10))
                    log.warning(f"Coinalyze rate limit — tunggu {retry_after}s")
                    time.sleep(retry_after + 1)
                    continue
                if r.status_code == 401:
                    log.error("Coinalyze API key invalid!")
                    return None
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if attempt < CONFIG["clz_retry_attempts"] - 1:
                    time.sleep(CONFIG["clz_retry_wait_sec"])
        return None

    def get_future_markets(self) -> List[dict]:
        """Daftar semua future markets di Coinalyze (untuk symbol mapping)."""
        cache_key = "future_markets"
        if cache_key in self._cache:
            return self._cache[cache_key]
        data = self._get("future-markets", {})
        result = data if isinstance(data, list) else []
        self._cache[cache_key] = result
        return result

    def _batch_fetch(self, endpoint: str, symbols: List[str],
                     extra_params: dict) -> Dict[str, list]:
        """
        Batch fetch endpoint untuk daftar symbol.
        Membagi menjadi batch ukuran clz_batch_size, menggabungkan hasilnya.
        Returns: {symbol: [candle_dicts]}
        """
        batch_size = CONFIG["clz_batch_size"]
        result: Dict[str, list] = {}

        for i in range(0, len(symbols), batch_size):
            batch   = symbols[i:i + batch_size]
            sym_str = ",".join(batch)
            params  = {"symbols": sym_str, **extra_params}
            data    = self._get(endpoint, params)

            if not isinstance(data, list):
                log.warning(f"Coinalyze {endpoint}: respons tidak valid untuk batch {i//batch_size + 1}")
                continue

            for item in data:
                sym     = item.get("symbol", "")
                history = item.get("history", [])
                if sym and history:
                    result[sym] = history

        return result

    def fetch_ohlcv_batch(self, symbols: List[str],
                          from_ts: int, to_ts: int) -> Dict[str, list]:
        """
        Fetch OHLCV + btx + bv untuk list symbol.
        Returns: {clz_symbol: [{t, o, h, l, c, v, bv, tx, btx}]}
        """
        extra = {
            "interval": CONFIG["coinalyze_interval"],
            "from":     from_ts,
            "to":       to_ts,
        }
        return self._batch_fetch("ohlcv-history", symbols, extra)

    def fetch_liquidations_batch(self, symbols: List[str],
                                 from_ts: int, to_ts: int) -> Dict[str, list]:
        """
        Fetch liquidation history untuk list symbol.
        Returns: {clz_symbol: [{t, l (long_liq), s (short_liq)}]}
        """
        extra = {
            "interval":       CONFIG["coinalyze_interval"],
            "from":           from_ts,
            "to":             to_ts,
            "convert_to_usd": "true",
        }
        return self._batch_fetch("liquidation-history", symbols, extra)

    def fetch_oi_batch(self, symbols: List[str],
                       from_ts: int, to_ts: int) -> Dict[str, list]:
        """
        Fetch OI history untuk list symbol.
        Returns: {clz_symbol: [{t, o, h, l, c}]}
        """
        extra = {
            "interval":       CONFIG["coinalyze_interval"],
            "from":           from_ts,
            "to":             to_ts,
            "convert_to_usd": "true",
        }
        return self._batch_fetch("open-interest-history", symbols, extra)

    def clear_cache(self) -> None:
        self._cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  🗺️  SYMBOL MAPPER — Bitget ↔ Coinalyze
# ══════════════════════════════════════════════════════════════════════════════
class SymbolMapper:
    """
    Membangun mapping antara symbol Bitget (e.g. BTCUSDT) dan
    symbol Coinalyze (e.g. BTCUSDT_PERP.7).

    Coinalyze menggunakan format: {BASE}{QUOTE}_PERP.{exchange_code}
    Exchange code berbeda untuk setiap bursa.

    Strategy: Pada startup, fetch semua future markets dari Coinalyze,
    filter untuk Bitget, buat mapping otomatis.
    """
    def __init__(self, clz_client: CoinalyzeClient):
        self._client      = clz_client
        self._to_clz:  Dict[str, str] = {}   # bitget_sym → clz_sym
        self._has_btx: Dict[str, bool] = {}  # clz_sym → has buy/sell data
        self._loaded      = False

    def load(self) -> int:
        """
        Fetch dan build mapping. Kembalikan jumlah coin yang berhasil dimapping.
        Dipanggil sekali saat startup.
        """
        log.info("SymbolMapper: fetching Coinalyze future markets...")
        markets = self._client.get_future_markets()

        if not markets:
            log.error("SymbolMapper: gagal fetch markets dari Coinalyze!")
            return 0

        # Filter Bitget markets
        bitget_markets = [m for m in markets
                          if "bitget" in m.get("exchange", "").lower()]

        if not bitget_markets:
            # Fallback: coba cari dengan exchange code
            log.warning("SymbolMapper: tidak ada market berlabel 'bitget', "
                        "mencoba semua markets...")
            bitget_markets = markets

        log.info(f"SymbolMapper: {len(bitget_markets)} Bitget markets ditemukan")

        # Build mapping: symbol_on_exchange → coinalyze symbol
        # Bitget perps biasanya terdaftar sebagai "BTCUSDT" atau "BTCUSDT_UMCBL"
        mapped = 0
        for m in bitget_markets:
            clz_sym  = m.get("symbol", "")
            exch_sym = m.get("symbol_on_exchange", "")
            has_btx  = m.get("has_buy_sell_data", False)

            if not clz_sym:
                continue

            # Try exact match
            clean = exch_sym.replace("_UMCBL", "").replace("_DMCBL", "").upper()

            if clean in WHITELIST_SYMBOLS:
                self._to_clz[clean]  = clz_sym
                self._has_btx[clz_sym] = has_btx
                mapped += 1
            elif exch_sym.upper() in WHITELIST_SYMBOLS:
                self._to_clz[exch_sym.upper()] = clz_sym
                self._has_btx[clz_sym] = has_btx
                mapped += 1

        log.info(f"SymbolMapper: {mapped}/{len(WHITELIST_SYMBOLS)} coin berhasil dimapping")

        # Fallback mapping untuk coin yang tidak ditemukan
        # Gunakan format standar: XYZUSDT → XYZUSDT_PERP.{suffix}
        unmapped = [s for s in WHITELIST_SYMBOLS if s not in self._to_clz]
        if unmapped and bitget_markets:
            # Cari exchange code dari coin yang sudah ter-map
            sample_clz = list(self._to_clz.values())[0] if self._to_clz else ""
            suffix = sample_clz.split(".")[-1] if "." in sample_clz else ""
            if suffix:
                for sym in unmapped:
                    self._to_clz[sym] = f"{sym}_PERP.{suffix}"
                log.info(f"SymbolMapper: {len(unmapped)} coin menggunakan fallback suffix .{suffix}")

        self._loaded = True
        return mapped

    def to_coinalyze(self, bitget_sym: str) -> Optional[str]:
        return self._to_clz.get(bitget_sym)

    def has_buy_sell(self, clz_sym: str) -> bool:
        return self._has_btx.get(clz_sym, True)  # Default true: try anyway

    def get_clz_symbols_for(self, bitget_syms: List[str]) -> List[str]:
        """Convert list Bitget symbols ke Coinalyze symbols (skip yang tidak ada mapping)."""
        result = []
        for s in bitget_syms:
            clz = self.to_coinalyze(s)
            if clz:
                result.append(clz)
        return result

    def reverse(self, clz_sym: str) -> Optional[str]:
        """Coinalyze symbol → Bitget symbol."""
        rev = {v: k for k, v in self._to_clz.items()}
        return rev.get(clz_sym)

    @property
    def is_loaded(self) -> bool:
        return self._loaded


# ══════════════════════════════════════════════════════════════════════════════
#  📦  COIN DATA CONTAINER
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class CoinData:
    """Semua data yang dibutuhkan untuk scoring satu coin."""
    symbol:    str
    price:     float
    vol_24h:   float
    chg_24h:   float
    funding:   float
    candles:   List[dict] = field(default_factory=list)   # Bitget 1H OHLCV
    clz_ohlcv: List[dict] = field(default_factory=list)   # Coinalyze OHLCV+btx+bv
    clz_liq:   List[dict] = field(default_factory=list)   # Coinalyze liquidations
    clz_oi:    List[dict] = field(default_factory=list)   # Coinalyze OI

    @property
    def has_btx_data(self) -> bool:
        return bool(self.clz_ohlcv) and "btx" in (self.clz_ohlcv[-1] if self.clz_ohlcv else {})

    @property
    def has_liq_data(self) -> bool:
        return bool(self.clz_liq)

    @property
    def has_oi_data(self) -> bool:
        return bool(self.clz_oi)


# ══════════════════════════════════════════════════════════════════════════════
#  🔬  SCORING COMPONENTS
# ══════════════════════════════════════════════════════════════════════════════

def score_buy_tx(data: CoinData) -> Tuple[int, float, dict]:
    """
    [A] Buy Transaction Z-score — La Morgia 2023, feature importance #2
    btx = jumlah transaksi beli per candle (taker buy count)
    btx_ratio = btx / tx = proporsi transaksi yang merupakan pembelian agresif

    Kenaikan btx_ratio & btx_raw SEBELUM pump = rush orders mulai masuk.
    """
    cfg    = CONFIG
    weight = cfg["buy_tx_weight"]

    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "no btx data", "source": "coinalyze_missing"}

    # Ambil field btx dan tx (gunakan candle -2 untuk confirmed, bukan live)
    candles = data.clz_ohlcv
    cur     = candles[-2] if len(candles) >= 2 else candles[-1]
    btx     = cur.get("btx", 0)
    tx      = cur.get("tx", 0)

    if not btx or not tx:
        # Fallback: gunakan buy_pressure proxy dari OHLCV Bitget
        return _score_buy_tx_fallback(data), 0.0, {"reason": "btx=0, fallback used"}

    btx_ratio   = btx / tx if tx > 0 else 0.5
    btx_raw     = btx

    # Build baseline
    win_start = max(0, len(candles) - cfg["baseline_window"] * 4)
    win_end   = max(0, len(candles) - cfg["baseline_window"])
    baseline  = candles[win_start:win_end]

    baseline_ratios = []
    baseline_raws   = []
    for c in baseline:
        c_tx  = c.get("tx", 0)
        c_btx = c.get("btx", 0)
        if c_tx > 0 and c_btx > 0:
            baseline_ratios.append(c_btx / c_tx)
            baseline_raws.append(float(c_btx))

    if len(baseline_ratios) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient baseline for btx"}

    z_ratio = zscore(btx_ratio, baseline_ratios)
    z_raw   = zscore(btx_raw,   baseline_raws)
    z_use   = max(z_ratio, z_raw * 0.7)   # Kombinasi: ratio lebih penting

    score = score_from_z(z_use, cfg["buy_tx_z_strong"], cfg["buy_tx_z_medium"], weight)

    return score, round(z_use, 2), {
        "btx_ratio":    round(btx_ratio, 3),
        "btx_raw":      btx_raw,
        "z_ratio":      round(z_ratio, 2),
        "z_raw":        round(z_raw, 2),
    }


def _score_buy_tx_fallback(data: CoinData) -> int:
    """Fallback ke buy pressure proxy jika btx tidak tersedia."""
    if not data.candles or len(data.candles) < 20:
        return 0
    candles = data.candles
    cur     = candles[-2]
    rng     = cur["high"] - cur["low"]
    if rng <= 0:
        return 0
    bp      = (cur["close"] - cur["low"]) / rng
    # Simple threshold: bp > 0.65 = buyers agresif
    if bp > 0.75: return CONFIG["buy_tx_weight"] // 2
    if bp > 0.55: return CONFIG["buy_tx_weight"] // 4
    return 0


def score_buy_volume(data: CoinData) -> Tuple[int, float, dict]:
    """
    [B] Buy Volume Z-score — La Morgia 2023, feature importance #1 (rush orders)
    bv   = taker buy volume per candle (USD)
    bv/v = proporsi volume yang berasal dari pembelian agresif

    Anomali pada bv/v = buyers sangat agresif = potensi awal pump.
    """
    cfg    = CONFIG
    weight = cfg["buy_vol_weight"]

    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "no bv data"}

    candles = data.clz_ohlcv
    cur     = candles[-2] if len(candles) >= 2 else candles[-1]
    bv      = cur.get("bv", 0)
    v       = cur.get("v",  0)

    if not bv or not v:
        return 0, 0.0, {"reason": "bv=0"}

    bv_ratio = bv / v if v > 0 else 0.5

    win_start = max(0, len(candles) - cfg["baseline_window"] * 4)
    win_end   = max(0, len(candles) - cfg["baseline_window"])
    baseline  = candles[win_start:win_end]

    baseline_ratios = []
    for c in baseline:
        c_v  = c.get("v",  0)
        c_bv = c.get("bv", 0)
        if c_v > 0 and c_bv >= 0:
            baseline_ratios.append(c_bv / c_v)

    if len(baseline_ratios) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient baseline for bv"}

    z_ratio = zscore(bv_ratio, baseline_ratios)

    # Bonus: jika bv_ratio > 0.65 (lebih dari 65% volume adalah taker buy) → sangat bullish
    if bv_ratio > 0.65:
        z_ratio += 0.5

    score = score_from_z(z_ratio, cfg["buy_vol_z_strong"], cfg["buy_vol_z_medium"], weight)

    return score, round(z_ratio, 2), {
        "bv_ratio":   round(bv_ratio, 3),
        "bv_usd":     round(bv),
        "v_usd":      round(v),
        "z":          round(z_ratio, 2),
    }


def score_volume(data: CoinData) -> Tuple[int, float, dict]:
    """
    [C] Volume Z-score — Fantazzini 2023
    Anomali volume total vs baseline rolling.
    Sumber data: Bitget candles (selalu tersedia).
    """
    cfg     = CONFIG
    weight  = cfg["volume_weight"]
    candles = data.candles

    if len(candles) < cfg["baseline_min_samples"] + 10:
        return 0, 0.0, {"reason": "insufficient bitget candles"}

    cur_vol  = candles[-2]["volume_usd"]   # confirmed candle

    win_end   = max(0, len(candles) - cfg["baseline_window"])
    win_start = max(0, win_end - cfg["baseline_window"] * 4)
    baseline  = [c["volume_usd"] for c in candles[win_start:win_end]]

    if len(baseline) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient baseline for volume"}

    z = zscore(cur_vol, baseline)
    z_recent_avg = zscore(
        _mean([c["volume_usd"] for c in candles[-cfg["baseline_window"]:-1]]),
        baseline
    )
    z_use = max(z, z_recent_avg * 0.8)

    score = score_from_z(z_use, cfg["volume_z_strong"], cfg["volume_z_medium"], weight)
    vol_ratio = cur_vol / _mean(baseline) if _mean(baseline) > 0 else 1.0

    return score, round(z_use, 2), {
        "cur_vol":   round(cur_vol),
        "z":         round(z_use, 2),
        "vol_ratio": round(vol_ratio, 2),
    }


def score_short_liquidations(data: CoinData) -> Tuple[int, float, dict]:
    """
    [D] Short Liquidation Z-score — futures-specific short squeeze detector
    short_liq spike → posisi short di-force close → forced buying → harga naik

    Sumber: Coinalyze liquidation history, field 's' (short liquidations USD).
    """
    cfg    = CONFIG
    weight = cfg["short_liq_weight"]

    if not data.has_liq_data or len(data.clz_liq) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "no liquidation data"}

    liqs = data.clz_liq
    cur  = liqs[-2] if len(liqs) >= 2 else liqs[-1]
    short_liq = cur.get("s", 0) or 0   # Short liquidations in USD

    win_end   = max(0, len(liqs) - cfg["baseline_window"])
    win_start = max(0, win_end - cfg["baseline_window"] * 4)
    baseline  = [c.get("s", 0) or 0 for c in liqs[win_start:win_end]]

    # Jika semua baseline = 0, tidak ada liquidation normal → tidak informatif
    nonzero = [x for x in baseline if x > 0]
    if len(nonzero) < 5:
        return 0, 0.0, {"reason": "too many zero liquidations in baseline"}

    z = zscore(short_liq, baseline)
    score = score_from_z(z, cfg["short_liq_z_strong"], cfg["short_liq_z_medium"], weight)

    return score, round(z, 2), {
        "short_liq_usd": round(short_liq),
        "z":             round(z, 2),
    }


def score_oi_change(data: CoinData) -> Tuple[int, float, dict]:
    """
    [E] Open Interest Change Z-score — confirmation signal
    OI naik = posisi baru dibuka (bukan sekadar covering)
    Rising OI + price rally = bullish positioning

    Sumber: Coinalyze OI history (OHLC per candle), field 'c' (close).
    """
    cfg    = CONFIG
    weight = cfg["oi_change_weight"]

    if not data.has_oi_data or len(data.clz_oi) < cfg["baseline_min_samples"] + 2:
        return 0, 0.0, {"reason": "no OI data"}

    oi = data.clz_oi
    cur_oi  = oi[-2].get("c", 0) or 0
    prev_oi = oi[-3].get("c", 0) or 0 if len(oi) >= 3 else 0

    if prev_oi == 0:
        return 0, 0.0, {"reason": "prev_oi=0"}

    oi_change_pct = (cur_oi - prev_oi) / prev_oi

    win_end   = max(0, len(oi) - cfg["baseline_window"])
    win_start = max(0, win_end - cfg["baseline_window"] * 4)
    baseline_oi = [oi[i].get("c", 0) or 0 for i in range(win_start, win_end)]

    # Baseline changes
    baseline_changes = []
    for i in range(1, len(baseline_oi)):
        if baseline_oi[i - 1] > 0:
            baseline_changes.append((baseline_oi[i] - baseline_oi[i - 1]) / baseline_oi[i - 1])

    if len(baseline_changes) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient OI baseline changes"}

    z = zscore(oi_change_pct, baseline_changes)
    score = score_from_z(z, cfg["oi_z_strong"], cfg["oi_z_medium"], weight)

    return score, round(z, 2), {
        "oi_change_pct": round(oi_change_pct * 100, 2),
        "z":             round(z, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(data: CoinData) -> Optional[dict]:
    candles = data.candles
    if len(candles) < 20:
        return None

    price = data.price
    trs   = [
        max(c["high"] - c["low"],
            abs(c["high"] - candles[i - 1]["close"]),
            abs(c["low"]  - candles[i - 1]["close"]))
        for i, c in enumerate(candles[-15:], 1) if i < 15
    ]
    atr = _mean(trs) if trs else price * 0.02

    entry   = price
    sl      = entry - atr * CONFIG["atr_sl_mult"]
    sl_pct  = round((entry - sl) / entry * 100, 1)

    t1 = max(entry * (1 + CONFIG["min_target_pct"] / 100), entry + atr * 3)
    t2 = max(entry * 1.20, entry + atr * 6)
    t1_pct = round((t1 - entry) / entry * 100, 1)
    t2_pct = round((t2 - entry) / entry * 100, 1)
    rr     = round((t1 - entry) / (entry - sl), 2) if (entry - sl) > 0 else 0.0

    return {
        "entry":  round(entry, 8),
        "sl":     round(sl, 8),
        "sl_pct": sl_pct,
        "t1":     round(t1, 8),
        "t2":     round(t2, 8),
        "t1_pct": t1_pct,
        "t2_pct": t2_pct,
        "rr":     rr,
        "atr":    round(atr, 8),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  MASTER SCORER
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ScoreResult:
    symbol:     str
    score:      int
    confidence: str
    components: dict
    entry:      Optional[dict]
    price:      float
    vol_24h:    float
    chg_24h:    float
    funding:    float
    urgency:    str
    data_quality: dict


def score_coin(data: CoinData) -> Optional[ScoreResult]:
    """
    Jalankan 5 komponen scoring, gabungkan, return ScoreResult atau None.
    """
    cfg = CONFIG

    # ── Volume pre-check ────────────────────────────────────────────────────
    if data.vol_24h < cfg["min_vol_24h"]:
        return None
    if data.chg_24h > cfg["gate_chg_24h_max"]:
        return None

    # ── Run 5 components ────────────────────────────────────────────────────
    a_score, a_z, a_d = score_buy_tx(data)
    b_score, b_z, b_d = score_buy_volume(data)
    c_score, c_z, c_d = score_volume(data)
    d_score, d_z, d_d = score_short_liquidations(data)
    e_score, e_z, e_d = score_oi_change(data)

    total = a_score + b_score + c_score + d_score + e_score

    # ── Minimum active components filter ────────────────────────────────────
    active = sum([a_score > 3, b_score > 3, c_score > 3, d_score > 2, e_score > 1])
    if active < cfg["min_active_components"]:
        return None

    if total < cfg["score_threshold"]:
        return None

    # ── Confidence ──────────────────────────────────────────────────────────
    if total >= cfg["score_very_strong"]:
        confidence = "very_strong"
    elif total >= cfg["score_strong"]:
        confidence = "strong"
    else:
        confidence = "watch"

    # ── Data quality info ────────────────────────────────────────────────────
    dq = {
        "has_btx":    data.has_btx_data,
        "has_liq":    data.has_liq_data,
        "has_oi":     data.has_oi_data,
        "candles":    len(data.candles),
        "clz_ohlcv":  len(data.clz_ohlcv),
    }

    # ── Urgency ─────────────────────────────────────────────────────────────
    liq_str = f"${d_d.get('short_liq_usd', 0)/1e3:.0f}K liq" if d_score > 4 else ""
    if a_z >= 2.0 and b_z >= 1.5:
        urgency = f"🔴 TINGGI — BuyTX + BuyVol sama-sama anomali {liq_str}"
    elif d_score >= 8:
        urgency = f"🔴 TINGGI — Short squeeze aktif {liq_str}"
    elif a_z >= 1.5 or b_z >= 1.5:
        urgency = "🟠 SEDANG — Buy pressure meningkat"
    elif c_z >= 2.0:
        urgency = "🟡 SEDANG — Volume anomali"
    else:
        urgency = "⚪ WATCH — Akumulasi awal"

    return ScoreResult(
        symbol      = data.symbol,
        score       = total,
        confidence  = confidence,
        components  = {
            "A_buy_tx":    {"score": a_score, "z": a_z, "details": a_d},
            "B_buy_vol":   {"score": b_score, "z": b_z, "details": b_d},
            "C_volume":    {"score": c_score, "z": c_z, "details": c_d},
            "D_short_liq": {"score": d_score, "z": d_z, "details": d_d},
            "E_oi_change": {"score": e_score, "z": e_z, "details": e_d},
        },
        entry        = calc_entry_targets(data),
        price        = data.price,
        vol_24h      = data.vol_24h,
        chg_24h      = data.chg_24h,
        funding      = data.funding,
        urgency      = urgency,
        data_quality = dq,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def _conf_emoji(conf: str) -> str:
    return {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(conf, "⚪")

def _dq_badge(dq: dict) -> str:
    parts = []
    if dq.get("has_btx"):  parts.append("btx✓")
    if dq.get("has_liq"):  parts.append("liq✓")
    if dq.get("has_oi"):   parts.append("oi✓")
    return " ".join(parts) if parts else "basic"

def build_alert(r: ScoreResult, rank: int) -> str:
    e     = r.entry
    vol_s = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    bar   = "█" * min(20, r.score // 5) + "░" * max(0, 20 - r.score // 5)
    dq    = _dq_badge(r.data_quality)
    comp  = r.components

    entry_line = ""
    if e:
        entry_line = (
            f"\n   Entry: <b>${e['entry']:.6g}</b> | SL: ${e['sl']:.6g} (-{e['sl_pct']}%)"
            f"\n   T1: +{e['t1_pct']}% | T2: +{e['t2_pct']}% | R/R: {e['rr']}"
        )

    a = comp["A_buy_tx"];  b = comp["B_buy_vol"]
    c = comp["C_volume"];  d = comp["D_short_liq"]
    ee = comp["E_oi_change"]

    return (
        f"#{rank} {_conf_emoji(r.confidence)} <b>{r.symbol}</b>  "
        f"Score: <b>{r.score}/100</b>  [{dq}]\n"
        f"   {bar}\n"
        f"   {r.urgency}\n"
        f"   Vol:{vol_s} | Δ24h:{r.chg_24h:+.1f}% | F:{r.funding:.5f}\n"
        f"   [A]BuyTX:{a['score']}({a['z']:+.1f}σ) "
        f"[B]BuyVol:{b['score']}({b['z']:+.1f}σ) "
        f"[C]Vol:{c['score']}({c['z']:+.1f}σ)\n"
        f"   [D]ShortLiq:{d['score']}({d['z']:+.1f}σ) "
        f"[E]OI:{ee['score']}({ee['z']:+.1f}σ)"
        f"{entry_line}\n"
    )

def build_summary(results: List[ScoreResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = f"🔍 <b>PRE-PUMP SCANNER v5.0</b> — {now}\n"
    msg += f"📡 Data: Bitget + Coinalyze (btx/bv/liq/OI)\n"
    msg += f"📊 {len(results)} sinyal terdeteksi\n\n"
    for i, r in enumerate(results, 1):
        e    = r.entry
        t1   = f"+{e['t1_pct']}%" if e else "?"
        comp = r.components
        msg += (
            f"{i}. <b>{r.symbol}</b> [{r.score}pts] "
            f"A:{comp['A_buy_tx']['score']} B:{comp['B_buy_vol']['score']} "
            f"C:{comp['C_volume']['score']} D:{comp['D_short_liq']['score']} "
            f"→ T1:{t1}\n"
        )
    return msg

def send_telegram(msg: str) -> bool:
    bot_token = CONFIG["bot_token"]
    chat_id   = CONFIG["chat_id"]
    if not bot_token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    start_ts = time.time()
    log.info(f"{'='*70}")
    log.info(f"  PRE-PUMP SCANNER v5.0 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"{'='*70}")

    # ── Init clients ────────────────────────────────────────────────────────
    clz_client = CoinalyzeClient(CONFIG["coinalyze_api_key"])
    mapper     = SymbolMapper(clz_client)

    mapped_count = mapper.load()
    if mapped_count == 0:
        log.warning("SymbolMapper gagal: akan menggunakan hanya data Bitget")

    # ── Fetch Bitget tickers ─────────────────────────────────────────────────
    log.info("Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner v5: Gagal fetch Bitget tickers")
        return
    log.info(f"Bitget tickers: {len(tickers)}")

    # ── Build candidate list ─────────────────────────────────────────────────
    candidates = []
    skip_stats = defaultdict(int)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:             skip_stats["excluded"]+= 1;continue
        if is_on_cooldown(sym):               skip_stats["cooldown"]+= 1;continue
        if sym not in tickers:                skip_stats["not_found"]+= 1;continue
        t = tickers[sym]
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = float(t.get("change24h",   0)) * 100
        except Exception:
            skip_stats["parse_error"] += 1; continue
        if vol < CONFIG["pre_filter_vol"]:    skip_stats["vol_low"]+= 1;continue
        if vol > CONFIG["max_vol_24h"]:       skip_stats["vol_high"]+= 1;continue
        if chg > CONFIG["gate_chg_24h_max"]: skip_stats["pumped"]+= 1;continue
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    # ── Coinalyze bulk fetch ─────────────────────────────────────────────────
    now_ts       = int(time.time())
    from_ts      = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    cand_syms    = [sym for sym, _ in candidates]
    clz_syms     = mapper.get_clz_symbols_for(cand_syms)

    log.info(f"Fetching Coinalyze data untuk {len(clz_syms)} coin...")

    if clz_syms:
        log.info("  → OHLCV+btx+bv...")
        clz_ohlcv_all = clz_client.fetch_ohlcv_batch(clz_syms, from_ts, now_ts)
        log.info(f"  → OHLCV received: {len(clz_ohlcv_all)} symbols")

        log.info("  → Liquidations...")
        clz_liq_all = clz_client.fetch_liquidations_batch(clz_syms, from_ts, now_ts)
        log.info(f"  → Liq received: {len(clz_liq_all)} symbols")

        log.info("  → Open Interest...")
        clz_oi_all = clz_client.fetch_oi_batch(clz_syms, from_ts, now_ts)
        log.info(f"  → OI received: {len(clz_oi_all)} symbols")
    else:
        clz_ohlcv_all = clz_liq_all = clz_oi_all = {}
        log.warning("Tidak ada Coinalyze symbols — menggunakan fallback Bitget only")

    # ── Score each coin ──────────────────────────────────────────────────────
    results: List[ScoreResult] = []
    BitgetClient.clear_cache()

    for i, (sym, ticker) in enumerate(candidates):
        log.info(f"[{i+1}/{len(candidates)}] {sym}")
        try:
            # Fetch Bitget candles + funding
            candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
            if len(candles) < 60:
                log.debug(f"  Skip {sym}: candles kurang ({len(candles)})")
                continue

            price   = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            chg_24h = float(ticker.get("change24h",   0)) * 100
            funding = BitgetClient.get_funding(sym)

            if price <= 0:
                continue

            # Get Coinalyze data for this coin
            clz_sym  = mapper.to_coinalyze(sym)
            ohlcv_c  = clz_ohlcv_all.get(clz_sym, []) if clz_sym else []
            liq_c    = clz_liq_all.get(clz_sym, [])   if clz_sym else []
            oi_c     = clz_oi_all.get(clz_sym, [])     if clz_sym else []

            coin_data = CoinData(
                symbol    = sym,
                price     = price,
                vol_24h   = vol_24h,
                chg_24h   = chg_24h,
                funding   = funding,
                candles   = candles,
                clz_ohlcv = ohlcv_c,
                clz_liq   = liq_c,
                clz_oi    = oi_c,
            )

            result = score_coin(coin_data)
            if result:
                results.append(result)
                log.info(
                    f"  ✅ Score={result.score} ({result.confidence}) | "
                    f"A:{result.components['A_buy_tx']['score']} "
                    f"B:{result.components['B_buy_vol']['score']} "
                    f"C:{result.components['C_volume']['score']} "
                    f"D:{result.components['D_short_liq']['score']} "
                    f"E:{result.components['E_oi_change']['score']}"
                )

        except Exception as exc:
            log.warning(f"  Error {sym}: {exc}")

        time.sleep(CONFIG["sleep_between_coins"])

    # ── Sort & send ──────────────────────────────────────────────────────────
    results.sort(key=lambda x: x.score, reverse=True)
    top = results[:CONFIG["max_alerts"]]

    elapsed = round(time.time() - start_ts, 1)
    log.info(f"\nTotal sinyal: {len(results)} | Dikirim: {len(top)} | Waktu: {elapsed}s")

    if not top:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    send_telegram(build_summary(top))
    time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank))
        if ok:
            set_cooldown(r.symbol)
            log.info(f"📤 Alert #{rank}: {r.symbol} score={r.score}")
        time.sleep(2)

    log.info(f"=== SELESAI — {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)
    run_scan()
