#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NEXUS-SR v2.3 — Research-Validated Support Zone Bounce Scanner             ║
║                                                                              ║
║  PERBAIKAN:                                                                  ║
║  · Endpoint Long/Short sekarang menggunakan suffix _UMCBL (fix 404)         ║
║  · Logging score components untuk debugging                                 ║
║  · Error handling lebih robust                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
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

# ── Logging ───────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch   = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/nexus_sr_v2.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG  — semua parameter di sini, zero hardcode di kode
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── CREDENTIALS ────────────────────────────────────────────────────────
    "bot_token":           os.getenv("BOT_TOKEN"),
    "chat_id":             os.getenv("CHAT_ID"),
    "coinalyze_api_key":   os.getenv("COINALYZE_API_KEY",
                                     "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),

    # ── UNIVERSE FILTER ─────────────────────────────────────────────────────
    "min_vol_24h":         50_000,      # $50K/day floor absolut
    "max_vol_24h":      2_000_000_000,  # $2B ceiling
    "gate_chg_24h_max":       60.0,     # skip jika SUDAH pump >60%

    # ── PINE SCRIPT PARAMETERS ──────────────────────────────────────────────
    "lookback_period":          20,     # pivot kiri & kanan
    "vol_len":                   2,     # delta vol filter window
    "box_width_multiplier":    1.0,     # ATR × ini = zone height
    "candle_limit_1h":         200,     # 1H candles per coin (~8 hari)
    "candle_limit_1w":          60,     # 1W candles untuk BTC/ETH regime check

    # ── INDICATORS ──────────────────────────────────────────────────────────
    "atr_period":              200,
    "ema_weekly_period":        50,

    # ── SCORING — komponen independen (total 100 pts) ──────────────────────

    # A. Volume Z-score (0-30 pts)
    "score_vol_max":            30,
    "vol_baseline_window":      24,
    "vol_z_strong":            2.0,
    "vol_z_medium":            1.0,

    # B. Taker Buy Z-score (0-25 pts)
    "score_btx_max":            25,
    "btx_z_strong":            2.0,
    "btx_z_medium":            1.0,
    "btx_baseline_window":      24,

    # C. Funding Rate (0-20 pts)
    "score_fund_max":           20,
    "fund_strongly_neg":    -0.0005,
    "fund_mod_neg":         -0.0001,
    "fund_neutral_hi":       0.0001,

    # D. OI Change Direction (0-15 pts)
    "score_oi_max":             15,
    "oi_significant_increase":  0.03,
    "oi_moderate_increase":     0.01,

    # E. Long/Short Ratio (0-10 pts)
    "score_ls_max":             10,
    "ls_strongly_short":       0.4,
    "ls_mod_short":            0.6,
    "ls_neutral":              0.8,

    # ── THRESHOLDS ─────────────────────────────────────────────────────────
    "score_threshold_normal":   55,
    "score_threshold_caution":  70,
    "score_strong":             75,

    # ── GATES (binary pass/fail) ───────────────────────────────────────────
    "max_break_count":           3,
    "vol_outlier_mult":         5.0,
    "vol_outlier_lookback":     20,

    # ── COINALYZE ───────────────────────────────────────────────────────────
    "clz_interval":         "1hour",
    "clz_lookback_h":          168,
    "clz_min_interval_sec":    1.6,
    "clz_batch_size":           20,
    "clz_retry":                 3,
    "clz_retry_wait":            5,

    # ── OUTPUT ─────────────────────────────────────────────────────────────
    "top_n":                    10,
    "max_alerts":                5,
    "alert_cooldown_sec":     3600,
    "cooldown_file":  "/tmp/nexus_sr_v2_state.json",
    "sleep_between_coins":     0.2,
}

MANUAL_EXCLUDE: set = set()


# ══════════════════════════════════════════════════════════════════════════════
#  💾  STATE FILE  (cooldown + previous OI)
# ══════════════════════════════════════════════════════════════════════════════
def _load_state() -> dict:
    try:
        path = CONFIG["cooldown_file"]
        if os.path.exists(path):
            with open(path) as f:
                state = json.load(f)
            now = time.time()
            state["cooldowns"] = {
                k: v for k, v in state.get("cooldowns", {}).items()
                if now - v < CONFIG["alert_cooldown_sec"]
            }
            return state
    except Exception:
        pass
    return {"cooldowns": {}, "prev_oi": {}}

def _save_state(state: dict) -> None:
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_state = _load_state()
log.info(f"State loaded: {len(_state['cooldowns'])} cooldowns, "
         f"{len(_state['prev_oi'])} OI records")

def is_on_cooldown(sym: str) -> bool:
    return (time.time() - _state["cooldowns"].get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym: str) -> None:
    _state["cooldowns"][sym] = time.time()
    _save_state(_state)

def get_prev_oi(sym: str) -> float:
    return float(_state["prev_oi"].get(sym, 0.0))

def set_prev_oi(sym: str, oi: float) -> None:
    _state["prev_oi"][sym] = oi


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
    if len(series) < min_samples:
        return 0.0
    sigma = _std(series)
    if sigma == 0:
        return 0.0
    return (value - _mean(series)) / sigma

def score_from_z(z: float, z_strong: float, z_medium: float, weight: int) -> int:
    if z >= z_strong:
        return weight
    if z >= z_medium:
        ratio = (z - z_medium) / (z_strong - z_medium)
        return int(weight // 2 + ratio * (weight - weight // 2))
    if z >= 0:
        ratio = z / z_medium if z_medium > 0 else 0
        return int(ratio * weight // 2)
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT (dengan perbaikan Long/Short suffix)
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
                log.warning(f"Bitget HTTP {e.response.status_code} for {url}")
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, granularity: str = "1H",
                    limit: int = 200) -> List[dict]:
        cache_key = f"{symbol}:{granularity}:{limit}"
        if cache_key in cls._candle_cache:
            return cls._candle_cache[cache_key]

        def _parse(raw):
            out = []
            for row in raw:
                try:
                    vol = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                    out.append({
                        "ts":    int(row[0]),
                        "open":  float(row[1]),
                        "high":  float(row[2]),
                        "low":   float(row[3]),
                        "close": float(row[4]),
                        "vol":   vol,
                    })
                except (IndexError, ValueError):
                    continue
            return out

        if limit <= 200:
            data = cls._get(
                f"{cls.BASE}/api/v2/mix/market/candles",
                params={"symbol": symbol, "productType": "USDT-FUTURES",
                        "granularity": granularity, "limit": limit}
            )
            if not data or data.get("code") != "00000":
                return []
            candles = sorted(_parse(data.get("data", [])), key=lambda x: x["ts"])
            cls._candle_cache[cache_key] = candles
            return candles

        collected: Dict[int, dict] = {}
        end_time = None
        for page in range(math.ceil(limit / 200)):
            params = {"symbol": symbol, "productType": "USDT-FUTURES",
                      "granularity": granularity, "limit": 200}
            if end_time is not None:
                params["endTime"] = str(end_time)
            data = cls._get(f"{cls.BASE}/api/v2/mix/market/candles", params=params)
            if not data or data.get("code") != "00000":
                break
            raw = data.get("data", [])
            if not raw:
                break
            for c in _parse(raw):
                collected[c["ts"]] = c
            if len(raw) < 200:
                break
            end_time = min(c["ts"] for c in _parse(raw)) - 1
            if len(collected) >= limit:
                break
            time.sleep(0.15)

        result = sorted(collected.values(), key=lambda x: x["ts"])[-limit:]
        cls._candle_cache[cache_key] = result
        return result

    @classmethod
    def get_long_short_ratio(cls, symbol: str) -> Optional[float]:
        """
        Endpoint long-short Bitget membutuhkan suffix _UMCBL untuk USDT-M futures.
        Contoh: BTCUSDT → BTCUSDT_UMCBL
        """
        # Tambahkan suffix jika belum ada
        if not symbol.endswith("_UMCBL"):
            ls_symbol = f"{symbol}_UMCBL"
        else:
            ls_symbol = symbol

        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/long-short",
            params={"symbol": ls_symbol, "productType": "USDT-FUTURES",
                    "period": "1h"}
        )
        if not data or data.get("code") != "00000":
            return None
        items = data.get("data", [])
        if not items:
            return None
        try:
            item = items[-1] if isinstance(items, list) else items
            return float(item.get("longRatio", item.get("longShortRatio", 0.5)))
        except Exception:
            return None

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  📡  COINALYZE CLIENT  (untuk komponen B: taker buy btx)
# ══════════════════════════════════════════════════════════════════════════════
class CoinalyzeClient:
    BASE      = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _wait(self) -> None:
        elapsed = time.time() - CoinalyzeClient._last_call
        wait    = CONFIG["clz_min_interval_sec"] - elapsed
        if wait > 0:
            time.sleep(wait)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[list]:
        params["api_key"] = self.api_key
        for attempt in range(CONFIG["clz_retry"]):
            self._wait()
            try:
                r = requests.get(f"{self.BASE}/{endpoint}", params=params, timeout=15)
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 10))
                    log.warning(f"Coinalyze rate limit — tunggu {retry_after}s")
                    time.sleep(retry_after + 1)
                    continue
                if r.status_code == 401:
                    log.error("Coinalyze API key invalid")
                    return None
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if attempt < CONFIG["clz_retry"] - 1:
                    time.sleep(CONFIG["clz_retry_wait"])
        return None

    def get_future_markets(self) -> List[dict]:
        data = self._get("future-markets", {})
        return data if isinstance(data, list) else []

    def fetch_ohlcv_batch(self, symbols: List[str],
                          from_ts: int, to_ts: int) -> Dict[str, list]:
        if not symbols:
            return {}
        result = {}
        batch_size = CONFIG["clz_batch_size"]
        extra = {"interval": CONFIG["clz_interval"],
                 "from": from_ts, "to": to_ts}
        for i in range(0, len(symbols), batch_size):
            batch  = symbols[i: i + batch_size]
            params = {"symbols": ",".join(batch), **extra}
            data   = self._get("ohlcv-history", params)
            if not isinstance(data, list):
                continue
            for item in data:
                sym     = item.get("symbol", "")
                history = item.get("history", [])
                if sym and history:
                    result[sym] = history
        return result


# ══════════════════════════════════════════════════════════════════════════════
#  🗺️  SYMBOL MAPPER  (Bitget ↔ Coinalyze)
# ══════════════════════════════════════════════════════════════════════════════
class SymbolMapper:
    def __init__(self, clz: CoinalyzeClient):
        self._clz    = clz
        self._to_clz: Dict[str, str] = {}
        self._loaded = False

    def load(self, whitelist: set) -> int:
        log.info("SymbolMapper: fetching Coinalyze markets …")
        markets = self._clz.get_future_markets()
        if not markets:
            log.error("SymbolMapper: gagal fetch markets")
            return 0

        bitget_mkts = [m for m in markets
                       if "bitget" in m.get("exchange", "").lower()]
        if not bitget_mkts:
            bitget_mkts = markets

        mapped = 0
        for m in bitget_mkts:
            clz_sym  = m.get("symbol", "")
            exch_sym = m.get("symbol_on_exchange", "")
            if not clz_sym:
                continue
            clean = exch_sym.replace("_UMCBL", "").replace("_DMCBL", "").upper()
            for candidate in [clean, exch_sym.upper()]:
                if candidate in whitelist:
                    self._to_clz[candidate] = clz_sym
                    mapped += 1
                    break

        unmapped = [s for s in whitelist if s not in self._to_clz]
        if unmapped and self._to_clz:
            sample = list(self._to_clz.values())[0]
            suffix = sample.split(".")[-1] if "." in sample else ""
            if suffix:
                for sym in unmapped:
                    self._to_clz[sym] = f"{sym}_PERP.{suffix}"

        self._loaded = True
        log.info(f"SymbolMapper: {mapped}/{len(whitelist)} terpetakan")
        return mapped

    def to_clz(self, bitget_sym: str) -> Optional[str]:
        return self._to_clz.get(bitget_sym)

    def get_clz_symbols(self, bitget_syms: List[str]) -> List[str]:
        return [self._to_clz[s] for s in bitget_syms if s in self._to_clz]


# ══════════════════════════════════════════════════════════════════════════════
#  📊  FEATURE ENGINEERING (Pine Script compatible)
# ══════════════════════════════════════════════════════════════════════════════
def _wilder_ema(values: list, period: int) -> list:
    if not values:
        return []
    alpha  = 1.0 / period
    result = [values[0]]
    for i in range(1, len(values)):
        result.append(alpha * values[i] + (1.0 - alpha) * result[-1])
    return result

def _std_ema(values: list, period: int) -> list:
    if not values:
        return []
    alpha  = 2.0 / (period + 1)
    result = [values[0]]
    for i in range(1, len(values)):
        result.append(alpha * values[i] + (1.0 - alpha) * result[-1])
    return result

def compute_delta_volume(candles: List[dict]) -> List[float]:
    result = []
    is_buy = True
    for c in candles:
        if   c["close"] > c["open"]: is_buy = True
        elif c["close"] < c["open"]: is_buy = False
        result.append(c["vol"] if is_buy else -c["vol"])
    return result

def compute_vol_thresholds(dv: List[float],
                           vol_len: int) -> Tuple[List[float], List[float]]:
    n      = len(dv)
    scaled = [v / 2.5 for v in dv]
    vol_hi = [max(scaled[max(0, i - vol_len + 1): i + 1]) for i in range(n)]
    vol_lo = [min(scaled[max(0, i - vol_len + 1): i + 1]) for i in range(n)]
    return vol_hi, vol_lo

def compute_atr(candles: List[dict], period: int = 200) -> List[float]:
    trs = []
    for i, c in enumerate(candles):
        pc  = candles[i-1]["close"] if i > 0 else c["close"]
        trs.append(max(c["high"]-c["low"],
                       abs(c["high"]-pc), abs(c["low"]-pc)))
    return _wilder_ema(trs, period)

def find_pivot_lows(lows: List[float], lookback: int) -> List[Optional[float]]:
    n = len(lows)
    result = [None] * n
    for i in range(lookback, n - lookback):
        val = lows[i]
        if val <= min(lows[i-lookback:i]) and val <= min(lows[i+1:i+lookback+1]):
            result[i] = val
    return result

def detect_support_zones(candles: List[dict], symbol: str) -> List[dict]:
    cfg  = CONFIG
    lb   = cfg["lookback_period"]
    vl   = cfg["vol_len"]
    bw   = cfg["box_width_multiplier"]
    ap   = cfg["atr_period"]

    if len(candles) < lb * 2 + ap // 10 + 5:
        return []

    dv      = compute_delta_volume(candles)
    vh, _   = compute_vol_thresholds(dv, vl)
    atr     = compute_atr(candles, ap)
    lows    = [c["low"] for c in candles]
    pivots  = find_pivot_lows(lows, lb)

    zones = []
    for i, piv in enumerate(pivots):
        if piv is None:
            continue
        if dv[i] <= vh[i]:
            continue
        if math.isnan(atr[i]) or atr[i] <= 0:
            continue

        zone_top    = piv
        zone_bottom = piv - atr[i] * bw
        n           = len(candles)

        break_count = 0
        touch_count = 0
        in_zone     = False
        broke_at    = n

        for j in range(i + lb, n):
            c_low  = candles[j]["low"]
            c_high = candles[j]["high"]

            if j < broke_at:
                if not in_zone:
                    if zone_bottom <= c_low <= zone_top:
                        in_zone      = True
                        touch_count += 1
                else:
                    if c_low > zone_top:
                        in_zone = False
                    elif c_high < zone_bottom:
                        break_count += 1
                        in_zone  = False
                        broke_at = j

        zones.append({
            "bar":          i,
            "zone_top":     zone_top,
            "zone_bottom":  zone_bottom,
            "delta_vol":    dv[i],
            "atr":          atr[i],
            "touch_count":  touch_count,
            "break_count":  break_count,
        })
    return zones

def get_zone_state(zone: dict, current_low: float, current_high: float) -> str:
    top    = zone["zone_top"]
    bottom = zone["zone_bottom"]
    if current_high < bottom:
        return "BROKEN"
    if bottom <= current_low <= top:
        return "TESTING"
    return "VALID"


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  SCORING COMPONENTS (5 independen)
# ══════════════════════════════════════════════════════════════════════════════
def score_A_volume_zscore(candles: List[dict]) -> Tuple[int, float]:
    cfg = CONFIG
    win = cfg["vol_baseline_window"]
    if len(candles) < win + 5:
        return 0, 0.0

    cur_vol  = candles[-2]["vol"]
    baseline = [c["vol"] for c in candles[-(win + 5):-5]]
    if len(baseline) < 10:
        return 0, 0.0

    z     = zscore(cur_vol, baseline)
    score = score_from_z(z, cfg["vol_z_strong"], cfg["vol_z_medium"], cfg["score_vol_max"])
    return score, round(z, 2)

def score_B_taker_buy(clz_ohlcv: List[dict]) -> Tuple[int, float, str]:
    cfg = CONFIG
    if not clz_ohlcv or len(clz_ohlcv) < cfg["btx_baseline_window"] + 5:
        return 0, 0.0, "no_clz_data"

    win = cfg["btx_baseline_window"]
    cur = clz_ohlcv[-2] if len(clz_ohlcv) >= 2 else clz_ohlcv[-1]
    btx = cur.get("btx", 0)
    tx  = cur.get("tx",  0)

    if not btx or not tx:
        return 0, 0.0, "btx_zero"

    btx_ratio = btx / tx

    baseline_slice = clz_ohlcv[-(win + 5):-5]
    baseline_ratios = []
    for c in baseline_slice:
        c_tx  = c.get("tx",  0)
        c_btx = c.get("btx", 0)
        if c_tx > 0 and c_btx >= 0:
            baseline_ratios.append(c_btx / c_tx)

    if len(baseline_ratios) < 10:
        return 0, 0.0, "insufficient_baseline"

    z     = zscore(btx_ratio, baseline_ratios)
    score = score_from_z(z, cfg["btx_z_strong"], cfg["btx_z_medium"], cfg["score_btx_max"])
    return score, round(z, 2), "coinalyze"

def score_C_funding_rate(funding_rate: float) -> Tuple[int, float]:
    cfg = CONFIG
    f   = funding_rate
    if f <= cfg["fund_strongly_neg"]:
        return cfg["score_fund_max"], round(f, 6)
    if f <= cfg["fund_mod_neg"]:
        ratio = (cfg["fund_mod_neg"] - f) / (cfg["fund_mod_neg"] - cfg["fund_strongly_neg"])
        score = int(cfg["score_fund_max"] // 2 + ratio * cfg["score_fund_max"] // 2)
        return min(score, cfg["score_fund_max"]), round(f, 6)
    if f <= cfg["fund_neutral_hi"]:
        return cfg["score_fund_max"] // 4, round(f, 6)
    return 0, round(f, 6)

def score_D_oi_change(symbol: str, current_oi: float,
                      current_price: float, prev_price: float) -> Tuple[int, str]:
    cfg = CONFIG
    prev_oi = get_prev_oi(symbol)
    set_prev_oi(symbol, current_oi)

    if prev_oi <= 0 or current_oi <= 0:
        return 0, "no_prev_oi"

    oi_change_pct  = (current_oi - prev_oi) / prev_oi
    price_declined = current_price < prev_price * 0.999

    if oi_change_pct >= cfg["oi_significant_increase"] and price_declined:
        return cfg["score_oi_max"], f"oi+{oi_change_pct*100:.1f}%_price↓"
    if oi_change_pct >= cfg["oi_moderate_increase"] and price_declined:
        return cfg["score_oi_max"] // 2, f"oi+{oi_change_pct*100:.1f}%_price↓"
    if oi_change_pct >= cfg["oi_moderate_increase"] and not price_declined:
        return cfg["score_oi_max"] // 4, f"oi+{oi_change_pct*100:.1f}%_price↑"
    return 0, f"oi{oi_change_pct*100:+.1f}%"

def score_E_long_short_ratio(long_ratio: Optional[float]) -> Tuple[int, str]:
    cfg = CONFIG
    if long_ratio is None:
        return 0, "no_data"
    if long_ratio <= cfg["ls_strongly_short"]:
        return cfg["score_ls_max"], f"L:{long_ratio:.2f}_crowded_short"
    if long_ratio <= cfg["ls_mod_short"]:
        ratio = (cfg["ls_mod_short"] - long_ratio) / (cfg["ls_mod_short"] - cfg["ls_strongly_short"])
        score = int(cfg["score_ls_max"] // 2 + ratio * cfg["score_ls_max"] // 2)
        return min(score, cfg["score_ls_max"]), f"L:{long_ratio:.2f}"
    if long_ratio <= cfg["ls_neutral"]:
        return cfg["score_ls_max"] // 4, f"L:{long_ratio:.2f}_neutral"
    return 0, f"L:{long_ratio:.2f}_long_dom"


# ══════════════════════════════════════════════════════════════════════════════
#  🌍  REGIME CHECK (threshold adjuster)
# ══════════════════════════════════════════════════════════════════════════════
def is_caution_mode(btc_1w: List[dict], eth_1w: List[dict]) -> bool:
    ema_p = CONFIG["ema_weekly_period"]
    if len(btc_1w) < ema_p or len(eth_1w) < ema_p:
        return False
    ema_btc = _std_ema([c["close"] for c in btc_1w], ema_p)
    ema_eth = _std_ema([c["close"] for c in eth_1w], ema_p)
    return (btc_1w[-1]["close"] < ema_btc[-1] and
            eth_1w[-1]["close"] < ema_eth[-1])


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM & OUTPUT
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(msg: str) -> bool:
    bot  = CONFIG["bot_token"]
    chat = CONFIG["chat_id"]
    if not bot or not chat:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False

def build_table(results: list, caution: bool) -> str:
    mode = "⚠️ CAUTION (thr=70)" if caution else "✅ NORMAL (thr=55)"
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "",
        "═" * 120,
        f"  NEXUS-SR v2.3  |  Bitget Futures  |  {now}  |  {mode}",
        "═" * 120,
        f"  {'#':>2}  {'Coin':<13} {'Price':>12} {'ZoneTop':>10} {'ZoneBot':>10} "
        f"{'Score':>6} {'A:Vol':>6} {'B:btx':>6} {'C:Fund':>7} {'D:OI':>5} {'E:L/S':>5}  "
        f"{'Fund%':>7}  Strength",
        "─" * 120,
    ]
    for i, r in enumerate(results, 1):
        raw     = r["symbol"].replace("USDT", "")
        strength = r["strength"]
        fund_str = f"{r['funding']*100:.4f}%"
        lines.append(
            f"  {i:>2}  {raw:<13} {r['price']:>12.6f} {r['zone_top']:>10.5f} "
            f"{r['zone_bot']:>10.5f} {r['score']:>6.1f} "
            f"{r['sa']:>6} {r['sb']:>6} {r['sc']:>7} {r['sd']:>5} {r['se']:>5}  "
            f"{fund_str:>7}  {strength}"
        )
    lines += [
        "─" * 120,
        "  A=Volume(0-30)  B=TakerBuy(0-25)  C=Funding(0-20)  "
        "D=OI_Change(0-15)  E=L/S_Ratio(0-10)",
        "═" * 120,
    ]
    return "\n".join(lines)

def build_telegram_msg(results: list, caution: bool, readiness: str) -> str:
    mode = "⚠️ CAUTION" if caution else "🟢 NORMAL"
    now  = datetime.now(timezone.utc).strftime("%H:%M UTC")
    txt  = (f"🎯 <b>NEXUS-SR v2.3</b> [{now}]\n"
            f"Mode: {mode} | {readiness}\n{'─'*28}\n")
    for i, r in enumerate(results, 1):
        raw = r["symbol"].replace("USDT", "")
        txt += (
            f"{i}. <b>{raw}</b>\n"
            f"   Price <code>{r['price']:.6f}</code> | "
            f"Zone <code>{r['zone_bot']:.4f}–{r['zone_top']:.4f}</code>\n"
            f"   🎯 +15% target: <code>{r['target']:.4f}</code>\n"
            f"   Score: <b>{r['score']:.0f}</b> ({r['strength']}) | "
            f"Fund:{r['funding']*100:.3f}% "
            f"A:{r['sa']} B:{r['sb']} C:{r['sc']} D:{r['sd']} E:{r['se']}\n\n"
        )
    txt += "⚠️ <i>Paper mode. Verifikasi sebelum live trade.</i>"
    return txt


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    cfg      = CONFIG
    start_ts = time.time()

    log.info("=" * 70)
    log.info(f"  NEXUS-SR v2.3 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 70)

    # ── 1. Regime check ────────────────────────────────────────────────────
    log.info("Fetching BTC/ETH weekly data …")
    btc_1w  = BitgetClient.get_candles("BTCUSDT", "1W", cfg["candle_limit_1w"])
    eth_1w  = BitgetClient.get_candles("ETHUSDT", "1W", cfg["candle_limit_1w"])
    caution = is_caution_mode(btc_1w, eth_1w)
    threshold = cfg["score_threshold_caution"] if caution else cfg["score_threshold_normal"]
    log.info(f"Market regime: {'⚠️ CAUTION (thr=70)' if caution else '✅ NORMAL (thr=55)'}")

    # ── 2. Fetch tickers ───────────────────────────────────────────────────
    log.info("Fetching Bitget tickers …")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        send_telegram("⚠️ NEXUS-SR v2: Gagal fetch tickers")
        return
    log.info(f"Tickers received: {len(tickers)}")

    # ── 3. Build candidate universe ────────────────────────────────────────
    skip_stats = defaultdict(int)
    candidates = []
    for sym, t in tickers.items():
        if sym in MANUAL_EXCLUDE:
            skip_stats["excluded"] += 1; continue
        if is_on_cooldown(sym):
            skip_stats["cooldown"] += 1; continue
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = abs(float(t.get("change24h", 0)) * 100)
        except Exception:
            skip_stats["parse_err"] += 1; continue
        if vol < cfg["min_vol_24h"]:
            skip_stats["vol_low"] += 1; continue
        if vol > cfg["max_vol_24h"]:
            skip_stats["vol_high"] += 1; continue
        if chg > cfg["gate_chg_24h_max"]:
            skip_stats["pumped"] += 1; continue
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    # ── 4. Coinalyze init ──────────────────────────────────────────────────
    clz_key = cfg["coinalyze_api_key"]
    clz_client = CoinalyzeClient(clz_key) if clz_key else None
    mapper     = SymbolMapper(clz_client) if clz_client else None
    cand_syms  = {sym for sym, _ in candidates}
    if mapper:
        mapper.load(cand_syms)

    # ── 5. Phase 1: candle fetch + zone detection → TESTING candidates ─────
    log.info("Phase 1: fetching candles + detecting TESTING zones …")
    BitgetClient.clear_cache()

    testing_candidates = []

    for idx, (sym, ticker) in enumerate(candidates):
        if (idx + 1) % 50 == 0:
            log.info(f"  Phase 1: {idx+1}/{len(candidates)}")

        try:
            price = float(ticker.get("lastPr", 0))
            if price <= 0:
                skip_stats["no_price"] += 1; continue

            candles = BitgetClient.get_candles(sym, "1H", cfg["candle_limit_1h"])
            if len(candles) < cfg["lookback_period"] * 2 + 30:
                skip_stats["candle_short"] += 1; continue

            # Gate: volume outlier
            lb  = cfg["vol_outlier_lookback"]
            cur = candles[-1]["vol"]
            avg = _mean([c["vol"] for c in candles[-lb-1:-1]])
            if avg > 0 and cur > cfg["vol_outlier_mult"] * avg:
                skip_stats["vol_outlier"] += 1; continue

            zones = detect_support_zones(candles, sym)
            if not zones:
                skip_stats["no_zones"] += 1; continue

            # Cari TESTING zone (last 2 candles)
            c_low  = candles[-1]["low"]
            c_high = candles[-1]["high"]
            testing_zones = []
            for z in zones:
                if z["break_count"] >= cfg["max_break_count"]:
                    continue
                for lookback_i in (1, 2):
                    if lookback_i >= len(candles):
                        continue
                    c = candles[-lookback_i]
                    if get_zone_state(z, c["low"], c["high"]) == "TESTING":
                        testing_zones.append(z)
                        break

            if not testing_zones:
                skip_stats["no_testing"] += 1; continue

            testing_candidates.append((sym, ticker, candles, testing_zones))

        except Exception as e:
            log.debug(f"  Error Phase1 {sym}: {e}")

        time.sleep(cfg["sleep_between_coins"])

    log.info(f"Phase 1 done: {len(testing_candidates)} TESTING candidates")

    if not testing_candidates:
        print(build_table([], caution))
        log.info("Tidak ada TESTING zone saat ini")
        return

    # ── 6. Phase 2: Coinalyze batch fetch ──────────────────────────────────
    log.info(f"Phase 2: Coinalyze + L/S fetch untuk {len(testing_candidates)} candidates …")
    clz_data: Dict[str, list] = {}
    if mapper and clz_client:
        cand_names = [sym for sym, _, _, _ in testing_candidates]
        clz_syms   = mapper.get_clz_symbols(cand_names)
        if clz_syms:
            now_ts  = int(time.time())
            from_ts = now_ts - cfg["clz_lookback_h"] * 3600
            clz_data = clz_client.fetch_ohlcv_batch(clz_syms, from_ts, now_ts)
            log.info(f"  Coinalyze: {len(clz_data)} symbols fetched")

    # ── 7. Score each testing candidate ────────────────────────────────────
    results = []
    for sym, ticker, candles, testing_zones in testing_candidates:
        try:
            price     = float(ticker.get("lastPr", 0))
            cur_oi    = float(ticker.get("holdingAmount", 0))
            funding   = float(ticker.get("fundingRate", 0))
            prev_price = candles[-2]["close"] if len(candles) >= 2 else price

            # Coinalyze data
            clz_sym   = mapper.to_clz(sym) if mapper else None
            clz_ohlcv = clz_data.get(clz_sym, []) if clz_sym else []

            # L/S Ratio (dengan suffix _UMCBL sudah ditangani di method)
            long_ratio = BitgetClient.get_long_short_ratio(sym)
            time.sleep(1.05)  # rate limit 1 req/sec

            # Pilih zone terbaik (paling dekat harga saat ini)
            best_zone = min(testing_zones, key=lambda z: abs(z["zone_top"] - price))

            # Score components
            sa, za = score_A_volume_zscore(candles)
            sb, zb, btx_src = score_B_taker_buy(clz_ohlcv)
            sc, _  = score_C_funding_rate(funding)
            sd, oi_note = score_D_oi_change(sym, cur_oi, price, prev_price)
            se, ls_note = score_E_long_short_ratio(long_ratio)

            total_score = sa + sb + sc + sd + se

            if total_score < threshold:
                log.debug(f"  {sym}: score {total_score} < {threshold} — skip")
                continue

            strength = ("STRONG"   if total_score >= cfg["score_strong"]  else
                        "MODERATE" if total_score >= cfg["score_threshold_normal"] else
                        "WEAK")

            results.append({
                "symbol":    sym,
                "price":     round(price, 8),
                "zone_top":  round(best_zone["zone_top"], 8),
                "zone_bot":  round(best_zone["zone_bottom"], 8),
                "target":    round(price * 1.15, 8),
                "stop":      round(best_zone["zone_bottom"] * 0.99, 8),
                "score":     round(total_score, 1),
                "sa":        sa, "za": za,
                "sb":        sb, "zb": zb,
                "sc":        sc,
                "sd":        sd,
                "se":        se,
                "funding":   funding,
                "oi_note":   oi_note,
                "ls_note":   ls_note,
                "btx_src":   btx_src,
                "strength":  strength,
                "caution":   caution,
                "touches":   best_zone["touch_count"],
                "breaks":    best_zone["break_count"],
            })

            log.info(
                f"  ✅ {sym}: score={total_score} ({strength}) | "
                f"A={sa}(z={za}) B={sb}({btx_src}) "
                f"C={sc}(f={funding*100:.3f}%) D={sd}({oi_note}) E={se}({ls_note})"
            )

        except Exception as e:
            log.warning(f"  Error Phase2 {sym}: {e}", exc_info=False)

    # ── 8. Sort & output ────────────────────────────────────────────────────
    results.sort(key=lambda x: (-x["score"], -x["sa"], -x["sc"]))
    top = results[:cfg["top_n"]]

    elapsed = round(time.time() - start_ts, 1)
    readiness = f"Paper mode | {len(results)} sinyal"
    log.info(f"\nTotal signals: {len(results)} | Shown: {len(top)} | Time: {elapsed}s")

    print(build_table(top, caution))

    if not top:
        log.info("Tidak ada sinyal saat ini.")
        _save_state(_state)
        return

    # ── 9. Telegram ─────────────────────────────────────────────────────────
    strong = [r for r in top if r["strength"] == "STRONG"]
    send_targets = strong[:cfg["max_alerts"]] if strong else top[:cfg["max_alerts"]]

    if send_targets:
        ok = send_telegram(build_telegram_msg(send_targets, caution, readiness))
        if ok:
            log.info(f"📤 Telegram: {len(send_targets)} signals sent")
            for r in send_targets:
                set_cooldown(r["symbol"])

    _save_state(_state)
    log.info(f"=== SELESAI — {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")


if __name__ == "__main__":
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)
    run_scan()
