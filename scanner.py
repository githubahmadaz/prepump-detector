#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP FEATURE DISCOVERY v2 — 5 COIN, 24 JAM SAJA                         ║
║                                                                              ║
║  Hanya memproses: JOEUSDT, PUFFERUSDT, CHRUSDT, ORDERUSDT, ZECUSDT          ║
║  Timeframe: 1H, limit 24 candle (24 jam terakhir)                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import csv
import json
import logging
import math
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# ── Output directory ──────────────────────────────────────────────────────────
OUT_DIR = Path(__file__).parent
OUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(OUT_DIR / "feature_discovery_5coin_24h.log"), encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  UNIVERSE — 5 coin spesifik, tidak perlu sampling
# ══════════════════════════════════════════════════════════════════════════════

CANDIDATE_SYMBOLS = [
    "JOEUSDT",
    "PUFFERUSDT",
    "CHRUSDT",
    "ORDERUSDT",
    "ZECUSDT",
]

SAMPLE_SIZE          = len(CANDIDATE_SYMBOLS)   # 5
CANDLE_LIMIT         = 24                       # 24 jam terakhir (1H)
MIN_HISTORY_CANDLES  = 10                       # minimal history untuk label (diperkecil)
PUMP_THRESHOLD_PCT   = 15.0
DUMP_THRESHOLD_PCT   = -10.0
PUMP_WINDOW_H        = 6
PUMP_COOLDOWN_H      = 8
DUMP_COOLDOWN_H      = 8
MIN_SAMPLES_PER_CLASS = 1                       # karena data kecil, minimal 1 sample per kelas

# Labels
LABEL_PUMP         = "PUMP"
LABEL_DUMP         = "DUMP"
LABEL_DISTRIBUTION = "DISTRIBUTION"
LABEL_RANGING      = "RANGING"


# ══════════════════════════════════════════════════════════════════════════════
#  📊  FEATURE VECTOR — 46 fitur (sama persis seperti v2)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class FeatureVector:
    sym:            str
    ts:             int
    label:          str
    future_max_pct: float

    # ── Grup 1 (original): Volatilitas & squeeze ───────────────────────────
    bb_w:           float
    bb_w_ratio:     float
    atr_now:        float
    atr_ratio:      float
    range_pct:      float

    # ── Grup 2 (original): Volume dasar ────────────────────────────────────
    vol_ratio_3h:   float
    vol_spike:      float
    vol_trend:      float
    body_ratio:     float
    upper_wick:     float
    lower_wick:     float

    # ── Grup 3 (original): Momentum & tren ─────────────────────────────────
    chg_1h:         float
    chg_3h:         float
    chg_6h:         float
    chg_24h:        float
    green_streak:   int
    red_streak:     int
    price_vs_hi24:  float
    price_vs_lo24:  float

    # ── Grup 4 (original): Candle terakhir ─────────────────────────────────
    last_body_pct:  float
    last_wick_up:   float
    last_wick_dn:   float
    momentum_accel: float

    # ── Grup 5 (original): RS vs BTC ───────────────────────────────────────
    rs_3h:          float
    rs_1h:          float

    # ════════════════════════════════════════════════════════════════════════
    # FITUR BARU
    # ════════════════════════════════════════════════════════════════════════

    # ── Grup A: Support / Resistance ───────────────────────────────────────
    dist_to_support:    float
    support_strength:   int
    inside_compression: int

    # ── Grup B: Volume pattern ─────────────────────────────────────────────
    vol_dry_then_spike: int
    vol_divergence:     float
    abnormal_vol:       int

    # ── Grup C: Candle pattern biner ───────────────────────────────────────
    is_hammer:          int
    is_engulfing_bull:  int
    is_inside_bar:      int
    is_doji:            int

    # ── Grup D: Multi-timeframe proxy (4H dari agregasi 4×1H) ──────────────
    trend_4h:           float
    ema9_above_ema21:   int
    ma_bullish_stack:   int
    dist_from_ma20:     float

    # ── Grup E: Market regime (dari BTC candle) ─────────────────────────────
    btc_regime:         int
    btc_vol_spike:      float
    rs_24h:             float

    # ── Grup F: Time-of-day ─────────────────────────────────────────────────
    hour_utc:           int
    is_asia_open:       int
    is_london_open:     int
    is_ny_overlap:      int
    weekday:            int


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  API
# ══════════════════════════════════════════════════════════════════════════════

def bitget_get(url: str, params: dict, retries: int = 3) -> Optional[dict]:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("  Rate limit — wait 12s")
                time.sleep(12)
                continue
            break
        except Exception:
            if attempt < retries - 1:
                time.sleep(3)
    return None


def fetch_candles(symbol: str, limit: int = CANDLE_LIMIT) -> List[dict]:
    data = bitget_get(
        "https://api.bitget.com/api/v2/mix/market/candles",
        {"symbol": symbol, "productType": "USDT-FUTURES",
         "granularity": "1H", "limit": limit},
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for row in data.get("data", []):
        try:
            vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
            candles.append({
                "ts":    int(row[0]),
                "open":  float(row[1]),
                "high":  float(row[2]),
                "low":   float(row[3]),
                "close": float(row[4]),
                "vol":   vol_usd,
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


# ══════════════════════════════════════════════════════════════════════════════
#  🔧  HELPERS (toleran terhadap data pendek)
# ══════════════════════════════════════════════════════════════════════════════

def _mean(v): return sum(v) / len(v) if v else 0.0
def _std(v):
    if len(v) < 2: return 0.0
    m = _mean(v); return math.sqrt(sum((x-m)**2 for x in v)/len(v))

def _slope(v):
    n = len(v)
    if n < 2: return 0.0
    xs = list(range(n)); mx, my = _mean(xs), _mean(v)
    num = sum((x-mx)*(y-my) for x,y in zip(xs,v))
    den = sum((x-mx)**2 for x in xs)
    return num/den if den > 0 else 0.0

def _pct_chg(candles, n_back):
    if len(candles) < n_back + 2: return 0.0
    p_now = candles[-1]["close"]; p_prev = candles[-(n_back+1)]["close"]
    return (p_now-p_prev)/p_prev*100 if p_prev > 0 else 0.0

def _calc_atr(candles, n=14):
    if len(candles) < 2: return 0.0
    trs = []
    for i in range(1, min(n+1, len(candles))):
        c, pc = candles[-i], candles[-(i+1)]
        if pc["close"] > 0:
            trs.append(max(
                (c["high"]-c["low"])/pc["close"],
                abs(c["high"]-pc["close"])/pc["close"],
                abs(c["low"]-pc["close"])/pc["close"],
            ))
    return _mean(trs)*100 if trs else 0.0

def _calc_bbw(candles, n=20):
    if len(candles) < n: return 0.0
    closes = [c["close"] for c in candles[-n:]]
    sma = _mean(closes)
    return (4*_std(closes))/sma if sma > 0 else 0.0

def _ema(candles, n):
    if len(candles) < n:
        return candles[-1]["close"] if candles else 0.0
    k = 2/(n+1)
    e = _mean([c["close"] for c in candles[:n]])
    for c in candles[n:]:
        e = c["close"]*k + e*(1-k)
    return e


# ══════════════════════════════════════════════════════════════════════════════
#  🏷️  EVENT LABELER (disesuaikan untuk data pendek)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class MarketEvent:
    sym: str; idx: int; ts: int; label: str
    future_max_pct: float; future_min_pct: float


def label_events(sym: str, candles: List[dict]) -> List[MarketEvent]:
    events = []
    last_pump_idx = -PUMP_COOLDOWN_H
    last_dump_idx = -DUMP_COOLDOWN_H

    # Karena data hanya 24 candle, batasi i agar tidak melebihi batas
    max_idx = len(candles) - PUMP_WINDOW_H - 1
    for i in range(MIN_HISTORY_CANDLES, max_idx):
        c = candles[i]
        ref = c["close"]
        if ref <= 0: continue

        future   = candles[i+1 : i+1+PUMP_WINDOW_H]
        future24 = candles[i+1 : i+1+24]  # 24 jam ke depan (tapi data terbatas)
        max_hi   = max((f["high"] for f in future),   default=ref)
        min_lo   = min((f["low"]  for f in future),   default=ref)
        max_pct  = (max_hi - ref)/ref*100
        min_pct  = (min_lo - ref)/ref*100
        # Untuk hi24/lo24, gunakan data yang tersedia
        if future24:
            hi24     = max((f["high"] for f in future24), default=ref)
            lo24     = min((f["low"]  for f in future24), default=ref)
        else:
            hi24 = lo24 = ref
        hi24p    = (hi24-ref)/ref*100
        lo24p    = (lo24-ref)/ref*100

        if max_pct >= PUMP_THRESHOLD_PCT and (i-last_pump_idx) >= PUMP_COOLDOWN_H:
            events.append(MarketEvent(sym, i, c["ts"], LABEL_PUMP, max_pct, min_pct))
            last_pump_idx = i; continue

        if min_pct <= DUMP_THRESHOLD_PCT and (i-last_dump_idx) >= DUMP_COOLDOWN_H:
            events.append(MarketEvent(sym, i, c["ts"], LABEL_DUMP, max_pct, min_pct))
            last_dump_idx = i; continue

        # Untuk data pendek, skip DISTRIBUTION dan RANGING (opsional)
        # Agar tidak terlalu banyak noise, tetap bisa diaktifkan dengan probabilitas kecil
        if 0 < hi24p < 5 and lo24p < -8:
            if len(events) < 5:  # batasi
                events.append(MarketEvent(sym, i, c["ts"], LABEL_DISTRIBUTION, max_pct, min_pct))
            continue

        if abs(hi24p) < 5 and abs(lo24p) < 5:
            if len(events) < 5:
                events.append(MarketEvent(sym, i, c["ts"], LABEL_RANGING, max_pct, min_pct))

    return events


# ══════════════════════════════════════════════════════════════════════════════
#  🔬  FEATURE EXTRACTOR — 46 fitur, toleran terhadap data pendek
# ══════════════════════════════════════════════════════════════════════════════

def extract_features(sym: str, candles: List[dict], event: MarketEvent,
                     btc_candles: List[dict]) -> Optional[FeatureVector]:
    idx = event.idx
    if idx < MIN_HISTORY_CANDLES: return None
    w = candles[:idx]
    if len(w) < 5: return None
    ref = w[-1]; p = ref["close"]
    if p <= 0: return None

    # ════════════════════════════════════════
    # GRUP 1–5 (dengan fallback jika data kurang)
    # ════════════════════════════════════════

    bb_w_now   = _calc_bbw(w, 20)
    bb_w_old   = _calc_bbw(w[:-20], 20) if len(w) >= 44 else bb_w_now
    bb_w_ratio = bb_w_now/bb_w_old if bb_w_old > 0 else 1.0
    atr_now    = _calc_atr(w[-16:], 14)
    atr_base   = _calc_atr(w[-50:-14], 14) if len(w) >= 50 else atr_now
    atr_ratio  = atr_now/atr_base if atr_base > 0 else 1.0
    ranges     = [(c["high"]-c["low"])/((c["high"]+c["low"])/2)*100
                  for c in w[-3:] if (c["high"]+c["low"])/2 > 0]
    range_pct  = _mean(ranges)

    avg_vol_24 = _mean([c["vol"] for c in w[-24:]]) if len(w) >= 24 else _mean([c["vol"] for c in w])
    avg_vol_3  = _mean([c["vol"] for c in w[-3:]])
    avg_vol_6  = _mean([c["vol"] for c in w[-6:]]) if len(w) >= 6 else avg_vol_3
    vol_ratio_3h = avg_vol_3/avg_vol_24 if avg_vol_24 > 0 else 1.0
    vol_spike    = w[-1]["vol"]/avg_vol_24 if avg_vol_24 > 0 else 1.0
    vol_trend    = _slope([c["vol"] for c in w[-6:]])/avg_vol_6 if avg_vol_6 > 0 else 0.0

    br, uw, lw = [], [], []
    for c in w[-3:]:
        rng = c["high"]-c["low"]
        if rng <= 0: continue
        body = abs(c["close"]-c["open"])
        br.append(body/rng)
        uw.append((c["high"]-max(c["close"],c["open"]))/rng)
        lw.append((min(c["close"],c["open"])-c["low"])/rng)
    body_ratio = _mean(br); upper_wick = _mean(uw); lower_wick = _mean(lw)

    chg_1h  = _pct_chg(w, 1)
    chg_3h  = _pct_chg(w, 3)
    chg_6h  = _pct_chg(w, 6)
    chg_24h = _pct_chg(w, 24) if len(w) >= 25 else chg_6h

    green_streak = red_streak = 0
    for c in reversed(w[-10:]):
        if c["close"] > c["open"]:
            if red_streak > 0: break
            green_streak += 1
        else:
            if green_streak > 0: break
            red_streak += 1

    hi_24 = max(c["high"] for c in w[-24:]) if len(w) >= 24 else max(c["high"] for c in w)
    lo_24 = min(c["low"]  for c in w[-24:]) if len(w) >= 24 else min(c["low"] for c in w)
    rng_24 = hi_24-lo_24
    price_vs_hi24 = (p-lo_24)/rng_24 if rng_24 > 0 else 0.5
    price_vs_lo24 = (hi_24-p)/rng_24 if rng_24 > 0 else 0.5

    last    = w[-1]; last_rng = last["high"]-last["low"]
    last_body_pct = (last["close"]-last["open"])/last["open"]*100 if last["open"] > 0 else 0.0
    last_wick_up  = (last["high"]-max(last["close"],last["open"]))/last["close"]*100 if last_rng > 0 else 0.0
    last_wick_dn  = (min(last["close"],last["open"])-last["low"])/last["close"]*100  if last_rng > 0 else 0.0

    prev_chgs      = [_pct_chg(w[:-i], 1) for i in range(1,4) if len(w) > i+2]
    momentum_accel = chg_1h - _mean(prev_chgs) if prev_chgs else 0.0

    btc_chg_1h = btc_chg_3h = btc_chg_24h = 0.0; btc_vol_spike = 1.0
    if len(btc_candles) >= 5:
        ts_ref = w[-1]["ts"]
        bi     = min(range(len(btc_candles)), key=lambda i: abs(btc_candles[i]["ts"]-ts_ref))
        bw     = btc_candles[:bi+1]
        if len(bw) >= 3:
            btc_chg_1h  = _pct_chg(bw, 1)
            btc_chg_3h  = _pct_chg(bw, 3)
        if len(bw) >= 25:
            btc_chg_24h = _pct_chg(bw, 24)
            btc_avg_vol = _mean([c["vol"] for c in bw[-24:]])
            btc_vol_spike = bw[-1]["vol"]/btc_avg_vol if btc_avg_vol > 0 else 1.0

    rs_1h  = chg_1h  - btc_chg_1h
    rs_3h  = chg_3h  - btc_chg_3h
    rs_24h = chg_24h - btc_chg_24h

    # ════════════════════════════════════════
    # GRUP A: Support / Resistance (dengan data terbatas)
    # ════════════════════════════════════════
    lookback_sr = w[-72:] if len(w) >= 72 else w
    lows_sr = [c["low"] for c in lookback_sr]
    bucket_size = p * 0.005
    clusters: Dict[float, int] = {}
    for lo in lows_sr:
        bucket = round(lo / bucket_size) * bucket_size
        clusters[bucket] = clusters.get(bucket, 0) + 1

    supports_below = [(lvl, cnt) for lvl, cnt in clusters.items()
                      if lvl < p and cnt >= 2]
    supports_below.sort(key=lambda x: x[0], reverse=True)

    if supports_below:
        nearest_support, support_cnt = supports_below[0]
        dist_to_support   = (p - nearest_support) / p * 100
        support_strength  = min(support_cnt, 10)
    else:
        dist_to_support   = 10.0
        support_strength  = 0

    inside_compression = 1 if dist_to_support < 3.0 and support_strength >= 3 else 0

    # ════════════════════════════════════════
    # GRUP B: Volume pattern
    # ════════════════════════════════════════
    if avg_vol_24 > 0 and len(w) >= 7:
        dry_candles = sum(1 for c in w[-6:-1] if c["vol"] < avg_vol_24 * 0.60)
        vol_dry_then_spike = 1 if dry_candles >= 3 and w[-1]["vol"] > avg_vol_24 * 2.0 else 0
    else:
        vol_dry_then_spike = 0

    if len(w) >= 7:
        price_range_6h = (max(c["high"] for c in w[-6:]) - min(c["low"] for c in w[-6:])) / p * 100
        vol_slope_6h   = _slope([c["vol"] for c in w[-6:]])
        vol_divergence = vol_slope_6h / avg_vol_24 if (price_range_6h < 2.0 and avg_vol_24 > 0) else 0.0
    else:
        vol_divergence = 0.0

    abnormal_vol = 1 if avg_vol_24 > 0 and w[-1]["vol"] > avg_vol_24 * 3.0 else 0

    # ════════════════════════════════════════
    # GRUP C: Candle patterns
    # ════════════════════════════════════════
    last = w[-1]; prev = w[-2] if len(w) >= 2 else last
    lrng = last["high"] - last["low"]
    lbody = abs(last["close"] - last["open"])
    llower = min(last["close"], last["open"]) - last["low"]
    is_hammer = 1 if lrng > 0 and llower > 2*lbody and lbody < 0.35*lrng else 0

    prev_rng  = prev["high"] - prev["low"]
    prev_body = abs(prev["close"] - prev["open"])
    is_engulfing_bull = 0
    if (last["close"] > last["open"] and
        prev["close"] < prev["open"] and
        last["open"]  < prev["close"] and
        last["close"] > prev["open"]  and
        lbody > prev_body):
        is_engulfing_bull = 1

    is_inside_bar = 1 if lrng > 0 and lrng < prev_rng and \
                         last["low"] > prev["low"] and last["high"] < prev["high"] else 0
    is_doji = 1 if lrng > 0 and lbody < 0.10*lrng else 0

    # ════════════════════════════════════════
    # GRUP D: Multi-timeframe
    # ════════════════════════════════════════
    if len(w) >= 32:
        closes_4h = [w[-(32 - i*4)]["close"] for i in range(8) if 32 - i*4 <= len(w)]
        trend_4h  = _slope(closes_4h) / p * 100 if p > 0 else 0.0
    else:
        trend_4h = 0.0

    if len(w) >= 25:
        ema9  = _ema(w, 9)
        ema21 = _ema(w, 21)
        ema9_above_ema21 = 1 if ema9 > ema21 else 0
    else:
        ema9_above_ema21 = 0

    if len(w) >= 52:
        ma20 = _mean([c["close"] for c in w[-20:]])
        ma50 = _mean([c["close"] for c in w[-50:]])
        ma_bullish_stack = 1 if p > ma20 > ma50 else 0
        dist_from_ma20   = (p - ma20) / ma20 * 100 if ma20 > 0 else 0.0
    else:
        # fallback: pakai MA20 dari data yang ada
        window = min(20, len(w))
        ma20 = _mean([c["close"] for c in w[-window:]])
        ma_bullish_stack = 0
        dist_from_ma20   = (p - ma20) / ma20 * 100 if ma20 > 0 else 0.0

    # ════════════════════════════════════════
    # GRUP E: Market regime (BTC)
    # ════════════════════════════════════════
    if len(btc_candles) >= 5:
        bi = min(range(len(btc_candles)), key=lambda i: abs(btc_candles[i]["ts"]-w[-1]["ts"]))
        bw_full = btc_candles[:bi+1]
        if len(bw_full) >= 25:
            btc_chg_24h_val = _pct_chg(bw_full, 24)
            btc_atr_24h     = _calc_atr(bw_full[-16:], 14)
            if btc_chg_24h_val < -3.0:
                btc_regime = -1
            elif btc_chg_24h_val > 3.0 and btc_atr_24h > 1.5:
                btc_regime = 1
            else:
                btc_regime = 0
        else:
            btc_regime = 0
    else:
        btc_regime = 0

    # ════════════════════════════════════════
    # GRUP F: Time-of-day
    # ════════════════════════════════════════
    candle_ts_sec = w[-1]["ts"] / 1000
    dt            = datetime.fromtimestamp(candle_ts_sec, tz=timezone.utc)
    hour_utc      = dt.hour
    weekday       = dt.weekday()
    is_asia_open   = 1 if 0 <= hour_utc <= 3  else 0
    is_london_open = 1 if 7 <= hour_utc <= 10 else 0
    is_ny_overlap  = 1 if 13 <= hour_utc <= 17 else 0

    return FeatureVector(
        sym=sym, ts=w[-1]["ts"], label=event.label, future_max_pct=event.future_max_pct,
        # Grup 1–5
        bb_w=bb_w_now, bb_w_ratio=bb_w_ratio, atr_now=atr_now, atr_ratio=atr_ratio,
        range_pct=range_pct, vol_ratio_3h=vol_ratio_3h, vol_spike=vol_spike,
        vol_trend=vol_trend, body_ratio=body_ratio, upper_wick=upper_wick,
        lower_wick=lower_wick, chg_1h=chg_1h, chg_3h=chg_3h, chg_6h=chg_6h,
        chg_24h=chg_24h, green_streak=green_streak, red_streak=red_streak,
        price_vs_hi24=price_vs_hi24, price_vs_lo24=price_vs_lo24,
        last_body_pct=last_body_pct, last_wick_up=last_wick_up,
        last_wick_dn=last_wick_dn, momentum_accel=momentum_accel,
        rs_3h=rs_3h, rs_1h=rs_1h,
        # Grup A
        dist_to_support=dist_to_support, support_strength=support_strength,
        inside_compression=inside_compression,
        # Grup B
        vol_dry_then_spike=vol_dry_then_spike, vol_divergence=vol_divergence,
        abnormal_vol=abnormal_vol,
        # Grup C
        is_hammer=is_hammer, is_engulfing_bull=is_engulfing_bull,
        is_inside_bar=is_inside_bar, is_doji=is_doji,
        # Grup D
        trend_4h=trend_4h, ema9_above_ema21=ema9_above_ema21,
        ma_bullish_stack=ma_bullish_stack, dist_from_ma20=dist_from_ma20,
        # Grup E
        btc_regime=btc_regime, btc_vol_spike=btc_vol_spike, rs_24h=rs_24h,
        # Grup F
        hour_utc=hour_utc, is_asia_open=is_asia_open,
        is_london_open=is_london_open, is_ny_overlap=is_ny_overlap,
        weekday=weekday,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📊  FEATURE NAMES (sama seperti v2)
# ══════════════════════════════════════════════════════════════════════════════

FEATURE_NAMES = [
    # Grup 1
    "bb_w","bb_w_ratio","atr_now","atr_ratio","range_pct",
    # Grup 2
    "vol_ratio_3h","vol_spike","vol_trend","body_ratio","upper_wick","lower_wick",
    # Grup 3
    "chg_1h","chg_3h","chg_6h","chg_24h","green_streak","red_streak",
    "price_vs_hi24","price_vs_lo24",
    # Grup 4
    "last_body_pct","last_wick_up","last_wick_dn","momentum_accel",
    # Grup 5
    "rs_3h","rs_1h",
    # Grup A
    "dist_to_support","support_strength","inside_compression",
    # Grup B
    "vol_dry_then_spike","vol_divergence","abnormal_vol",
    # Grup C
    "is_hammer","is_engulfing_bull","is_inside_bar","is_doji",
    # Grup D
    "trend_4h","ema9_above_ema21","ma_bullish_stack","dist_from_ma20",
    # Grup E
    "btc_regime","btc_vol_spike","rs_24h",
    # Grup F
    "hour_utc","is_asia_open","is_london_open","is_ny_overlap","weekday",
]

FEATURE_DESCRIPTIONS = {
    "bb_w":               "Bollinger Band Width 20-periode",
    "bb_w_ratio":         "BBW sekarang / BBW 20 candle lalu",
    "atr_now":            "ATR 14-periode (% harga)",
    "atr_ratio":          "ATR sekarang / ATR baseline",
    "range_pct":          "Avg high-low range 3 candle (%)",
    "vol_ratio_3h":       "Avg vol 3h / avg vol 24h",
    "vol_spike":          "Vol candle terakhir / avg vol 24h",
    "vol_trend":          "Slope vol 6 candle (normalized)",
    "body_ratio":         "Body ratio (|C-O|/range), avg 3 candle",
    "upper_wick":         "Upper wick ratio, avg 3 candle",
    "lower_wick":         "Lower wick ratio, avg 3 candle",
    "chg_1h":             "% perubahan 1 jam",
    "chg_3h":             "% perubahan 3 jam",
    "chg_6h":             "% perubahan 6 jam",
    "chg_24h":            "% perubahan 24 jam",
    "green_streak":       "Candle hijau berturut-turut",
    "red_streak":         "Candle merah berturut-turut",
    "price_vs_hi24":      "Posisi harga vs range 24h (0=low,1=high)",
    "price_vs_lo24":      "Jarak dari high 24h",
    "last_body_pct":      "% body candle terakhir (+hijau,-merah)",
    "last_wick_up":       "Upper wick candle terakhir (% harga)",
    "last_wick_dn":       "Lower wick candle terakhir (% harga)",
    "momentum_accel":     "chg_1h - avg(chg_1h 3 candle lalu)",
    "rs_3h":              "RS vs BTC 3 jam",
    "rs_1h":              "RS vs BTC 1 jam",
    # Baru
    "dist_to_support":    "jarak harga ke support terdekat",
    "support_strength":   "berapa kali bounce di level support",
    "inside_compression": "harga dalam 3% di atas support kuat",
    "vol_dry_then_spike": "3+ candle dry lalu spike",
    "vol_divergence":     "vol naik saat harga sideways",
    "abnormal_vol":       "vol terakhir > 3× mean 24h",
    "is_hammer":          "hammer/pin bar",
    "is_engulfing_bull":  "bullish engulfing pattern",
    "is_inside_bar":      "inside bar",
    "is_doji":            "doji (indecision)",
    "trend_4h":           "slope harga di proxy 4H",
    "ema9_above_ema21":   "EMA9 > EMA21",
    "ma_bullish_stack":   "close > MA20 > MA50",
    "dist_from_ma20":     "% jarak dari MA20",
    "btc_regime":         "BTC regime: -1=dump, 0=sideways, 1=trending",
    "btc_vol_spike":      "vol BTC relatif",
    "rs_24h":             "RS 24h coin vs BTC",
    "hour_utc":           "jam UTC",
    "is_asia_open":       "sesi Asia open (00-03 UTC)",
    "is_london_open":     "sesi London open (07-10 UTC)",
    "is_ny_overlap":      "NY-London overlap (13-17 UTC)",
    "weekday":            "hari (0=Senin,6=Minggu)",
}


# ══════════════════════════════════════════════════════════════════════════════
#  📊  STATISTICAL ANALYSIS (tetap sama)
# ══════════════════════════════════════════════════════════════════════════════

def get_val(fv: FeatureVector, name: str) -> float:
    return float(getattr(fv, name, 0.0))

def cohen_d(a: List[float], b: List[float]) -> float:
    if len(a) < 2 or len(b) < 2: return 0.0
    na, nb = len(a), len(b)
    pooled = math.sqrt(((na-1)*_std(a)**2 + (nb-1)*_std(b)**2) / (na+nb-2))
    return (_mean(a)-_mean(b))/pooled if pooled > 1e-9 else 0.0

def corr_pb(labels: List[int], vals: List[float]) -> float:
    n = len(vals)
    if n < 10: return 0.0
    s = _std(vals)
    if s < 1e-9: return 0.0
    m = _mean(vals); n1 = sum(labels); n0 = n - n1
    if n1 == 0 or n0 == 0: return 0.0
    m1 = _mean([v for v,l in zip(vals,labels) if l==1])
    return (m1-m)/s * math.sqrt(n1*n0/n**2)

def lift_q4(vals: List[float], labels: List[int], q: float=0.75,
            direction: str="high") -> float:
    if not vals: return 0.0
    base = sum(labels)/len(labels)
    if base <= 0: return 0.0
    pairs = sorted(zip(vals,labels), key=lambda x:x[0], reverse=(direction=="high"))
    qn    = max(1, int(len(pairs)*(1-q)))
    qr    = sum(l for _,l in pairs[:qn]) / qn
    return qr / base

def analyze_feature(name: str, fvecs: List[FeatureVector]) -> dict:
    by_class = defaultdict(list)
    for fv in fvecs:
        by_class[fv.label].append(get_val(fv, name))

    pump_v  = by_class.get(LABEL_PUMP, [])
    dump_v  = by_class.get(LABEL_DUMP, [])
    dist_v  = by_class.get(LABEL_DISTRIBUTION, [])
    range_v = by_class.get(LABEL_RANGING, [])
    non_pump = dump_v + dist_v + range_v

    all_v  = [get_val(fv,name) for fv in fvecs]
    all_l  = [1 if fv.label==LABEL_PUMP else 0 for fv in fvecs]

    def stats(v):
        if not v: return {"n":0,"mean":0,"median":0,"std":0,"p25":0,"p75":0}
        sv = sorted(v); n = len(sv)
        return {"n":n,"mean":round(_mean(v),4),"median":round(sv[n//2],4),
                "std":round(_std(v),4),"p25":round(sv[n//4],4),
                "p75":round(sv[3*n//4],4)}

    d_all    = cohen_d(pump_v, non_pump)
    d_range  = cohen_d(pump_v, range_v)
    d_dump   = cohen_d(pump_v, dump_v)
    c_pb     = corr_pb(all_l, all_v)
    lift_hi  = lift_q4(all_v, all_l, direction="high")
    lift_lo  = lift_q4(all_v, all_l, direction="low")
    best_lift = lift_hi if abs(lift_hi-1) >= abs(lift_lo-1) else lift_lo
    best_dir  = "high" if abs(lift_hi-1) >= abs(lift_lo-1) else "low"
    disc      = abs(c_pb)*40 + min(abs(d_all),2.0)*30 + min(abs(best_lift-1),2.0)*20 + min(abs(d_range),2.0)*10

    if abs(c_pb) >= 0.12 and best_lift >= 1.5:    assessment = "STRONG"
    elif abs(c_pb) >= 0.07 or best_lift >= 1.3:   assessment = "MODERATE"
    elif abs(c_pb) >= 0.04 or best_lift >= 1.15:  assessment = "WEAK"
    else:                                           assessment = "NOISE"

    return {
        "feature": name, "description": FEATURE_DESCRIPTIONS.get(name,""),
        "assessment": assessment, "disc_score": round(disc,2),
        "corr_pb": round(c_pb,4), "cohen_d_vs_all": round(d_all,4),
        "cohen_d_vs_ranging": round(d_range,4), "cohen_d_vs_dump": round(d_dump,4),
        "lift_best": round(best_lift,3), "lift_direction": best_dir,
        "lift_high": round(lift_hi,3), "lift_low": round(lift_lo,3),
        "pump": stats(pump_v), "dump": stats(dump_v),
        "distribution": stats(dist_v), "ranging": stats(range_v),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📤  OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def write_results_csv(analyses: List[dict], path: str):
    fields = [
        "rank","feature","assessment","disc_score","corr_pb",
        "cohen_d_vs_all","cohen_d_vs_ranging","cohen_d_vs_dump",
        "lift_best","lift_direction","lift_high","lift_low",
        "pump_n","pump_mean","pump_median","pump_std",
        "dump_mean","dump_median","distribution_mean","distribution_median",
        "ranging_mean","ranging_median","description",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for rank, a in enumerate(analyses, 1):
            w.writerow({
                "rank":rank,"feature":a["feature"],"assessment":a["assessment"],
                "disc_score":a["disc_score"],"corr_pb":a["corr_pb"],
                "cohen_d_vs_all":a["cohen_d_vs_all"],"cohen_d_vs_ranging":a["cohen_d_vs_ranging"],
                "cohen_d_vs_dump":a["cohen_d_vs_dump"],"lift_best":a["lift_best"],
                "lift_direction":a["lift_direction"],"lift_high":a["lift_high"],
                "lift_low":a["lift_low"],"pump_n":a["pump"]["n"],
                "pump_mean":a["pump"]["mean"],"pump_median":a["pump"]["median"],
                "pump_std":a["pump"]["std"],"dump_mean":a["dump"]["mean"],
                "dump_median":a["dump"]["median"],
                "distribution_mean":a["distribution"]["mean"],
                "distribution_median":a["distribution"]["median"],
                "ranging_mean":a["ranging"]["mean"],"ranging_median":a["ranging"]["median"],
                "description":a["description"],
            })
    log.info(f"  → {path}")

def write_raw_csv(fvecs: List[FeatureVector], path: str):
    fields = ["sym","ts","datetime","label","future_max_pct"] + FEATURE_NAMES
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for fv in fvecs:
            dt = datetime.fromtimestamp(fv.ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            row = {"sym":fv.sym,"ts":fv.ts,"datetime":dt,
                   "label":fv.label,"future_max_pct":round(fv.future_max_pct,2)}
            for name in FEATURE_NAMES:
                row[name] = round(get_val(fv,name),5)
            w.writerow(row)
    log.info(f"  → {path}")

def write_json(analyses: List[dict], class_counts: dict, n_sym: int, path: str):
    strong   = [a["feature"] for a in analyses if a["assessment"]=="STRONG"]
    moderate = [a["feature"] for a in analyses if a["assessment"]=="MODERATE"]
    weak     = [a["feature"] for a in analyses if a["assessment"]=="WEAK"]
    noise    = [a["feature"] for a in analyses if a["assessment"]=="NOISE"]

    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "version": "v2 — 5 coin, 24 jam",
        "n_symbols": n_sym,
        "class_counts": class_counts,
        "feature_ranking": [
            {"rank":i+1,"feature":a["feature"],"description":a["description"],
             "assessment":a["assessment"],"disc_score":a["disc_score"],
             "lift":a["lift_best"],"lift_dir":a["lift_direction"],
             "corr":a["corr_pb"],"cohen_d":a["cohen_d_vs_all"],
             "pump_median":a["pump"]["median"],"ranging_median":a["ranging"]["median"]}
            for i,a in enumerate(analyses[:30])
        ],
        "assessment_summary": {
            "STRONG": strong, "MODERATE": moderate, "WEAK": weak, "NOISE": noise
        },
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    log.info(f"  → {path}")


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("═"*68)
    log.info("  PRE-PUMP FEATURE DISCOVERY v2 — 5 COIN, 24 JAM SAJA")
    log.info(f"  46 fitur | Coin: {CANDIDATE_SYMBOLS}")
    log.info(f"  Candle limit: {CANDLE_LIMIT} jam (1H)")
    log.info("═"*68)

    # ── Step 1: Universe sudah fixed, tidak perlu filter ─────────────────────
    universe = CANDIDATE_SYMBOLS[:]
    log.info(f"  Universe: {universe}")

    # ── Step 2: BTC candles (24 jam) ─────────────────────────────────────────
    log.info("₿  Step 2: Fetch BTC candles (24 jam)...")
    btc_candles = fetch_candles("BTCUSDT", CANDLE_LIMIT)
    log.info(f"  BTC: {len(btc_candles)} candles")

    # ── Step 3: Fetch + label + extract ──────────────────────────────────────
    log.info(f"📈 Step 3: Fetch candles + labeling + feature extraction ({len(universe)} syms)...")
    all_fvecs: List[FeatureVector] = []
    class_counts: Dict[str, int]   = defaultdict(int)
    failed = 0

    for i, sym in enumerate(universe):
        try:
            candles = fetch_candles(sym, CANDLE_LIMIT)
            if len(candles) < MIN_HISTORY_CANDLES + PUMP_WINDOW_H + 2:
                log.warning(f"  [{i+1:3d}/{len(universe)}] {sym}: hanya {len(candles)} candles — skip")
                failed += 1
                continue

            events = label_events(sym, candles)
            cc = defaultdict(int)
            for e in events:
                cc[e.label] += 1

            sym_fvecs = []
            for ev in events:
                fv = extract_features(sym, candles, ev, btc_candles)
                if fv:
                    sym_fvecs.append(fv)
                    all_fvecs.append(fv)
                    class_counts[ev.label] += 1

            log.info(f"  [{i+1:3d}/{len(universe)}] {sym}: {len(candles)}c | "
                     f"P={cc.get(LABEL_PUMP,0)} D={cc.get(LABEL_DUMP,0)} "
                     f"Dist={cc.get(LABEL_DISTRIBUTION,0)} R={cc.get(LABEL_RANGING,0)} "
                     f"| fv={len(sym_fvecs)}")
            time.sleep(0.12)
        except Exception as e:
            log.warning(f"  [{i+1:3d}/{len(universe)}] {sym}: ERROR {e}")
            failed += 1

    log.info(f"\n  Total feature vectors: {len(all_fvecs):,}")
    log.info(f"  Class distribution:")
    for lbl in [LABEL_PUMP, LABEL_DUMP, LABEL_DISTRIBUTION, LABEL_RANGING]:
        n = class_counts.get(lbl, 0)
        log.info(f"    {lbl:14s}: {n:5d}")

    pump_n = class_counts.get(LABEL_PUMP, 0)
    if pump_n < MIN_SAMPLES_PER_CLASS:
        log.warning(f"⚠️ Hanya {pump_n} PUMP samples — analisis tetap dijalankan namun kurang representatif")

    # ── Step 4: Analisis statistik ───────────────────────────────────────────
    log.info(f"\n🔬 Step 4: Analisis {len(FEATURE_NAMES)} fitur...")
    base_rate = pump_n / len(all_fvecs) if all_fvecs else 0
    log.info(f"  Base rate PUMP: {pump_n}/{len(all_fvecs)} = {base_rate:.1%}")

    analyses = [analyze_feature(name, all_fvecs) for name in FEATURE_NAMES]
    analyses.sort(key=lambda x: x["disc_score"], reverse=True)

    # ── Step 5: Print ranking ─────────────────────────────────────────────────
    log.info(f"\n{'═'*78}")
    log.info("  RANKING FITUR (46 total) — DATA 24 JAM")
    log.info(f"{'═'*78}")
    log.info(f"  {'Rk':>3} {'Fitur':24} {'Disc':>6} {'Corr':>7} {'d':>6} {'Lift':>6} {'Dir':>5} │ PUMP_med RANG_med")
    log.info(f"  {'-'*78}")
    icons = {"STRONG":"✅","MODERATE":"🟡","WEAK":"🔶","NOISE":"❌"}
    for rank, a in enumerate(analyses, 1):
        ic = icons.get(a["assessment"],"")
        log.info(f"  {rank:3d} {a['feature']:24} {a['disc_score']:6.1f} "
                 f"{a['corr_pb']:+7.4f} {a['cohen_d_vs_all']:+6.3f} "
                 f"{a['lift_best']:6.2f}x {a['lift_direction']:>5} │ "
                 f"{a['pump']['median']:+8.4f} {a['ranging']['median']:+8.4f}  "
                 f"{ic} {a['assessment']}")

    # ── Step 6: Output files ──────────────────────────────────────────────────
    log.info(f"\n📁 Step 5: Menulis output ke {OUT_DIR}...")
    write_results_csv(analyses, str(OUT_DIR / "feature_importance_5coin_24h.csv"))
    write_raw_csv(all_fvecs,   str(OUT_DIR / "feature_raw_data_5coin_24h.csv"))
    write_json(analyses, dict(class_counts), len(universe)-failed,
               str(OUT_DIR / "feature_importance_5coin_24h.json"))

    log.info(f"\n{'═'*68}")
    log.info("  SELESAI")
    log.info(f"  Total events: {len(all_fvecs)} | "
             f"P={class_counts.get(LABEL_PUMP,0)} "
             f"D={class_counts.get(LABEL_DUMP,0)} "
             f"Dist={class_counts.get(LABEL_DISTRIBUTION,0)} "
             f"R={class_counts.get(LABEL_RANGING,0)}")
    log.info(f"  Output:")
    log.info(f"    {OUT_DIR}/feature_importance_5coin_24h.csv")
    log.info(f"    {OUT_DIR}/feature_raw_data_5coin_24h.csv")
    log.info(f"    {OUT_DIR}/feature_importance_5coin_24h.json")
    log.info(f"{'═'*68}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️  Stopped by user"); sys.exit(0)
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True); sys.exit(1)
