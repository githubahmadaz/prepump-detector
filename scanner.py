"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  SUPPORT HUNTER SCANNER — Anti-SL Pump Entry at Strong Support             ║
║  Based on "Support and Resistance (High Volume Boxes) [ChartPrime]"        ║
║  Detects altcoins sitting on high‑volume support zones ready to bounce.    ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests
import time
import os
import math
import json
import logging
import logging.handlers as _lh
import html as _html_mod
from datetime import datetime, timezone
from collections import defaultdict

# ── Persistent HTTP session ──────────────────────────────────────────────────
_http_session = requests.Session()
_http_session.headers.update({"User-Agent": "SupportHunter/1.0", "Accept-Encoding": "gzip"})

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

_fh = _lh.RotatingFileHandler("/tmp/support_scanner.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Support Hunter Scanner — log aktif: /tmp/support_scanner.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG — Support‑specific parameters added
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_support":        30,
    "max_alerts_per_run":       15,

    # ── Volume 24h filter ────────────────────────────────────────────────────
    "min_vol_24h":          10_000,
    "max_vol_24h":      50_000_000,
    "pre_filter_vol":       10_000,

    # ── Open Interest minimum (optional) ──────────────────────────────────────
    "min_oi_usd":          100_000,

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h":               200,   # enough for pivot detection

    # ── Cooldown ──────────────────────────────────────────────────────────────
    "alert_cooldown_sec":     3600,
    "sleep_coins":             0.15,
    "sleep_error":             3.0,
    "cooldown_file":          "./cooldown_support.json",
    "funding_snapshot_file":  "./funding_support.json",
    "oi_snapshot_file":       "./oi_support.json",

    # ── Support Detection Parameters (from ChartPrime indicator) ──────────────
    "support_lookback":        20,          # pivot lookback period
    "support_vol_len":         2,           # window for volume extreme
    "support_box_width":       1.0,         # ATR multiplier for box height
    "support_vol_mult":        1.5,         # min delta volume multiple to be significant
    "support_max_age":         50,          # max candles since pivot formation
    "support_bounce_candles":  3,           # look for bullish candles after touch

    # ── Scoring weights ───────────────────────────────────────────────────────
    "score_vol_strength":      10,
    "score_price_proximity":   15,
    "score_recent_bullish":    8,
    "score_flip_resistance":   12,
    "score_freshness":         5,
    "score_oi_confirmation":   6,
    "score_funding_neg":       5,

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    "entry_buffer":            0.002,       # 0.2% above support level
    "sl_buffer":               0.005,       # 0.5% below box bottom
    "tp_atr_mult":             2.0,         # TP = entry + ATR * multiplier
    "tp2_atr_mult":            3.5,
}

# Exclusions and whitelist same as original
MANUAL_EXCLUDE = set()
EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]
WHITELIST_SYMBOLS = { ... }   # (same as original, omitted for brevity – use the full list from scanner_v28)

GRAN_MAP    = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
BITGET_BASE = "https://api.bitget.com"
_cache      = {}

# ── Cooldown, Funding, OI snapshots (same as original) ───────────────────────
# (copy load_cooldown, save_cooldown, load_funding_snapshots, etc. unchanged)
# ... (omitted for brevity – include all those functions from scanner_v28)

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS (same as original)
# ══════════════════════════════════════════════════════════════════════════════
# (include get_all_tickers, get_candles, get_funding, get_btc_candles_cached,
#  get_funding_stats, get_open_interest, get_oi_change – identical to original)

# ══════════════════════════════════════════════════════════════════════════════
#  🆕  SUPPORT/RESISTANCE CORE FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def calc_delta_volume(candles):
    """
    Compute signed volume (delta) for each candle using Close Location Value.
    delta = volume * (2 * (close-low)/(high-low) - 1)
    """
    deltas = []
    for c in candles:
        rng = c["high"] - c["low"]
        if rng > 0:
            clv = 2.0 * (c["close"] - c["low"]) / rng - 1.0
        else:
            clv = 0.0
        deltas.append(c["volume_usd"] * clv)
    return deltas

def find_pivot_highs(candles, lookback):
    """
    Return list of (index, high, delta_vol) for pivot highs.
    A pivot high is where high is greater than all highs in previous `lookback`
    and next `lookback` candles. Since we only have past data, we only consider
    candles that have at least `lookback` candles after them.
    """
    pivots = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        left  = max(c["high"] for c in candles[i-lookback:i])
        right = max(c["high"] for c in candles[i+1:i+1+lookback])
        if candles[i]["high"] > left and candles[i]["high"] > right:
            pivots.append((i, candles[i]["high"]))
    return pivots

def find_pivot_lows(candles, lookback):
    pivots = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        left  = min(c["low"] for c in candles[i-lookback:i])
        right = min(c["low"] for c in candles[i+1:i+1+lookback])
        if candles[i]["low"] < left and candles[i]["low"] < right:
            pivots.append((i, candles[i]["low"]))
    return pivots

def calc_atr_at_index(candles, idx, period=14):
    """Compute ATR at given index using preceding period candles."""
    if idx < period:
        return 0.0
    trs = []
    for i in range(idx - period + 1, idx + 1):
        if i < 1:
            continue
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 0.0

def find_support_boxes(candles, deltas, lookback, vol_len, box_width):
    """
    Generate support boxes from pivot lows with volume confirmation.
    Returns list of dicts: {
        "type": "support",
        "pivot_idx": int,
        "level": float,
        "zone_low": float,
        "zone_high": float,
        "delta_vol": float,
        "atr": float,
        "vol_ratio": float,   # delta / avg_abs_delta
    }
    """
    # Compute rolling average of absolute delta over vol_len
    abs_deltas = [abs(d) for d in deltas]
    avg_abs_delta = []
    for i in range(len(deltas)):
        start = max(0, i - vol_len + 1)
        window = abs_deltas[start:i+1]
        avg_abs_delta.append(sum(window)/len(window) if window else 0)

    pivots = find_pivot_lows(candles, lookback)
    boxes = []
    for idx, level in pivots:
        delta = deltas[idx]
        avg_abs = avg_abs_delta[idx]
        if avg_abs > 0 and delta > avg_abs * CONFIG["support_vol_mult"]:
            atr = calc_atr_at_index(candles, idx, period=14)
            zone_low = level - atr * box_width
            boxes.append({
                "type": "support",
                "pivot_idx": idx,
                "level": level,
                "zone_low": zone_low,
                "zone_high": level,
                "delta_vol": delta,
                "atr": atr,
                "vol_ratio": delta / avg_abs if avg_abs else 0,
            })
    return boxes

def find_resistance_boxes(candles, deltas, lookback, vol_len, box_width):
    """
    Similar for resistance (pivot highs with negative delta volume).
    """
    abs_deltas = [abs(d) for d in deltas]
    avg_abs_delta = []
    for i in range(len(deltas)):
        start = max(0, i - vol_len + 1)
        window = abs_deltas[start:i+1]
        avg_abs_delta.append(sum(window)/len(window) if window else 0)

    pivots = find_pivot_highs(candles, lookback)
    boxes = []
    for idx, level in pivots:
        delta = deltas[idx]
        avg_abs = avg_abs_delta[idx]
        if avg_abs > 0 and delta < -avg_abs * CONFIG["support_vol_mult"]:
            atr = calc_atr_at_index(candles, idx, period=14)
            zone_high = level + atr * box_width
            boxes.append({
                "type": "resistance",
                "pivot_idx": idx,
                "level": level,
                "zone_low": level,
                "zone_high": zone_high,
                "delta_vol": delta,
                "atr": atr,
                "vol_ratio": -delta / avg_abs if avg_abs else 0,
            })
    return boxes

def is_box_broken(candles, box, current_idx):
    """
    Check if price has closed below the box's zone_low (for support) since its formation.
    If any candle close < zone_low, box is broken.
    """
    for i in range(box["pivot_idx"], current_idx + 1):
        if candles[i]["close"] < box["zone_low"]:
            return True
    return False

def find_active_support(candles, deltas, current_price, current_idx):
    """
    Find the strongest active support box below current price.
    Returns box dict or None.
    """
    boxes = find_support_boxes(candles, deltas,
                               lookback=CONFIG["support_lookback"],
                               vol_len=CONFIG["support_vol_len"],
                               box_width=CONFIG["support_box_width"])
    # Filter boxes that are not too old and not broken
    active = []
    for b in boxes:
        age = current_idx - b["pivot_idx"]
        if age > CONFIG["support_max_age"]:
            continue
        if is_box_broken(candles, b, current_idx):
            continue
        # Box must be below current price (support is below)
        if b["zone_high"] > current_price:
            continue
        active.append(b)

    if not active:
        return None

    # Choose the box with highest level (closest to price)
    active.sort(key=lambda x: x["level"], reverse=True)
    return active[0]

def score_support_box(box, candles, current_price, current_idx, deltas, oi_data, funding):
    """
    Compute a score for the support box.
    """
    score = 0
    signals = []

    # Volume strength at pivot (normalized by average)
    vol_score = min(box["vol_ratio"] * 5, 10)   # cap at 10
    score += vol_score
    signals.append(f"📊 Delta Volume: {box['vol_ratio']:.1f}x avg → +{vol_score:.0f}")

    # Price proximity to support level
    dist_to_level = (current_price - box["level"]) / box["level"] * 100
    if dist_to_level < 1.0:
        prox_score = CONFIG["score_price_proximity"]
        score += prox_score
        signals.append(f"📍 Price within {dist_to_level:.2f}% of support → +{prox_score}")
    elif dist_to_level < 3.0:
        score += prox_score // 2
        signals.append(f"📍 Price {dist_to_level:.1f}% above support (moderate) → +{prox_score//2}")

    # Recent bullish candles (after last touch of support zone)
    # Look for candles that closed higher after touching the zone
    touch_idx = None
    for i in range(current_idx, box["pivot_idx"], -1):
        if candles[i]["low"] <= box["zone_high"] and candles[i]["close"] > candles[i]["open"]:
            touch_idx = i
            break
    if touch_idx is not None:
        bullish_count = 0
        for i in range(touch_idx, current_idx + 1):
            if candles[i]["close"] > candles[i]["open"]:
                bullish_count += 1
        if bullish_count >= 2:
            score += CONFIG["score_recent_bullish"]
            signals.append(f"🟢 Bullish candles after support touch → +{CONFIG['score_recent_bullish']}")

    # Freshness (younger is better)
    age = current_idx - box["pivot_idx"]
    if age < 20:
        score += CONFIG["score_freshness"]
        signals.append(f"⏱️ Fresh support ({age} candles) → +{CONFIG['score_freshness']}")

    # OI confirmation (if available)
    if not oi_data.get("is_new") and oi_data["change_pct"] > 3.0:
        score += CONFIG["score_oi_confirmation"]
        signals.append(f"📈 OI +{oi_data['change_pct']:.1f}% → +{CONFIG['score_oi_confirmation']}")

    # Funding negative (short squeeze potential)
    if funding < 0:
        score += CONFIG["score_funding_neg"]
        signals.append(f"💸 Funding {funding*100:.3f}% neg → +{CONFIG['score_funding_neg']}")

    return score, signals

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE (support version)
# ══════════════════════════════════════════════════════════════════════════════
def master_score_support(symbol, ticker):
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    if len(c1h) < 50:
        log.info(f"  {symbol}: Candle 1h tidak cukup ({len(c1h)} < 50)")
        return None

    try:
        vol_24h   = float(ticker.get("quoteVolume", 0))
        chg_24h   = float(ticker.get("change24h", 0)) * 100
        price_now = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]
    except Exception:
        return None

    if vol_24h < CONFIG["min_vol_24h"] or vol_24h > CONFIG["max_vol_24h"]:
        return None

    # OI data (optional)
    oi_data = get_oi_change(symbol)
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)

    # Compute delta volume
    deltas = calc_delta_volume(c1h)

    current_idx = len(c1h) - 1

    # Find active support
    box = find_active_support(c1h, deltas, price_now, current_idx)
    if not box:
        log.info(f"  {symbol}: Tidak ada support aktif")
        return None

    # Score the setup
    score, signals = score_support_box(box, c1h, price_now, current_idx, deltas, oi_data, funding)

    # Minimum score threshold
    if score < CONFIG["min_score_support"]:
        log.info(f"  {symbol}: Support score {score} < {CONFIG['min_score_support']}")
        return None

    # Determine alert level
    if score >= 60:
        alert_level = "STRONG ALERT"
    elif score >= 45:
        alert_level = "ALERT"
    else:
        alert_level = "WATCHLIST"

    # Entry, SL, TP
    entry = box["level"] * (1.0 + CONFIG["entry_buffer"])
    sl    = box["zone_low"] * (1.0 - CONFIG["sl_buffer"])
    atr   = calc_atr_at_index(c1h, current_idx, period=14)
    tp1   = entry + atr * CONFIG["tp_atr_mult"]
    tp2   = entry + atr * CONFIG["tp2_atr_mult"]

    # Build result dict
    return {
        "symbol": symbol,
        "score": score,
        "signals": signals,
        "price": price_now,
        "support_level": box["level"],
        "zone_low": box["zone_low"],
        "zone_high": box["zone_high"],
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "atr": atr,
        "pivot_age": current_idx - box["pivot_idx"],
        "vol_24h": vol_24h,
        "chg_24h": chg_24h,
        "alert_level": alert_level,
        "oi_data": oi_data,
        "funding": funding,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER (adapted)
# ══════════════════════════════════════════════════════════════════════════════
def _fmt_price(p):
    if p == 0: return "0"
    if p >= 100: return f"{p:.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"

def build_alert_support(r, rank=None):
    level_icon = {"STRONG ALERT": "🔥", "ALERT": "📡", "WATCHLIST": "👁"}.get(r["alert_level"], "👁")

    msg = f"{level_icon} <b>{r['symbol']} — {r['alert_level']}</b>  #{rank}\n"
    msg += f"<b>Score:</b> {r['score']}  |  <b>Support Age:</b> {r['pivot_age']} candles\n"
    msg += f"<b>Scan:</b> {utc_now()}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    msg += f"<b>Harga :</b> <code>{_fmt_price(r['price'])}</code> ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>Support:</b> <code>{_fmt_price(r['support_level'])}</code>\n"
    msg += f"<b>Zone  :</b> <code>{_fmt_price(r['zone_low'])}</code> – <code>{_fmt_price(r['zone_high'])}</code>\n"
    msg += f"<b>ATR   :</b> {r['atr']/r['price']*100:.2f}%\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(r['entry'])}</code> (buffer {CONFIG['entry_buffer']*100:.1f}%)\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(r['sl'])}</code> ({(r['entry']-r['sl'])/r['entry']*100:.2f}%)\n"
    msg += f"🎯 <b>TP1   :</b> <code>{_fmt_price(r['tp1'])}</code> (+{(r['tp1']-r['entry'])/r['entry']*100:.1f}%)\n"
    msg += f"🎯 <b>TP2   :</b> <code>{_fmt_price(r['tp2'])}</code> (+{(r['tp2']-r['entry'])/r['entry']*100:.1f}%)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    # Signals
    msg += "<b>Sinyal:</b>\n"
    for s in r["signals"][:7]:
        msg += f"• {s}\n"

    # OI / Funding
    if r["oi_data"]["oi_now"] > 0:
        ov = r["oi_data"]["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs = f"({r['oi_data']['change_pct']:+.1f}%)" if not r["oi_data"].get("is_new") else "(baseline)"
        msg += f"📈 OI: {os_str} {cs}\n"
    if r["funding"] != 0:
        msg += f"💸 Funding: {r['funding']*100:.3f}%\n"

    msg += f"\n<i>⚠️ Bukan financial advice</i>"
    return msg

def build_summary(results):
    top = results[:5]
    msg = f"📋 <b>TOP {len(top)} SUPPORT SETUPS — Support Hunter</b>\n{utc_now()}\n{'━'*30}\n"
    for i, r in enumerate(top, 1):
        sym = r["symbol"].replace("USDT", "")
        msg += f"\n{i}. <b>{sym}</b> Score:{r['score']} | Age:{r['pivot_age']}c\n"
        msg += f"   Support: {_fmt_price(r['support_level'])} | Entry: {_fmt_price(r['entry'])}\n"
        msg += f"   SL: {_fmt_price(r['sl'])} | TP1: +{(r['tp1']-r['entry'])/r['entry']*100:.1f}%\n"
        if r["signals"]:
            msg += f"   • {r['signals'][0][:70]}\n"
    msg += f"\n{'━'*30}\n<i>⚠️ Bukan financial advice</i>"
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST (same as original, uses WHITELIST_SYMBOLS)
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    # (identical to original scanner's function, returns list of (sym, ticker))
    ...

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== SUPPORT HUNTER SCANNER — {utc_now()} ===")

    load_funding_snapshots()
    load_oi_snapshots()

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    candidates = build_candidate_list(tickers)
    results = []

    for i, (sym, t) in enumerate(candidates):
        if (i+1) % 10 == 0:
            log.info(f"[{i+1}/{len(candidates)}] {sym}...")
        try:
            res = master_score_support(sym, t)
            if res:
                log.info(f"  ✅ Score={res['score']} | {res['alert_level']} | Support age:{res['pivot_age']}")
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}")
            continue
        time.sleep(CONFIG["sleep_coins"])

    save_all_funding_snapshots()
    save_oi_snapshots()

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"\nLolos threshold: {len(results)} coin")

    if not results:
        log.info("Tidak ada sinyal support saat ini.")
        return

    top = results[:CONFIG["max_alerts_per_run"]]

    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert_support(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║           SUPPORT HUNTER SCANNER — v1.0                      ║")
    log.info("║   Based on ChartPrime Support/Resistance High Volume Boxes  ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)
    run_scan()
