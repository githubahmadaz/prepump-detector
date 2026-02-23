"""
╔══════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v5.0 — DATA-DRIVEN FORENSIC EDITION          ║
║                                                                  ║
║  Dibangun berdasarkan analisis forensik 19 coin nyata:          ║
║  8 pumped (ORCA+80%, BIO+50%, AGLD+90%, POWER+150%,            ║
║            ENSO+95%, SNX+45%, SIREN+200%, AZTEC+90%)           ║
║  11 non-pumped (PHA, PEAQ, PORTAL, OM, GTC, A2Z, NFP,          ║
║                 NTRN, LINK, ZEC, HYPE)                          ║
║                                                                  ║
║  TEMUAN KUNCI FORENSIK:                                          ║
║  ✅ BBW percentile BUKAN pembeda tunggal (ENSO pump BBW=99%ile) ║
║  ✅ Vol ratio 0.18-2.0x = zona sehat (bukan terlalu mati/ramai) ║
║  ✅ Gate funding < -0.0005 penting (OM pattern = dump)          ║
║  ✅ Price change 7d: pumped semua di -10% s/d +8%               ║
║  ✅ Vol 24h > $500K/jam = sudah terlalu populer (false pos)     ║
║  ✅ Awakening 6h/24h: pumped avg 0.89x, delisting avg 0.02x    ║
╚══════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
from datetime import datetime, timezone
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG — dikalibrasi dari data forensik nyata
# ══════════════════════════════════════════════════════════════
CONFIG = {
    "min_score_alert":          48,    # backtest: 74% akurasi di 45-50
    "min_whale_score":          15,

    # Volume: dari forensik, pumped all di 0.227x - 1.242x baseline
    "min_volume_usd_24h":       80_000,   # $80K/jam minimum
    "max_volume_usd_24h":       500_000,  # $500K/jam max (LINK/HYPE false pos di atas ini)
    "pre_filter_min_vol":       30_000,

    # Gate values dari forensik
    "gate_price_change_7d_max":  12.0,   # pumped max +4.8%, gate di +12% (margin)
    "gate_price_change_7d_min": -11.0,   # pumped min -7.9%, gate di -11%
    "gate_funding_min":         -0.0005, # OM pattern: -0.00131 = dump
    "gate_awakening_min":        0.05,   # delisting avg 0.019, gate di 0.05
    "gate_vol_ratio_min":        0.18,   # delisting avg 0.10, gate di 0.18

    # Target
    "min_target_distance_pct":   8.0,

    # Candles
    "candle_limit_1h":          168,    # 7 hari (forensik butuh 7d data)
    "candle_limit_15m":         96,     # 24 jam

    # Ops
    "alert_cooldown_sec":       3600,
    "max_alerts_per_run":       8,
    "sleep_between_coins":      1.2,
    "sleep_after_error":        5.0,
    "cooldown_file":            "/tmp/pump_scanner_v5_cooldown.json",
    "pre_filter_max_pump":      20.0,
}

GRAN_MAP = {
    "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D",
}

# ══════════════════════════════════════════════════════════════
#  📋  TARGET COINS
# ══════════════════════════════════════════════════════════════
TARGET_COINS = [
    # DeFi — terbukti pump masif (ENSO, SNX, SIREN)
    "SNXUSDT", "ENSOUSDT", "SIRENUSDT", "CRVUSDT", "CVXUSDT",
    "COMPUSDT", "AAVEUSDT", "UNIUSDT", "DYDXUSDT", "COWUSDT",
    "PENDLEUSDT", "MORPHOUSDT", "FLUIDUSDT", "SSVUSDT", "LRCUSDT",
    "RSRUSDT", "NMRUSDT", "UMAUSDT", "BALUSDT",

    # ZK/Privacy — AZTEC +90%
    "AZTECUSDT", "MINAUSDT", "STRKUSDT", "ZORAUSDT", "ZRXUSDT", "POLYXUSDT",

    # DeSci — BIO +50%
    "BIOUSDT", "ATHUSDT",

    # AI Crypto
    "FETUSDT", "RENDERUSDT", "TAOUSDT", "GRASSUSDT", "AKTUSDT",
    "VANAUSDT", "COAIUSDT", "UAIUSDT", "GRTUSDT",

    # Solana — ORCA +80%
    "ORCAUSDT", "RAYUSDT", "JTOUSDT", "DRIFTUSDT", "WIFUSDT", "JUPUSDT",

    # L1 small-mid
    "APTUSDT", "SUIUSDT", "SEIUSDT", "INJUSDT", "KASUSDT",
    "BERAUSDT", "MOVEUSDT", "KAIAUSDT", "TIAUSDT", "EGLDUSDT",

    # L2
    "ARBUSDT", "OPUSDT", "CELOUSDT",

    # Liquid Staking
    "RPLUSDT", "ETHFIUSDT", "ANKRUSDT",

    # Gaming
    "AXSUSDT", "GALAUSDT", "IMXUSDT", "SANDUSDT", "APEUSDT", "SUPERUSDT",

    # Low cap explosive — VVV, POWER, AGLD
    "VVVUSDT", "POWERUSDT", "ARCUSDT", "AGLDUSDT", "VIRTUALUSDT",
    "SPXUSDT", "ONDOUSDT", "ENAUSDT", "EIGENUSDT", "STXUSDT",
    "RUNEUSDT", "ORDIUSDT", "ACHUSDT", "ALCHUSDT", "AEROUSDT",
    "AVNTUSDT", "AWEUSDT", "AXLUSDT", "BATUSDT", "BLURUSDT",
    "CFXUSDT", "CHZUSDT", "CYSUSDT", "DASHUSDT", "DEEPUSDT",
    "DEXEUSDT", "ENSUSDT", "FOGOUSDT", "FORMUSDT", "GASUSDT",
    "GLMUSDT", "GPSUSDT", "GUNUSDT", "GWEIUSDT", "HNTUSDT",
    "HOMEUSDT", "ICNTUSDT", "IDUSDT", "IOTAUSDT", "IPUSDT",
    "IRYSUSDT", "JASMYUSDT", "JSTUSDT", "KITEUSDT", "KMNOUSDT",
    "KSMUSDT", "LITUSDT", "LPTUSDT", "LYNUSDT", "MASKUSDT",
    "MERLUSDT", "MOCAUSDT", "MONUSDT", "MYXUSDT", "NEOUSDT",
    "NIGHTUSDT", "NXPCUSDT", "PARTIUSDT", "PENGUUSDT", "PLUMEUSDT",
    "PNUTUSDT", "QNTUSDT", "RAVEUSDT", "RIVERUSDT", "ROSEUSDT",
    "SAHARAUSDT", "SENTUSDT", "SIGNUSDT", "SKRUSDT", "SKYUSDT",
    "SOMIUSDT", "SOONUSDT", "SPKUSDT", "STGUSDT", "TAGUSDT",
    "THETAUSDT", "TRBUSDT", "TURBOUSDT", "UBUSDT", "VETUSDT",
    "VTHOUSDT", "WALUSDT", "WLDUSDT", "WLFIUSDT", "XDCUSDT",
    "XLMUSDT", "XPLUSDT", "XTZUSDT", "XVGUSDT", "ZAMAUSDT",
    "ZENUSDT", "ZETAUSDT", "ZILUSDT", "ZROUSDT", "0GUSDT",
    "ASTERUSDT", "ASTRUSDT", "BANUSDT", "BARDUSDT", "BEATUSDT",
    "BIRBUSDT", "BRETTUSDT", "CAKEUSDT", "COINUSDT", "CROUSDT",
    "FARTCOINUSDT", "FFUSDT", "FILUSDT", "FLOKIUSDT", "MEMEUSDT",
    "MOODENGUSDT", "PEPEUSDT", "POPCATUSDT", "PUMPUSDT", "SHIBUSDT",
    "TOSHIUSDT", "TRUMPUSDT", "1000BONKUSDT", "1000RATSUSDT",
]
TARGET_COINS = list(dict.fromkeys(TARGET_COINS))

SECTOR_MAP = {
    "DEFI":         ["SNXUSDT","ENSOUSDT","SIRENUSDT","CRVUSDT","CVXUSDT","COMPUSDT","AAVEUSDT","UNIUSDT","DYDXUSDT","COWUSDT","PENDLEUSDT","MORPHOUSDT","FLUIDUSDT","SSVUSDT","LRCUSDT","RSRUSDT","NMRUSDT","UMAUSDT"],
    "ZK_PRIVACY":   ["AZTECUSDT","MINAUSDT","STRKUSDT","ZORAUSDT","ZRXUSDT","POLYXUSDT"],
    "DESCI":        ["BIOUSDT","ATHUSDT"],
    "AI_CRYPTO":    ["FETUSDT","RENDERUSDT","TAOUSDT","GRASSUSDT","AKTUSDT","VANAUSDT","COAIUSDT","UAIUSDT","GRTUSDT"],
    "SOLANA_ECO":   ["ORCAUSDT","RAYUSDT","JTOUSDT","DRIFTUSDT","WIFUSDT","JUPUSDT","1000BONKUSDT"],
    "LAYER1":       ["APTUSDT","SUIUSDT","SEIUSDT","INJUSDT","KASUSDT","BERAUSDT","MOVEUSDT","KAIAUSDT","TIAUSDT","EGLDUSDT"],
    "GAMING":       ["AXSUSDT","GALAUSDT","IMXUSDT","SANDUSDT","APEUSDT","SUPERUSDT"],
    "LOW_CAP":      ["VVVUSDT","POWERUSDT","ARCUSDT","AGLDUSDT","VIRTUALUSDT","SPXUSDT","ONDOUSDT","ENAUSDT","EIGENUSDT","STXUSDT","RUNEUSDT","ORDIUSDT"],
}
SECTOR_LOOKUP = {coin: sec for sec, coins in SECTOR_MAP.items() for coin in coins}

BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_cache = {}


# ══════════════════════════════════════════════════════════════
#  💾  COOLDOWN PERSISTEN
# ══════════════════════════════════════════════════════════════

def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items() if now - v < CONFIG["alert_cooldown_sec"]}
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

def is_cooldown(sym): return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]
def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)


# ══════════════════════════════════════════════════════════════
#  🌐  HTTP UTILITIES
# ══════════════════════════════════════════════════════════════

def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("Rate limit Bitget — tunggu 15s")
                time.sleep(15)
            break
        except Exception as e:
            if attempt == 0:
                time.sleep(CONFIG["sleep_after_error"])
    return None

def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=15
        )
        return r.status_code == 200
    except:
        return False

def utc_now():  return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
def utc_hour(): return datetime.now(timezone.utc).hour


# ══════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════

def get_all_tickers():
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/tickers",
                    params={"productType": "usdt-futures"})
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
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/candles",
                    params={"symbol": symbol, "granularity": g,
                            "limit": str(limit), "productType": "usdt-futures"})
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({"ts": int(c[0]), "open": float(c[1]), "high": float(c[2]),
                             "low": float(c[3]), "close": float(c[4]),
                             "volume": float(c[5]), "volume_usd": vol_usd})
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
    return 0

def get_trades(symbol, limit=200):
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/fills",
                    params={"symbol": symbol, "productType": "usdt-futures", "limit": str(limit)})
    if data and data.get("code") == "00000":
        trades = []
        for t in data.get("data", []):
            try:
                trades.append({"price": float(t["price"]), "size": float(t["size"]),
                                "side": t.get("side","").lower()})
            except:
                pass
        return trades
    return []

def get_cg_trending():
    key = "cg_trend"
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < 600:
            return val
    data = safe_get(f"{COINGECKO_BASE}/search/trending")
    result = [c["item"]["symbol"].upper() for c in (data or {}).get("coins", [])]
    _cache[key] = (time.time(), result)
    return result


# ══════════════════════════════════════════════════════════════
#  📐  MATH HELPERS
# ══════════════════════════════════════════════════════════════

def bbw_percentile(candles, period=20):
    """Hitung BBW dan percentile-nya dari 100 bar terakhir."""
    closes = [c["close"] for c in candles]
    if len(closes) < period + 10:
        return 0, 50
    bbws = []
    for i in range(period - 1, len(closes)):
        w    = closes[i - period + 1:i + 1]
        mean = sum(w) / period
        std  = math.sqrt(sum((x - mean)**2 for x in w) / period)
        bbws.append((4 * std / mean * 100) if mean else 0)
    if not bbws:
        return 0, 50
    cur = bbws[-1]
    pct = sum(1 for b in bbws[:-1] if b < cur) / max(len(bbws)-1, 1) * 100
    return cur, pct

def calc_vwap_support(candles):
    """VWAP + lower band, dijamin di bawah harga sekarang."""
    cum_tv, cum_v = 0, 0
    vals = []
    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_tv += tp * c["volume"]
        cum_v  += c["volume"]
        vals.append(cum_tv / cum_v if cum_v else tp)
    if not vals:
        return None, None
    vwap = vals[-1]
    devs = [abs(candles[i]["close"] - vals[i]) for i in range(len(candles))]
    std  = math.sqrt(sum(d**2 for d in devs) / len(devs)) if devs else 0
    z1   = vwap - 1.5 * std
    cur  = candles[-1]["close"]
    if z1 >= cur:
        z1 = cur * 0.97
    return vwap, z1

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
            vol_bkt[b] += c["volume"] / nb
    poc_b = max(vol_bkt, key=vol_bkt.get) if vol_bkt else 20
    return pmin + (poc_b + 0.5) * bsize

def find_targets(candles, cur):
    """Swing high minimal 8% di atas harga sekarang."""
    min_t = cur * (1 + CONFIG["min_target_distance_pct"] / 100)
    swings = []
    for i in range(2, len(candles) - 2):
        h = candles[i]["high"]
        if h >= min_t and h > candles[i-1]["high"] and h > candles[i-2]["high"] \
                and h > candles[i+1]["high"] and h > candles[i+2]["high"]:
            swings.append(h)
    swings.sort()
    t1 = swings[0] if swings else cur * 1.10
    t2 = swings[1] if len(swings) >= 2 else t1 * 1.08
    return round(t1, 8), round(t2, 8)


# ══════════════════════════════════════════════════════════════
#  🔬  FORENSIC SCORING ENGINE
#  Formula berbasis data nyata dari 19 coin
# ══════════════════════════════════════════════════════════════

def calc_forensic_score(symbol, candles_1h, candles_15m, ticker, funding):
    """
    Scoring engine yang dikalibrasi dari forensik 8 pumped vs 11 non-pumped.
    Return: (total_score, breakdown_dict, signals_list, gate_reason_or_None)
    """
    score  = 0
    sigs   = []
    bd     = {}  # breakdown

    cur = candles_1h[-1]["close"] if candles_1h else 0
    if cur <= 0:
        return 0, {}, [], "harga tidak valid"

    vols_1h = [c["volume_usd"] for c in candles_1h]

    # ── KALKULASI METRIK FORENSIK ──────────────────────────
    # Vol baseline: median 24-168 jam (seperti forensik)
    baseline_window = vols_1h[24:] if len(vols_1h) >= 48 else vols_1h
    baseline_sorted = sorted(baseline_window)
    vol_baseline    = baseline_sorted[len(baseline_sorted)//2] if baseline_sorted else 1

    vol_24h_avg = sum(vols_1h[:24]) / min(24, len(vols_1h)) if vols_1h else 0
    vol_6h_avg  = sum(vols_1h[:6])  / min(6,  len(vols_1h)) if vols_1h else 0

    vol_ratio    = vol_24h_avg / vol_baseline if vol_baseline > 0 else 0
    awakening    = vol_6h_avg / vol_24h_avg   if vol_24h_avg  > 0 else 0

    # Price metrics
    closes      = [c["close"] for c in candles_1h]
    price_7d    = closes[-168] if len(closes) >= 168 else closes[0]
    price_chg7d = (cur - price_7d) / price_7d * 100 if price_7d > 0 else 0

    high_24h  = max(c["high"] for c in candles_1h[:24]) if len(candles_1h) >= 24 else cur
    low_24h   = min(c["low"]  for c in candles_1h[:24]) if len(candles_1h) >= 24 else cur
    range_24h = (high_24h - low_24h) / low_24h * 100 if low_24h > 0 else 99

    # Coiling: candle demi candle kecil berurutan
    coiling = 0
    for c in reversed(candles_1h[:48]):
        body = abs(c["close"] - c["open"]) / c["open"] * 100 if c["open"] else 99
        if body < 1.0:
            coiling += 1
        else:
            break

    # BBW
    bbw_val, bbw_pct = bbw_percentile(candles_1h)

    # Ticker data
    try:
        vol_24h_usd = float(ticker.get("quoteVolume", 0))
        chg_24h     = float(ticker.get("change24h", 0)) * 100
    except:
        vol_24h_usd = 0
        chg_24h     = 0

    # ══════════════════════════════════════════════════════
    #  🚧  GATE ELIMINASI — dari forensik nyata
    # ══════════════════════════════════════════════════════

    # Gate 1: Awakening terlalu rendah → coin mati/delisting
    # Forensik: delisting avg awakening = 0.019, pumped min = 0.175
    if awakening < CONFIG["gate_awakening_min"]:
        return 0, {}, [], f"GATE: coin mati (awakening={awakening:.3f}, min={CONFIG['gate_awakening_min']})"

    # Gate 2: Vol ratio terlalu rendah → tidak ada aktivitas sama sekali
    # Forensik: delisting avg vol_ratio = 0.099, pumped min = 0.227
    if vol_ratio < CONFIG["gate_vol_ratio_min"]:
        return 0, {}, [], f"GATE: likuiditas terlalu rendah (vol_ratio={vol_ratio:.3f})"

    # Gate 3: Price change 7d terlalu ekstrem
    # Forensik: pumped semua di -7.9% s/d +4.8%. Gate longgar di -11% dan +12%
    if price_chg7d > CONFIG["gate_price_change_7d_max"]:
        return 0, {}, [], f"GATE: sudah overbought 7d ({price_chg7d:+.1f}%)"
    if price_chg7d < CONFIG["gate_price_change_7d_min"]:
        return 0, {}, [], f"GATE: downtrend kuat 7d ({price_chg7d:+.1f}%)"

    # Gate 4: Funding terlalu negatif (OM pattern = dump, bukan pump)
    # Forensik: OM funding=-0.00131 → turun -11%. Pumped max negatif = -0.000256 (AZTEC)
    if funding < CONFIG["gate_funding_min"]:
        return 0, {}, [], f"GATE: funding overcrowded short ({funding:.5f})"

    # Gate 5: Range 24h terlalu lebar = pump sudah berjalan
    if range_24h > 50:
        return 0, {}, [], f"GATE: range 24h={range_24h:.0f}% — pump sudah berjalan"

    # Gate 6: Volume 24h USD terlalu besar → coin terlalu populer (LINK, HYPE, ZEC pattern)
    # Forensik: LINK=$1M/jam, HYPE=$977K/jam → false positive. Pumped max = $596K (POWER).
    if vol_24h_usd > CONFIG["max_volume_usd_24h"] * 24 * 1.5:
        return 0, {}, [], f"GATE: vol terlalu besar (${vol_24h_usd/1e6:.1f}M) — sudah terlalu populer"

    # ══════════════════════════════════════════════════════
    #  📊  VOLUME SCORE (max 35)
    #  Zona sehat dari forensik: 0.18x - 2.0x baseline
    #  Pumped avg: 0.71x, non-pumped-turun avg: 0.80x (overlap)
    #  Diferensiator: awakening ratio lebih penting
    # ══════════════════════════════════════════════════════

    vs = 0
    if 0.18 <= vol_ratio < 0.50:
        vs += 15
        sigs.append(f"Vol diam tapi hidup ({vol_ratio:.2f}x baseline)")
    elif 0.50 <= vol_ratio < 1.30:
        vs += 25
        sigs.append(f"Vol aktif zona stealth ({vol_ratio:.2f}x baseline)")
    elif 1.30 <= vol_ratio < 2.50:
        vs += 15
        sigs.append(f"Vol mulai diperhatikan ({vol_ratio:.2f}x baseline)")

    if awakening > 1.5:
        vs += 10
        sigs.append(f"🌅 Volume awakening kuat ({awakening:.2f}x 6h vs 24h)")
    elif awakening > 1.0:
        vs += 5
        sigs.append(f"Volume mulai bangun ({awakening:.2f}x)")

    score += min(vs, 35)
    bd["vol_score"] = min(vs, 35)

    # ══════════════════════════════════════════════════════
    #  📐  BBW SCORE (max 25)
    #  CATATAN FORENSIK: BBW bukan pembeda tunggal!
    #  ENSO pump+95% tapi BBW=99%ile. Tapi coin dengan
    #  BBW rendah CENDERUNG pump lebih eksplosif.
    #  Kombinasikan dengan kondisi lain.
    # ══════════════════════════════════════════════════════

    bs = 0
    if bbw_pct < 15:
        bs += 25
        sigs.append(f"BBW Squeeze Ekstrem ({bbw_pct:.0f}%ile — sangat jarang)")
    elif bbw_pct < 30:
        bs += 18
        sigs.append(f"BBW Squeeze Kuat ({bbw_pct:.0f}%ile)")
    elif bbw_pct < 50:
        bs += 8
        sigs.append(f"BBW Menyempit ({bbw_pct:.0f}%ile)")
    if bbw_pct > 85:
        bs -= 10  # expanding = bahaya (LINK, PORTAL pattern)
        sigs.append(f"⚠️ BBW Expanding ({bbw_pct:.0f}%ile) — awas volatilitas turun")

    score += max(min(bs, 25), 0)
    bd["bbw_score"] = max(min(bs, 25), 0)

    # ══════════════════════════════════════════════════════
    #  📍  PRICE SCORE (max 20)
    #  Dari forensik: pumped semua price_chg7d = -7.9% s/d +4.8%
    #  Price range 24h: pumped avg 12% (tapi ada outlier POWER+AZTEC)
    #  Non-pumped avg 7.6%
    #  Yang lebih reliable: range 24h < 4% = stealth positioning
    # ══════════════════════════════════════════════════════

    ps = 0
    if range_24h < 4:
        ps += 20
        sigs.append(f"Harga sangat flat 24h ({range_24h:.1f}%) — stealth positioning")
    elif range_24h < 8:
        ps += 12
        sigs.append(f"Harga relatif flat 24h ({range_24h:.1f}%)")
    elif range_24h < 15:
        ps += 5

    # Bonus: koreksi sehat (sedikit turun = akumulasi zona bawah)
    if -10 <= price_chg7d <= -2:
        ps += 8
        sigs.append(f"Koreksi sehat 7d ({price_chg7d:+.1f}%) — akumulasi di bawah")
    elif -2 < price_chg7d <= 8:
        ps += 5

    score += min(ps, 20)
    bd["price_score"] = min(ps, 20)

    # ══════════════════════════════════════════════════════
    #  💰  FUNDING SCORE (max 15)
    #  Forensik kuat: pumped di -0.000256 s/d +0.0001
    #  OM (dump): -0.001310 → sudah digate
    #  Slight negatif = short squeeze setup terbaik
    # ══════════════════════════════════════════════════════

    fs = 0
    if -0.0004 <= funding <= -0.00001:
        fs  = 15
        sigs.append(f"Funding negatif ringan ({funding:.5f}%) — short squeeze setup")
    elif 0 <= funding <= 0.0001:
        fs  = 10
        sigs.append(f"Funding netral sehat ({funding:.5f}%)")
    elif 0.0001 < funding <= 0.0002:
        fs  = 5
    elif funding < -0.0004:
        fs  = 0  # sudah di-gate tapi ini backup

    score += fs
    bd["funding_score"] = fs

    # ══════════════════════════════════════════════════════
    #  🔇  COILING SCORE (max 15)
    #  Forensik: SIREN 51 bars! AGLD/ORCA 8 bars, pumped avg 9.75
    #  Non-pumped avg 6 bars. Perbedaan ada tapi tidak dramatis.
    # ══════════════════════════════════════════════════════

    cs = 0
    if coiling >= 20:
        cs  = 15
        sigs.append(f"Coiling {coiling}h — energi terkumpul sangat lama")
    elif coiling >= 8:
        cs  = 10
        sigs.append(f"Coiling {coiling}h")
    elif coiling >= 3:
        cs  = 5
        sigs.append(f"Mulai diam {coiling}h")

    score += cs
    bd["coiling_score"] = cs

    # ══════════════════════════════════════════════════════
    #  🔄  SECTOR ROTATION (max 20)
    # ══════════════════════════════════════════════════════
    bd["sector_score"] = 0  # akan diisi di master_score

    # ══════════════════════════════════════════════════════
    #  📱  SOCIAL BONUS (max 15)
    # ══════════════════════════════════════════════════════
    bd["social_score"] = 0  # akan diisi di master_score

    bd["raw_base_score"] = score
    return score, bd, sigs, None  # None = tidak ada gate


# ══════════════════════════════════════════════════════════════
#  🐋  WHALE DETECTION
# ══════════════════════════════════════════════════════════════

def calc_whale(symbol, candles_15m, funding):
    ws = 0
    ev = []
    cur = candles_15m[-1]["close"] if candles_15m else 0

    trades = get_trades(symbol)
    if trades:
        buy_v = sum(t["size"] for t in trades if t["side"] == "buy")
        tot_v = sum(t["size"] for t in trades)
        tr    = buy_v / tot_v if tot_v > 0 else 0.5
        if tr > 0.70:
            ws += 30
            ev.append(f"✅ Taker Buy {tr:.0%} — pembeli dominan")
        elif tr > 0.60:
            ws += 15
            ev.append(f"🔶 Taker Buy {tr:.0%} — bias beli")

        # Large trade dominance
        total_usd = sum(t["size"] * t["price"] for t in trades)
        lbuy_usd  = sum(t["size"] * t["price"] for t in trades
                        if t["side"] == "buy" and t["size"] * t["price"] > 10_000)
        if total_usd > 0 and lbuy_usd / total_usd > 0.35:
            ws += 25
            ev.append(f"✅ Smart money {lbuy_usd/total_usd:.0%} vol (${lbuy_usd:,.0f})")

        # Iceberg
        at_level = [t for t in trades if t["side"] == "buy"
                    and cur > 0 and abs(t["price"] - cur) / cur * 100 < 0.2]
        if len(at_level) >= 10:
            tot_ice = sum(t["size"] * t["price"] for t in at_level)
            if tot_ice > 15_000:
                ws += 20
                ev.append(f"✅ Iceberg: ${tot_ice:,.0f} ({len(at_level)} tx kecil)")

    # Harga flat 4h = stealth
    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15
            ev.append("✅ Harga sangat flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7
            ev.append("🔶 Harga relatif flat 4h")

    # Funding negatif kecil = bonus whale
    if -0.0004 <= funding <= -0.00001:
        ws += 10
        ev.append(f"✅ Funding {funding:.5f}% — short squeeze tersembunyi")

    ws  = min(ws, 100)
    cls = ("🐋 WHALE ACCUMULATION" if ws >= 65
           else "🦈 SMART MONEY"    if ws >= 40
           else "👀 POSSIBLE INST." if ws >= 15
           else "🔇 NO SIGNAL")
    return ws, cls, ev


# ══════════════════════════════════════════════════════════════
#  🔄  SECTOR + SOCIAL + TIME
# ══════════════════════════════════════════════════════════════

def layer_sector(symbol, tickers_dict):
    sector = SECTOR_LOOKUP.get(symbol, "MISC")
    peers  = SECTOR_MAP.get(sector, [])
    pumped = [(p.replace("USDT",""), float(tickers_dict[p].get("change24h",0))*100)
              for p in peers if p != symbol and p in tickers_dict
              and float(tickers_dict[p].get("change24h",0))*100 > 8]
    pumped.sort(key=lambda x: x[1], reverse=True)
    if pumped:
        top = pumped[0]
        sc  = 20 if top[1] > 20 else 12 if top[1] > 10 else 6
        sig = f"🔄 Sektor {sector}: {top[0]} +{top[1]:.0f}% — rotasi mungkin"
        return sc, sig, sector
    return 0, "", sector

def layer_social(symbol):
    name = symbol.replace("USDT","").replace("1000","").upper()
    if name in get_cg_trending():
        return 15, f"🔥 {name} trending CoinGecko"
    return 0, ""

def get_time_mult():
    h = utc_hour()
    if h in [5,6,7,8,11,12,13,19,20,21]:
        return 1.20, f"⏰ High-prob window ({h}:00 UTC)"
    if h in [1,2,3,4]:
        return 0.80, f"Low-prob window"
    return 1.0, ""


# ══════════════════════════════════════════════════════════════
#  🎯  ENTRY ZONE — DIJAMIN VALID
# ══════════════════════════════════════════════════════════════

def calc_entry(candles_1h):
    cur = candles_1h[-1]["close"]

    # Support
    vwap, z1 = calc_vwap_support(candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h)
    z2_raw   = calc_poc(candles_1h[-48:] if len(candles_1h) >= 48 else candles_1h)
    z2       = z2_raw if z2_raw and z2_raw < cur * 0.999 else cur * 0.98

    support = max(z1 or cur*0.97, z2)
    if support >= cur:
        support = cur * 0.97
    entry = min(support * 1.002, cur * 0.998)
    sl    = support * 0.967

    # Target
    t1, t2  = find_targets(candles_1h, cur)
    if t1 <= cur * 1.05:
        t1 = cur * 1.10
    if t2 <= t1:
        t2 = t1 * 1.08

    risk    = cur - sl
    reward  = t1 - cur
    rr      = round(reward / risk, 1) if risk > 0 else 0
    t1_pct  = round((t1 - cur) / cur * 100, 1)

    return {"cur": cur, "vwap": round(vwap,8) if vwap else 0,
            "z1": round(z1,8) if z1 else 0, "z2": round(z2,8),
            "entry": round(entry,8), "sl": round(sl,8),
            "t1": round(t1,8), "t2": round(t2,8),
            "rr": rr, "liq_pct": t1_pct}


# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════

def master_score(symbol, ticker, tickers_dict):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_limit_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_limit_15m"])

    if len(c1h) < 48 or len(c15m) < 20:
        return None

    funding = get_funding(symbol)

    # Core forensic scoring
    score, bd, sigs, gate = calc_forensic_score(symbol, c1h, c15m, ticker, funding)
    if gate:
        log.info(f"  {symbol}: {gate}")
        return None

    # Sector rotation
    sec_sc, sec_sig, sector = layer_sector(symbol, tickers_dict)
    if sec_sig:
        sigs.append(sec_sig)
    score   += sec_sc
    bd["sector_score"] = sec_sc

    # Social
    soc_sc, soc_sig = layer_social(symbol)
    if soc_sig:
        sigs.append(soc_sig)
    score += soc_sc
    bd["social_score"] = soc_sc

    # Whale
    ws, wcls, wev = calc_whale(symbol, c15m, funding)
    whale_bonus = 18 if ws >= 65 else 10 if ws >= 40 else 0
    score += whale_bonus
    bd["whale_bonus"] = whale_bonus

    # Time multiplier
    tmult, tsig = get_time_mult()
    score = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    score = min(score, 100)

    # Entry zone
    entry = calc_entry(c1h)

    # Skip jika target terlalu dekat
    if entry["liq_pct"] < CONFIG["min_target_distance_pct"]:
        log.info(f"  {symbol}: target hanya +{entry['liq_pct']:.1f}%, skip")
        return None

    try:
        price_now   = float(ticker.get("lastPr", 0))
        chg_24h     = float(ticker.get("change24h", 0)) * 100
        vol_24h_usd = float(ticker.get("quoteVolume", 0))
    except:
        price_now   = c1h[-1]["close"]
        chg_24h     = 0
        vol_24h_usd = 0

    return {
        "symbol":  symbol, "score": score, "signals": sigs,
        "ws": ws, "wcls": wcls, "wev": wev,
        "entry": entry, "sector": sector, "funding": funding,
        "bd": bd, "price": price_now, "chg_24h": chg_24h, "vol_24h": vol_24h_usd,
    }


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════

def build_alert(r, rank=None):
    sc  = r["score"]
    bar = "█" * int(sc/5) + "░" * (20 - int(sc/5))
    e   = r["entry"]
    rk  = f"#{rank} " if rank else ""
    vol = (f"${r['vol_24h']/1e6:.1f}M" if r['vol_24h'] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K")

    msg = (f"🚨 <b>PRE-PUMP INTELLIGENCE {rk}</b>\n\n"
           f"<b>Symbol :</b> {r['symbol']}\n"
           f"<b>Score  :</b> {sc}/100  {bar}\n"
           f"<b>Sektor :</b> {r['sector']}\n"
           f"<b>Harga  :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h)\n"
           f"<b>Vol 24h:</b> {vol}\n\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🐋 <b>WHALE: {r['ws']}/100</b>\n"
           f"<i>{r['wcls']}</i>\n")
    for ev in r["wev"]:
        msg += f"  {ev}\n"

    if e:
        msg += (f"\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📍 <b>ENTRY ZONES</b>\n"
                f"  🟢 VWAP  : ${e['z1']}\n"
                f"  🟢 POC   : ${e['z2']}\n"
                f"  📌 Entry : ${e['entry']}\n"
                f"  🛑 SL    : ${e['sl']}\n\n"
                f"🎯 <b>TARGET</b>\n"
                f"  T1 : ${e['t1']}  (+{e['liq_pct']:.1f}%)\n"
                f"  T2 : ${e['t2']}\n"
                f"  R/R: 1:{e['rr']}\n")

    msg += f"\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL</b>\n"
    for s in r["signals"][:8]:
        msg += f"  • {s}\n"

    bd = r.get("bd", {})
    msg += (f"\n📐 <b>BREAKDOWN</b>\n"
            f"  Vol:{bd.get('vol_score',0)} BBW:{bd.get('bbw_score',0)} "
            f"Price:{bd.get('price_score',0)} Fund:{bd.get('funding_score',0)} "
            f"Coil:{bd.get('coiling_score',0)} Sector:{bd.get('sector_score',0)} "
            f"Whale:{bd.get('whale_bonus',0)}\n\n"
            f"⏰ Window: 1-6 jam  📡 Funding: {r['funding']:.5f}%\n"
            f"🕐 {utc_now()}\n\n"
            f"<i>Bukan financial advice. Manage risk.</i>")
    return msg

def build_summary(results):
    msg = f"📋 <b>SCAN — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        bar  = "█" * int(r['score']/10) + "░" * (10-int(r['score']/10))
        vol  = (f"${r['vol_24h']/1e6:.1f}M" if r['vol_24h'] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K")
        t1p  = r['entry']['liq_pct'] if r['entry'] else 0
        msg += (f"{i}. <b>{r['symbol']}</b> [{r['score']}/100 {bar}]\n"
                f"   🐋{r['ws']} | {vol} | T1:+{t1p:.0f}% | {r['chg_24h']:+.1f}%\n")
    return msg


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════

def run_scan():
    log.info(f"=== SCANNER v5.0 FORENSIC EDITION — {utc_now()} ===")
    log.info(f"Total target: {len(TARGET_COINS)} | Vol filter: ${CONFIG['min_volume_usd_24h']:,}-${CONFIG['max_volume_usd_24h']:,}/h")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ <b>Scanner Error</b>: Gagal ambil ticker Bitget")
        return

    # Pre-filter
    candidates = []
    for sym in TARGET_COINS:
        t = tickers.get(sym)
        if not t or is_cooldown(sym):
            continue
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = abs(float(t.get("change24h", 0)) * 100)
            prc = float(t.get("lastPr", 0))
        except:
            continue
        if vol < CONFIG["pre_filter_min_vol"] * 24:
            continue
        if chg > CONFIG["pre_filter_max_pump"]:
            continue
        if prc <= 0:
            continue
        candidates.append(sym)

    log.info(f"Pre-filter lolos: {len(candidates)} coin")

    results = []
    for i, sym in enumerate(candidates):
        t = tickers.get(sym)
        try:
            vol_usd = float(t.get("quoteVolume", 0))
            chg     = abs(float(t.get("change24h", 0)) * 100)
        except:
            continue

        if vol_usd < CONFIG["min_volume_usd_24h"] * 24:
            continue
        if chg > 20:
            continue

        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol_usd/1e3:.0f}K/hari)...")

        try:
            res = master_score(sym, t, tickers)
            if res:
                log.info(f"  Score={res['score']} Whale={res['ws']} T1=+{res['entry']['liq_pct']:.1f}%")
                if res["score"] >= CONFIG["min_score_alert"]:
                    results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_between_coins"])

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"Kandidat: {len(results)} coin")

    qualified = [r for r in results
                 if r["ws"] >= CONFIG["min_whale_score"] or r["score"] >= 65]

    if not qualified:
        log.info("Tidak ada sinyal yang memenuhi syarat")
        return

    top = qualified[:CONFIG["max_alerts_per_run"]]
    if len(top) > 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"✅ Alert #{rank}: {r['symbol']} S={r['score']} W={r['ws']}")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert ===")


if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        log.error("BOT_TOKEN / CHAT_ID tidak ada!")
        exit(1)
    run_scan()
