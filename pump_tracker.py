#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PUMP TRACKER v1.0 — Ground Truth Pump Event Logger                         ║
║                                                                              ║
║  Tujuan:                                                                     ║
║    Mencatat SEMUA coin yang pump ≥15% dalam 24 jam terakhir,                ║
║    TERLEPAS dari apakah scanner menghasilkan sinyal atau tidak.              ║
║                                                                              ║
║    Data ini digunakan untuk mengukur RECALL scanner:                         ║
║    → Berapa pump yang berhasil ditangkap (true positive)                     ║
║    → Berapa pump yang luput sama sekali (false negative / missed pump)       ║
║                                                                              ║
║  Cara pakai:                                                                 ║
║    python pump_tracker.py              → catat pump events sekarang          ║
║    python pump_tracker.py --report     → tampilkan recall report             ║
║    python pump_tracker.py --days 7     → report 7 hari terakhir             ║
║                                                                              ║
║  Dijalankan paralel dengan scanner di GitHub Actions — tidak mengubah        ║
║  scanner sama sekali.                                                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_history.db")
BITGET_BASE = "https://api.bitget.com"

PUMP_THRESHOLD   = 15.0   # % minimum untuk dianggap pump
PUMP_THRESHOLD_M =  8.0   # % medium pump — dicatat tapi label berbeda
SCAN_WINDOW_H    = 24     # jam lookback untuk deteksi pump

# Whitelist sama dengan scanner — coin yang dipantau
WHITELIST = [
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
]


# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def init_pump_table(conn):
    """Tambah tabel pump_events ke DB yang sama dengan scanner."""
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS pump_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol       TEXT NOT NULL,
            detected_at  INTEGER NOT NULL,
            pump_start   INTEGER,          -- estimasi kapan pump mulai (candle low)
            chg_24h      REAL,             -- % perubahan 24h saat deteksi
            chg_4h       REAL,             -- % perubahan 4h saat deteksi
            chg_1h       REAL,             -- % perubahan 1h saat deteksi
            price_now    REAL,
            price_24h_ago REAL,
            vol_24h      REAL,
            label        TEXT,             -- 'PUMP_MAJOR' / 'PUMP_MEDIUM' / 'PUMP_SMALL'
            scanner_alerted INTEGER DEFAULT 0,  -- 1 jika scanner sempat beri sinyal
            signal_id    INTEGER DEFAULT NULL,  -- FK ke signal_outcomes.id jika ada
            hours_before_peak REAL DEFAULT NULL -- berapa jam sebelum pump scanner alert
        )
    """)
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pump_sym_time
        ON pump_events(symbol, detected_at)
    """)

    # Tambah kolom baru ke signal_outcomes jika belum ada
    existing = {row[1] for row in c.execute("PRAGMA table_info(signal_outcomes)")}
    if "missed_pump" not in existing:
        c.execute("ALTER TABLE signal_outcomes ADD COLUMN missed_pump INTEGER DEFAULT 0")
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
#  BITGET DATA FETCH
# ══════════════════════════════════════════════════════════════════════════════

def get_tickers() -> Dict[str, dict]:
    """Fetch semua ticker dari Bitget USDT-Futures."""
    try:
        resp = requests.get(
            f"{BITGET_BASE}/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
            timeout=15
        )
        data = resp.json()
        if data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}
    except Exception as e:
        print(f"  ⚠️  get_tickers error: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
#  PUMP DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_pumps(tickers: Dict[str, dict]) -> List[dict]:
    """
    Deteksi semua coin yang pump signifikan dari ticker data.
    Pakai change24h (decimal) dari Bitget ticker.
    """
    pumps = []
    now = int(time.time())

    for sym in WHITELIST:
        ticker = tickers.get(sym)
        if not ticker:
            continue

        try:
            price = float(ticker.get("lastPr", 0) or 0)
            vol   = float(ticker.get("quoteVolume", 0) or 0)
            if price <= 0 or vol < 100_000:   # skip volume terlalu kecil
                continue

            # change24h adalah decimal di Bitget: 0.239 = +23.9%
            raw_chg = ticker.get("change24h")
            if raw_chg is None:
                continue
            chg_24h = float(raw_chg) * 100

            # Hanya catat pump ke atas
            if chg_24h < PUMP_THRESHOLD_M:
                continue

            # Label berdasarkan magnitude
            if chg_24h >= 50:
                label = "PUMP_MASSIVE"
            elif chg_24h >= PUMP_THRESHOLD:
                label = "PUMP_MAJOR"
            else:
                label = "PUMP_MEDIUM"

            # Estimasi harga 24h lalu dari open24h
            price_24h_ago = float(ticker.get("open24h", 0) or 0)

            pumps.append({
                "symbol":       sym,
                "detected_at":  now,
                "chg_24h":      round(chg_24h, 2),
                "chg_1h":       None,   # tidak tersedia dari ticker
                "chg_4h":       None,
                "price_now":    price,
                "price_24h_ago": price_24h_ago if price_24h_ago > 0 else None,
                "vol_24h":      vol,
                "label":        label,
            })

        except Exception:
            continue

    return pumps


# ══════════════════════════════════════════════════════════════════════════════
#  CROSS-REFERENCE DENGAN SCANNER SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

def crossref_with_signals(conn, pumps: List[dict]) -> List[dict]:
    """
    Untuk tiap pump event, cek apakah scanner pernah beri sinyal
    dalam 6 jam sebelum pump terdeteksi.
    """
    c = conn.cursor()
    window = 6 * 3600   # 6 jam sebelum pump

    for pump in pumps:
        sym = pump["symbol"]
        detected = pump["detected_at"]

        # Cari sinyal scanner untuk coin ini dalam window 6h sebelum pump
        c.execute("""
            SELECT id, alerted_at, score, phase
            FROM signal_outcomes
            WHERE symbol=? AND alerted_at BETWEEN ? AND ?
            ORDER BY alerted_at DESC LIMIT 1
        """, (sym, detected - window, detected))
        row = c.fetchone()

        if row:
            signal_id, alerted_at, score, phase = row
            hours_before = round((detected - alerted_at) / 3600, 1)
            pump["scanner_alerted"]    = 1
            pump["signal_id"]          = signal_id
            pump["hours_before_peak"]  = hours_before
            pump["signal_score"]       = score
            pump["signal_phase"]       = phase
        else:
            pump["scanner_alerted"]    = 0
            pump["signal_id"]          = None
            pump["hours_before_peak"]  = None
            pump["signal_score"]       = None
            pump["signal_phase"]       = None

    return pumps


def save_pumps(conn, pumps: List[dict]) -> int:
    """Simpan pump events ke DB, skip duplikat."""
    c = conn.cursor()
    saved = 0
    for p in pumps:
        try:
            c.execute("""
                INSERT OR IGNORE INTO pump_events
                (symbol, detected_at, chg_24h, chg_1h, chg_4h,
                 price_now, price_24h_ago, vol_24h, label,
                 scanner_alerted, signal_id, hours_before_peak)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                p["symbol"], p["detected_at"], p["chg_24h"],
                p.get("chg_1h"), p.get("chg_4h"),
                p["price_now"], p.get("price_24h_ago"), p["vol_24h"],
                p["label"], p["scanner_alerted"],
                p.get("signal_id"), p.get("hours_before_peak"),
            ))
            if c.rowcount > 0:
                saved += 1
        except Exception as e:
            print(f"  ⚠️  save_pumps error {p['symbol']}: {e}")
    conn.commit()
    return saved


# ══════════════════════════════════════════════════════════════════════════════
#  RECALL REPORT
# ══════════════════════════════════════════════════════════════════════════════

def print_recall_report(conn, days: int = 7):
    """Cetak recall report — berapa pump yang ditangkap vs terlewat."""
    c = conn.cursor()
    cutoff = int(time.time()) - days * 86400

    print("\n" + "═" * 65)
    print("  🎯 PUMP TRACKER — RECALL REPORT")
    print("═" * 65)

    # ── Overall recall ────────────────────────────────────────────
    c.execute("""
        SELECT COUNT(*), SUM(scanner_alerted),
               COUNT(CASE WHEN label='PUMP_MAJOR' OR label='PUMP_MASSIVE' THEN 1 END),
               SUM(CASE WHEN (label='PUMP_MAJOR' OR label='PUMP_MASSIVE')
                        AND scanner_alerted=1 THEN 1 ELSE 0 END)
        FROM pump_events WHERE detected_at >= ?
    """, (cutoff,))
    row = c.fetchone()
    if not row or not row[0]:
        print(f"\n  Belum ada pump events dalam {days} hari terakhir.")
    else:
        total, caught, major, major_caught = row
        caught = caught or 0
        major_caught = major_caught or 0
        missed = total - caught
        print(f"\n  PERIODE: {days} hari terakhir")
        print(f"\n  SEMUA PUMP (≥{PUMP_THRESHOLD_M}%):")
        print(f"    Total pump events : {total}")
        print(f"    Scanner alert     : {caught} ({caught/total*100:.0f}%)")
        print(f"    Missed (luput)    : {missed} ({missed/total*100:.0f}%)")
        if major:
            print(f"\n  PUMP MAJOR (≥{PUMP_THRESHOLD}%):")
            print(f"    Total             : {major}")
            print(f"    Scanner alert     : {major_caught} ({major_caught/major*100:.0f}%)")
            print(f"    Missed            : {major - major_caught} ({(major-major_caught)/major*100:.0f}%)")

    # ── Missed pumps detail ───────────────────────────────────────
    c.execute("""
        SELECT symbol, datetime(detected_at,'unixepoch','localtime'),
               chg_24h, label, vol_24h
        FROM pump_events
        WHERE scanner_alerted=0 AND detected_at >= ?
          AND (label='PUMP_MAJOR' OR label='PUMP_MASSIVE')
        ORDER BY chg_24h DESC LIMIT 20
    """, (cutoff,))
    rows = c.fetchall()
    if rows:
        print(f"\n  MISSED PUMPS — MAJOR (terlewat scanner, {len(rows)} terbaru):")
        for sym, dt, chg, label, vol in rows:
            vol_str = f"${vol/1e6:.1f}M" if vol >= 1e6 else f"${vol/1e3:.0f}K"
            print(f"    {sym:16s}  {chg:+6.1f}%  {vol_str:8s}  {dt}")

    # ── Caught pumps detail ───────────────────────────────────────
    c.execute("""
        SELECT pe.symbol, datetime(pe.detected_at,'unixepoch','localtime'),
               pe.chg_24h, pe.hours_before_peak, so.score, so.phase
        FROM pump_events pe
        LEFT JOIN signal_outcomes so ON pe.signal_id = so.id
        WHERE pe.scanner_alerted=1 AND pe.detected_at >= ?
        ORDER BY pe.detected_at DESC LIMIT 10
    """, (cutoff,))
    rows = c.fetchall()
    if rows:
        print(f"\n  CAUGHT PUMPS — scanner berhasil alert sebelumnya:")
        for sym, dt, chg, hrs, score, phase in rows:
            hrs_str = f"{hrs:.1f}h sebelum" if hrs is not None else "?"
            score_str = f"score={score}" if score else ""
            print(f"    {sym:16s}  {chg:+6.1f}%  alert {hrs_str}  {score_str} [{phase}]  {dt}")

    # ── Pump rate by label ────────────────────────────────────────
    c.execute("""
        SELECT label, COUNT(*), SUM(scanner_alerted), AVG(chg_24h)
        FROM pump_events WHERE detected_at >= ?
        GROUP BY label ORDER BY AVG(chg_24h) DESC
    """, (cutoff,))
    rows = c.fetchall()
    if rows:
        print(f"\n  PER LABEL:")
        for label, n, caught, avg_chg in rows:
            caught = caught or 0
            print(f"    [{label:14s}] {n:3d} events | "
                  f"caught={caught/n*100:.0f}% | avg={avg_chg:+.1f}%")

    print("\n" + "═" * 65 + "\n")


def print_latest_pumps(tickers: Dict[str, dict], top_n: int = 10):
    """Tampilkan pump terbesar saat ini dari ticker."""
    pumps = []
    for sym in WHITELIST:
        ticker = tickers.get(sym)
        if not ticker:
            continue
        try:
            raw = ticker.get("change24h")
            if raw is None:
                continue
            chg = float(raw) * 100
            vol = float(ticker.get("quoteVolume", 0) or 0)
            if chg >= PUMP_THRESHOLD_M and vol >= 100_000:
                pumps.append((sym, chg, vol))
        except Exception:
            continue

    pumps.sort(key=lambda x: -x[1])
    print(f"\n  TOP {top_n} PUMP SEKARANG (≥{PUMP_THRESHOLD_M}%):")
    for sym, chg, vol in pumps[:top_n]:
        vol_str = f"${vol/1e6:.1f}M" if vol >= 1e6 else f"${vol/1e3:.0f}K"
        print(f"    {sym:16s}  {chg:+6.1f}%  {vol_str}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Pump Tracker — Ground Truth Logger")
    parser.add_argument("--report", action="store_true",
                        help="Tampilkan recall report saja")
    parser.add_argument("--days",   type=int, default=7,
                        help="Jumlah hari untuk report (default: 7)")
    parser.add_argument("--db",     type=str, default=DB_PATH,
                        help=f"Path DB (default: {DB_PATH})")
    args = parser.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"❌ DB tidak ditemukan: {db_path}")
        print(f"   Pastikan scanner sudah pernah jalan.")
        return 1

    conn = sqlite3.connect(db_path)
    init_pump_table(conn)

    if args.report:
        print_recall_report(conn, days=args.days)
        conn.close()
        return 0

    # ── Fetch tickers ─────────────────────────────────────────────
    print("📡 Fetching Bitget tickers...")
    tickers = get_tickers()
    if not tickers:
        print("❌ Tidak bisa fetch tickers dari Bitget")
        conn.close()
        return 1

    print_latest_pumps(tickers)

    # ── Detect pumps ──────────────────────────────────────────────
    pumps = detect_pumps(tickers)
    print(f"\n🔍 Detected {len(pumps)} pump events (≥{PUMP_THRESHOLD_M}%)")

    if not pumps:
        print("  Tidak ada pump signifikan saat ini.")
        print_recall_report(conn, days=args.days)
        conn.close()
        return 0

    # ── Cross-reference dengan scanner signals ────────────────────
    pumps = crossref_with_signals(conn, pumps)

    # ── Print summary sebelum save ────────────────────────────────
    caught  = [p for p in pumps if p["scanner_alerted"]]
    missed  = [p for p in pumps if not p["scanner_alerted"]]
    major_missed = [p for p in missed if p["chg_24h"] >= PUMP_THRESHOLD]

    if caught:
        print(f"\n  ✅ CAUGHT ({len(caught)}) — scanner sempat alert:")
        for p in sorted(caught, key=lambda x: -x["chg_24h"]):
            hrs = p.get("hours_before_peak")
            hrs_str = f"{hrs:.1f}h sebelum" if hrs else "?"
            print(f"    {p['symbol']:16s}  {p['chg_24h']:+6.1f}%  "
                  f"alert {hrs_str}  score={p.get('signal_score','?')}")

    if major_missed:
        print(f"\n  ❌ MISSED MAJOR ({len(major_missed)}) — pump ≥{PUMP_THRESHOLD}% tanpa sinyal:")
        for p in sorted(major_missed, key=lambda x: -x["chg_24h"]):
            vol_str = f"${p['vol_24h']/1e6:.1f}M" if p['vol_24h'] >= 1e6 else f"${p['vol_24h']/1e3:.0f}K"
            print(f"    {p['symbol']:16s}  {p['chg_24h']:+6.1f}%  {vol_str}  ← MISSED")

    if missed and not major_missed:
        medium = [p for p in missed if p["chg_24h"] < PUMP_THRESHOLD]
        if medium:
            print(f"\n  🟡 MEDIUM MISSED ({len(medium)}) — pump {PUMP_THRESHOLD_M}-{PUMP_THRESHOLD}%:")
            for p in sorted(medium, key=lambda x: -x["chg_24h"])[:5]:
                print(f"    {p['symbol']:16s}  {p['chg_24h']:+6.1f}%")

    # ── Save ke DB ────────────────────────────────────────────────
    saved = save_pumps(conn, pumps)
    print(f"\n💾 {saved} pump events baru disimpan ke DB")

    # ── Recall report ─────────────────────────────────────────────
    print_recall_report(conn, days=args.days)

    conn.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
