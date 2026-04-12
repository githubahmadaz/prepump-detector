#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  OUTCOME ANALYZER v1.0 — Pre-Pump Scanner Signal Evaluator                  ║
║                                                                              ║
║  Cara pakai:                                                                 ║
║    python outcome_analyzer.py            → evaluate semua sinyal pending     ║
║    python outcome_analyzer.py --report   → tampilkan precision report        ║
║    python outcome_analyzer.py --hours 6  → evaluate sinyal dalam 6 jam      ║
║    python outcome_analyzer.py --all      → re-evaluate semua sinyal          ║
║                                                                              ║
║  Mekanisme:                                                                  ║
║    1. Ambil semua sinyal dari signal_outcomes DB                             ║
║    2. Fetch historical candle 1m/5m dari Bitget untuk tiap sinyal            ║
║    3. Hitung return_1h, return_2h, return_3h, max_return, hit_sl             ║
║    4. Update DB dengan hasil aktual                                           ║
║    5. Print summary report                                                   ║
║                                                                              ║
║  Jalankan kapan saja — misal jam 07:00 ada sinyal, jam 10:00 jalankan ini.   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

DB_PATH  = os.getenv("SCANNER_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_history.db"))
BITGET_BASE = "https://api.bitget.com"


# ══════════════════════════════════════════════════════════════════════════════
#  BITGET HISTORICAL CANDLE FETCHER
# ══════════════════════════════════════════════════════════════════════════════

def fetch_candles_range(symbol: str, start_ts: int, end_ts: int,
                        granularity: str = "5m") -> List[dict]:
    """
    Fetch historical candles dari Bitget antara start_ts dan end_ts.
    start_ts, end_ts: Unix timestamp dalam detik.
    Bitget API pakai milliseconds — dikonversi di dalam fungsi ini.
    Granularity: '1m', '5m', '15m', '1H'
    """
    gran_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1H": "1H", "4H": "4H"}
    gran = gran_map.get(granularity, "5m")

    url = f"{BITGET_BASE}/api/v2/mix/market/history-candles"
    params = {
        "symbol":      symbol,
        "productType": "USDT-FUTURES",
        "granularity": gran,
        "startTime":   str(start_ts * 1000),   # Bitget pakai ms
        "endTime":     str(end_ts   * 1000),
        "limit":       "200",
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != "00000":
                return []
            candles = []
            for row in data.get("data", []):
                try:
                    candles.append({
                        "ts":    int(row[0]) // 1000,   # konversi ms → detik
                        "open":  float(row[1]),
                        "high":  float(row[2]),
                        "low":   float(row[3]),
                        "close": float(row[4]),
                        "vol":   float(row[5]),
                    })
                except Exception:
                    continue
            candles.sort(key=lambda x: x["ts"])
            return candles
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
    return []


def get_price_at(candles: List[dict], target_ts: int) -> Optional[float]:
    """Ambil close price terdekat ke target_ts dari candles."""
    if not candles:
        return None
    best = min(candles, key=lambda c: abs(c["ts"] - target_ts))
    # Hanya valid jika dalam 15 menit dari target
    if abs(best["ts"] - target_ts) > 15 * 60:
        return None
    return best["close"]


def get_max_price_in_range(candles: List[dict],
                            start_ts: int, end_ts: int) -> Optional[float]:
    """Ambil high tertinggi antara start_ts dan end_ts."""
    in_range = [c["high"] for c in candles if start_ts <= c["ts"] <= end_ts]
    return max(in_range) if in_range else None


def get_min_price_in_range(candles: List[dict],
                            start_ts: int, end_ts: int) -> Optional[float]:
    """Ambil low terendah antara start_ts dan end_ts."""
    in_range = [c["low"] for c in candles if start_ts <= c["ts"] <= end_ts]
    return min(in_range) if in_range else None


# ══════════════════════════════════════════════════════════════════════════════
#  EVALUATOR UTAMA
# ══════════════════════════════════════════════════════════════════════════════

def evaluate_signal(row: dict) -> Optional[dict]:
    """
    Evaluasi satu sinyal menggunakan historical candle data.
    Ambil candles dari alerted_at sampai alerted_at + 3h.
    Hitung: return_1h, return_2h, return_3h, max_return, hit_sl, hit_10pct, hit_15pct.
    """
    symbol      = row["symbol"]
    alerted_at  = row["alerted_at"]
    entry_price = row["entry_price"]
    sl_price    = row["sl_price"]
    tp1_price   = row["tp1_price"]

    if not entry_price or entry_price <= 0:
        return None

    now = int(time.time())
    window_end = alerted_at + 12 * 3600  # [SPRINT2-FIX-3] diperlebar ke 12h

    # Jika sinyal baru saja masuk dan belum 1 jam
    if now < alerted_at + 3600:
        return None

    # Fetch candles 5m dari alerted_at sampai window_end (atau sekarang jika belum 12h)
    fetch_end = min(window_end, now)
    candles = fetch_candles_range(symbol, alerted_at, fetch_end, granularity="5m")

    if not candles:
        # Fallback ke 15m jika 5m tidak tersedia
        candles = fetch_candles_range(symbol, alerted_at, fetch_end, granularity="15m")
    if not candles:
        print(f"    ⚠️  {symbol}: tidak ada candle data")
        return None

    # Harga di tiap window
    p1h  = get_price_at(candles, alerted_at + 1 * 3600)
    p2h  = get_price_at(candles, alerted_at + 2 * 3600)
    p3h  = get_price_at(candles, alerted_at + 3 * 3600)
    p6h  = get_price_at(candles, alerted_at + 6 * 3600)
    p12h = get_price_at(candles, alerted_at + 12 * 3600)

    ret_1h  = round((p1h  - entry_price) / entry_price * 100, 2) if p1h  else None
    ret_2h  = round((p2h  - entry_price) / entry_price * 100, 2) if p2h  else None
    ret_3h  = round((p3h  - entry_price) / entry_price * 100, 2) if p3h  else None
    ret_6h  = round((p6h  - entry_price) / entry_price * 100, 2) if p6h  else None
    ret_12h = round((p12h - entry_price) / entry_price * 100, 2) if p12h else None

    # Max price dalam window (untuk max_return)
    max_price = get_max_price_in_range(candles, alerted_at, fetch_end)
    max_ret   = round((max_price - entry_price) / entry_price * 100, 2) if max_price else None

    # Min price dalam window (untuk hit_sl check)
    min_price = get_min_price_in_range(candles, alerted_at, fetch_end)

    # Hit flags
    hit_15 = 1 if max_ret is not None and max_ret >= 15.0 else 0
    hit_10 = 1 if max_ret is not None and max_ret >= 10.0 else 0
    hit_sl = 1 if (sl_price and min_price and min_price <= sl_price) else 0

    # Selesai setelah 12h window
    is_done = (now >= window_end)

    return {
        "id":         row["id"],
        "symbol":     symbol,
        "return_1h":  ret_1h,
        "return_2h":  ret_2h,
        "return_3h":  ret_3h  if p3h  else None,
        "return_6h":  ret_6h  if p6h  else None,
        "return_12h": ret_12h if is_done else None,
        "max_return": max_ret,
        "hit_15pct":  hit_15  if is_done else None,
        "hit_10pct":  hit_10  if is_done else None,
        "hit_sl":     hit_sl,
        "checked":    1 if is_done else 0,
        "elapsed_h":  round((now - alerted_at) / 3600, 1),
        "is_done":    is_done,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  DB OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

def migrate_db(conn):
    """Tambahkan kolom baru jika belum ada (backward compat dengan DB lama)."""
    c = conn.cursor()
    existing = {row[1] for row in c.execute("PRAGMA table_info(signal_outcomes)")}
    new_cols = {
        "sl_price":       "REAL DEFAULT NULL",
        "tp1_price":      "REAL DEFAULT NULL",
        "tp2_price":      "REAL DEFAULT NULL",
        "sl_pct":         "REAL DEFAULT NULL",
        "tp1_pct":        "REAL DEFAULT NULL",
        "chg_1h_signal":  "REAL DEFAULT NULL",
        "chg_4h_signal":  "REAL DEFAULT NULL",
        "chg_24h_signal": "REAL DEFAULT NULL",
        "funding_signal": "REAL DEFAULT NULL",
        "vol_24h_signal": "REAL DEFAULT NULL",
        "tier1":          "INTEGER DEFAULT NULL",
        "tier2":          "INTEGER DEFAULT NULL",
        "tier3":          "INTEGER DEFAULT NULL",
        "return_6h":      "REAL DEFAULT NULL",
        "return_12h":     "REAL DEFAULT NULL",
        "max_return":     "REAL DEFAULT NULL",
        "hit_10pct":      "INTEGER DEFAULT NULL",
        "hit_sl":         "INTEGER DEFAULT NULL",
        "data_version":   "TEXT DEFAULT NULL",
    }
    for col, typedef in new_cols.items():
        if col not in existing:
            c.execute(f"ALTER TABLE signal_outcomes ADD COLUMN {col} {typedef}")

    # Tag data lama sebagai v1_3h
    c.execute("""
        UPDATE signal_outcomes SET data_version='v1_3h'
        WHERE checked=1 AND data_version IS NULL
    """)
    conn.commit()


def get_pending_signals(conn, hours_back: int = 72,
                        force_all: bool = False) -> List[dict]:
    """Ambil sinyal yang belum di-evaluate atau belum complete."""
    c = conn.cursor()
    cutoff = int(time.time()) - hours_back * 3600
    if force_all:
        c.execute("""
            SELECT id, symbol, alerted_at, entry_price, sl_price, tp1_price,
                   return_1h, return_2h, return_3h, checked, score, phase,
                   cat_a_score, cat_b_score, cat_c_score, cat_d_score, btc_regime
            FROM signal_outcomes
            WHERE alerted_at >= ? ORDER BY alerted_at DESC
        """, (cutoff,))
    else:
        c.execute("""
            SELECT id, symbol, alerted_at, entry_price, sl_price, tp1_price,
                   return_1h, return_2h, return_3h, checked, score, phase,
                   cat_a_score, cat_b_score, cat_c_score, cat_d_score, btc_regime
            FROM signal_outcomes
            WHERE (checked=0 OR return_3h IS NULL) AND alerted_at >= ?
            ORDER BY alerted_at DESC
        """, (cutoff,))
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, row)) for row in c.fetchall()]


def save_result(conn, result: dict):
    """Update signal_outcomes dengan hasil evaluasi."""
    c = conn.cursor()
    updates = []
    params  = []

    for field in ["return_1h", "return_2h", "return_3h", "return_6h", "return_12h",
                  "max_return", "hit_15pct", "hit_10pct", "hit_sl", "checked"]:
        if result.get(field) is not None:
            updates.append(f"{field}=?")
            params.append(result[field])

    if not updates:
        return

    params.append(result["id"])
    c.execute(f"UPDATE signal_outcomes SET {', '.join(updates)} WHERE id=?", params)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
#  PRECISION REPORT
# ══════════════════════════════════════════════════════════════════════════════

def print_report(conn):
    """Cetak precision report lengkap dari signal_outcomes."""
    c = conn.cursor()

    print("\n" + "═" * 65)
    print("  📊 OUTCOME ANALYZER — PRECISION REPORT")
    print("═" * 65)

    # ── Overall — pisah v1 (3h window) dan v2 (12h window) ──────
    # v1_3h: data lama, checked=1 berdasarkan 3h window
    # v2_12h: data baru, checked=1 berdasarkan 12h window
    # NULL data_version = sinyal baru yang belum selesai

    c.execute("SELECT COUNT(*) FROM signal_outcomes WHERE data_version='v1_3h'")
    n_v1 = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM signal_outcomes WHERE data_version='v2_12h'")
    n_v2 = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM signal_outcomes WHERE checked=0")
    n_pending = c.fetchone()[0]

    print(f"\n  DATA: {n_v1} sinyal v1(3h) | {n_v2} sinyal v2(12h) | {n_pending} pending")

    # Precision dari data v2 (12h) — lebih akurat
    c.execute("""
        SELECT COUNT(*), SUM(hit_15pct), SUM(hit_10pct), SUM(hit_sl),
               AVG(return_3h), AVG(return_6h), AVG(return_12h),
               AVG(max_return), AVG(return_1h)
        FROM signal_outcomes WHERE data_version='v2_12h'
    """)
    row = c.fetchone()
    if row and row[0]:
        n, h15, h10, hsl, avg3, avg6, avg12, avgmax, avg1 = row
        h15 = h15 or 0; h10 = h10 or 0; hsl = hsl or 0
        print(f"\n  OVERALL v2 — window 12h ({n} sinyal)")
        print(f"    Precision @15%  : {h15}/{n} = {h15/n*100:.1f}%")
        print(f"    Precision @10%  : {h10}/{n} = {h10/n*100:.1f}%")
        print(f"    Hit SL rate     : {hsl}/{n} = {hsl/n*100:.1f}%")
        print(f"    Avg return_1h   : {avg1:+.2f}%" if avg1 else "    Avg return_1h   : N/A")
        print(f"    Avg return_3h   : {avg3:+.2f}%" if avg3 else "    Avg return_3h   : N/A")
        print(f"    Avg return_6h   : {avg6:+.2f}%" if avg6 else "    Avg return_6h   : N/A")
        print(f"    Avg return_12h  : {avg12:+.2f}%" if avg12 else "    Avg return_12h  : N/A")
        print(f"    Avg max_return  : {avgmax:+.2f}%" if avgmax else "    Avg max_return  : N/A")
    else:
        print(f"\n  OVERALL v2 — belum ada sinyal v2 selesai (window 12h)")

    # Precision dari data v1 (3h) — referensi historis
    c.execute("""
        SELECT COUNT(*), SUM(hit_15pct), SUM(hit_10pct), SUM(hit_sl),
               AVG(return_3h), AVG(max_return), AVG(return_1h)
        FROM signal_outcomes WHERE data_version='v1_3h'
    """)
    row = c.fetchone()
    if row and row[0]:
        n, h15, h10, hsl, avg3, avgmax, avg1 = row
        h15 = h15 or 0; h10 = h10 or 0; hsl = hsl or 0
        print(f"\n  REFERENSI v1 — window 3h ({n} sinyal lama, sebelum Sprint 2)")
        print(f"    Precision @15%  : {h15}/{n} = {h15/n*100:.1f}%")
        print(f"    Precision @10%  : {h10}/{n} = {h10/n*100:.1f}%")
        print(f"    Avg return_3h   : {avg3:+.2f}%" if avg3 else "    Avg return_3h   : N/A")
        print(f"    Avg max_return  : {avgmax:+.2f}%" if avgmax else "    Avg max_return  : N/A")

    # ── Per Phase ─────────────────────────────────────────────────
    c.execute("""
        SELECT phase, COUNT(*), SUM(hit_15pct), SUM(hit_10pct),
               AVG(return_3h), AVG(max_return)
        FROM signal_outcomes WHERE checked=1
        GROUP BY phase ORDER BY COUNT(*) DESC
    """)
    rows = c.fetchall()
    if rows:
        print("\n  PER PHASE:")
        for phase, n, h15, h10, avg3, avgmax in rows:
            h15 = h15 or 0; h10 = h10 or 0
            print(f"    [{phase:12s}] {n:3d} sinyal | "
                  f"@15%={h15/n*100:.0f}% | @10%={h10/n*100:.0f}% | "
                  f"avg3h={avg3:+.1f}% | maxR={avgmax:+.1f}%" if avg3 else
                  f"    [{phase:12s}] {n:3d} sinyal | @15%={h15/n*100:.0f}% | @10%={h10/n*100:.0f}%")

    # ── CAT-B = 0 vs > 0 ──────────────────────────────────────────
    c.execute("""
        SELECT
            CASE WHEN cat_b_score >= 8 THEN 'B>=8' ELSE 'B<8' END as b_group,
            COUNT(*), SUM(hit_15pct), AVG(return_3h), AVG(max_return)
        FROM signal_outcomes WHERE checked=1
        GROUP BY b_group
    """)
    rows = c.fetchall()
    if rows:
        print("\n  CAT-B SPLIT (threshold 8):")
        for grp, n, h15, avg3, avgmax in rows:
            h15 = h15 or 0
            avg3_str  = f"{avg3:+.1f}%"  if avg3  else "N/A"
            avgmax_str = f"{avgmax:+.1f}%" if avgmax else "N/A"
            print(f"    [{grp}] {n:3d} sinyal | "
                  f"precision={h15/n*100:.0f}% | avg3h={avg3_str} | maxR={avgmax_str}")

    # ── BTC Regime ────────────────────────────────────────────────
    c.execute("""
        SELECT btc_regime, COUNT(*), SUM(hit_15pct), AVG(return_3h)
        FROM signal_outcomes WHERE checked=1
        GROUP BY btc_regime ORDER BY COUNT(*) DESC
    """)
    rows = c.fetchall()
    if rows:
        print("\n  PER BTC REGIME:")
        for regime, n, h15, avg3 in rows:
            h15 = h15 or 0
            avg3_str = f"{avg3:+.1f}%" if avg3 else "N/A"
            print(f"    [{regime:8s}] {n:3d} sinyal | "
                  f"precision={h15/n*100:.0f}% | avg3h={avg3_str}")

    # ── Sinyal Pending ────────────────────────────────────────────
    c.execute("""
        SELECT symbol, datetime(alerted_at,'unixepoch','localtime'),
               score, phase, cat_a_score, cat_b_score, cat_c_score, cat_d_score,
               return_1h, return_2h
        FROM signal_outcomes WHERE checked=0
        ORDER BY alerted_at DESC LIMIT 10
    """)
    rows = c.fetchall()
    if rows:
        print(f"\n  SINYAL PENDING ({len(rows)} terbaru, belum 3h):")
        for sym, dt, sc, ph, a, b, cc, d, r1, r2 in rows:
            r1_str = f"r1h={r1:+.1f}%" if r1 is not None else "r1h=..."
            r2_str = f"r2h={r2:+.1f}%" if r2 is not None else "r2h=..."
            print(f"    {sym:16s} [{ph:12s}] score={sc} "
                  f"A={a} B={b} C={cc} D={d} | {r1_str} {r2_str} | {dt}")

    print("\n" + "═" * 65 + "\n")


def print_signal_detail(conn, hours_back: int = 24):
    """Cetak detail tiap sinyal dalam N jam terakhir dengan outcome-nya."""
    c = conn.cursor()
    cutoff = int(time.time()) - hours_back * 3600
    c.execute("""
        SELECT symbol, alerted_at, score, phase,
               cat_a_score, cat_b_score, cat_c_score, cat_d_score,
               entry_price, sl_price, tp1_price,
               return_1h, return_2h, return_3h, return_6h, return_12h, max_return,
               hit_15pct, hit_10pct, hit_sl, checked,
               chg_1h_signal, chg_4h_signal, funding_signal, btc_regime
        FROM signal_outcomes
        WHERE alerted_at >= ? ORDER BY alerted_at DESC
    """, (cutoff,))
    rows = c.fetchall()
    if not rows:
        print(f"  Tidak ada sinyal dalam {hours_back} jam terakhir.")
        return

    print(f"\n  DETAIL SINYAL — {hours_back} JAM TERAKHIR ({len(rows)} sinyal)")
    print("  " + "─" * 63)
    for row in rows:
        (sym, alerted_at, score, phase,
         a, b, cc, d,
         entry, sl, tp1,
         r1, r2, r3, r6, r12, maxr,
         h15, h10, hsl, checked,
         c1h, c4h, fund, regime) = row

        dt_wib = datetime.fromtimestamp(alerted_at + 7*3600, tz=timezone.utc)
        dt_str = dt_wib.strftime("%m/%d %H:%M WIB")

        status = "✅ HIT15" if h15 else ("🟡 HIT10" if h10 else ("🔴 HIT_SL" if hsl else ("⏳ PENDING" if not checked else "❌ MISS")))

        print(f"\n  {status}  {sym:16s}  Score:{score:3d}  [{phase}]  {dt_str}")
        print(f"    A={a} B={b} C={cc} D={d}  |  BTC={regime}")
        sl_str  = f"{sl:.6f}"  if sl  else "N/A"
        tp1_str = f"{tp1:.6f}" if tp1 else "N/A"
        print(f"    Entry={entry:.6f}  SL={sl_str}  TP1={tp1_str}")

        fund_str = f"{fund*100:.4f}%" if fund else "N/A"
        c1h_str  = f"{c1h:+.1f}%" if c1h is not None else "N/A"
        c4h_str  = f"{c4h:+.1f}%" if c4h is not None else "N/A"
        print(f"    Δ1h={c1h_str}  Δ4h={c4h_str}  Fund={fund_str}")

        r1_str   = f"{r1:+.1f}%"   if r1   is not None else "..."
        r2_str   = f"{r2:+.1f}%"   if r2   is not None else "..."
        r3_str   = f"{r3:+.1f}%"   if r3   is not None else "..."
        r6_str   = f"{r6:+.1f}%"   if r6   is not None else "..."
        r12_str  = f"{r12:+.1f}%"  if r12  is not None else "..."
        maxr_str = f"{maxr:+.1f}%" if maxr  is not None else "..."
        print(f"    Return: 1h={r1_str}  2h={r2_str}  3h={r3_str}  6h={r6_str}  12h={r12_str}  max={maxr_str}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Outcome Analyzer for Pre-Pump Scanner")
    parser.add_argument("--report",  action="store_true",
                        help="Tampilkan precision report saja (tanpa fetch data baru)")
    parser.add_argument("--hours",   type=int, default=72,
                        help="Evaluasi sinyal dalam N jam terakhir (default: 72)")
    parser.add_argument("--detail",  type=int, default=24,
                        help="Tampilkan detail sinyal N jam terakhir (default: 24)")
    parser.add_argument("--all",     action="store_true",
                        help="Re-evaluate semua sinyal (bukan hanya pending)")
    parser.add_argument("--db",      type=str, default=DB_PATH,
                        help=f"Path ke DB file (default: {DB_PATH})")
    args = parser.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"❌ DB tidak ditemukan: {db_path}")
        print(f"   Pastikan scanner sudah pernah jalan dan menghasilkan sinyal.")
        print(f"   Atau set path dengan: --db /path/ke/scanner_v16_history.db")
        return 1

    conn = sqlite3.connect(db_path)
    migrate_db(conn)  # tambah kolom baru jika DB lama

    if args.report:
        print_report(conn)
        print_signal_detail(conn, hours_back=args.detail)
        conn.close()
        return 0

    # ── Fetch & evaluate ─────────────────────────────────────────
    pending = get_pending_signals(conn, hours_back=args.hours, force_all=args.all)

    if not pending:
        print(f"✅ Tidak ada sinyal pending dalam {args.hours} jam terakhir.")
        print_report(conn)
        print_signal_detail(conn, hours_back=args.detail)
        conn.close()
        return 0

    print(f"\n🔍 Evaluating {len(pending)} sinyal...\n")

    evaluated = 0
    for row in pending:
        sym   = row["symbol"]
        dt_wib = datetime.fromtimestamp(row["alerted_at"] + 7*3600, tz=timezone.utc)
        elapsed = (time.time() - row["alerted_at"]) / 3600
        print(f"  [{dt_wib.strftime('%H:%M WIB')}] {sym:16s} (elapsed {elapsed:.1f}h) ... ", end="", flush=True)

        result = evaluate_signal(row)
        if result is None:
            print("skip (belum 1h atau no data)")
            continue

        save_result(conn, result)
        evaluated += 1

        # Print ringkas hasil
        parts = []
        if result["return_1h"]  is not None: parts.append(f"r1h={result['return_1h']:+.1f}%")
        if result["return_2h"]  is not None: parts.append(f"r2h={result['return_2h']:+.1f}%")
        if result["return_3h"]  is not None: parts.append(f"r3h={result['return_3h']:+.1f}%")
        if result["return_6h"]  is not None: parts.append(f"r6h={result['return_6h']:+.1f}%")
        if result["return_12h"] is not None: parts.append(f"r12h={result['return_12h']:+.1f}%")
        if result["max_return"] is not None: parts.append(f"max={result['max_return']:+.1f}%")
        if result["hit_sl"]:    parts.append("🔴 HIT_SL")
        if result["hit_15pct"]: parts.append("✅ HIT15%")
        elif result["hit_10pct"]: parts.append("🟡 HIT10%")
        status = "DONE" if result["is_done"] else "partial"
        print(f"{status} | {' | '.join(parts)}")

        time.sleep(0.3)  # rate limit protection

    print(f"\n✅ {evaluated}/{len(pending)} sinyal di-update.")

    # ── Print report ─────────────────────────────────────────────
    print_report(conn)
    print_signal_detail(conn, hours_back=args.detail)

    conn.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
