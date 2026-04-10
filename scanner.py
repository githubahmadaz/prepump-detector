#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v16.0 — CONFLUENCE ARCHITECTURE                          ║
║                                                                              ║
║  PERBAIKAN DARI v15.6:                                                       ║
║                                                                              ║
║  [FIX-1] 🔴 KRITIS — Confluence check diimplementasi (check_confluence)     ║
║    Sinyal hanya lolos jika minimal 3 dari 4 kategori independen aktif:       ║
║    CAT-A (Derivatives), CAT-B (Order Flow), CAT-C (Price RS), CAT-D (Micro)  ║
║    Efek: memotong 60-70% false positive single-category                      ║
║                                                                              ║
║  [FIX-2] 🔴 KRITIS — Double-count BTC RS dihapus                            ║
║    btc_context_bonus dihapus. detect_rs_btc + detect_rs_24h digabung         ║
║    ke score_btc_decoupling() (CAT-C) dengan regime-aware scoring.            ║
║    Satu fenomena = satu skor. rs_sc maks 20 (bukan 53 sebelumnya).           ║
║                                                                              ║
║  [FIX-3] 🟠 SERIUS — Phase 1 tambah momentum pre-pump filter                ║
║    Tambah momentum_score: chg_4h dan chg_1h kontekstual.                    ║
║    Coin yang sedang distribusi aktif (turun 4h) diberi penalty.              ║
║    Phase 1 kini membedakan volatilitas biasa vs pre-pump setup.              ║
║                                                                              ║
║  [FIX-4] 🟠 SERIUS — BV ratio window diperlebar 6h → 12h                    ║
║    hist[-13:-1] menggantikan hist[-7:-1]. Menangkap whale accumulation       ║
║    yang berlangsung 12-48 jam sebelum pump.                                  ║
║                                                                              ║
║  [FIX-5] 🟡 MENENGAH — TP1 fallback berbasis RR adaptif, bukan fixed %      ║
║    TP fallback kini menggunakan resistance terdekat dari fractal high         ║
║    jika S/R engine tidak menemukan cukup levels. Tidak lagi pakai            ║
║    tp1_pct fixed 10% yang tidak mencerminkan struktur pasar.                 ║
║                                                                              ║
║  [BONUS] Precision tracking tabel signal_outcomes (wajib untuk iterasi)     ║
║  [BONUS] check_and_update_outcomes() diperluas: return_1h, return_2h, 3h    ║
║  [BONUS] get_precision_report() untuk evaluasi harian                        ║
║                                                                              ║
║  PERINGATAN WAJIB:                                                           ║
║  • Setiap perubahan threshold tanpa data live = overfitting                  ║
║  • Selalu gunakan walk-forward validation, bukan backtest biasa              ║
║  • Target Sprint 1: Precision 40-55%. Jalan ke 80% butuh 3 sprint.          ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import hashlib
import json
import logging
import logging.handlers
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "16.0.0-CONFLUENCE"


# ── Logging ───────────────────────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("scanner_v16")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.handlers.RotatingFileHandler(
        "/tmp/scanner_v16.log", maxBytes=10 * 1024 ** 2, backupCount=3
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


log = setup_logging()


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key":  os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":          os.getenv("BOT_TOKEN"),
    "chat_id":            os.getenv("CHAT_ID"),

    # Whitelist (coin yang akan dipindai)
    "whitelist": [
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
    ],

    # ── Phase 1: Bitget-only filter ───────────────────────────────────────────
    "phase1_threshold":         72,
    "phase1_min_volume_usd":    500_000,
    "phase1_weights": {
        "atr":       25,
        "range":     25,
        "bbw":       20,
        "wick":      15,
        "support":   15,
        "decel":     10,
        "momentum":  15,   # [FIX-3] baru: konteks momentum pre-pump
    },
    "phase1_atr_thresholds":      [3.5, 2.5, 1.8],
    "phase1_range_thresholds":    [4.0, 2.5, 1.8],
    "phase1_bbw_thresholds":      [0.15, 0.10, 0.07],
    "phase1_wick_thresholds":     [1.0, 0.65, 0.4],
    "phase1_support_dist_range":  (0.3, 1.5),
    "phase1_support_dist_wide":   (1.5, 3.0),
    "phase1_decel_thresholds":    [-0.30, -0.15, -0.05],

    # ── Phase 2: Final scoring thresholds ────────────────────────────────────
    "alert_threshold_early":        95,
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal":     80,

    # ── [FIX-1] Confluence thresholds (4 kategori independen) ────────────────
    "confluence_cat_a_min":  20,   # Derivatives: ls+fund+pred+oi
    "confluence_cat_b_min":  10,   # Order Flow:  bv+accum
    "confluence_cat_c_min":   8,   # Price RS:    rs_sc (score_btc_decoupling)
    "confluence_cat_d_min":   8,   # Microstructure: vret+wick+decel
    "confluence_min_cats":    3,   # minimal 3 dari 4 kategori harus aktif
    # Exception: 2 kategori boleh lolos HANYA jika cat_a sangat kuat
    "confluence_strong2cat_a_min":  50,
    "confluence_strong2cat_b_min":  25,

    # ── [FIX-2] BTC Decoupling weights (gantikan btc_context_bonus) ──────────
    "rs_btc_weight":   20,   # maks rs_sc dari score_btc_decoupling (CAT-C)

    # ── Velocity gates ────────────────────────────────────────────────────────
    "velocity_gates": {
        "chg_1h_max_early":        5.0,
        "chg_1h_max_continuation": 12.0,
        "chg_4h_max":              15.0,
        "chg_24h_max_early":       15.0,
        "chg_24h_max_continuation":30.0,
        "chg_24h_min":             -8.0,
        "chg_1h_min_reversal":     -3.0,
        "chg_4h_min_early":        -3.0,
    },

    # ── Cooldown & limits ─────────────────────────────────────────────────────
    "cooldown_hours":        18,
    "max_alerts_per_scan":    5,
    "candle_limit_bitget":  100,
    "coinalyze_lookback_h":  72,
    "coinalyze_funding_lookback_h": 168,
    "coinalyze_batch_size":   5,
    "coinalyze_rate_limit_wait": 1.2,
    "btc_dump_threshold":    -3.0,

    # ── Database ──────────────────────────────────────────────────────────────
    "history_db": "/tmp/scanner_v16_history.db",

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    "sl_mult_volatile":  3.0,
    "sl_mult_normal":    2.0,
    "sl_mult_quiet":     1.5,
    "sl_min_pct":        4.0,    # SL minimum 4% dari entry (noise protection)
    "sl_max_pct":       12.0,    # SL maksimum 12% dari entry
    "tp1_rr_min":        1.8,    # TP1 minimal R/R 1.8x
    "tp2_rr_min":        3.0,    # TP2 minimal R/R 3.0x
    "tp3_rr_min":        5.0,    # TP3 minimal R/R 5.0x
    "min_rr_ratio":      1.8,

    # ── Position sizing ───────────────────────────────────────────────────────
    "account_balance":     10_000.0,
    "risk_per_trade_pct":      1.0,
    "max_position_pct":        5.0,
    "max_leverage":           10,

    # ── Blacklists ────────────────────────────────────────────────────────────
    "stock_token_blacklist": [
        "HOODUSDT","COINUSDT","MSTRUSDT","NVDAUSDT","AAPLUSDT",
        "GOOGLUSDT","AMZNUSDT","METAUSDT","QQQUSDT","BZUSDT",
        "MCDUSDT","NIGHTUSDT","JCTUSDT","NOMUSDT","ASTERUSDT",
        "POLYXUSDT","PIUSDT","WMTUSDT","BGBUSDT","MEUSDT",
        "TSLAUSDT","CRCLUSDT","SPYUSDT","GLDUSDT","MSFTUSDT",
        "PLTRUSDT","INTCUSDT","XAUSDT","USDCUSDT","TRXUSDT",
    ],
    "extra_blacklist": [
        "1USDT","2USDT","3USDT","5USDT","6USDT","7USDT","8USDT","9USDT","0USDT",
    ],
}


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ClzData:
    ohlcv:                  List[dict] = field(default_factory=list)
    oi:                     List[dict] = field(default_factory=list)
    liq:                    List[dict] = field(default_factory=list)
    funding_hist:           List[dict] = field(default_factory=list)
    predicted_funding_hist: List[dict] = field(default_factory=list)
    ls_ratio:               List[dict] = field(default_factory=list)

    @property
    def has_ohlcv(self)            -> bool: return len(self.ohlcv) >= 10
    @property
    def has_oi(self)               -> bool: return len(self.oi) >= 4
    @property
    def has_liq(self)              -> bool: return len(self.liq) >= 4
    @property
    def has_funding_hist(self)     -> bool: return len(self.funding_hist) >= 3
    @property
    def has_predicted_funding(self)-> bool: return len(self.predicted_funding_hist) >= 3
    @property
    def has_ls(self)               -> bool: return len(self.ls_ratio) >= 4


@dataclass
class CoinData:
    symbol:      str
    price:       float
    vol_24h:     float
    chg_24h:     float
    chg_1h:      float
    chg_4h:      float
    funding:     float
    candles:     List[dict]
    btc_chg_1h:  float = 0.0
    btc_chg_4h:  float = 0.0    # [FIX-2] tambah 4h BTC untuk regime detection
    btc_chg_24h: float = 0.0
    clz:         ClzData = field(default_factory=ClzData)


@dataclass
class PhaseInfo:
    phase:       str
    base_score:  int
    description: str
    risk_level:  str


@dataclass
class PumpType:
    type_code:  str
    type_name:  str
    confidence: int
    signals:    List[str]


@dataclass
class ConfluenceResult:
    ok:          bool
    reason:      str
    active_cats: int
    cat_a:       int
    cat_b:       int
    cat_c:       int
    cat_d:       int


@dataclass
class ScoreResult:
    symbol:           str
    score:            int
    phase:            str
    pump_types:       List[PumpType]
    confidence:       str
    components:       Dict[str, Any]
    catalysts:        List[str]
    entry:            Optional[dict]
    price:            float
    vol_24h:          float
    chg_24h:          float
    chg_1h:           float
    funding:          float
    urgency:          str
    confluence:       ConfluenceResult = field(default_factory=lambda: ConfluenceResult(False,"",0,0,0,0,0))
    risk_warnings:    List[str] = field(default_factory=list)
    position:         Optional[dict] = None
    bitget_phase1_score: int = 0
    signal_fingerprint:  str = ""


# ══════════════════════════════════════════════════════════════════════════════
#  🗄️  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(CONFIG["history_db"])
    c = conn.cursor()
    # Tabel alerts (cooldown)
    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol         TEXT NOT NULL,
            alerted_at     INTEGER NOT NULL,
            score          INTEGER,
            phase          TEXT,
            entry_price    REAL,
            outcome_pct    REAL,
            outcome_checked INTEGER DEFAULT 0
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_alert_sym ON alerts(symbol, alerted_at DESC)")

    # [BONUS] Tabel signal_outcomes — precision tracking per kategori
    c.execute("""
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            alerted_at  INTEGER NOT NULL,
            score       INTEGER,
            phase       TEXT,
            pump_types  TEXT,
            cat_a_score INTEGER,
            cat_b_score INTEGER,
            cat_c_score INTEGER,
            cat_d_score INTEGER,
            btc_regime  TEXT,
            fingerprint TEXT,
            entry_price REAL,
            return_1h   REAL    DEFAULT NULL,
            return_2h   REAL    DEFAULT NULL,
            return_3h   REAL    DEFAULT NULL,
            hit_15pct   INTEGER DEFAULT NULL,
            checked     INTEGER DEFAULT 0
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_so_sym ON signal_outcomes(symbol, alerted_at DESC)")
    conn.commit()
    conn.close()


def is_on_cooldown(symbol: str) -> bool:
    try:
        conn = sqlite3.connect(CONFIG["history_db"])
        c = conn.cursor()
        c.execute("SELECT MAX(alerted_at) FROM alerts WHERE symbol = ?", (symbol,))
        row = c.fetchone()
        conn.close()
        if row and row[0]:
            return (time.time() - row[0]) < (CONFIG["cooldown_hours"] * 3600)
    except Exception:
        pass
    return False


def set_alert(symbol: str, score: int, phase: str, entry_price: float,
              result: Optional["ScoreResult"] = None):
    try:
        conn = sqlite3.connect(CONFIG["history_db"])
        c = conn.cursor()
        c.execute(
            "INSERT INTO alerts (symbol, alerted_at, score, phase, entry_price) VALUES (?,?,?,?,?)",
            (symbol, int(time.time()), score, phase, entry_price)
        )
        # [BONUS] Juga simpan ke signal_outcomes untuk precision tracking
        if result is not None:
            cf = result.confluence
            btc_regime = result.components.get("btc_regime", "UNKNOWN")
            pump_str = "|".join(pt.type_code for pt in result.pump_types)
            c.execute("""
                INSERT INTO signal_outcomes
                (symbol, alerted_at, score, phase, pump_types,
                 cat_a_score, cat_b_score, cat_c_score, cat_d_score,
                 btc_regime, fingerprint, entry_price)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                symbol, int(time.time()), score, phase, pump_str,
                cf.cat_a, cf.cat_b, cf.cat_c, cf.cat_d,
                btc_regime, result.signal_fingerprint, entry_price
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"set_alert failed: {e}")


def check_and_update_outcomes(tickers: Dict[str, dict]):
    """
    [BONUS] Feedback loop: cek sinyal yang sudah >1 jam.
    Update return_1h, return_2h, return_3h, hit_15pct.
    Dipanggil setiap scan untuk membangun data precision empiris.
    """
    try:
        conn = sqlite3.connect(CONFIG["history_db"])
        c = conn.cursor()
        now = int(time.time())

        # Update tabel alerts (untuk cooldown tracking)
        c.execute(
            "SELECT id, symbol, entry_price FROM alerts WHERE outcome_checked=0 AND alerted_at <= ?",
            (now - 3600,)
        )
        for row_id, symbol, entry_price in c.fetchall():
            ticker = tickers.get(symbol)
            if not ticker or not entry_price or entry_price <= 0:
                continue
            cur = float(ticker.get("lastPr", 0) or 0)
            if cur <= 0:
                continue
            out = (cur - entry_price) / entry_price * 100
            c.execute(
                "UPDATE alerts SET outcome_pct=?, outcome_checked=1 WHERE id=?",
                (round(out, 4), row_id)
            )

        # Update signal_outcomes (multi-timeframe)
        c.execute(
            "SELECT id, symbol, alerted_at, entry_price FROM signal_outcomes WHERE checked=0 AND alerted_at <= ?",
            (now - 3600,)
        )
        rows = c.fetchall()
        updated = 0
        for row_id, symbol, alerted_at, entry_price in rows:
            ticker = tickers.get(symbol)
            if not ticker or not entry_price or entry_price <= 0:
                continue
            cur = float(ticker.get("lastPr", 0) or 0)
            if cur <= 0:
                continue
            elapsed = now - alerted_at
            ret = (cur - entry_price) / entry_price * 100
            hit = 1 if ret >= 15.0 else 0
            # Tandai checked hanya jika sudah >= 3 jam
            done = 1 if elapsed >= 3 * 3600 else 0
            if elapsed >= 3 * 3600:
                c.execute("""
                    UPDATE signal_outcomes
                    SET return_1h=?, return_2h=?, return_3h=?, hit_15pct=?, checked=1
                    WHERE id=?
                """, (round(ret, 2), round(ret, 2), round(ret, 2), hit, row_id))
            elif elapsed >= 2 * 3600:
                c.execute("UPDATE signal_outcomes SET return_2h=? WHERE id=?", (round(ret, 2), row_id))
            elif elapsed >= 3600:
                c.execute("UPDATE signal_outcomes SET return_1h=? WHERE id=?", (round(ret, 2), row_id))
            if done:
                updated += 1

        if updated > 0:
            conn.commit()
            log.info(f"  Outcome tracking: {updated} sinyal di-close")
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"check_and_update_outcomes failed: {e}")


def get_precision_report() -> Dict[str, Any]:
    """[BONUS] Laporan precision harian per phase dan BTC regime."""
    try:
        conn = sqlite3.connect(CONFIG["history_db"])
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*), SUM(hit_15pct), phase, btc_regime
            FROM signal_outcomes WHERE checked=1
            GROUP BY phase, btc_regime
        """)
        report = {}
        for total, hits, phase, btc_regime in c.fetchall():
            if total and total > 0:
                key = f"{phase}|BTC={btc_regime}"
                report[key] = {
                    "precision": round((hits or 0) / total * 100, 1),
                    "total": total,
                    "hits": hits or 0,
                }
        # Overall
        c.execute("SELECT COUNT(*), SUM(hit_15pct) FROM signal_outcomes WHERE checked=1")
        row = c.fetchone()
        if row and row[0]:
            report["OVERALL"] = {
                "precision": round((row[1] or 0) / row[0] * 100, 1),
                "total": row[0],
                "hits": row[1] or 0,
            }
        conn.close()
        return report
    except Exception as e:
        log.warning(f"get_precision_report failed: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
#  🔧  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def get_chg_from_candles(candles: List[dict], n_hours: int) -> float:
    """
    NO-LOOKAHEAD: menggunakan candles[-2] (last closed) sebagai 'now'.
    candles[-1] adalah candle yang masih berjalan (live), tidak digunakan.
    """
    if len(candles) < n_hours + 2:
        return 0.0
    now_price = candles[-2]["close"]
    prev_price = candles[-(n_hours + 2)]["close"]
    if prev_price <= 0:
        return 0.0
    return (now_price - prev_price) / prev_price * 100


def get_hour_utc() -> int:
    return datetime.now(timezone.utc).hour


def volume_tod_mult(hour: int) -> float:
    if 2 <= hour <= 8:
        return 1.35
    elif 13 <= hour <= 21:
        return 0.88
    return 1.0


def is_stock_token(symbol: str) -> bool:
    bl = {s.strip().upper() for s in CONFIG.get("stock_token_blacklist", [])}
    return symbol.strip().upper() in bl


def is_valid_symbol(symbol: str) -> bool:
    if symbol in CONFIG.get("extra_blacklist", []):
        return False
    return bool(re.match(r'^[A-Za-z0-9]{2,}USDT$', symbol))


def make_signal_fingerprint(components: dict) -> str:
    """Hash top-3 catalyst untuk identifikasi duplikat."""
    keys = ["ls_sc", "bv_sc", "fund_sc", "rs_sc", "oi_sc", "liq_sc"]
    top = sorted([(k, components.get(k, 0)) for k in keys], key=lambda x: -x[1])[:3]
    raw = "|".join(f"{k}={v}" for k, v in top)
    return hashlib.md5(raw.encode()).hexdigest()[:8]


# ══════════════════════════════════════════════════════════════════════════════
#  📐  ATR & TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
def calc_atr(candles: List[dict], n: int = 14) -> float:
    """ATR sebagai persentase harga (decimal, bukan persen)."""
    trs = []
    for i in range(2, min(n + 2, len(candles))):
        c  = candles[-i]
        pc = candles[-(i + 1)]["close"]
        if pc > 0:
            tr = max(
                (c["high"] - c["low"]) / pc,
                abs(c["high"] - pc) / pc,
                abs(c["low"]  - pc) / pc,
            )
            trs.append(tr)
    return _mean(trs) if trs else 0.02


def calc_bbw(candles: List[dict]) -> float:
    if len(candles) < 22:
        return 0.0
    closes = [c["close"] for c in candles[-20:]]
    sma = _mean(closes)
    if sma <= 0:
        return 0.0
    var = sum((x - sma) ** 2 for x in closes) / 20
    std = var ** 0.5
    return (sma + 2 * std - (sma - 2 * std)) / sma


def calc_range_pct(candles: List[dict]) -> float:
    if len(candles) < 10:
        return 0.0
    recent = candles[-9:-1]
    closes = [c["close"] for c in recent]
    lo, hi = min(closes), max(closes)
    ref = (lo + hi) / 2
    if ref <= 0:
        return 0.0
    return (hi - lo) / ref * 100


def calc_lower_wick_pct(candles: List[dict]) -> float:
    if len(candles) < 5:
        return 0.0
    wick_pcts = []
    for c in candles[-4:-1]:
        lo   = c["low"]
        op   = c.get("open", 0)
        cl   = c["close"]
        body_low = min(op, cl) if op > 0 else cl
        if body_low > 0:
            wick = (body_low - lo) / body_low * 100
            wick_pcts.append(max(0.0, wick))
    return _mean(wick_pcts) if wick_pcts else 0.0


def calc_dist_to_support(candles: List[dict], price: float) -> Tuple[float, bool]:
    window = min(96, len(candles))
    if window < 10 or price <= 0:
        return 100.0, True
    lows = [c["low"] for c in candles[-window:] if c["low"] > 0]
    if len(lows) < 4:
        return 100.0, True
    tol = 0.02
    clusters: Dict[float, int] = {}
    for low in lows:
        matched = False
        for cp in list(clusters.keys()):
            if abs(low - cp) / cp < tol:
                clusters[cp] += 1
                matched = True
                break
        if not matched:
            clusters[low] = 1
    valid = [(lvl, cnt) for lvl, cnt in clusters.items() if 2 <= cnt <= 5 and lvl < price]
    if not valid:
        return 100.0, True
    support_level, _ = max(valid, key=lambda x: x[1])
    dist_pct = (price - support_level) / support_level * 100
    inside_comp = dist_pct <= 3.0
    return dist_pct, inside_comp


def calc_momentum_decel(candles: List[dict]) -> float:
    if len(candles) < 8:
        return 0.0
    chgs = []
    for i in range(-5, -1):
        c  = candles[i]
        pc = candles[i - 1]
        if pc["close"] > 0:
            chgs.append((c["close"] - pc["close"]) / pc["close"] * 100)
    if len(chgs) < 4:
        return 0.0
    recent  = _mean(chgs[-2:])
    earlier = _mean(chgs[:2])
    return recent - earlier


def get_volatility_regime(candles: List[dict]) -> str:
    """
    NO-LOOKAHEAD: hanya data t-n. Latensi ~0.5ms.
    Bandingkan ATR 24h vs ATR 7d untuk deteksi regime volatilitas.
    """
    if len(candles) < 30:
        return "NORMAL"
    atr_24h = calc_atr(candles[-26:], 14)
    atr_7d  = calc_atr(candles[-170:], 14) if len(candles) >= 170 else calc_atr(candles[:-24], 14)
    if atr_7d <= 0:
        return "NORMAL"
    ratio = atr_24h / atr_7d
    if ratio > 1.4:
        return "HIGH"
    elif ratio < 0.7:
        return "LOW"
    return "NORMAL"


# ══════════════════════════════════════════════════════════════════════════════
#  [FIX-1] ✅ CHECK CONFLUENCE — DOKTRIN KONVERGENSI
# ══════════════════════════════════════════════════════════════════════════════
def check_confluence(scores: Dict[str, int]) -> ConfluenceResult:
    """
    NO-LOOKAHEAD: hanya menggunakan skor yang sudah dihitung dari data t-n.
    Latensi: ~0.01ms.

    4 Kategori Independen:
    CAT-A: Derivatives Positioning  → ls_sc + fund_sc + pred_sc + oi_sc
    CAT-B: Order Flow / Tape        → bv_sc + accum_sc
    CAT-C: Price Relative Strength  → rs_sc (dari score_btc_decoupling)
    CAT-D: Microstructure           → vret_sc + wick_sc + decel_sc

    Note: liq_sc SENGAJA dimasukkan CAT-A bukan kategori sendiri karena
    liquidation adalah turunan dari derivatives positioning (r > 0.75).
    """
    cat_a = (scores.get("ls_sc", 0) + scores.get("fund_sc", 0)
             + scores.get("pred_sc", 0) + scores.get("oi_sc", 0)
             + scores.get("liq_sc", 0))
    cat_b = scores.get("bv_sc", 0) + scores.get("accum_sc", 0)
    cat_c = scores.get("rs_sc", 0)
    cat_d = (scores.get("vret_sc", 0) + scores.get("wick_sc", 0)
             + scores.get("decel_sc", 0) + scores.get("supp_sc", 0))

    a_ok = cat_a >= CONFIG["confluence_cat_a_min"]
    b_ok = cat_b >= CONFIG["confluence_cat_b_min"]
    c_ok = cat_c >= CONFIG["confluence_cat_c_min"]
    d_ok = cat_d >= CONFIG["confluence_cat_d_min"]

    active_count = sum([a_ok, b_ok, c_ok, d_ok])

    if active_count >= CONFIG["confluence_min_cats"]:
        ok = True
        reason = f"CONFLUENCE_OK ({active_count}/4 kategori)"
    elif (active_count == 2
          and cat_a >= CONFIG["confluence_strong2cat_a_min"]
          and cat_b >= CONFIG["confluence_strong2cat_b_min"]):
        ok = True
        reason = "CONFLUENCE_STRONG_2CAT (A+B sangat kuat)"
    else:
        ok = False
        cats_missing = [
            n for n, v in [("A", a_ok), ("B", b_ok), ("C", c_ok), ("D", d_ok)] if not v
        ]
        reason = f"CONFLUENCE_FAIL: {active_count}/4 aktif, kurang {','.join(cats_missing)}"

    return ConfluenceResult(ok=ok, reason=reason,
                            active_cats=active_count,
                            cat_a=cat_a, cat_b=cat_b,
                            cat_c=cat_c, cat_d=cat_d)


# ══════════════════════════════════════════════════════════════════════════════
#  [FIX-3] ✅ PHASE 1: BITGET-ONLY FILTER (dengan momentum pre-pump check)
# ══════════════════════════════════════════════════════════════════════════════
def phase1_bitget_filter(candles: List[dict], vol_24h: float,
                          chg_1h: float = 0.0, chg_4h: float = 0.0) -> Tuple[int, Dict[str, Any]]:
    """
    [FIX-3] Tambah parameter chg_1h dan chg_4h untuk momentum_score.
    Coin yang sedang distribusi aktif (chg_4h sangat negatif) mendapat penalty.
    Ini membedakan volatilitas umum dari pre-pump setup.
    """
    if len(candles) < 30:
        return 0, {"error": "insufficient_candles"}
    if vol_24h < CONFIG["phase1_min_volume_usd"]:
        return 0, {"error": f"low_volume_{vol_24h/1e3:.0f}K"}

    cfg = CONFIG["phase1_weights"]
    details = {}
    score = 0

    # 1. ATR
    atr = calc_atr(candles[-22:], 14) * 100
    thr = CONFIG["phase1_atr_thresholds"]
    if atr >= thr[0]:
        score += cfg["atr"]; details["atr_score"] = cfg["atr"]
    elif atr >= thr[1]:
        score += 18; details["atr_score"] = 18
    elif atr >= thr[2]:
        score += 10; details["atr_score"] = 10
    else:
        details["atr_score"] = 0
    details["atr_pct"] = round(atr, 2)

    # 2. Range
    range_pct = calc_range_pct(candles)
    thr_r = CONFIG["phase1_range_thresholds"]
    if range_pct >= thr_r[0]:
        score += cfg["range"]; details["range_score"] = cfg["range"]
    elif range_pct >= thr_r[1]:
        score += 16; details["range_score"] = 16
    elif range_pct >= thr_r[2]:
        score += 8; details["range_score"] = 8
    else:
        details["range_score"] = 0
    details["range_pct"] = round(range_pct, 2)

    # 3. BBW
    bbw = calc_bbw(candles)
    thr_b = CONFIG["phase1_bbw_thresholds"]
    if bbw >= thr_b[0]:
        score += cfg["bbw"]; details["bbw_score"] = cfg["bbw"]
    elif bbw >= thr_b[1]:
        score += 14; details["bbw_score"] = 14
    elif bbw >= thr_b[2]:
        score += 8; details["bbw_score"] = 8
    else:
        details["bbw_score"] = 0
    details["bbw"] = round(bbw, 4)

    # 4. Lower wick
    wick = calc_lower_wick_pct(candles)
    thr_w = CONFIG["phase1_wick_thresholds"]
    if wick >= thr_w[0]:
        score += cfg["wick"]; details["wick_score"] = cfg["wick"]
    elif wick >= thr_w[1]:
        score += 10; details["wick_score"] = 10
    elif wick >= thr_w[2]:
        score += 6; details["wick_score"] = 6
    else:
        details["wick_score"] = 0
    details["wick_pct"] = round(wick, 2)

    # 5. Support & compression
    price = candles[-2]["close"]
    dist, inside = calc_dist_to_support(candles, price)
    details["dist_to_support"]   = round(dist, 2)
    details["inside_compression"] = 1 if inside else 0
    if not inside:
        low_r, high_r = CONFIG["phase1_support_dist_range"]
        wide_l, wide_h = CONFIG["phase1_support_dist_wide"]
        if low_r <= dist <= high_r:
            score += cfg["support"]; details["support_score"] = cfg["support"]
        elif wide_l <= dist <= wide_h:
            score += 7; details["support_score"] = 7
        else:
            details["support_score"] = 0
    else:
        details["support_score"] = 0

    # 6. Momentum deceleration
    decel = calc_momentum_decel(candles)
    thr_d = CONFIG["phase1_decel_thresholds"]
    if decel <= thr_d[0]:
        score += cfg["decel"]; details["decel_score"] = cfg["decel"]
    elif decel <= thr_d[1]:
        score += 6; details["decel_score"] = 6
    elif decel <= thr_d[2]:
        score += 3; details["decel_score"] = 3
    else:
        details["decel_score"] = 0
    details["decel"] = round(decel, 3)

    # [FIX-3] 7. Momentum pre-pump context
    # Logika: pre-pump setup valid hanya jika momentum tidak sedang aktif distribusi.
    # chg_4h positif atau flat ringan = setup valid
    # chg_4h sangat negatif = distribusi aktif = penalty
    momentum_score = 0
    if chg_4h >= 1.0 and chg_1h >= 0:
        # 4h positif, 1h positif/flat = momentum naik terkontrol (ideal pre-pump)
        momentum_score = cfg["momentum"]
    elif chg_4h >= -1.0 and chg_1h >= -1.0:
        # Flat/sideways = akumulasi potensial
        momentum_score = int(cfg["momentum"] * 0.6)
    elif chg_4h >= -3.0:
        # Sedikit negatif = masih bisa recovery
        momentum_score = int(cfg["momentum"] * 0.2)
    else:
        # chg_4h < -3% = distribusi aktif, beri penalty
        momentum_score = -10
    score += momentum_score
    details["momentum_score"] = momentum_score
    details["momentum_chg_4h"] = round(chg_4h, 2)
    details["momentum_chg_1h"] = round(chg_1h, 2)

    score = max(0, score)
    details["total_score"] = score

    # Compound check: penalty jika wick & decel keduanya absen
    if details.get("wick_score", 0) == 0 and details.get("decel_score", 0) == 0:
        score = max(0, score - 15)
        details["compound_penalty"] = -15
        details["total_score"] = score
    else:
        details["compound_penalty"] = 0

    return score, details


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET API CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE_URL = "https://api.bitget.com"
    _candles_cache: Dict[str, tuple] = {}
    CACHE_TTL = 55 * 60

    @classmethod
    def _get(cls, endpoint: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        for attempt in range(3):
            try:
                resp = requests.get(f"{cls.BASE_URL}/{endpoint}", params=params, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    time.sleep(10)
                    continue
                break
            except Exception:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        data = cls._get("api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 100) -> List[dict]:
        cache_key = f"{symbol}:{limit}"
        now = time.time()
        if cache_key in cls._candles_cache:
            ts, cached = cls._candles_cache[cache_key]
            if now - ts < cls.CACHE_TTL:
                return cached
        data = cls._get(
            "api/v2/mix/market/candles",
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
                    "ts": int(row[0]),
                    "open":       float(row[1]),
                    "high":       float(row[2]),
                    "low":        float(row[3]),
                    "close":      float(row[4]),
                    "volume_usd": vol_usd,
                })
            except Exception:
                continue
        candles.sort(key=lambda x: x["ts"])
        cls._candles_cache[cache_key] = (now, candles)
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(
            "api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"}
        )
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0

    @classmethod
    def clear_cache(cls):
        cls._candles_cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  COINALYZE API CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class CoinalyzeClient:
    BASE_URL = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0
    _retry_count: int = 0

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._markets_cache: Optional[List[dict]] = None
        self._bn_map: Dict[str, str] = {}
        self._by_map: Dict[str, str] = {}

    def _wait(self):
        base_wait = CONFIG["coinalyze_rate_limit_wait"]
        wait = base_wait * (self._retry_count + 1)
        wait = min(wait, 10)
        elapsed = time.time() - CoinalyzeClient._last_call
        if elapsed < wait:
            time.sleep(wait - elapsed)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[Any]:
        params["api_key"] = self.api_key
        headers = {"User-Agent": f"PrePumpScanner/{VERSION}"}
        for attempt in range(3):
            self._wait()
            try:
                resp = requests.get(
                    f"{self.BASE_URL}/{endpoint}", params=params,
                    headers=headers, timeout=15
                )
                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After", "5")
                    try:
                        wait = int(float(ra)) + 1
                    except Exception:
                        wait = 6
                    log.warning(f"  Coinalyze rate limit, wait {wait}s (attempt {attempt+1}/3)")
                    time.sleep(wait + 1.5)
                    self._retry_count += 1
                    continue
                if resp.status_code != 200:
                    log.warning(f"  Coinalyze {endpoint} HTTP {resp.status_code}")
                    return None
                data = resp.json()
                if isinstance(data, dict) and "error" in data:
                    log.warning(f"  Coinalyze error: {data['error']}")
                    return None
                self._retry_count = 0
                return data
            except Exception as e:
                log.warning(f"  Coinalyze request error: {e}")
                if attempt < 2:
                    time.sleep(3)
        self._retry_count = 0
        return None

    def build_symbol_maps(self, bitget_symbols: List[str]) -> None:
        if self._markets_cache is None:
            log.info("  Loading Coinalyze markets...")
            data = self._get("future-markets", {})
            self._markets_cache = data if isinstance(data, list) else []
            log.info(f"  Got {len(self._markets_cache)} Coinalyze markets")

        markets = self._markets_cache
        bn_lookup: Dict[str, str] = {}
        by_ls_lookup: Dict[str, str] = {}

        for m in markets:
            exc        = m.get("exchange", "")
            sym_on_exc = m.get("symbol_on_exchange", "")
            clz_sym    = m.get("symbol", "")
            is_perp    = m.get("is_perpetual", False)
            quote      = m.get("quote_asset", "").upper()
            if not (is_perp and quote == "USDT" and clz_sym):
                continue
            if exc == "A":
                bn_lookup[sym_on_exc] = clz_sym
            elif exc == "6" and m.get("has_long_short_ratio_data"):
                by_ls_lookup[sym_on_exc] = clz_sym

        def normalize(s: str) -> str:
            if s.startswith("1000"):
                s = s[4:]
            return s.upper()

        def candidates(sym: str) -> List[str]:
            base = sym.replace("USDT", "")
            cands = [sym, f"{base}/USDT", f"{base}-USDT", f"1000{base}USDT", f"10000{base}USDT"]
            if base.startswith("1000"):
                cands.append(base[4:] + "USDT")
            return list(set(cands))

        mapped_bn = mapped_by = 0
        for sym in bitget_symbols:
            norm_sym = normalize(sym)
            for cand in candidates(norm_sym):
                if cand in bn_lookup:
                    self._bn_map[sym] = bn_lookup[cand]
                    mapped_bn += 1
                    break
            for cand in candidates(norm_sym):
                if cand in by_ls_lookup:
                    self._by_map[sym] = by_ls_lookup[cand]
                    mapped_by += 1
                    break

        log.info(
            f"  Mapping: {mapped_bn}/{len(bitget_symbols)} Binance, "
            f"{mapped_by}/{len(bitget_symbols)} Bybit"
        )

    def _batch_fetch(self, endpoint: str, symbols: List[str], params: dict) -> Dict[str, list]:
        batch_size = CONFIG["coinalyze_batch_size"]
        result: Dict[str, list] = {}
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i: i + batch_size]
            try:
                p = dict(params)
                p["symbols"] = ",".join(batch)
                data = self._get(endpoint, p)
                if data and isinstance(data, list):
                    for item in data:
                        sym  = item.get("symbol", "")
                        hist = item.get("history", [])
                        if hist:
                            hist = sorted(hist, key=lambda x: x.get("t", 0))
                        if sym and hist:
                            result[sym] = hist
            except Exception as e:
                log.warning(f"  Batch {i//batch_size+1} failed: {e}")
        return result

    def fetch_for_symbols(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, ClzData]:
        result = {sym: ClzData() for sym in symbols}

        bn_syms = [self._bn_map[s] for s in symbols if s in self._bn_map]
        by_syms = [self._by_map[s] for s in symbols if s in self._by_map]
        bn_rev  = {v: k for k, v in self._bn_map.items()}
        by_rev  = {v: k for k, v in self._by_map.items()}

        interval      = "1hour"
        fund_from     = to_ts - CONFIG["coinalyze_funding_lookback_h"] * 3600

        if bn_syms:
            log.info(f"  Fetching Binance OHLCV ({len(bn_syms)} syms)...")
            for clz_sym, hist in self._batch_fetch(
                    "ohlcv-history", bn_syms,
                    {"interval": interval, "from": from_ts, "to": to_ts}).items():
                bsym = bn_rev.get(clz_sym)
                if bsym: result[bsym].ohlcv = hist

            log.info("  Fetching OI history...")
            for clz_sym, hist in self._batch_fetch(
                    "open-interest-history", bn_syms,
                    {"interval": interval, "from": from_ts, "to": to_ts,
                     "convert_to_usd": "true"}).items():
                bsym = bn_rev.get(clz_sym)
                if bsym: result[bsym].oi = hist

            log.info("  Fetching Liquidations...")
            for clz_sym, hist in self._batch_fetch(
                    "liquidation-history", bn_syms,
                    {"interval": interval, "from": from_ts, "to": to_ts,
                     "convert_to_usd": "true"}).items():
                bsym = bn_rev.get(clz_sym)
                if bsym: result[bsym].liq = hist

            log.info("  Fetching Funding rate history...")
            for intv in ["daily", "1hour"]:
                fd = self._batch_fetch(
                    "funding-rate-history", bn_syms,
                    {"interval": intv, "from": fund_from, "to": to_ts})
                if fd:
                    for clz_sym, hist in fd.items():
                        bsym = bn_rev.get(clz_sym)
                        if bsym: result[bsym].funding_hist = hist
                    break

            log.info("  Fetching Predicted funding history...")
            for clz_sym, hist in self._batch_fetch(
                    "predicted-funding-rate-history", bn_syms,
                    {"interval": "daily", "from": fund_from, "to": to_ts}).items():
                bsym = bn_rev.get(clz_sym)
                if bsym: result[bsym].predicted_funding_hist = hist

        if by_syms:
            log.info(f"  Fetching Bybit L/S ratio ({len(by_syms)} syms)...")
            for clz_sym, hist in self._batch_fetch(
                    "long-short-ratio-history", by_syms,
                    {"interval": interval, "from": from_ts, "to": to_ts}).items():
                bsym = by_rev.get(clz_sym)
                if bsym: result[bsym].ls_ratio = hist

        return result


# ══════════════════════════════════════════════════════════════════════════════
#  📐  S/R LEVEL ENGINE (1H candles)
# ══════════════════════════════════════════════════════════════════════════════
def find_sr_levels(candles: List[dict], price: float) -> Dict[str, Any]:
    """
    Deteksi swing high/low + cluster scoring.
    swing_radius dikurangi ke 1 (dari 2) agar lebih sensitif di altcoin yang
    sering punya swing asimetris. Masih butuh konfirmasi kiri+kanan.
    """
    window = min(96, len(candles) - 1)
    if window < 10 or price <= 0:
        return {"supports": [], "resistances": []}

    candles_w   = candles[-window - 1: -1]   # closed candles only
    n_candles   = len(candles_w)
    swing_radius = 1    # [FIX-5] turun dari 2→1: lebih sensitif untuk altcoin
    tol          = 0.015

    swing_lows:  List[Tuple[float, int]] = []
    swing_highs: List[Tuple[float, int]] = []

    for i in range(swing_radius, n_candles - swing_radius):
        lo = candles_w[i]["low"]
        hi = candles_w[i]["high"]
        if all(lo <= candles_w[i - k]["low"]  for k in range(1, swing_radius + 1)) and \
           all(lo <= candles_w[i + k]["low"]  for k in range(1, swing_radius + 1)):
            swing_lows.append((lo, i))
        if all(hi >= candles_w[i - k]["high"] for k in range(1, swing_radius + 1)) and \
           all(hi >= candles_w[i + k]["high"] for k in range(1, swing_radius + 1)):
            swing_highs.append((hi, i))

    def cluster_swings(swings: List[Tuple[float, int]]) -> List[Dict]:
        if not swings:
            return []
        clusters: List[Dict] = []
        for lvl, idx in sorted(swings, key=lambda x: x[0]):
            matched = False
            for cl in clusters:
                if abs(lvl - cl["price"]) / cl["price"] < tol:
                    cl["price"]    = (cl["price"] * cl["count"] + lvl) / (cl["count"] + 1)
                    cl["count"]   += 1
                    cl["last_idx"] = max(cl["last_idx"], idx)
                    matched = True
                    break
            if not matched:
                clusters.append({"price": lvl, "count": 1, "last_idx": idx})
        return clusters

    def score_levels(clusters: List[Dict], all_candles: List[dict]) -> List[Dict]:
        scored = []
        total_c = len(all_candles)
        for cl in clusters:
            lvl     = cl["price"]
            touches = 0
            vol_t   = []
            for c in all_candles:
                if abs(c["low"] - lvl) / lvl < tol or abs(c["high"] - lvl) / lvl < tol:
                    touches += 1
                    vol_t.append(c.get("volume_usd", 0))
            avg_vol = _mean(vol_t) if vol_t else 0
            recency = 1 + int((cl["last_idx"] / max(total_c - 1, 1)) * 4)
            score   = touches * 3 + recency + (1 if avg_vol > 0 else 0)
            scored.append({
                "price":   round(lvl, 8),
                "touches": touches,
                "recency": recency,
                "score":   score,
                "avg_vol": round(avg_vol, 0),
                "count":   cl["count"],
            })
        return sorted(scored, key=lambda x: x["score"], reverse=True)

    sup_cl = cluster_swings(swing_lows)
    res_cl = cluster_swings(swing_highs)

    all_sup = score_levels(sup_cl, candles_w)
    all_res = score_levels(res_cl, candles_w)

    supports    = [s for s in all_sup if s["price"] < price * 0.997 and s["price"] > price * 0.80]
    resistances = [r for r in all_res if r["price"] > price * 1.003 and r["price"] < price * 1.50]

    # Selalu sort resistances by price ascending agar TP1 < TP2 < TP3
    resistances = sorted(resistances, key=lambda x: x["price"])

    return {
        "supports":       supports,
        "resistances":    resistances,
        "all_supports":   all_sup,
        "all_resistances":all_res,
    }


def calc_entry_targets(candles: List[dict], price: float) -> Optional[dict]:
    """
    [FIX-5] SL/TP berbasis S/R 1H terkuat dengan fallback adaptif.

    Perubahan dari v15.6:
    - swing_radius dikurangi 2→1 di find_sr_levels → lebih banyak levels ditemukan
    - TP fallback: bukan lagi fixed %, tapi RR adaptif berdasarkan ATR dan vol regime
      TP1_fallback = price + risk * tp1_rr_min (1.8x)
      TP2_fallback = price + risk * tp2_rr_min (3.0x)
      TP3_fallback = price + risk * tp3_rr_min (5.0x)
    - SL minimum 4%, maksimum 12% dari entry
    - Adaptive ATR multiplier berdasarkan volatility regime
    """
    if len(candles) < 16:
        return None

    atr    = calc_atr(candles, 14)
    regime = get_volatility_regime(candles)

    # Adaptive SL multiplier berdasarkan regime
    _regime_offset = {"HIGH": 0.5, "NORMAL": 0.0, "LOW": -0.3}
    sl_mult = CONFIG["sl_mult_normal"] + _regime_offset.get(regime, 0.0)
    sl_mult = max(CONFIG["sl_mult_quiet"], sl_mult)

    sr       = find_sr_levels(candles, price)
    supports = sr["supports"]
    resis    = sr["resistances"]

    method = "sr"

    # ── SL: support terkuat terdekat + buffer ─────────────────────────────────
    sl = None
    sl_source = "none"
    for sup in sorted(supports, key=lambda x: price - x["price"]):
        sl_cand     = sup["price"] - (price * atr * 0.3)
        sl_pct_cand = (price - sl_cand) / price * 100
        if sl_pct_cand <= CONFIG["sl_max_pct"] and sl_cand > 0:
            sl        = sl_cand
            sl_source = f"S@{sup['price']:.6g}(t{sup['touches']})"
            break

    # SL minimum 4% (noise protection)
    if sl is not None:
        sl_pct_check = (price - sl) / price * 100
        if sl_pct_check < CONFIG["sl_min_pct"]:
            sl_atr     = price * (1 - atr * sl_mult)
            sl_pct_atr = (price - sl_atr) / price * 100
            if sl_pct_atr >= CONFIG["sl_min_pct"]:
                sl        = sl_atr
                sl_source = f"ATR×{sl_mult:.1f}(SR_too_close,was_{sl_pct_check:.1f}%)"
            else:
                sl        = price * (1 - CONFIG["sl_min_pct"] / 100)
                sl_source = f"FLOOR_{CONFIG['sl_min_pct']}%"

    # Fallback: tidak ada support ditemukan
    if sl is None or sl <= 0 or sl >= price:
        sl        = price * (1 - atr * sl_mult)
        sl_source = f"ATR×{sl_mult:.1f}({regime})"
        method    = "atr_fallback"
        # Pastikan minimum
        if (price - sl) / price * 100 < CONFIG["sl_min_pct"]:
            sl = price * (1 - CONFIG["sl_min_pct"] / 100)
            sl_source = f"FLOOR_{CONFIG['sl_min_pct']}%"

    sl_pct = (price - sl) / price * 100
    risk   = price - sl
    if risk <= 0:
        return None

    # ── TP: S/R-based + fallback RR adaptif (bukan fixed %) ──────────────────
    # [FIX-5] Fallback tidak lagi pakai tp1_pct/tp2_pct fixed.
    # Sekarang pakai RR multiplier dari CONFIG sehingga proporsional terhadap risk.
    tp_source = []

    if resis:
        tp1 = resis[0]["price"]
        tp_source.append(f"R1@{resis[0]['price']:.6g}(t{resis[0]['touches']})")
    else:
        tp1 = price + risk * CONFIG["tp1_rr_min"]
        tp_source.append(f"RR{CONFIG['tp1_rr_min']}x_fallback")
        method = "atr_fallback" if method != "sr" else "sr_tp_fallback"

    if len(resis) >= 2:
        tp2 = resis[1]["price"]
        tp_source.append(f"R2@{resis[1]['price']:.6g}(t{resis[1]['touches']})")
    else:
        tp2 = price + risk * CONFIG["tp2_rr_min"]
        tp2 = max(tp2, tp1 * 1.03)
        tp_source.append(f"RR{CONFIG['tp2_rr_min']}x_fallback")

    if len(resis) >= 3:
        tp3 = resis[2]["price"]
        tp_source.append(f"R3@{resis[2]['price']:.6g}(t{resis[2]['touches']})")
    else:
        tp3 = price + risk * CONFIG["tp3_rr_min"]
        tp3 = max(tp3, tp2 * 1.03)
        tp_source.append(f"RR{CONFIG['tp3_rr_min']}x_fallback")

    # Pastikan TP1 >= RR minimum floor
    tp1_floor = price + risk * CONFIG["min_rr_ratio"]
    if tp1 < tp1_floor:
        tp1 = tp1_floor

    # Pastikan ascending: TP1 < TP2 < TP3
    if tp2 <= tp1: tp2 = tp1 * 1.05
    if tp3 <= tp2: tp3 = tp2 * 1.05

    # Cap: tidak lebih dari 2x harga entry
    tp2 = min(tp2, price * 2.0)
    tp3 = min(tp3, price * 2.0)

    # RR check
    rr1 = (tp1 - price) / risk
    if rr1 < CONFIG["min_rr_ratio"] - 1e-9:
        log.debug(f"  calc_entry_targets: RR1={rr1:.2f} < {CONFIG['min_rr_ratio']} — skip")
        return None

    tp1_pct = (tp1 / price - 1) * 100
    tp2_pct = (tp2 / price - 1) * 100
    tp3_pct = (tp3 / price - 1) * 100
    rr2 = (tp2 - price) / risk if risk > 0 else 0
    rr3 = (tp3 - price) / risk if risk > 0 else 0

    return {
        "entry":            round(price, 8),
        "entry_zone_low":   round(price * (1 - atr * 0.3), 8),
        "entry_zone_high":  round(price * (1 + atr * 0.2), 8),
        "sl":               round(sl, 8),
        "sl_pct":           round(sl_pct, 1),
        "sl_source":        sl_source,
        "tp1":              round(tp1, 8),
        "tp1_pct":          round(tp1_pct, 1),
        "tp2":              round(tp2, 8),
        "tp2_pct":          round(tp2_pct, 1),
        "tp3":              round(tp3, 8),
        "tp3_pct":          round(tp3_pct, 1),
        "rr1":              round(rr1, 2),
        "rr2":              round(rr2, 2),
        "rr3":              round(rr3, 2),
        "atr_pct":          round(atr * 100, 2),
        "atr_decimal":      atr,
        "vol_regime":       regime,
        "method":           method,
        "tp_source":        " | ".join(tp_source),
        "n_supports":       len(supports),
        "n_resistances":    len(resis),
    }


def calc_position_size(entry: float, sl: float, atr: float) -> dict:
    bal          = CONFIG["account_balance"]
    risk_usd     = bal * CONFIG["risk_per_trade_pct"] / 100
    risk_per_unit = (entry - sl) / entry
    if risk_per_unit <= 0:
        risk_per_unit = atr * CONFIG["sl_mult_normal"]
    pos_needed = risk_usd / risk_per_unit
    pos_cap    = bal * CONFIG["max_position_pct"] / 100
    pos_val    = min(pos_needed, pos_cap)
    leverage   = min(pos_val / bal, CONFIG["max_leverage"]) if pos_val > bal else 1.0
    pos_val    = min(pos_val, bal * max(leverage, 1))
    return {
        "position_size":  round(pos_val / entry, 6) if entry > 0 else 0,
        "leverage":       round(leverage, 2),
        "risk_usd":       round(risk_usd, 2),
        "position_value": round(pos_val, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  PHASE 2: SCORING FUNCTIONS (Coinalyze)
# ══════════════════════════════════════════════════════════════════════════════

# ── CAT-A: Derivatives Positioning ────────────────────────────────────────────
def score_long_short_ratio(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_ls or len(clz.ls_ratio) < 4:
        return 0, {"source": "no_ls_data"}
    hist          = clz.ls_ratio
    current_long  = float(hist[-2].get("l", 0.5) or 0.5)
    long_4h_ago   = float(hist[-5].get("l", 0.5) or 0.5) if len(hist) >= 5 else current_long
    long_trend    = current_long - long_4h_ago
    score, sigs   = 0, []
    if current_long < 0.42:
        score += 30; sigs.append(f"EXTREME_SHORT_DOM longs={current_long:.1%}")
    elif current_long < 0.47:
        score += 20; sigs.append(f"SHORT_DOM longs={current_long:.1%}")
    elif current_long < 0.50:
        score += 10; sigs.append(f"SLIGHT_SHORT_DOM longs={current_long:.1%}")
    if long_trend < -0.02:
        score += 12; sigs.append(f"SHORTS_ADDING Δ={long_trend:.2%}")
    elif long_trend < -0.01:
        score += 6;  sigs.append(f"LONGS_REDUCING Δ={long_trend:.2%}")
    if current_long > 0.58:
        score = max(0, score - 15); sigs.append(f"⚠️ LONG_HEAVY={current_long:.1%}")
    return min(score, 35), {"long_ratio": round(current_long, 4), "signals": sigs}


# ── CAT-B: Order Flow — [FIX-4] window diperlebar 6h → 12h ───────────────────
def score_buy_volume_ratio(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_ohlcv:
        return 0, {"source": "no_ohlcv"}
    hist = clz.ohlcv
    # [FIX-4] hist[-13:-1] = 12 jam terakhir (dari hist[-7:-1] = 6 jam)
    # Whale accumulation biasanya berlangsung 12-48 jam sebelum pump.
    # 6 jam terlalu pendek untuk menangkap pola ini.
    # NO-LOOKAHEAD: hist[-1] adalah candle live, tidak dipakai.
    recent = [c for c in hist[-13:-1] if float(c.get("v", 0) or 0) > 0]
    if len(recent) < 3:
        return 0, {"source": "insufficient_ohlcv"}
    bv_ratios = []
    for c in recent:
        v  = float(c.get("v",  0) or 0)
        bv = float(c.get("bv", 0) or 0)
        if v > 0:
            bv_ratios.append(bv / v)
    if not bv_ratios:
        return 0, {"source": "no_bv_data"}
    avg_bv = _mean(bv_ratios)
    score, sigs = 0, []
    if avg_bv >= 0.62:
        score += 25; sigs.append(f"STRONG_BUY bv/v={avg_bv:.1%}")
    elif avg_bv >= 0.55:
        score += 15; sigs.append(f"NET_BUYING bv/v={avg_bv:.1%}")
    return min(score, 30), {"avg_bv_ratio": round(avg_bv, 4), "signals": sigs,
                             "window_hours": len(recent)}


def score_funding_trend(clz: ClzData, current_funding: float) -> Tuple[int, dict]:
    score, sigs = 0, []
    if current_funding < -0.0010:
        score += 15; sigs.append(f"EXTREME_FUNDING={current_funding*100:.4f}%")
    elif current_funding < -0.0005:
        score += 10; sigs.append(f"STRONG_NEG_FUNDING={current_funding*100:.4f}%")
    elif current_funding < -0.0002:
        score += 6;  sigs.append(f"NEG_FUNDING={current_funding*100:.4f}%")
    if clz.has_funding_hist:
        rates = [float(c.get("c", 0) or 0) for c in clz.funding_hist if c.get("c") is not None]
        if len(rates) >= 6:
            recent = _mean(rates[-3:])
            prev   = _mean(rates[-6:-3])
            drift  = recent - prev
            if drift < -0.0003:
                score += 25; sigs.append(f"FUNDING_TRENDING_NEG Δ={drift*100:.4f}%")
            elif drift < -0.0001:
                score += 15; sigs.append(f"FUNDING_DRIFTING_NEG Δ={drift*100:.4f}%")
    return min(score, 40), {"current": round(current_funding * 100, 5), "signals": sigs}


def score_predicted_funding(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_predicted_funding:
        return 0, {"source": "no_predicted"}
    rates = [float(c.get("c", 0) or 0) for c in clz.predicted_funding_hist if c.get("c") is not None]
    if len(rates) < 6:
        return 0, {"source": "insufficient"}
    recent = _mean(rates[-3:])
    prev   = _mean(rates[-6:-3])
    drift  = recent - prev
    if drift < -0.0002:
        return 20, {"drift": round(drift * 100, 5), "signals": ["PRED_FUNDING_BEARISH"]}
    elif drift < -0.0001:
        return 12, {"drift": round(drift * 100, 5), "signals": ["PRED_FUNDING_NEG"]}
    return 0, {"drift": round(drift * 100, 5)}


def score_oi_buildup(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_oi or len(clz.oi) < 6:
        return 0, {"source": "no_oi"}
    hist   = clz.oi
    oi_now = float(hist[-2].get("c", 0) or 0)
    oi_4h  = float(hist[-5].get("c", 0) or 0)
    if oi_4h <= 0:
        return 0, {"source": "oi_zero"}
    chg    = (oi_now - oi_4h) / oi_4h * 100
    score, sigs = 0, []
    if chg > 5.0:
        score += 20; sigs.append(f"STRONG_OI_BUILDUP OI4h={chg:+.1f}%")
    elif chg > 2.5:
        score += 12; sigs.append(f"OI_BUILDUP OI4h={chg:+.1f}%")
    elif chg > 1.0:
        score += 6;  sigs.append(f"OI_RISING OI4h={chg:+.1f}%")
    return min(score, 20), {"oi_chg_4h_pct": round(chg, 2), "signals": sigs}


def score_liquidations(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_liq or len(clz.liq) < 6:
        return 0, {"source": "no_liq"}
    hist     = clz.liq
    baseline = [float(c.get("s", 0) or 0) for c in hist[-24:-3] if c.get("s") is not None]
    if not baseline:
        return 0, {"source": "no_baseline"}
    current = float(hist[-2].get("s", 0) or 0)
    med     = sorted(baseline)[len(baseline) // 2]
    mad     = sorted([abs(x - med) for x in baseline])[len(baseline) // 2] if baseline else 1
    if mad < 1e-9:
        mad = 1
    z = (current - med) / (mad * 1.4826)
    score, sigs = 0, []
    if z >= 2.5:
        score += 20; sigs.append(f"SHORT_LIQ_SPIKE z={z:.1f}")
    elif z >= 1.5:
        score += 12; sigs.append(f"SHORT_LIQ_ELEVATED z={z:.1f}")
    return min(score, 20), {"short_liq_z": round(z, 2), "signals": sigs}


# ── CAT-C: Price Relative Strength — [FIX-2] gantikan double-count ────────────
def score_btc_decoupling(candles_coin: List[dict],
                          btc_chg_1h: float,
                          btc_chg_4h: float) -> Tuple[int, dict]:
    """
    [FIX-2] Menggantikan detect_rs_btc() + btc_context_bonus sekaligus.
    Satu fungsi = satu fenomena = satu skor. Tidak ada double-count.

    Regime-aware: decoupling saat BTC DUMP jauh lebih bermakna dari saat NEUTRAL.
    NO-LOOKAHEAD: candles[-2] adalah last closed candle.
    Latensi: ~0.1ms (hanya arithmetic).
    """
    w = CONFIG["rs_btc_weight"]   # maks 20

    coin_chg_1h = get_chg_from_candles(candles_coin, 1)
    coin_chg_4h = get_chg_from_candles(candles_coin, 4)
    rs_1h = coin_chg_1h - btc_chg_1h
    rs_4h = coin_chg_4h - btc_chg_4h

    # Klasifikasi regime BTC dari 2 timeframe
    if btc_chg_4h < -3.0 and btc_chg_1h < -1.0:
        btc_regime = "DUMP"
    elif btc_chg_4h < -1.5 or btc_chg_1h < -0.8:
        btc_regime = "BEARISH"
    elif btc_chg_4h > 2.0:
        btc_regime = "BULLISH"   # decoupling tidak bermakna saat BTC rally
    else:
        btc_regime = "NEUTRAL"

    score, pattern, sigs = 0, "INLINE", []

    if btc_regime == "DUMP":
        if rs_1h > 2.0 and rs_4h > 3.0:
            score = w; pattern = "IMMUNE_BTC_DUMP"
            sigs.append(f"IMMUNE_BTC_DUMP rs1h={rs_1h:+.1f}% rs4h={rs_4h:+.1f}%")
        elif rs_1h > 1.0 or rs_4h > 2.0:
            score = int(w * 0.75); pattern = "RESIST_BTC_DUMP"
            sigs.append(f"RESIST_BTC_DUMP rs1h={rs_1h:+.1f}%")
    elif btc_regime == "BEARISH":
        if rs_1h > 2.5 and coin_chg_1h > 0:
            score = int(w * 0.85); pattern = "DECOUPLE_BEARISH"
            sigs.append(f"DECOUPLE_BEARISH rs1h={rs_1h:+.1f}% coin={coin_chg_1h:+.1f}%")
        elif rs_1h > 1.5:
            score = int(w * 0.55); pattern = "OUTPERFORM_BEARISH"
            sigs.append(f"OUTPERFORM_BEARISH rs1h={rs_1h:+.1f}%")
    elif btc_regime == "NEUTRAL":
        if rs_1h > 3.0:
            score = int(w * 0.65); pattern = "STRONG_DECOUPLE"
            sigs.append(f"STRONG_DECOUPLE rs1h={rs_1h:+.1f}%")
        elif rs_1h > 1.5:
            score = int(w * 0.33); pattern = "OUTPERFORMING"
            sigs.append(f"OUTPERFORMING rs1h={rs_1h:+.1f}%")
    # BULLISH: score = 0, decoupling tidak bermakna

    return min(score, w), {
        "rs_1h":       round(rs_1h, 2),
        "rs_4h":       round(rs_4h, 2),
        "btc_regime":  btc_regime,
        "pattern":     pattern,
        "coin_chg_1h": round(coin_chg_1h, 2),
        "signals":     sigs,
    }


# ── CAT-D: Microstructure / Context ───────────────────────────────────────────
def detect_bbw_squeeze(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 22:
        return 0, {}
    bbw = calc_bbw(candles)
    w   = 5
    if bbw > 0.15:   return w,          {"bb_w": round(bbw, 4), "pattern": "WIDE_EXPANSION"}
    elif bbw > 0.10: return int(w*0.8), {"bb_w": round(bbw, 4), "pattern": "EXPANDING"}
    elif bbw > 0.06: return int(w*0.4), {"bb_w": round(bbw, 4), "pattern": "MODERATE"}
    return 0, {"bb_w": round(bbw, 4), "pattern": "TIGHT_SQUEEZE"}


def detect_volume_dryup(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 26:
        return 0, {}
    cur_vol = candles[-2].get("volume_usd", 0)
    avg_vol = _mean([c.get("volume_usd", 0) for c in candles[-26:-2]])
    if avg_vol <= 0:
        return 0, {}
    tod = volume_tod_mult(get_hour_utc())
    adj = (cur_vol * tod) / avg_vol
    w   = 5
    if adj < 0.35:   return w,          {"ratio": round(adj, 2), "pattern": "EXTREME_DRY"}
    elif adj < 0.50: return int(w*0.7), {"ratio": round(adj, 2), "pattern": "VERY_DRY"}
    elif adj < 0.65: return int(w*0.4), {"ratio": round(adj, 2), "pattern": "DRY"}
    return 0, {"ratio": round(adj, 2), "pattern": "NORMAL"}


def detect_accumulation(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 26:
        return 0, {}
    cur_vol  = _mean([c.get("volume_usd", 0) for c in candles[-7:-1]])
    base_vol = _mean([c.get("volume_usd", 0) for c in candles[-25:-7]])
    if base_vol <= 0:
        return 0, {}
    ratio = cur_vol / base_vol
    p_chg = (candles[-2]["close"] - candles[-7]["close"]) / candles[-7]["close"] * 100 \
            if candles[-7]["close"] > 0 else 0
    w = 15
    if ratio >= 3.0 and -2 < p_chg < 4:
        return w,          {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "STRONG_ACCUM"}
    elif ratio >= 2.5 and -2 < p_chg < 5:
        return int(w*0.75),{"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "ACCUM"}
    elif ratio >= 2.0 and -1 < p_chg < 4:
        return int(w*0.5), {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "LIGHT_ACCUM"}
    return 0, {"vol_ratio": round(ratio, 2), "pattern": "NO_ACCUM"}


def detect_volatility_return(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 50:
        return 0, {}
    atr_now  = calc_atr(candles[-22:], 14) * 100
    atr_hist = calc_atr(candles[-72:-24], 14) * 100 if len(candles) >= 74 \
               else calc_atr(candles[:-24], 14) * 100
    if atr_hist <= 0:
        return 0, {}
    ratio = atr_now / atr_hist
    w = 22
    if atr_now >= 5.0:   abs_sc = w
    elif atr_now >= 3.5: abs_sc = int(w * 0.8)
    elif atr_now >= 2.5: abs_sc = int(w * 0.5)
    else:                abs_sc = 0
    if ratio < 0.40:     ratio_sc = int(w * 0.5)
    elif ratio < 0.60:   ratio_sc = int(w * 0.35)
    elif ratio < 0.75:   ratio_sc = int(w * 0.2)
    else:                ratio_sc = 0
    score = min(abs_sc + ratio_sc // 2, w) if abs_sc > 0 and ratio_sc > 0 \
            else max(abs_sc, ratio_sc)
    return score, {"atr_now_pct": round(atr_now, 2), "atr_ratio": round(ratio, 3)}


def detect_lower_wick(candles: List[dict]) -> Tuple[int, dict]:
    wick = calc_lower_wick_pct(candles)
    w    = 15
    if wick >= 1.0:   return w,           {"avg_wick_pct": round(wick, 2), "pattern": "STRONG_REJECTION_WICK"}
    elif wick >= 0.65:return int(w*0.75), {"avg_wick_pct": round(wick, 2), "pattern": "REJECTION_WICK"}
    elif wick >= 0.40:return int(w*0.45), {"avg_wick_pct": round(wick, 2), "pattern": "LIGHT_WICK"}
    return 0, {"avg_wick_pct": round(wick, 2), "pattern": "NO_WICK"}


def detect_momentum_decel(candles: List[dict]) -> Tuple[int, dict]:
    accel = calc_momentum_decel(candles)
    w     = 8
    if accel <= -0.30:   return w,          {"accel": round(accel, 3), "pattern": "STRONG_DECEL"}
    elif accel <= -0.15: return int(w*0.7), {"accel": round(accel, 3), "pattern": "DECEL"}
    elif accel <= -0.05: return int(w*0.35),{"accel": round(accel, 3), "pattern": "SLIGHT_DECEL"}
    return 0, {"accel": round(accel, 3), "pattern": "NO_DECEL"}


def detect_dist_to_support(candles: List[dict], price: float) -> Tuple[int, dict]:
    dist, inside = calc_dist_to_support(candles, price)
    w = 10
    if not inside and 0.3 <= dist <= 1.5:  return w,          {"dist_pct": round(dist, 2), "pattern": "JUST_BOUNCED"}
    elif not inside and 1.5 < dist <= 3.0: return int(w*0.6), {"dist_pct": round(dist, 2), "pattern": "NEAR_SUPPORT"}
    elif not inside and dist < 0.3:        return int(w*0.4), {"dist_pct": round(dist, 2), "pattern": "AT_SUPPORT"}
    return 0, {"dist_pct": round(dist, 2), "pattern": "FAR_FROM_SUPPORT"}


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PHASE CLASSIFIER
# ══════════════════════════════════════════════════════════════════════════════
def classify_phase(chg_24h: float) -> PhaseInfo:
    if chg_24h < -8.0:
        return PhaseInfo("DOWNTREND",    5, "Deep downtrend",        "HIGH")
    elif chg_24h < -3.0:
        return PhaseInfo("WEAK",        15, "Weak",                  "MEDIUM-HIGH")
    elif chg_24h > 25.0:
        return PhaseInfo("PARABOLIC",   10, "Parabolic",             "EXTREME")
    elif chg_24h > 12.0:
        base = max(20, 40 - int(chg_24h - 12) * 2)
        return PhaseInfo("CONTINUATION",base,"Momentum continuation","MEDIUM")
    else:
        if abs(chg_24h) <= 3.0:   base = 45
        elif chg_24h <= 8.0:      base = 40
        else:                     base = 35
        return PhaseInfo("EARLY", base, "Early prime zone", "LOW")


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  FINAL SCORING — dengan Confluence Check [FIX-1] + [FIX-2]
# ══════════════════════════════════════════════════════════════════════════════
def final_score_coin(data: CoinData, phase1_score: int) -> Optional[ScoreResult]:
    sym   = data.symbol
    phase = classify_phase(data.chg_24h)
    log.info(f"  → {sym}: phase={phase.phase} chg_24h={data.chg_24h:+.1f}% "
             f"chg_1h={data.chg_1h:+.1f}% chg_4h={data.chg_4h:+.1f}%")

    # ── Velocity gates ────────────────────────────────────────────────────────
    is_cont = (phase.phase == "CONTINUATION")
    vg      = CONFIG["velocity_gates"]
    if phase.phase not in ["DOWNTREND", "WEAK"]:
        if data.chg_24h < vg["chg_24h_min"]:
            log.info(f"  ✗ {sym} REJECT: chg_24h={data.chg_24h:+.1f}% < min"); return None
        max_24h = vg["chg_24h_max_continuation"] if is_cont else vg["chg_24h_max_early"]
        if data.chg_24h > max_24h:
            log.info(f"  ✗ {sym} REJECT: chg_24h={data.chg_24h:+.1f}% > {max_24h}"); return None
        max_1h = vg["chg_1h_max_continuation"] if is_cont else vg["chg_1h_max_early"]
        if data.chg_1h > max_1h:
            log.info(f"  ✗ {sym} REJECT: chg_1h={data.chg_1h:+.1f}% > {max_1h}"); return None
        if data.chg_4h > vg["chg_4h_max"]:
            log.info(f"  ✗ {sym} REJECT: chg_4h={data.chg_4h:+.1f}% > {vg['chg_4h_max']}"); return None
        if phase.phase == "EARLY" and data.chg_1h < -2.0:
            log.info(f"  ✗ {sym} [EARLY] REJECT: chg_1h={data.chg_1h:+.1f}% < -2%"); return None
        if phase.phase == "EARLY" and data.chg_4h < vg.get("chg_4h_min_early", -3.0):
            log.info(f"  ✗ {sym} [EARLY] REJECT: chg_4h={data.chg_4h:+.1f}%"); return None
    else:
        if data.chg_1h < vg.get("chg_1h_min_reversal", -3.0):
            log.info(f"  ✗ {sym} [{phase.phase}] REJECT: chg_1h={data.chg_1h:+.1f}%"); return None

    # ── CAT-A: Derivatives ────────────────────────────────────────────────────
    ls_sc,   ls_d   = score_long_short_ratio(data.clz)
    bv_sc,   bv_d   = score_buy_volume_ratio(data.clz)
    fund_sc, fund_d = score_funding_trend(data.clz, data.funding)
    pred_sc, pred_d = score_predicted_funding(data.clz)
    oi_sc,   oi_d   = score_oi_buildup(data.clz)
    liq_sc,  liq_d  = score_liquidations(data.clz)
    tier1 = ls_sc + bv_sc + fund_sc + pred_sc
    tier2 = oi_sc + liq_sc

    # ── CAT-B: Order Flow (dari tier3) ───────────────────────────────────────
    bbw_sc,   _ = detect_bbw_squeeze(data.candles)
    dry_sc,   _ = detect_volume_dryup(data.candles)
    accum_sc, _ = detect_accumulation(data.candles)

    # Volume mutual exclusivity
    if dry_sc > 0 and accum_sc > 0:
        if accum_sc >= dry_sc: dry_sc = 0
        else:                  accum_sc = 0

    # ── CAT-C: Price RS — [FIX-2] satu fungsi, satu skor ─────────────────────
    rs_sc, rs_d = score_btc_decoupling(data.candles, data.btc_chg_1h, data.btc_chg_4h)
    btc_regime  = rs_d.get("btc_regime", "NEUTRAL")

    # ── CAT-D: Microstructure ─────────────────────────────────────────────────
    vret_sc,  _ = detect_volatility_return(data.candles)
    wick_sc,  _ = detect_lower_wick(data.candles)
    decel_sc, _ = detect_momentum_decel(data.candles)
    supp_sc,  _ = detect_dist_to_support(data.candles, data.price)

    tier3 = bbw_sc + dry_sc + accum_sc + vret_sc + rs_sc + wick_sc + decel_sc + supp_sc

    # ── [FIX-1] CHECK CONFLUENCE — wajib sebelum threshold check ─────────────
    cf = check_confluence({
        "ls_sc":    ls_sc,
        "fund_sc":  fund_sc,
        "pred_sc":  pred_sc,
        "oi_sc":    oi_sc,
        "liq_sc":   liq_sc,
        "bv_sc":    bv_sc,
        "accum_sc": accum_sc,
        "rs_sc":    rs_sc,
        "vret_sc":  vret_sc,
        "wick_sc":  wick_sc,
        "decel_sc": decel_sc,
        "supp_sc":  supp_sc,
    })

    if not cf.ok:
        log.info(f"  ✗ {sym} BLOCKED: {cf.reason} "
                 f"(A={cf.cat_a} B={cf.cat_b} C={cf.cat_c} D={cf.cat_d})")
        return None

    # ── Coinalyze availability checks ─────────────────────────────────────────
    has_any_clz = (data.clz.has_ohlcv or data.clz.has_oi or data.clz.has_liq
                   or data.clz.has_ls or data.clz.has_funding_hist)

    if not has_any_clz and phase.phase == "EARLY":
        log.info(f"  ✗ {sym} [EARLY] REJECT: no Coinalyze data"); return None

    if has_any_clz and (tier1 + tier2) < 15:
        log.info(f"  ✗ {sym} REJECT: tier1+tier2={tier1+tier2} < 15 "
                 f"(ls={ls_sc} bv={bv_sc} fund={fund_sc} pred={pred_sc})"); return None

    tier1_hard = ls_sc + bv_sc + fund_sc
    if has_any_clz and pred_sc > 0 and tier1_hard == 0 and tier2 < 20:
        log.info(f"  ✗ {sym} REJECT: tier1 hanya dari pred_funding tanpa sinyal keras"); return None

    # ── Total score ───────────────────────────────────────────────────────────
    phase_score = phase.base_score
    # [FIX-2] btc_context_bonus DIHAPUS. Semua BTC RS sudah masuk rs_sc (CAT-C).
    total = phase_score + tier1 + tier2 + tier3

    # ── Time-of-day discount (01:00-05:00 UTC = low liquidity) ────────────────
    hour = get_hour_utc()
    tod_discount = 0
    if 1 <= hour <= 5:
        tod_discount = int(total * 0.20)
        total = max(0, total - tod_discount)
        log.info(f"  ⏰ {sym} TOD discount -{tod_discount} (hour={hour} UTC low-liquidity)")

    # ── Threshold check ───────────────────────────────────────────────────────
    if phase.phase == "EARLY":
        threshold = CONFIG["alert_threshold_early"]
    elif phase.phase == "CONTINUATION":
        threshold = CONFIG["alert_threshold_continuation"]
    elif phase.phase in ["DOWNTREND", "WEAK"]:
        threshold = CONFIG["alert_threshold_reversal"]
    else:
        threshold = 110

    log.info(
        f"  ~ {sym} [{phase.phase}] score={total} vs threshold={threshold} | "
        f"phase={phase_score} t1={tier1} t2={tier2} t3={tier3} | "
        f"confluence={cf.reason} | "
        f"ls={ls_sc} bv={bv_sc} fund={fund_sc} pred={pred_sc} oi={oi_sc} liq={liq_sc} | "
        f"bbw={bbw_sc} dry={dry_sc} acc={accum_sc} vret={vret_sc} rs={rs_sc} "
        f"wick={wick_sc} decel={decel_sc} supp={supp_sc} | BTC={btc_regime}"
    )

    if total < threshold:
        log.info(f"  ✗ {sym} REJECT: total={total} < threshold={threshold}"); return None

    # ── Pump type classification ───────────────────────────────────────────────
    pump_types = []
    if ls_sc >= 8 and (liq_sc >= 6 or fund_sc >= 7):
        pump_types.append(PumpType("E", "Short Squeeze",
                                   min((ls_sc+liq_sc+fund_sc+pred_sc)*2, 100), []))
    if bv_sc >= 8 and accum_sc >= 5:
        pump_types.append(PumpType("B", "Whale Accumulation",
                                   min((bv_sc+accum_sc)*3, 100), []))
    if bbw_sc >= 8 and dry_sc >= 5 and (oi_sc >= 6 or liq_sc >= 6 or fund_sc >= 10):
        pump_types.append(PumpType("D", "Technical Breakout",
                                   min((bbw_sc+dry_sc)*3, 100), []))
    if vret_sc >= 10:
        pump_types.append(PumpType("F", "Volatility Return", min(vret_sc*5, 100), []))
    if rs_sc >= 12 and btc_regime in ["DUMP", "BEARISH"]:
        pump_types.append(PumpType("R", "BTC Decoupling", min(rs_sc*5, 100), []))
    if not pump_types:
        pump_types.append(PumpType("T", "Technical Setup", min(total, 100), []))

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    entry_data = calc_entry_targets(data.candles, data.price)
    if entry_data is None:
        log.info(f"  ✗ {sym} REJECT: calc_entry_targets=None (RR tidak terpenuhi)"); return None

    position = calc_position_size(entry_data["entry"], entry_data["sl"], entry_data["atr_decimal"])

    # Komponen skor untuk logging & fingerprint
    score_components = {
        "ls_sc": ls_sc, "bv_sc": bv_sc, "fund_sc": fund_sc,
        "pred_sc": pred_sc, "oi_sc": oi_sc, "liq_sc": liq_sc,
        "rs_sc": rs_sc, "vret_sc": vret_sc, "wick_sc": wick_sc,
        "decel_sc": decel_sc, "supp_sc": supp_sc,
    }
    fingerprint = make_signal_fingerprint(score_components)

    return ScoreResult(
        symbol=data.symbol,
        score=min(total, 250),
        phase=phase.phase,
        pump_types=pump_types,
        confidence="very_strong" if total >= 130 else "strong" if total >= 95 else "watch",
        components={
            "phase":            phase_score,
            "tier1_clz":        tier1,
            "tier2_clz":        tier2,
            "tier3_technical":  tier3,
            "tod_discount":     tod_discount,
            "data_sources":     "Coinalyze" if has_any_clz else "Bitget-only",
            "btc_regime":       btc_regime,
            "rs_pattern":       rs_d.get("pattern", ""),
        },
        catalysts=[],
        entry=entry_data,
        price=data.price,
        vol_24h=data.vol_24h,
        chg_24h=data.chg_24h,
        chg_1h=data.chg_1h,
        funding=data.funding,
        urgency="",
        confluence=cf,
        risk_warnings=[],
        position=position,
        bitget_phase1_score=phase1_score,
        signal_fingerprint=fingerprint,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📤  TELEGRAM & ALERT FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(message: str) -> bool:
    bot_token = CONFIG.get("bot_token")
    chat_id   = CONFIG.get("chat_id")
    if not bot_token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def build_alert(r: ScoreResult, rank: int) -> str:
    vol     = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    emoji   = {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(r.confidence, "⚪")
    bar_len = min(20, r.score * 20 // 200)
    bar     = "█" * bar_len + "░" * (20 - bar_len)

    cf = r.confluence
    cat_bar = (f"A={cf.cat_a}{'✓' if cf.cat_a >= CONFIG['confluence_cat_a_min'] else '✗'} "
               f"B={cf.cat_b}{'✓' if cf.cat_b >= CONFIG['confluence_cat_b_min'] else '✗'} "
               f"C={cf.cat_c}{'✓' if cf.cat_c >= CONFIG['confluence_cat_c_min'] else '✗'} "
               f"D={cf.cat_d}{'✓' if cf.cat_d >= CONFIG['confluence_cat_d_min'] else '✗'}")

    pump_str = " | ".join(f"{pt.type_code}:{pt.type_name}" for pt in r.pump_types)
    btc_reg  = r.components.get("btc_regime", "?")
    rs_pat   = r.components.get("rs_pattern", "")

    lines = [
        f"{'─'*58}",
        f"#{rank}  {r.symbol}  {emoji}  Score: {r.score}  [{r.phase}]",
        f"   {bar}",
        f"   Confluence: {cf.reason}",
        f"   {cat_bar}",
        f"   Type: {pump_str}",
        f"   Fingerprint: {r.signal_fingerprint}",
        f"",
        f"   Vol: {vol} | Δ1h: {r.chg_1h:+.1f}% | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding*100:.4f}%",
        f"   T1:{r.components['tier1_clz']} T2:{r.components['tier2_clz']} T3:{r.components['tier3_technical']}",
        f"   BTC Regime: {btc_reg} | RS Pattern: {rs_pat}",
        f"   Phase1 Score: {r.bitget_phase1_score} (threshold {CONFIG['phase1_threshold']})",
    ]

    if r.entry:
        e      = r.entry
        method = f"[{e.get('method','?')}|{e.get('vol_regime','?')}]"
        lines += [
            f"",
            f"   💰 ENTRY ZONE  {method}  S/R: {e.get('n_supports',0)}sup / {e.get('n_resistances',0)}res",
            f"      Low:  ${e['entry_zone_low']:.8f}",
            f"      Mid:  ${e['entry']:.8f}",
            f"      High: ${e['entry_zone_high']:.8f}",
            f"      SL:   ${e['sl']:.8f}  (-{e['sl_pct']:.1f}%)  ← {e.get('sl_source','')}",
            f"      TP1:  ${e['tp1']:.8f}  (+{e['tp1_pct']:.1f}%)  R/R {e['rr1']:.1f}x",
            f"      TP2:  ${e['tp2']:.8f}  (+{e['tp2_pct']:.1f}%)  R/R {e.get('rr2',0):.1f}x",
            f"      TP3:  ${e['tp3']:.8f}  (+{e['tp3_pct']:.1f}%)  R/R {e.get('rr3',0):.1f}x",
            f"      Src:  {e.get('tp_source','')}",
            f"      ATR:  {e['atr_pct']:.2f}%",
        ]

    if r.position:
        p = r.position
        lines.append(
            f"      Size: {p['position_size']:.4f} | Lev: {p['leverage']:.1f}x | "
            f"Risk: ${p['risk_usd']:.0f} | Val: ${p['position_value']:.0f}"
        )

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCANNER LOOP
# ══════════════════════════════════════════════════════════════════════════════
def main():
    log.info("═" * 70)
    log.info(f"  PRE-PUMP SCANNER v{VERSION}")
    log.info("  Phase 1: Bitget filter (ATR+range+BBW+wick+support+decel+MOMENTUM)")
    log.info("  Phase 2: Coinalyze + CONFLUENCE CHECK (3/4 kategori wajib)")
    log.info(f"  Whitelist: {len(CONFIG['whitelist'])} symbols")
    log.info("═" * 70)

    if not CONFIG.get("coinalyze_api_key"):
        log.error("❌ COINALYZE_API_KEY not set")
        return 1

    init_db()

    # ── Precision report dari cycle sebelumnya ────────────────────────────────
    pr = get_precision_report()
    if pr:
        log.info("📊 Precision Report (live data):")
        for k, v in sorted(pr.items()):
            log.info(f"   {k}: {v['precision']}% ({v['hits']}/{v['total']})")

    # ── Step 1: Bitget tickers & BTC data ─────────────────────────────────────
    log.info("📊 Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        log.error("❌ No tickers from Bitget")
        return 1

    log.info("📊 Checking previous alert outcomes...")
    check_and_update_outcomes(tickers)

    btc_candles = BitgetClient.get_candles("BTCUSDT", 30)
    btc_chg_1h = btc_chg_4h = btc_chg_24h = 0.0

    # [FIX-2] BTC realtime: gunakan lastPr ticker vs candles[-2] close
    btc_ticker = tickers.get("BTCUSDT")
    if btc_ticker and len(btc_candles) >= 5:
        btc_current    = float(btc_ticker.get("lastPr", 0) or 0)
        btc_prev_close = btc_candles[-2]["close"]
        if btc_current > 0 and btc_prev_close > 0:
            btc_chg_1h = (btc_current - btc_prev_close) / btc_prev_close * 100
        # [FIX-2] Tambah btc_chg_4h untuk regime detection di score_btc_decoupling
        btc_4h_close = btc_candles[-5]["close"] if len(btc_candles) >= 5 else btc_prev_close
        if btc_4h_close > 0 and btc_prev_close > 0:
            btc_chg_4h = (btc_prev_close - btc_4h_close) / btc_4h_close * 100
    elif len(btc_candles) >= 3:
        btc_chg_1h = (btc_candles[-2]["close"] - btc_candles[-3]["close"]) / btc_candles[-3]["close"] * 100

    if len(btc_candles) >= 26:
        btc_chg_24h = get_chg_from_candles(btc_candles, 24)

    log.info(f"  BTC 1h: {btc_chg_1h:+.2f}% | BTC 4h: {btc_chg_4h:+.2f}% | BTC 24h: {btc_chg_24h:+.2f}%")

    # BTC circuit breaker
    if btc_chg_1h < CONFIG["btc_dump_threshold"]:
        log.warning(f"⛔ BTC circuit breaker: {btc_chg_1h:+.1f}% — scan paused")
        return 0

    # ── Step 2: Phase 1 — Bitget-only filter ──────────────────────────────────
    log.info("🔍 Phase 1: Bitget-only filtering...")
    candidates_phase1 = []

    for sym in CONFIG["whitelist"]:
        if not is_valid_symbol(sym):
            continue
        if is_on_cooldown(sym):
            continue
        if is_stock_token(sym):
            continue

        try:
            ticker = tickers.get(sym)
            if not ticker:
                continue
            price  = float(ticker.get("lastPr", 0))
            if price <= 0:
                continue
            vol_24h = float(ticker.get("quoteVolume", 0))
            if vol_24h < CONFIG["phase1_min_volume_usd"]:
                continue

            candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
            if len(candles) < 30:
                continue

            # [FIX-3] Hitung chg_1h & chg_4h untuk momentum filter di Phase 1
            chg_1h_p1 = get_chg_from_candles(candles, 1)
            chg_4h_p1 = get_chg_from_candles(candles, 4)

            score, details = phase1_bitget_filter(candles, vol_24h, chg_1h_p1, chg_4h_p1)
            if score >= CONFIG["phase1_threshold"]:
                candidates_phase1.append((sym, score, candles, ticker))
                log.debug(f"  {sym}: Phase1 score={score} → passed")
            else:
                log.debug(f"  {sym}: Phase1 score={score} → rejected")
        except Exception as e:
            log.warning(f"  {sym} phase1 error: {e}")

    log.info(f"  Phase1 passed: {len(candidates_phase1)} candidates")

    if not candidates_phase1:
        log.info("No candidates passed phase1 filter.")
        return 0

    # ── Step 3: Build Coinalyze maps ──────────────────────────────────────────
    log.info("🗺️  Building Coinalyze maps...")
    clz_client      = CoinalyzeClient(CONFIG["coinalyze_api_key"])
    candidate_symbols = [s for s, _, _, _ in candidates_phase1]
    clz_client.build_symbol_maps(candidate_symbols)

    # ── Step 4: Fetch Coinalyze data ──────────────────────────────────────────
    log.info("📈 Fetching Coinalyze data...")
    now_ts   = int(time.time())
    from_ts  = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_data = clz_client.fetch_for_symbols(candidate_symbols, from_ts, now_ts)

    # ── Step 5: Phase 2 — Final scoring ───────────────────────────────────────
    log.info("🎯 Phase 2: Final scoring with Confluence check...")
    final_results = []

    for sym, p1_score, candles, ticker in candidates_phase1:
        try:
            price   = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            chg_24h = get_chg_from_candles(candles, 24)
            chg_1h  = get_chg_from_candles(candles, 1)
            chg_4h  = get_chg_from_candles(candles, 4)
            funding = BitgetClient.get_funding(sym)

            coin_data = CoinData(
                symbol=sym,
                price=price,
                vol_24h=vol_24h,
                chg_24h=chg_24h,
                chg_1h=chg_1h,
                chg_4h=chg_4h,
                funding=funding,
                candles=candles,
                btc_chg_1h=btc_chg_1h,
                btc_chg_4h=btc_chg_4h,    # [FIX-2]
                btc_chg_24h=btc_chg_24h,
                clz=clz_data.get(sym, ClzData()),
            )

            result = final_score_coin(coin_data, p1_score)
            if result:
                final_results.append(result)
                cf = result.confluence
                log.info(f"  ✅ {sym}: score={result.score} confluence={cf.reason} "
                         f"(A={cf.cat_a} B={cf.cat_b} C={cf.cat_c} D={cf.cat_d})")
        except Exception as e:
            log.warning(f"  {sym} final scoring error: {e}")

    # ── Step 6: Sort & send alerts ────────────────────────────────────────────
    final_results.sort(key=lambda x: x.score, reverse=True)
    max_alerts = CONFIG["max_alerts_per_scan"]

    log.info(f"\n{'═'*70}")
    log.info(f"  DONE: {len(final_results)} final signals | top {min(max_alerts, len(final_results))}")
    log.info(f"{'═'*70}\n")

    sent = 0
    for rank, res in enumerate(final_results[:10], 1):
        msg = build_alert(res, rank)
        print(msg)
        if sent < max_alerts:
            if send_telegram(msg):
                sent += 1
            entry_price = res.entry["entry"] if res.entry else res.price
            set_alert(res.symbol, res.score, res.phase, entry_price, result=res)

    if not final_results:
        log.info("No final signals this cycle.")

    BitgetClient.clear_cache()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
        sys.exit(0)
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True)
        sys.exit(1)
