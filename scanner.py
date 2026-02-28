#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRE-PUMP SCANNER v14.1-RESEARCH (Bitget Futures)
═══════════════════════════════════════════════════════════════════════════════
Berdasarkan riset perbandingan 12 koin pump vs 16 non-pump (Feb 2026, ~17k candle)

PERBAIKAN DARI v14.0:
  [FIX 1]  VWAP dihitung per sesi harian (reset midnight UTC), bukan cumsum total window
  [FIX 2]  Funding streak memiliki tiered score: 5-9=+1, 10-14=+2, ≥15=+3
  [FIX 3]  Above VWAP threshold naik: ≥9/12 (75%) untuk parsial, ≥10/12 (83%) full score
  [FIX 4]  Entry zone dihitung dari close/EMA20/VWAP + SL dari low 3 candle
  [FIX 5]  Ditambahkan EMA7, EMA20, EMA50 untuk analisis entry/SL
  [FIX 6]  CoinGecko pakai search endpoint + mapping ID yang benar
  [FIX 7]  Cooldown persisten ke file JSON (tidak reset saat script restart)
  [FIX 8]  Telegram menggunakan parse_mode=Markdown dengan format yang benar
  [FIX 9]  Higher Low: deteksi swing low lebih fleksibel (window 5, bukan hanya 2)
  [FIX 10] Volume filter MAX turun ke 15M (fokus small/mid cap sesuai riset)
  [FIX 11] BB Squeeze menggunakan percentile 20 dari full window (robust vs NaN)
  [FIX 12] Ditambah skor cum_funding < -0.05 (bahan bakar short squeeze)
  [FIX 13] BOS up gunakan close confirmation, bukan hanya high

ARSITEKTUR SCORING (Maks 32 poin):
  ─ GATE  : neg_streak ≥ 5  ATAU  (neg_pct ≥ 70% DAN cum_funding < -0.015)
  ─ TIER A (Funding)    : tiered streak + cum_funding sangat negatif     [maks 5]
  ─ TIER B (Struktural) : above_vwap + bos_up + higher_lows              [maks 15]
  ─ TIER C (Momentum)   : RSI + volume + ATR + BB_pct                    [maks 10]
  ─ TIER D (Kontekstual): funding_reset + small_cap + bb_squeeze          [maks 4]  ← turun dari 5
  → WATCH  : ≥ 10
  → ALERT  : ≥ 16
  → STRONG : ≥ 20
═══════════════════════════════════════════════════════════════════════════════
"""

import ccxt
import requests
import pandas as pd
import numpy as np
import time
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

# =============================================================================
# KONFIGURASI
# =============================================================================
TIMEFRAME       = '5m'
CANDLE_LIMIT    = 120         # Lebih banyak candle agar VWAP sesi hari ini akurat
FUNDING_LIMIT   = 100         # Jumlah periode funding history

MIN_VOLUME_USDT = 100_000     # [FIX 10] Minimal volume 24h
MAX_VOLUME_USDT = 15_000_000  # [FIX 10] Maks 15M → fokus small/mid cap
MAX_PRICE_CHG   = 30          # Hindari koin yang sudah pump/dump ekstrem 24h

SCORE_WATCH  = 10
SCORE_ALERT  = 16
SCORE_STRONG = 20

COOLDOWN_HOURS = 6
COOLDOWN_FILE  = Path('/tmp/scanner_v14_cooldown.json')  # [FIX 7] Persisten

# Telegram
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
CHAT_ID   = os.environ.get('CHAT_ID', '')

# Bitget exchange
exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/tmp/scanner_v14.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('scanner')

# =============================================================================
# [FIX 7] COOLDOWN PERSISTEN
# =============================================================================
def load_cooldown() -> dict:
    """Baca cooldown dari file JSON (timestamp Unix float)."""
    if COOLDOWN_FILE.exists():
        try:
            return json.loads(COOLDOWN_FILE.read_text())
        except Exception:
            pass
    return {}

def save_cooldown(cd: dict):
    """Simpan cooldown ke file JSON."""
    try:
        COOLDOWN_FILE.write_text(json.dumps(cd))
    except Exception as e:
        log.warning(f"Gagal simpan cooldown: {e}")

def is_on_cooldown(symbol: str, cd: dict) -> bool:
    return symbol in cd and time.time() < cd[symbol]

def set_cooldown(symbol: str, cd: dict):
    cd[symbol] = time.time() + COOLDOWN_HOURS * 3600
    save_cooldown(cd)

# =============================================================================
# FETCH MARKET LIST
# =============================================================================
def fetch_bitget_futures() -> list:
    """Ambil semua pair USDT perpetual aktif dari Bitget."""
    try:
        markets = exchange.load_markets()
        symbols = [
            sym for sym, m in markets.items()
            if m['quote'] == 'USDT' and m['swap'] and m['active']
        ]
        log.info(f"Total USDT perpetual: {len(symbols)}")
        return symbols
    except Exception as e:
        log.error(f"Gagal load markets: {e}")
        return []

# =============================================================================
# FUNDING RATE
# =============================================================================
def fetch_funding_history(symbol: str) -> pd.DataFrame:
    """Ambil funding rate history dari Bitget via ccxt."""
    try:
        data = exchange.fetch_funding_rate_history(symbol, limit=FUNDING_LIMIT)
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df['fundingRate'] = df['fundingRate'].astype(float)
        df['timestamp']   = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        return df.sort_values('timestamp').reset_index(drop=True)
    except Exception as e:
        log.debug(f"Funding history error {symbol}: {e}")
        return pd.DataFrame()

def compute_funding_stats(df: pd.DataFrame) -> dict | None:
    """
    Hitung statistik funding rate.
    Return None jika data tidak cukup.
    """
    if df.empty or len(df) < 10:
        return None
    rates = df['fundingRate'].tolist()

    # Streak negatif berturut-turut terbaru
    neg_streak = 0
    for r in reversed(rates):
        if r < 0:
            neg_streak += 1
        else:
            break

    neg_count  = sum(1 for r in rates if r < 0)
    neg_pct    = neg_count / len(rates) * 100
    cumulative = sum(rates)
    avg6       = float(np.mean(rates[-6:])) if len(rates) >= 6 else float(np.mean(rates))

    return {
        'neg_streak': neg_streak,
        'neg_count' : neg_count,
        'neg_pct'   : neg_pct,
        'cumulative': cumulative,
        'avg6'      : avg6,
        'total'     : len(rates),
    }

# =============================================================================
# OHLCV + INDIKATOR
# =============================================================================
def fetch_ohlcv(symbol: str, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=limit)
        df  = pd.DataFrame(raw, columns=['timestamp','open','high','low','close','volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        return df
    except Exception as e:
        log.debug(f"OHLCV error {symbol}: {e}")
        return pd.DataFrame()


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hitung semua indikator teknikal yang dibutuhkan scanner.
    Minimal 30 baris diperlukan; lebih banyak lebih baik.
    """
    if df.empty or len(df) < 30:
        return df

    close = df['close']
    high  = df['high']
    low   = df['low']
    vol   = df['volume']

    # ── EMA [FIX 5] ───────────────────────────────────────────────────────────
    df['ema7']  = close.ewm(span=7,  adjust=False).mean()
    df['ema20'] = close.ewm(span=20, adjust=False).mean()
    df['ema50'] = close.ewm(span=50, adjust=False).mean()

    # ── VWAP per sesi harian [FIX 1] ──────────────────────────────────────────
    # Reset VWAP setiap pergantian hari UTC (bukan cumsum seluruh window)
    tp = (high + low + close) / 3
    df['date_utc'] = df['timestamp'].dt.date
    df['tp_vol']   = tp * vol
    # Group by tanggal → cumsum dalam hari yang sama
    df['vwap_num'] = df.groupby('date_utc')['tp_vol'].cumsum()
    df['vwap_den'] = df.groupby('date_utc')['volume'].cumsum()
    df['vwap']     = df['vwap_num'] / (df['vwap_den'] + 1e-12)
    df.drop(columns=['tp_vol','vwap_num','vwap_den','date_utc'], inplace=True)

    # ── RSI 14 ────────────────────────────────────────────────────────────────
    delta = close.diff()
    gain  = delta.where(delta > 0, 0.0).ewm(span=14, adjust=False).mean()
    loss  = (-delta.where(delta < 0, 0.0)).ewm(span=14, adjust=False).mean()
    df['rsi14'] = 100 - (100 / (1 + gain / (loss + 1e-12)))

    # ── ATR 14 ────────────────────────────────────────────────────────────────
    hl  = high - low
    hcp = (high - close.shift()).abs()
    lcp = (low  - close.shift()).abs()
    tr         = pd.concat([hl, hcp, lcp], axis=1).max(axis=1)
    df['atr14']  = tr.rolling(14).mean()
    df['atr_pct']= df['atr14'] / close * 100

    # ── Bollinger Bands (20, 2) ───────────────────────────────────────────────
    bb_mid      = close.rolling(20).mean()
    bb_std      = close.rolling(20).std(ddof=0)
    bb_up       = bb_mid + 2 * bb_std
    bb_dn       = bb_mid - 2 * bb_std
    bb_range    = bb_up - bb_dn
    df['bb_mid']  = bb_mid
    df['bb_up']   = bb_up
    df['bb_dn']   = bb_dn
    df['bb_pct']  = (close - bb_dn) / (bb_range + 1e-12)
    df['bb_width']= bb_range / (bb_mid + 1e-12)

    # [FIX 11] BB Squeeze: lebar di bawah persentil ke-20 dari seluruh window
    bb_w_p20      = df['bb_width'].quantile(0.20)
    df['bb_squeeze'] = df['bb_width'] < bb_w_p20

    # ── Volume Ratio ──────────────────────────────────────────────────────────
    df['vol_ma12'] = vol.rolling(12).mean()
    df['vol_ratio']= vol / (df['vol_ma12'] + 1e-12)

    # ── BOS Up [FIX 13] ───────────────────────────────────────────────────────
    # Break of Structure: CLOSE candle saat ini melewati HIGH tertinggi 20 candle
    # sebelumnya (gunakan close, bukan high, untuk konfirmasi lebih ketat)
    prev_max_high  = high.rolling(20).max().shift(1)
    df['bos_up']   = close > prev_max_high

    # ── Higher Low [FIX 9] ────────────────────────────────────────────────────
    # Swing low: low saat ini lebih tinggi dari 2 low sebelumnya dalam window 5
    # (lebih fleksibel daripada hanya low > low[-1] > low[-2])
    low_min5       = low.rolling(5, min_periods=3).min().shift(1)
    df['higher_low']= low > low_min5

    # ── Above VWAP ────────────────────────────────────────────────────────────
    df['above_vwap']= close > df['vwap']

    return df


def compute_above_vwap_count(df: pd.DataFrame, n: int = 12) -> int:
    """Hitung berapa candle dari n terakhir yang close-nya di atas VWAP."""
    if df.empty or len(df) < n or 'above_vwap' not in df.columns:
        return 0
    return int(df['above_vwap'].tail(n).sum())

# =============================================================================
# [FIX 6] COINGECKO CONTEXT
# =============================================================================
# Peta ticker umum → CoinGecko ID
# Tambahkan mapping jika ditemukan koin yang gagal resolve
CG_OVERRIDE = {
    'pepe'    : 'pepe',
    'orca'    : 'orca',
    'om'      : 'mantra-dao',
    'ygg'     : 'yield-guild-games',
    'ace'     : 'ace',
    'zec'     : 'zcash',
    'kite'    : 'dextf',
    'skr'     : 'skrilla-token',
    'enso'    : 'enso-finance',
    'bio'     : 'biopassport',
    'sahara'  : 'sahara-ai',
    'holo'    : 'holotoken',
    'op'      : 'optimism',
    'ada'     : 'cardano',
    'sui'     : 'sui',
    'tao'     : 'bittensor',
    'ena'     : 'ethena',
    'trx'     : 'tron',
    'fil'     : 'filecoin',
    'uni'     : 'uniswap',
    'atom'    : 'cosmos',
    'zk'      : 'zksync',
    'sol'     : 'solana',
    'avax'    : 'avalanche-2',
    'link'    : 'chainlink',
    'dot'     : 'polkadot',
    'bnb'     : 'binancecoin',
    'arb'     : 'arbitrum',
    'near'    : 'near',
    'inj'     : 'injective-protocol',
    'apt'     : 'aptos',
    'sei'     : 'sei-network',
    'mnt'     : 'mantle',
    'aevo'    : 'aevo',
    'strk'    : 'starknet',
    'w'       : 'wormhole',
    'jup'     : 'jupiter-exchange-solana',
    'pyth'    : 'pyth-network',
    'ondo'    : 'ondo-finance',
    'io'      : 'io-net',
    'render'  : 'render-token',
    'fet'     : 'fetch-ai',
}

_cg_rank_cache: dict[str, dict] = {}

def get_coingecko_context(ticker: str) -> dict:
    """
    Cari rank dan ath_dist dari CoinGecko.
    Menggunakan override map terlebih dahulu, lalu search endpoint sebagai fallback.
    Return {} jika gagal (scanner tetap berjalan tanpa konteks ini).
    """
    ticker_lower = ticker.lower()
    if ticker_lower in _cg_rank_cache:
        return _cg_rank_cache[ticker_lower]

    # Resolve coin ID
    coin_id = CG_OVERRIDE.get(ticker_lower)
    if not coin_id:
        # Fallback: search by ticker
        try:
            search_res = requests.get(
                'https://api.coingecko.com/api/v3/search',
                params={'query': ticker_lower}, timeout=8
            ).json()
            coins = search_res.get('coins', [])
            if coins:
                # Ambil hasil pertama yang symbolnya cocok persis
                for c in coins[:5]:
                    if c.get('symbol', '').lower() == ticker_lower:
                        coin_id = c['id']
                        break
                if not coin_id:
                    coin_id = coins[0]['id']
        except Exception:
            pass

    if not coin_id:
        return {}

    try:
        data = requests.get(
            f'https://api.coingecko.com/api/v3/coins/{coin_id}',
            params={'localization': 'false', 'tickers': 'false',
                    'market_data': 'true', 'community_data': 'false'},
            timeout=10
        ).json()
        md = data.get('market_data', {})
        result = {
            'rank'    : data.get('market_cap_rank', 9999) or 9999,
            'ath_dist': md.get('ath_change_percentage', {}).get('usd', 0) or 0,
        }
        _cg_rank_cache[ticker_lower] = result
        return result
    except Exception as e:
        log.debug(f"CoinGecko error ({ticker}): {e}")
        return {}

# =============================================================================
# SCORING ENGINE
# =============================================================================
def compute_score(
    symbol: str,
    funding: dict,
    df: pd.DataFrame,
    coin_ctx: dict
) -> tuple[int, list[str]]:
    """
    Hitung skor pra-pump (maks 32).
    Semua tier mengacu hasil riset pump vs non-pump Feb 2026.

    Returns
    -------
    score   : int   total skor
    signals : list  label sinyal yang aktif
    """
    score   = 0
    signals = []
    last    = df.iloc[-1]

    # ── TIER A: Funding [maks 5] ──────────────────────────────────────────────
    # [FIX 2] Tiered streak score (sebelumnya hanya ≥15 yang dapat poin)
    streak = funding['neg_streak']
    if streak >= 15:
        score += 3
        signals.append(f"neg_streak_extreme ({streak}×)")
    elif streak >= 10:
        score += 2
        signals.append(f"neg_streak_high ({streak}×)")
    elif streak >= 5:
        score += 1
        signals.append(f"neg_streak ({streak}×)")

    # [FIX 12] Cumulative funding sangat negatif → bahan bakar short squeeze
    cum = funding['cumulative']
    if cum < -0.05:
        score += 2
        signals.append(f"cum_funding_extreme ({cum:.4f})")
    elif cum < -0.02:
        score += 1
        signals.append(f"cum_funding_neg ({cum:.4f})")

    # ── TIER B: Sinyal Struktural [maks 15] ───────────────────────────────────
    # [FIX 3] Above VWAP: threshold naik
    above_count = compute_above_vwap_count(df, 12)
    if above_count >= 10:           # ≥83% = riset 90.6%
        score += 5
        signals.append(f"above_vwap_strong ({above_count}/12)")
    elif above_count >= 9:          # ≥75%
        score += 3
        signals.append(f"above_vwap_moderate ({above_count}/12)")
    elif above_count >= 8:          # ≥67% (sinyal lemah tetap dicatat)
        score += 1
        signals.append(f"above_vwap_weak ({above_count}/12)")

    # [FIX 13] BOS Up: gunakan close > prev max high (confirmation lebih ketat)
    if df['bos_up'].tail(6).any():
        score += 5
        signals.append("bos_up_confirmed")

    # [FIX 9] Higher Low: lebih fleksibel
    if df['higher_low'].tail(12).sum() >= 2:
        score += 2
        signals.append("higher_lows_forming")
    elif df['higher_low'].tail(12).sum() == 1:
        score += 1
        signals.append("higher_low_emerging")

    # ── TIER C: Sinyal Momentum [maks 10] ─────────────────────────────────────
    rsi = last.get('rsi14', 50)
    if rsi > 60:
        score += 3
        signals.append(f"rsi_strong ({rsi:.1f})")
    elif rsi > 55:
        score += 2
        signals.append(f"rsi_above55 ({rsi:.1f})")

    vol_surge = (df['vol_ratio'].tail(6) > 1.3).sum()
    if vol_surge >= 4:
        score += 3
        signals.append("volume_surge_strong")
    elif vol_surge >= 3:
        score += 2
        signals.append("volume_surge")

    atr_pct = last.get('atr_pct', 0)
    if atr_pct > 1.0:
        score += 2
        signals.append(f"atr_expanding ({atr_pct:.2f}%)")
    elif atr_pct > 0.7:
        score += 1
        signals.append(f"atr_moderate ({atr_pct:.2f}%)")

    bb_pct = last.get('bb_pct', 0.5)
    if bb_pct > 0.65:
        score += 2
        signals.append(f"upper_band ({bb_pct:.2f})")

    # ── TIER D: Sinyal Kontekstual [maks 4] ───────────────────────────────────
    # [FIX 2] Funding reset: neg_streak ≥5 namun avg6 sudah mendingan
    avg6 = funding.get('avg6', -1)
    if streak >= 5 and avg6 >= -0.00005:
        score += 2
        signals.append(f"funding_reset (avg6={avg6:.6f})")

    # Small cap (rank > 1000)
    rank = coin_ctx.get('rank', 0)
    if 1000 < rank <= 5000:
        score += 1
        signals.append(f"small_cap (rank #{rank})")
    elif rank > 5000:
        score += 1
        signals.append(f"micro_cap (rank #{rank})")

    # [FIX 11] BB Squeeze robust
    if df['bb_squeeze'].tail(12).sum() >= 4:
        score += 1
        signals.append("bb_squeezed")

    return score, signals

# =============================================================================
# [FIX 4] ENTRY ZONE — berdasarkan riset
# =============================================================================
def compute_entry_zone(df: pd.DataFrame, current_price: float) -> dict:
    """
    Hitung zona entry, SL, dan TP sesuai riset:
    - Entry : close BOS atau di atas VWAP / EMA20 (pilih yang lebih konservatif)
    - SL    : bawah low 3 candle terakhir (buffer 0.3%)
    - TP1   : +0.8%  (median pump_max_pct)
    - TP2   : +1.8%  (persentil 75)
    - TP3   : +3.7%  (persentil 90)
    """
    last     = df.iloc[-1]
    vwap     = last.get('vwap', current_price)
    ema20    = last.get('ema20', current_price)
    ema50    = last.get('ema50', current_price)

    # Entry: lebih tinggi antara VWAP dan EMA20 (smart money support)
    entry_level  = max(vwap, ema20) * 1.001   # sedikit di atas untuk konfirmasi

    # Pastikan entry tidak terlalu jauh dari harga sekarang (maks 2%)
    if abs(entry_level - current_price) / current_price > 0.02:
        entry_level = current_price

    # SL: bawah low 3 candle terakhir, dengan buffer kecil
    sl_base  = df['low'].tail(3).min()
    sl_level = sl_base * 0.997   # 0.3% buffer di bawah

    # Pastikan SL tidak terlalu jauh (maks 4% dari entry)
    max_sl_dist = entry_level * 0.04
    if (entry_level - sl_level) > max_sl_dist:
        sl_level = entry_level * 0.97   # fallback: 3% di bawah entry

    # TP berdasarkan distribusi pump_max_pct riset
    tp1 = entry_level * 1.008   # +0.8%
    tp2 = entry_level * 1.018   # +1.8%
    tp3 = entry_level * 1.037   # +3.7%

    # SL kandidat tambahan: bawah EMA50 (struktural)
    sl_ema50 = ema50 * 0.998

    return {
        'entry'   : round(entry_level, 8),
        'sl'      : round(sl_level, 8),
        'sl_ema50': round(sl_ema50, 8),
        'tp1'     : round(tp1, 8),
        'tp2'     : round(tp2, 8),
        'tp3'     : round(tp3, 8),
        'rr_tp2'  : round((tp2 - entry_level) / max(entry_level - sl_level, 1e-12), 2),
    }

# =============================================================================
# [FIX 8] TELEGRAM — format Markdown yang benar
# =============================================================================
def send_telegram_alert(
    symbol   : str,
    score    : int,
    signals  : list,
    price    : float,
    funding  : dict,
    entry_ez : dict,
    alert_lvl: str
):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("BOT_TOKEN / CHAT_ID belum di-set di environment")
        return

    icon = "🔴" if alert_lvl == "STRONG" else "🟠" if alert_lvl == "ALERT" else "🟡"
    ts   = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M') + " UTC"

    lines = [
        f"{icon} *PRE-PUMP SCANNER v14.1-RESEARCH*",
        f"",
        f"*Symbol* : `{symbol}`",
        f"*Score*  : `{score}/32`  \\[{alert_lvl}\\]",
        f"*Harga*  : `${price:.8g}`",
        f"",
        f"*Funding*",
        f"  Streak  : `{funding.get('neg_streak',0)}×` neg beruntun",
        f"  Neg%    : `{funding.get('neg_pct',0):.1f}%`",
        f"  Cumul   : `{funding.get('cumulative',0):.5f}`",
        f"  Avg-6   : `{funding.get('avg6',0):.6f}`",
        f"",
        f"*Sinyal Aktif*",
    ]
    for s in signals[:8]:
        lines.append(f"  • {s}")
    if len(signals) > 8:
        lines.append(f"  \\+ {len(signals)-8} sinyal lainnya")

    if entry_ez:
        rr = entry_ez.get('rr_tp2', 0)
        lines += [
            f"",
            f"*Entry Zone \\(indikatif\\)*",
            f"  Entry  : `${entry_ez['entry']:.8g}`",
            f"  SL     : `${entry_ez['sl']:.8g}`",
            f"  TP1    : `${entry_ez['tp1']:.8g}` \\(+0\\.8%\\)",
            f"  TP2    : `${entry_ez['tp2']:.8g}` \\(+1\\.8%\\) R:R={rr:.1f}",
            f"  TP3    : `${entry_ez['tp3']:.8g}` \\(+3\\.7%\\)",
        ]

    lines += [
        f"",
        f"_⏰ {ts}_",
        f"_⚠️ Bukan financial advice. DYOR._",
    ]

    msg = "\n".join(lines)

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            'chat_id'   : CHAT_ID,
            'text'      : msg,
            'parse_mode': 'MarkdownV2'
        }, timeout=10)
        resp.raise_for_status()
        log.info(f"✅ Alert Telegram terkirim: {symbol} [{alert_lvl}] score={score}")
    except Exception as e:
        log.error(f"Telegram error ({symbol}): {e}")

# =============================================================================
# MAIN SCANNER
# =============================================================================
def main():
    log.info("=" * 60)
    log.info("PRE-PUMP SCANNER v14.1-RESEARCH (Bitget Futures)")
    log.info(f"Config: SCORE_WATCH={SCORE_WATCH}, SCORE_ALERT={SCORE_ALERT}, "
             f"SCORE_STRONG={SCORE_STRONG}")
    log.info(f"Filter: vol_min={MIN_VOLUME_USDT:,}, vol_max={MAX_VOLUME_USDT:,}")
    log.info("=" * 60)

    # [FIX 7] Load cooldown persisten
    cooldown = load_cooldown()

    # 1. Ambil universe pair
    symbols = fetch_bitget_futures()
    if not symbols:
        log.error("Tidak ada symbol ditemukan. Keluar.")
        return

    candidates = []
    total      = len(symbols)

    for idx, symbol in enumerate(symbols, 1):

        # Cek cooldown
        if is_on_cooldown(symbol, cooldown):
            log.debug(f"[{idx}/{total}] {symbol} — cooldown aktif")
            continue

        log.info(f"[{idx}/{total}] {symbol}")

        # ── Filter Volume & Price Change ──────────────────────────────────────
        try:
            ticker    = exchange.fetch_ticker(symbol)
            volume_24 = ticker.get('quoteVolume') or 0
            change_24 = ticker.get('percentage')  or 0
            price     = ticker.get('last')         or 0

            if not MIN_VOLUME_USDT <= volume_24 <= MAX_VOLUME_USDT:
                log.debug(f"  Skip: volume={volume_24:,.0f} (rentang {MIN_VOLUME_USDT:,}–{MAX_VOLUME_USDT:,})")
                continue
            if abs(change_24) > MAX_PRICE_CHG:
                log.debug(f"  Skip: change={change_24:.1f}% > {MAX_PRICE_CHG}%")
                continue
        except Exception as e:
            log.debug(f"  Ticker error: {e}")
            continue

        # ── Funding History ───────────────────────────────────────────────────
        df_fund = fetch_funding_history(symbol)
        funding = compute_funding_stats(df_fund)
        if not funding:
            log.debug("  Skip: data funding tidak cukup")
            continue

        # ── PREREQUISITE GATE ─────────────────────────────────────────────────
        gate = (
            funding['neg_streak'] >= 5
            or (funding['neg_pct'] >= 70 and funding['cumulative'] < -0.015)
        )
        if not gate:
            log.debug(
                f"  Gate gagal: streak={funding['neg_streak']}, "
                f"neg%={funding['neg_pct']:.0f}%, "
                f"cumul={funding['cumulative']:.4f}"
            )
            continue

        log.info(
            f"  ✓ Gate lolos: streak={funding['neg_streak']}, "
            f"neg%={funding['neg_pct']:.0f}%, cumul={funding['cumulative']:.4f}"
        )

        # ── OHLCV & Indikator ─────────────────────────────────────────────────
        df = fetch_ohlcv(symbol, limit=CANDLE_LIMIT)
        if df.empty or len(df) < 30:
            log.debug("  Skip: candle tidak cukup")
            continue
        df = compute_indicators(df)

        # ── CoinGecko Context (opsional, tidak blocking) ──────────────────────
        base     = symbol.split('/')[0].lower()
        coin_ctx = get_coingecko_context(base)

        # ── Hitung Skor ───────────────────────────────────────────────────────
        score, signals = compute_score(symbol, funding, df, coin_ctx)
        log.info(f"  Score={score} | Sinyal: {', '.join(signals)}")

        if score >= SCORE_WATCH:
            candidates.append({
                'symbol' : symbol,
                'score'  : score,
                'signals': signals,
                'price'  : price,
                'funding': funding,
                'df'     : df,
                'ctx'    : coin_ctx,
            })

        # Rate limit CoinGecko (free tier)
        if coin_ctx:
            time.sleep(0.3)

    # ── Urutkan berdasarkan skor ──────────────────────────────────────────────
    candidates.sort(key=lambda x: x['score'], reverse=True)
    log.info(f"\n{'─'*60}")
    log.info(f"KANDIDAT LOLOS (score ≥ {SCORE_WATCH}): {len(candidates)} coin")

    if candidates:
        log.info("\nTop candidates:")
        for c in candidates[:10]:
            lvl = ("STRONG" if c['score'] >= SCORE_STRONG else
                   "ALERT"  if c['score'] >= SCORE_ALERT  else "WATCH")
            log.info(f"  {c['symbol']:25s} score={c['score']:2d}  [{lvl}]")

    # ── Kirim alert untuk ALERT/STRONG ───────────────────────────────────────
    alert_count = 0
    for cand in candidates:
        if cand['score'] < SCORE_ALERT:
            continue  # tampilkan hanya ALERT ke atas

        lvl    = "STRONG" if cand['score'] >= SCORE_STRONG else "ALERT"
        ez     = compute_entry_zone(cand['df'], cand['price'])
        rr_ok  = ez.get('rr_tp2', 0) >= 1.5   # minimal R:R 1:1.5

        # Log entry zone
        log.info(
            f"\n  → {cand['symbol']} [{lvl}] score={cand['score']}\n"
            f"     entry=${ez['entry']:.8g}  SL=${ez['sl']:.8g}  "
            f"TP2=${ez['tp2']:.8g}  R:R={ez['rr_tp2']:.1f}  "
            f"{'✅ RR OK' if rr_ok else '⚠️ RR tipis'}"
        )

        send_telegram_alert(
            symbol    = cand['symbol'],
            score     = cand['score'],
            signals   = cand['signals'],
            price     = cand['price'],
            funding   = cand['funding'],
            entry_ez  = ez,
            alert_lvl = lvl,
        )
        alert_count += 1
        set_cooldown(cand['symbol'], cooldown)

        if alert_count >= 15:   # Batas maks per sesi
            log.info("  Batas 15 alert per sesi tercapai")
            break

    log.info(f"\n{'═'*60}")
    log.info(f"SELESAI — {alert_count} alert terkirim dari {len(candidates)} kandidat")
    log.info(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
