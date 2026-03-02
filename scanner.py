#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRE-PUMP SCANNER v14.2-RESEARCH (Bitget Futures)
================================================================================
POST-MORTEM v14.1 — Scan 27 Feb 2026 jam 05:00 WIB:
  5 alert dikirim → hanya IOTX pump +7%, empat lainnya dump (LYN, BIRB, NEWT, ACE)

  ROOT CAUSE yang ditemukan:
  1. MICRO-CAP BARU (BIRB, LYN, NEWT): koin baru listed <7 hari
     → data candle pendek, BOS/VWAP tidak valid, indikator palsu
     → score 20-21 dari koin garbage karena rolling window hampir = semua data
  2. STOCK TOKENS ikut diproses: AAPL, NVDA, TSLA, META, QQQ, SPY dll (~30 simbol)
     → synthetic asset, perilaku beda dari altcoin crypto native
  3. VOLUME QUALITY tidak dicek: banyak candle volume=0 pada koin micro baru
  4. MIN_VOLUME terlalu rendah (100K): bisa berasal dari listing pump semu

PERBAIKAN v14.2:
  [FIX A] BLACKLIST: stock sintetis + stablecoin + metal/komoditas
  [FIX B] Fetch 300 candle; WAJIB tersedia >= 200 candle valid (proxy >= 16 jam)
  [FIX C] Volume quality: >= 60% candle harus punya volume > 0
  [FIX D] Avg candle volume >= 5.000 USDT (filter volume semu dari listing pump)
  [FIX E] MIN_VOLUME naik 100K → 500K
  [FIX F] Listing age: first_candle → last_candle >= 72 jam
  [FIX G] Anti falling-knife: skip jika drop > 15% dalam 50 candle terakhir
  [FIX H] BOS validity: pakai min_periods=20 agar rolling(20) tidak trigger semu
  [FIX I] Log detail: alasan skip + semua sinyal aktif ditampilkan
  [FIX J] Sort: utama score DESC, tie-breaker neg_streak DESC

ARSITEKTUR SCORING (Maks 32 poin):
  GATE       : neg_streak >= 5  ATAU  (neg_pct >= 70% DAN cum_funding < -0.015)
  TIER A     : Funding tiered + cum_funding extreme                     [maks 5]
  TIER B     : above_vwap + bos_up + higher_lows                       [maks 15]
  TIER C     : RSI + volume + ATR + BB_pct                             [maks 10]
  TIER D     : funding_reset + small_cap + bb_squeeze                   [maks 4]
  WATCH >= 10 | ALERT >= 16 | STRONG >= 20
================================================================================
"""

import ccxt
import requests
import pandas as pd
import numpy as np
import time
import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

# =============================================================================
# KONFIGURASI
# =============================================================================
TIMEFRAME         = '5m'
CANDLE_LIMIT      = 300     # [FIX B] Fetch lebih banyak
CANDLE_MIN_VALID  = 200     # [FIX B] Min candle tersedia (>= 16 jam)
CANDLE_MIN_AGE_H  = 24      # Min usia: 24 jam (coin bisa baru di Bitget tapi coin lama)
FUNDING_LIMIT     = 100

MIN_VOLUME_USDT   = 500_000  # [FIX E] Naik dari 100K
MAX_VOLUME_USDT   = 15_000_000
MAX_PRICE_CHG_24H = 30
MIN_AVG_CANDLE_VOL = 5_000   # [FIX D] Min rata-rata volume per candle (USDT)
MIN_VOL_QUALITY   = 0.60     # [FIX C] Min 60% candle harus volume > 0
MAX_DROP_50C      = -15.0    # [FIX G] Tolak jika drop > 15% dalam 50 candle

SCORE_WATCH  = 10
SCORE_ALERT  = 18    # [FIX K] Dinaikkan dari 16 → 18 (lebih selektif, kurangi ACE-like FP)
SCORE_STRONG = 22    # [FIX K] Dinaikkan dari 20 → 22
MAX_ALERTS_PER_SCAN = 3    # [FIX N] Hard cap — jika lebih dari ini terkirim, ada yang salah

COOLDOWN_HOURS = 6
# [FIX COOLDOWN] GitHub Actions = fresh container setiap run, /tmp hilang.
# Simpan cooldown.json di direktori kerja (repo) agar bisa di-commit kembali.
COOLDOWN_FILE  = Path('cooldown.json')   # path relatif = dalam repo

BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
CHAT_ID   = os.environ.get('CHAT_ID', '')

exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# =============================================================================
# [FIX A] BLACKLIST — ticker yang TIDAK boleh diproses
# =============================================================================
BLACKLIST_TICKERS = {
    # Synthetic Stocks & ETFs (Bitget mRWA / tokenized stocks)
    'AAPL', 'GOOGL', 'AMZN', 'META', 'MSFT', 'NVDA', 'TSLA', 'INTC', 'IBM',
    'BABA', 'ASML', 'ARM', 'PLTR', 'ORCL', 'GE', 'GME', 'MRVL', 'MSTR',
    'COIN', 'HOOD', 'RDDT', 'FUTU', 'JD', 'QQQ', 'SPY', 'MCD', 'MA',
    'UNH', 'LLY', 'PEP', 'CSCO', 'ACN', 'AVGO', 'MU', 'APP',
    # Stablecoins / tokenized fiat
    'USDC', 'BUSD', 'DAI', 'TUSD', 'USDP', 'FRAX', 'LUSD', 'GUSD', 'EURS',
    'USDD', 'CRVUSD', 'PYUSD', 'FDUSD', 'USTC', 'USDT',
    # Nama token yang identik dengan stablecoin / misleading
    'STABLE', 'STBL', 'STBLE',
    # Wrapped / synthetic
    'WBTC', 'WETH', 'WBNB', 'WMATIC', 'WAVAX',
    # Tokenized metals / komoditas
    'XAUT', 'PAXG', 'XAU', 'XAG', 'XPD', 'XPT',
    # Dead / algorithmic yang tidak relevan
    'LUNA', 'LUNC',
}


def is_blacklisted(symbol: str) -> bool:
    ticker = symbol.split('/')[0].upper()
    return ticker in BLACKLIST_TICKERS


# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/tmp/scanner_v142.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('scanner')


# =============================================================================
# COOLDOWN PERSISTEN
# =============================================================================
def load_cooldown() -> dict:
    if COOLDOWN_FILE.exists():
        try:
            return json.loads(COOLDOWN_FILE.read_text())
        except Exception:
            pass
    return {}


def save_cooldown(cd: dict):
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
# FETCH MARKETS
# =============================================================================
def fetch_bitget_futures() -> list:
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
    try:
        data = exchange.fetch_funding_rate_history(symbol, limit=FUNDING_LIMIT)
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df['fundingRate'] = df['fundingRate'].astype(float)
        df['timestamp']   = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        return df.sort_values('timestamp').reset_index(drop=True)
    except Exception as e:
        log.debug(f"Funding error {symbol}: {e}")
        return pd.DataFrame()


def compute_funding_stats(df: pd.DataFrame):
    if df.empty or len(df) < 10:
        return None
    rates = df['fundingRate'].tolist()
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
# OHLCV
# =============================================================================
def fetch_ohlcv(symbol: str, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=limit)
        df  = pd.DataFrame(raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        return df
    except Exception as e:
        log.debug(f"OHLCV error {symbol}: {e}")
        return pd.DataFrame()


# =============================================================================
# [FIX K] BTC MARKET REGIME FILTER
# Jika BTC sedang downtrend 4h, hampir semua altcoin ikut turun
# Ini adalah filter paling kuat untuk menghindari "pump signal di bear market"
# =============================================================================
_btc_regime_cache = {'ts': 0, 'bullish': True, 'btc_neg_pct': 0}

def get_btc_regime() -> dict:
    """
    Evaluasi kondisi pasar BTC secara menyeluruh.
    
    Return dict:
      'ok'       : bool   — apakah kondisi layak untuk alert
      'reason'   : str    — alasan jika tidak layak
      'btc_chg4h': float  — perubahan harga BTC dalam 4 jam terakhir (%)
      'btc_vs_ema': str   — 'above' / 'below'
    """
    now = time.time()
    if now - _btc_regime_cache['ts'] < 1800:
        return _btc_regime_cache

    result = {'ts': now, 'ok': True, 'reason': 'OK', 'btc_chg4h': 0.0, 'btc_vs_ema': 'above'}

    try:
        raw = exchange.fetch_ohlcv('BTC/USDT:USDT', timeframe='4h', limit=30)
        df  = pd.DataFrame(raw, columns=['ts','o','h','l','c','v'])
        df['ema20'] = df['c'].ewm(span=20, adjust=False).mean()
        last = df.iloc[-1]
        price    = last['c']
        ema20    = last['ema20']
        chg_4h   = (price - df['c'].iloc[-2]) / df['c'].iloc[-2] * 100
        chg_12h  = (price - df['c'].iloc[-4]) / df['c'].iloc[-4] * 100

        result['btc_chg4h']  = round(chg_4h, 2)
        result['btc_vs_ema'] = 'above' if price > ema20 else 'below'

        reasons = []
        if price < ema20:
            reasons.append(f"BTC di bawah EMA20-4H (${price:,.0f} < ${ema20:,.0f})")
        if chg_4h < -2.0:
            reasons.append(f"BTC turun {chg_4h:.1f}% dalam 4 jam")
        if chg_12h < -4.0:
            reasons.append(f"BTC turun {chg_12h:.1f}% dalam 12 jam")

        if reasons:
            result['ok']     = False
            result['reason'] = ' | '.join(reasons)

        log.info(
            f"BTC REGIME: {'✅ BULLISH' if result['ok'] else '🔴 BEARISH'} | "
            f"${price:,.0f} vs EMA20=${ema20:,.0f} | "
            f"4h={chg_4h:+.1f}% | 12h={chg_12h:+.1f}%"
        )
    except Exception as e:
        log.warning(f"Gagal cek BTC regime: {e}. Lanjut dengan asumsi OK.")
        result['ok'] = True

    _btc_regime_cache.update(result)
    return result


# =============================================================================
# [FIX B,C,D,F,G] DATA QUALITY CHECKS
# =============================================================================
def check_data_quality(df: pd.DataFrame):
    """
    Jalankan semua quality check SEBELUM komputasi indikator.
    Return (pass: bool, alasan: str)
    """
    n = len(df)

    # [FIX B] Cukup data historis
    if n < CANDLE_MIN_VALID:
        return False, f"candle terlalu sedikit ({n} < {CANDLE_MIN_VALID})"

    # [FIX F] Usia koin cukup
    age_h = (df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).total_seconds() / 3600
    if age_h < CANDLE_MIN_AGE_H:
        return False, f"koin terlalu baru ({age_h:.1f}h < {CANDLE_MIN_AGE_H}h)"

    # [FIX C] Volume quality
    vol_qual = (df['volume'] > 0).mean()
    if vol_qual < MIN_VOL_QUALITY:
        return False, f"vol quality rendah ({vol_qual*100:.0f}% < {MIN_VOL_QUALITY*100:.0f}%)"

    # [FIX D] Avg candle volume (USDT) — proxy: volume * close
    avg_vol = (df['volume'] * df['close']).mean()
    if avg_vol < MIN_AVG_CANDLE_VOL:
        return False, f"avg candle vol rendah (${avg_vol:,.0f} < ${MIN_AVG_CANDLE_VOL:,})"

    # [FIX G] Anti falling-knife
    last50 = df.tail(50)
    if len(last50) >= 10:
        p_start = last50['close'].iloc[0]
        p_end   = last50['close'].iloc[-1]
        if p_start > 0:
            pct = (p_end - p_start) / p_start * 100
            if pct < MAX_DROP_50C:
                return False, f"falling knife ({pct:.1f}% dalam 50 candle)"

    return True, "OK"


# =============================================================================
# INDIKATOR TEKNIKAL
# =============================================================================
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 30:
        return df

    close = df['close']
    high  = df['high']
    low   = df['low']
    vol   = df['volume']

    # EMA
    df['ema7']  = close.ewm(span=7,  adjust=False).mean()
    df['ema20'] = close.ewm(span=20, adjust=False).mean()
    df['ema50'] = close.ewm(span=50, adjust=False).mean()

    # VWAP per sesi harian (reset tiap midnight UTC)
    tp             = (high + low + close) / 3
    df['date_utc'] = df['timestamp'].dt.date
    df['_tpv']     = tp * vol
    df['vwap']     = (df.groupby('date_utc')['_tpv'].cumsum()
                      / (df.groupby('date_utc')['volume'].cumsum() + 1e-12))
    df.drop(columns=['_tpv', 'date_utc'], inplace=True)

    # RSI 14
    delta = close.diff()
    gain  = delta.where(delta > 0, 0.0).ewm(span=14, adjust=False).mean()
    loss  = (-delta.where(delta < 0, 0.0)).ewm(span=14, adjust=False).mean()
    df['rsi14'] = 100 - (100 / (1 + gain / (loss + 1e-12)))

    # ATR 14
    hl  = high - low
    hcp = (high - close.shift()).abs()
    lcp = (low  - close.shift()).abs()
    tr          = pd.concat([hl, hcp, lcp], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    df['atr_pct'] = df['atr14'] / (close + 1e-12) * 100

    # Bollinger Bands (20, 2)
    bb_mid        = close.rolling(20).mean()
    bb_std        = close.rolling(20).std(ddof=0)
    bb_up         = bb_mid + 2 * bb_std
    bb_dn         = bb_mid - 2 * bb_std
    bb_range      = bb_up - bb_dn
    df['bb_mid']  = bb_mid
    df['bb_pct']  = (close - bb_dn) / (bb_range + 1e-12)
    df['bb_width']= bb_range / (bb_mid + 1e-12)
    bb_w_p20      = df['bb_width'].quantile(0.20)
    df['bb_squeeze'] = df['bb_width'] < bb_w_p20

    # Volume ratio
    df['vol_ma12'] = vol.rolling(12).mean()
    df['vol_ratio']= vol / (df['vol_ma12'] + 1e-12)

    # [FIX H] BOS Up: pakai min_periods=20 agar tidak trigger semu pada data awal
    prev_max_high  = high.rolling(20, min_periods=20).max().shift(1)
    df['bos_up']   = close > prev_max_high

    # Higher Low
    low_min5       = low.rolling(5, min_periods=3).min().shift(1)
    df['higher_low'] = low > low_min5

    # Above VWAP
    df['above_vwap'] = close > df['vwap']

    return df


def compute_above_vwap_count(df: pd.DataFrame, n: int = 12) -> int:
    if df.empty or len(df) < n or 'above_vwap' not in df.columns:
        return 0
    return int(df['above_vwap'].tail(n).sum())


# =============================================================================
# COINGECKO CONTEXT (opsional, graceful fallback)
# =============================================================================
CG_OVERRIDE = {
    'pepe': 'pepe', 'orca': 'orca', 'om': 'mantra-dao',
    'ygg': 'yield-guild-games', 'ace': 'ace', 'zec': 'zcash',
    'kite': 'dextf', 'enso': 'enso-finance', 'bio': 'biopassport',
    'sahara': 'sahara-ai', 'holo': 'holotoken', 'op': 'optimism',
    'ada': 'cardano', 'sui': 'sui', 'tao': 'bittensor', 'ena': 'ethena',
    'trx': 'tron', 'fil': 'filecoin', 'uni': 'uniswap', 'atom': 'cosmos',
    'zk': 'zksync', 'sol': 'solana', 'avax': 'avalanche-2',
    'link': 'chainlink', 'dot': 'polkadot', 'bnb': 'binancecoin',
    'arb': 'arbitrum', 'near': 'near', 'inj': 'injective-protocol',
    'apt': 'aptos', 'sei': 'sei-network', 'mnt': 'mantle', 'aevo': 'aevo',
    'strk': 'starknet', 'w': 'wormhole', 'jup': 'jupiter-exchange-solana',
    'pyth': 'pyth-network', 'ondo': 'ondo-finance', 'io': 'io-net',
    'render': 'render-token', 'fet': 'fetch-ai', 'iotx': 'iotex',
    'shib': 'shiba-inu', 'floki': 'floki', 'doge': 'dogecoin',
    'ltc': 'litecoin', 'xrp': 'ripple', 'xlm': 'stellar',
    'vet': 'vechain', 'hbar': 'hedera-hashgraph',
}
_cg_cache: dict = {}


def get_coingecko_context(ticker: str) -> dict:
    t = ticker.lower()
    if t in _cg_cache:
        return _cg_cache[t]
    coin_id = CG_OVERRIDE.get(t)
    if not coin_id:
        try:
            r = requests.get(
                'https://api.coingecko.com/api/v3/search',
                params={'query': t}, timeout=8
            )
            coins = r.json().get('coins', [])
            for c in coins[:5]:
                if c.get('symbol', '').lower() == t:
                    coin_id = c['id']
                    break
            if not coin_id and coins:
                coin_id = coins[0]['id']
        except Exception:
            pass
    if not coin_id:
        return {}
    try:
        data = requests.get(
            f'https://api.coingecko.com/api/v3/coins/{coin_id}',
            params={
                'localization': 'false', 'tickers': 'false',
                'market_data': 'true', 'community_data': 'false'
            },
            timeout=10
        ).json()
        md = data.get('market_data', {})
        result = {
            'rank'    : data.get('market_cap_rank', 9999) or 9999,
            'ath_dist': md.get('ath_change_percentage', {}).get('usd', 0) or 0,
        }
        _cg_cache[t] = result
        return result
    except Exception:
        return {}


# =============================================================================
# SCORING ENGINE
# =============================================================================
def compute_score(funding: dict, df: pd.DataFrame, coin_ctx: dict):
    """Hitung skor pra-pump (maks 32). Return (score, signals)."""
    score   = 0
    signals = []
    last    = df.iloc[-1]

    # ── TIER A: Funding [maks 5] ──────────────────────────────────────────
    streak = funding['neg_streak']
    if streak >= 15:
        score += 3
        signals.append(f"neg_streak_extreme ({streak}x)")
    elif streak >= 10:
        score += 2
        signals.append(f"neg_streak_high ({streak}x)")
    elif streak >= 5:
        score += 1
        signals.append(f"neg_streak ({streak}x)")

    cum = funding['cumulative']
    if cum < -0.05:
        score += 2
        signals.append(f"cum_funding_extreme ({cum:.4f})")
    elif cum < -0.02:
        score += 1
        signals.append(f"cum_funding_neg ({cum:.4f})")

    # ── TIER B: Struktural [maks 15] ─────────────────────────────────────
    above_count = compute_above_vwap_count(df, 12)
    if above_count >= 10:
        score += 5
        signals.append(f"above_vwap_strong ({above_count}/12)")
    elif above_count >= 9:
        score += 3
        signals.append(f"above_vwap_moderate ({above_count}/12)")
    elif above_count >= 8:
        score += 1
        signals.append(f"above_vwap_weak ({above_count}/12)")

    if df['bos_up'].tail(6).any():
        score += 5
        signals.append("bos_up_confirmed")

    hl = int(df['higher_low'].tail(12).sum())
    if hl >= 2:
        score += 2
        signals.append("higher_lows_forming")
    elif hl == 1:
        score += 1
        signals.append("higher_low_emerging")

    # ── TIER C: Momentum [maks 10] ───────────────────────────────────────
    rsi = last.get('rsi14', 50)
    if rsi > 60:
        score += 3
        signals.append(f"rsi_strong ({rsi:.1f})")
    elif rsi > 55:
        score += 2
        signals.append(f"rsi_above55 ({rsi:.1f})")

    vol_surge = int((df['vol_ratio'].tail(6) > 1.3).sum())
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

    if last.get('bb_pct', 0) > 0.65:
        score += 2
        signals.append(f"upper_band ({last['bb_pct']:.2f})")

    # ── TIER D: Kontekstual [maks 4] ─────────────────────────────────────
    if streak >= 5 and funding.get('avg6', -1) >= -0.00005:
        score += 2
        signals.append(f"funding_reset (avg6={funding['avg6']:.6f})")

    rank = coin_ctx.get('rank', 0)
    if 1000 < rank <= 5000:
        score += 1
        signals.append(f"small_cap (#{rank})")
    elif rank > 5000:
        score += 1
        signals.append(f"micro_cap (#{rank})")

    if int(df['bb_squeeze'].tail(12).sum()) >= 4:
        score += 1
        signals.append("bb_squeezed")

    return score, signals


# =============================================================================
# ENTRY ZONE — Fibonacci Extension Method
# =============================================================================
# Filosofi:
#   Target kita adalah pump 15-50% dalam beberapa jam (short squeeze setup).
#   TP berbasis persentase flat (+1.8%, +3.7%) TIDAK MASUK AKAL untuk target ini.
#
#   Metode baru:
#   A = swing_low  (support terkuat dalam N candle terakhir)
#   B = swing_high (resistance/puncak terakhir dalam N candle)
#   C = entry      (harga saat ini = titik retracement/akumulasi)
#
#   SL: di bawah swing_low dengan buffer 1% (struktur invalidated jika tembus)
#       → Min SL distance: 3% dari entry (noise filter)
#       → Max SL distance: 8% dari entry (lebih dari ini = setup terlalu berisiko)
#
#   TP: Fibonacci Extension dari range A-B, diproyeksikan dari C
#       TP1 = C + (B-A) × 0.618   →  target ~5-12%  (partial, aman ambil profit)
#       TP2 = C + (B-A) × 1.000   →  target ~15-25% (target utama)
#       TP3 = C + (B-A) × 1.618   →  target ~25-40% (ambitious, hold lebih lama)
#
#   Jika range A-B terlalu kecil (<8% dari A), fallback ke target minimum:
#       TP1 = +8%, TP2 = +18%, TP3 = +30%
#       (menjamin target 15%+ tetap visible bahkan di koin konsolidasi ketat)
#
#   Setup DIBATALKAN (return None) jika R:R ke TP2 < 2.0
#   Artinya: setup tidak layak masuk, skip dan jangan kirim alert.
# =============================================================================
def compute_entry_zone(df: pd.DataFrame, price: float) -> dict | None:
    """
    Hitung entry/SL/TP berbasis Fibonacci Extension dari swing A-B.
    Return None jika setup tidak memenuhi syarat R:R minimum.
    """
    # ── 1. Identifikasi swing low (A) dan swing high (B) ──────────────────
    # Gunakan 50 candle terakhir untuk menemukan range yang relevan
    lookback   = min(60, len(df) - 5)
    window     = df.tail(lookback)
    swing_low  = float(window['low'].min())
    swing_high = float(window['high'].max())
    range_ab   = swing_high - swing_low

    # ── 2. Entry = harga saat ini (scanner deteksi momentum, masuk ASAP) ──
    entry = float(price)

    # Sanity: entry tidak boleh terlalu jauh dari swing range
    if entry > swing_high * 1.05:
        entry = swing_high * 1.02  # harga sudah overtake → adjust

    # ── 3. SL: di bawah swing_low dengan buffer 1% ────────────────────────
    sl_structural = swing_low * 0.99   # tepat di bawah support struktur
    sl_dist_pct   = (entry - sl_structural) / entry if entry > 0 else 0.05

    if sl_dist_pct < 0.03:
        # Swing low terlalu dekat dengan harga → gunakan 3% flat
        sl = entry * 0.97
    elif sl_dist_pct > 0.10:
        # Swing low terlalu jauh → SL max 8% (lebih = terlalu berisiko)
        sl = entry * 0.92
    else:
        sl = sl_structural

    # Pastikan sl < entry (selalu)
    if sl >= entry:
        sl = entry * 0.95

    # ── 4. TP: Fibonacci Extension dari A-B, diproyeksikan dari C ─────────
    range_pct = range_ab / swing_low if swing_low > 0 else 0

    if range_pct >= 0.08:
        # Range cukup signifikan → pakai Fibonacci Extension
        tp1 = entry + range_ab * 0.618   # 61.8% extension
        tp2 = entry + range_ab * 1.000   # 100% extension
        tp3 = entry + range_ab * 1.618   # 161.8% extension
        method = "Fib Ext"
    else:
        # Range terlalu kecil → fallback ke minimum guarantee
        # Pastikan TP2 minimal +15% agar sesuai target user
        tp1 = entry * 1.08    # +8%
        tp2 = entry * 1.18    # +18%
        tp3 = entry * 1.30    # +30%
        method = "Min Target"

    # ── 5. Pastikan TP2 minimal +12% dari entry ───────────────────────────
    # (kita ingin 15%, tapi 12% masih acceptable dengan R:R baik)
    if (tp2 - entry) / entry < 0.12:
        scale = (entry * 0.15) / (tp2 - entry)  # scale up ke 15%
        tp1   = entry + (tp1 - entry) * scale
        tp2   = entry * 1.15
        tp3   = entry + (tp3 - entry) * scale

    # ── 6. Hitung R:R dan validasi ────────────────────────────────────────
    sl_dist = max(entry - sl, entry * 0.001)  # hindari div by zero
    rr_tp1  = (tp1 - entry) / sl_dist
    rr_tp2  = (tp2 - entry) / sl_dist
    rr_tp3  = (tp3 - entry) / sl_dist

    # Setup tidak layak jika R:R ke TP2 < 2.0
    # Artinya risiko lebih besar dari setengah potensi profit → SKIP
    if rr_tp2 < 2.0:
        return None

    tp1_pct = (tp1 - entry) / entry * 100
    tp2_pct = (tp2 - entry) / entry * 100
    tp3_pct = (tp3 - entry) / entry * 100
    sl_pct  = (entry - sl) / entry * 100

    return {
        'entry'      : round(entry, 8),
        'sl'         : round(sl, 8),
        'sl_pct'     : round(sl_pct, 2),
        'swing_low'  : round(swing_low, 8),
        'swing_high' : round(swing_high, 8),
        'tp1'        : round(tp1, 8),
        'tp2'        : round(tp2, 8),
        'tp3'        : round(tp3, 8),
        'tp1_pct'    : round(tp1_pct, 1),
        'tp2_pct'    : round(tp2_pct, 1),
        'tp3_pct'    : round(tp3_pct, 1),
        'rr_tp2'     : round(rr_tp2, 1),
        'rr_tp3'     : round(rr_tp3, 1),
        'method'     : method,
    }


# =============================================================================
# TELEGRAM
# =============================================================================
def send_telegram_alert(symbol, score, signals, price, funding, entry_ez, alert_lvl):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("BOT_TOKEN/CHAT_ID belum di-set")
        return

    icon      = "KUAT" if alert_lvl == "STRONG" else "ALERT"
    ts        = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    sym_clean = symbol.replace('/USDT:USDT', '').replace('/USDT', '')

    lines = [
        f"[{icon}] PRE-PUMP DETECTED",
        f"Coin   : {sym_clean}  |  Score: {score}/32",
        f"Harga  : ${price:.8g}",
        f"",
        f"[Funding Negatif]",
        f"  Streak : {funding.get('neg_streak',0)}x beruntun",
        f"  Neg%   : {funding.get('neg_pct',0):.0f}% dari 100 periode",
        f"  Cumul  : {funding.get('cumulative',0):.4f}",
        f"",
        f"[Sinyal ({len(signals)})]",
    ]
    for s in signals[:6]:
        lines.append(f"  + {s}")

    if entry_ez:
        rr2 = entry_ez.get('rr_tp2', 0)
        rr3 = entry_ez.get('rr_tp3', 0)
        met = entry_ez.get('method', 'Fib Ext')
        lines += [
            f"",
            f"[Entry Plan - {met}]",
            f"  Entry : ${entry_ez['entry']:.8g}",
            f"  SL    : ${entry_ez['sl']:.8g}  (-{entry_ez['sl_pct']:.1f}%)",
            f"",
            f"  TP1   : ${entry_ez['tp1']:.8g}  (+{entry_ez['tp1_pct']:.1f}%)",
            f"  TP2   : ${entry_ez['tp2']:.8g}  (+{entry_ez['tp2_pct']:.1f}%)  R:R={rr2:.1f}x  << TARGET",
            f"  TP3   : ${entry_ez['tp3']:.8g}  (+{entry_ez['tp3_pct']:.1f}%)  R:R={rr3:.1f}x",
            f"",
            f"  Basis : Swing {entry_ez['swing_low']:.8g} -- {entry_ez['swing_high']:.8g}",
        ]

    lines += [
        f"",
        f"{ts}",
        f"Bukan financial advice. DYOR.",
    ]
    msg = "\n".join(lines)

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={'chat_id': CHAT_ID, 'text': msg},
            timeout=10
        )
        r.raise_for_status()
        log.info(f"Alert terkirim: {symbol} [{alert_lvl}] score={score}")
    except Exception as e:
        try:
            body = r.text[:300] if 'r' in dir() else '(no response)'
        except Exception:
            body = '(no response)'
        log.error(f"Telegram error ({symbol}): {e} | Response: {body}")


# =============================================================================
# MAIN SCANNER
# =============================================================================
def main():
    log.info("=" * 65)
    log.info("PRE-PUMP SCANNER v14.2-RESEARCH (Bitget Futures)")
    log.info(f"WATCH>={SCORE_WATCH} | ALERT>={SCORE_ALERT} | STRONG>={SCORE_STRONG}")
    log.info(f"Vol: [{MIN_VOLUME_USDT:,} - {MAX_VOLUME_USDT:,}] USDT")
    log.info(f"Quality: candle>={CANDLE_MIN_VALID} | age>={CANDLE_MIN_AGE_H}h | "
             f"vol_qual>={MIN_VOL_QUALITY*100:.0f}% | avg_vol>={MIN_AVG_CANDLE_VOL:,}")
    log.info("=" * 65)

    # [FIX Q] Test koneksi Telegram di awal agar error terdeteksi lebih awal
    if BOT_TOKEN and CHAT_ID:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=5
            )
            if r.ok:
                bot_name = r.json().get('result', {}).get('username', '?')
                log.info(f"✅ Telegram OK: @{bot_name} | CHAT_ID={CHAT_ID}")
            else:
                log.error(f"❌ Telegram GAGAL: {r.status_code} {r.text[:200]}")
                log.error("   Cek BOT_TOKEN di GitHub Secrets. Scanner tetap lanjut tapi alert tidak terkirim.")
        except Exception as e:
            log.error(f"❌ Telegram connect error: {e}")
    else:
        log.warning("BOT_TOKEN/CHAT_ID tidak di-set — alert tidak akan terkirim")

    cooldown = load_cooldown()
    symbols  = fetch_bitget_futures()
    if not symbols:
        log.error("Tidak ada symbol. Keluar.")
        return

    # ═══════════════════════════════════════════════════════════════
    # [P0] HARD BTC REGIME GATE
    # Jika BTC bearish → jangan kirim alert APAPUN hari ini.
    # Negative funding di bear market ≠ short squeeze fuel.
    # ═══════════════════════════════════════════════════════════════
    btc_regime = get_btc_regime()
    if not btc_regime['ok']:
        log.warning(f"🔴 BTC REGIME BEARISH — SCAN DIHENTIKAN")
        log.warning(f"   Alasan: {btc_regime['reason']}")
        log.warning(f"   Negative funding saat market turun = semua orang short dengan benar.")
        log.warning(f"   Tidak ada alert dikirim. Tunggu BTC recovery.")
        # Kirim notifikasi Telegram bahwa scan diskip
        if BOT_TOKEN and CHAT_ID:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={'chat_id': CHAT_ID,
                          'text': (f"🔴 SCAN DIHENTIKAN — BTC BEARISH\n"
                                   f"Alasan: {btc_regime['reason']}\n"
                                   f"Scanner tidak mengirim alert saat market dump.\n"
                                   f"Tunggu BTC recovery ke atas EMA20-4H.")},
                    timeout=10
                )
            except Exception:
                pass
        return  # ← EXIT: tidak lanjut scan sama sekali

    stats = {
        'total': len(symbols), 'blacklist': 0, 'cooldown': 0,
        'vol_filter': 0, 'quality_fail': 0, 'funding_fail': 0,
        'gate_fail': 0, 'passed': 0
    }
    candidates = []
    total = len(symbols)

    for idx, symbol in enumerate(symbols, 1):

        # [FIX A] Blacklist
        if is_blacklisted(symbol):
            stats['blacklist'] += 1
            log.debug(f"[{idx}/{total}] {symbol} BLACKLIST")
            continue

        # Cooldown
        if is_on_cooldown(symbol, cooldown):
            stats['cooldown'] += 1
            log.debug(f"[{idx}/{total}] {symbol} cooldown")
            continue

        log.info(f"[{idx}/{total}] {symbol}")

        # Volume & price change filter
        try:
            ticker    = exchange.fetch_ticker(symbol)
            volume_24 = ticker.get('quoteVolume') or 0
            change_24 = ticker.get('percentage')  or 0
            price     = ticker.get('last')         or 0

            if not (MIN_VOLUME_USDT <= volume_24 <= MAX_VOLUME_USDT):
                stats['vol_filter'] += 1
                log.debug(f"  skip vol={volume_24:,.0f}")
                continue
            if abs(change_24) > MAX_PRICE_CHG_24H:
                stats['vol_filter'] += 1
                log.debug(f"  skip change={change_24:.1f}%")
                continue
        except Exception as e:
            log.debug(f"  skip ticker error: {e}")
            continue

        # Fetch candle
        df = fetch_ohlcv(symbol, limit=CANDLE_LIMIT)
        if df.empty:
            log.debug("  skip: tidak ada candle")
            continue

        # [FIX B,C,D,F,G] Quality checks
        ok, reason = check_data_quality(df)
        if not ok:
            stats['quality_fail'] += 1
            log.info(f"  skip quality: {reason}")
            continue

        # Funding
        df_fund = fetch_funding_history(symbol)
        funding = compute_funding_stats(df_fund)
        if not funding:
            stats['funding_fail'] += 1
            log.debug("  skip: funding tidak cukup")
            continue

        # PREREQUISITE GATE
        gate = (
            funding['neg_streak'] >= 5
            or (funding['neg_pct'] >= 70 and funding['cumulative'] < -0.015)
        )
        if not gate:
            stats['gate_fail'] += 1
            log.debug(
                f"  gate fail: streak={funding['neg_streak']}, "
                f"neg%={funding['neg_pct']:.0f}%, cumul={funding['cumulative']:.4f}"
            )
            continue

        log.info(
            f"  GATE OK: streak={funding['neg_streak']}, "
            f"neg%={funding['neg_pct']:.0f}%, cumul={funding['cumulative']:.4f}"
        )

        # Indikator
        df = compute_indicators(df)

        # CoinGecko (opsional)
        base     = symbol.split('/')[0].lower()
        coin_ctx = get_coingecko_context(base)
        if coin_ctx:
            time.sleep(0.3)

        # Hitung skor
        score, signals = compute_score(funding, df, coin_ctx)

        # [FIX I] Log detail
        log.info(
            f"  Score={score} | streak={funding['neg_streak']} | "
            + (', '.join(signals) if signals else '(no signals)')
        )

        stats['passed'] += 1
        if score >= SCORE_WATCH:
            candidates.append({
                'symbol'    : symbol,
                'score'     : score,
                'signals'   : signals,
                'price'     : price,
                'funding'   : funding,
                'df'        : df,
                'ctx'       : coin_ctx,
                'neg_streak': funding['neg_streak'],
            })

    # [FIX J] Sort: score DESC, neg_streak DESC sebagai tie-breaker
    candidates.sort(key=lambda x: (x['score'], x['neg_streak']), reverse=True)

    # Summary filter
    log.info(f"\n{'─'*65}")
    log.info("FILTER SUMMARY:")
    for k, v in stats.items():
        log.info(f"  {k:<15}: {v}")
    log.info(f"  {'kandidat':<15}: {len(candidates)}")

    # ═══════════════════════════════════════════════════════════════
    # [P0] CIRCUIT BREAKER — terlalu banyak kandidat = market bearish
    # Pelajaran dari scan 28 Feb 01:00 UTC: 47 kandidat = semua false positive.
    # Jika >20 koin lulus gate secara bersamaan, ini bukan 20 peluang,
    # ini sinyal bahwa market sedang dalam distribusi/dump masif.
    # ═══════════════════════════════════════════════════════════════
    CIRCUIT_BREAKER_LIMIT = 12   # [FIX O] Scan #2 pelajaran: 47 kandidat = 0% win rate
    if len(candidates) > CIRCUIT_BREAKER_LIMIT:
        log.warning(
            f"\n⚡ CIRCUIT BREAKER AKTIF — {len(candidates)} kandidat > {CIRCUIT_BREAKER_LIMIT}"
        )
        log.warning(
            f"   Terlalu banyak koin dengan funding negatif secara bersamaan."
        )
        log.warning(
            f"   Ini indikasi market dump masif, bukan setup pump individual."
        )
        log.warning(f"   Tidak ada alert dikirim.")
        if BOT_TOKEN and CHAT_ID:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={'chat_id': CHAT_ID,
                          'text': (f"⚡ CIRCUIT BREAKER — {len(candidates)} kandidat terdeteksi\n"
                                   f"Terlalu banyak koin dengan funding negatif bersamaan.\n"
                                   f"Market mungkin dalam kondisi dump masif.\n"
                                   f"Tidak ada alert dikirim. Cek kondisi BTC.")},
                    timeout=10
                )
            except Exception:
                pass
        return  # ← EXIT: tidak kirim alert apapun

    if candidates:
        log.info(f"\nTOP KANDIDAT (score >= {SCORE_WATCH}):")
        log.info(f"  {'Symbol':<28} {'Scr':>4} {'Stk':>4}  Level    Sinyal Utama")
        log.info(f"  {'-'*28} {'-'*4} {'-'*4}  {'-'*8} {'-'*30}")
        for c in candidates[:15]:
            lvl = ("STRONG" if c['score'] >= SCORE_STRONG
                   else "ALERT" if c['score'] >= SCORE_ALERT else "watch")
            top = c['signals'][0] if c['signals'] else '-'
            log.info(
                f"  {c['symbol']:<28} {c['score']:>4} {c['neg_streak']:>4}  "
                f"{lvl:<8} {top}"
            )

    # Kirim alert
    alert_count  = 0
    skipped_rr   = 0
    for cand in candidates:
        if cand['score'] < SCORE_ALERT:
            continue

        lvl = "STRONG" if cand['score'] >= SCORE_STRONG else "ALERT"
        ez  = compute_entry_zone(cand['df'], cand['price'])

        # Jika setup tidak memenuhi R:R minimum (return None) → skip
        if ez is None:
            log.info(
                f"  {cand['symbol']} [{lvl}] score={cand['score']} → "
                f"SKIP: R:R < 2.0 setelah Fibonacci, setup tidak layak"
            )
            skipped_rr += 1
            continue

        log.info(
            f"\n  {cand['symbol']} [{lvl}] score={cand['score']}\n"
            f"  entry={ez['entry']:.8g}  SL={ez['sl']:.8g} (-{ez['sl_pct']:.1f}%)\n"
            f"  TP1={ez['tp1']:.8g} (+{ez['tp1_pct']:.1f}%) | "
            f"TP2={ez['tp2']:.8g} (+{ez['tp2_pct']:.1f}%) | "
            f"TP3={ez['tp3']:.8g} (+{ez['tp3_pct']:.1f}%)\n"
            f"  R:R ke TP2={ez['rr_tp2']:.1f}x | ke TP3={ez['rr_tp3']:.1f}x | "
            f"Metode: {ez['method']}"
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

        if alert_count >= MAX_ALERTS_PER_SCAN:
            log.info(f"Batas {MAX_ALERTS_PER_SCAN} alert per sesi tercapai.")
            break

    if skipped_rr > 0:
        log.info(f"  ({skipped_rr} kandidat dilewati karena R:R < 2.0)")

    log.info(f"\n{'='*65}")
    log.info(f"SELESAI — {alert_count} alert terkirim dari {len(candidates)} kandidat")
    log.info(f"{'='*65}\n")


if __name__ == "__main__":
    main()
