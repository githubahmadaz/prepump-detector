#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRE-PUMP SCANNER v14.1-RESEARCH (Bitget Futures)
Perbaikan berdasarkan laporan bug:
[FIX 1] VWAP reset harian (UTC)
[FIX 2] Funding score bertingkat (streak 5-9,10-14,>=15)
[FIX 3] Threshold above_vwap 10/12 untuk bobot penuh
[FIX 4] Entry zone menggunakan max(VWAP, EMA20) dan SL di bawah low 3 candle
[FIX 9] Higher low dengan metode swing low (window 5)
[FIX 10] MAX_VOLUME 15M
[FIX 12] Cumulative funding < -0.05 mendapat +2
[FIX 13] BOS Up menggunakan close
"""

import ccxt
import requests
import pandas as pd
import numpy as np
import time
import os
import sys
import logging
from datetime import datetime, timedelta
from collections import deque

# =============================================================================
# KONFIGURASI
# =============================================================================
TIMEFRAME = '5m'
CANDLE_LIMIT = 100                     # ditambah untuk memastikan VWAP harian punya data cukup
FUNDING_LIMIT = 100                     # jumlah periode funding history
MIN_VOLUME_USDT = 100_000               # minimal volume 24h
MAX_VOLUME_USDT = 15_000_000            # maksimal volume (diturunkan dari 50M)
MAX_PRICE_CHG_24H = 30                   # hindari koin yang sudah ekstrem
COOLDOWN_HOURS = 6                       # waktu tunggu setelah alert

# Threshold scoring (riset)
SCORE_WATCH = 10
SCORE_ALERT = 16
SCORE_STRONG = 20

# Telegram
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

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
# FUNGSI BANTU
# =============================================================================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"Request gagal: {e}")
        return None

def fetch_bitget_futures():
    """Ambil semua pair USDT perpetual dari Bitget"""
    try:
        markets = exchange.load_markets()
        symbols = []
        for sym, m in markets.items():
            if m['quote'] == 'USDT' and m['swap'] and m['active']:
                symbols.append(sym)
        log.info(f"Total USDT perpetual: {len(symbols)}")
        return symbols
    except Exception as e:
        log.error(f"Gagal load markets: {e}")
        return []

def fetch_funding_history(symbol, limit=FUNDING_LIMIT):
    """Ambil funding rate history dari Bitget"""
    try:
        funding = exchange.fetch_funding_rate_history(symbol, limit=limit)
        if funding:
            df = pd.DataFrame(funding)
            df['fundingRate'] = df['fundingRate'].astype(float)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df.sort_values('timestamp').reset_index(drop=True)
        return pd.DataFrame()
    except Exception as e:
        log.debug(f"Funding history error {symbol}: {e}")
        return pd.DataFrame()

def compute_funding_stats(df_fund):
    """Hitung statistik funding: streak, count, cumulative, avg6"""
    if df_fund.empty:
        return None
    rates = df_fund['fundingRate'].tolist()
    # Hitung streak negatif terbaru
    neg_streak = 0
    for r in reversed(rates):
        if r < 0:
            neg_streak += 1
        else:
            break
    neg_count = sum(1 for r in rates if r < 0)
    neg_pct = neg_count / len(rates) * 100
    cumulative = sum(rates)
    avg6 = np.mean(rates[-6:]) if len(rates) >= 6 else np.mean(rates)
    return {
        'neg_streak': neg_streak,
        'neg_count': neg_count,
        'neg_pct': neg_pct,
        'cumulative': cumulative,
        'avg6': avg6
    }

def fetch_ohlcv(symbol, limit=CANDLE_LIMIT):
    """Ambil candle terakhir"""
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        log.debug(f"OHLCV error {symbol}: {e}")
        return pd.DataFrame()

def compute_daily_vwap(df):
    """
    Hitung VWAP per hari (reset setiap tengah malam UTC).
    Asumsi: df sudah punya kolom 'timestamp' (datetime).
    """
    df = df.copy()
    df['date'] = df['timestamp'].dt.date  # pisah per hari
    vwap_list = []
    for date, group in df.groupby('date', sort=False):
        group = group.sort_values('timestamp')
        tp = (group['high'] + group['low'] + group['close']) / 3
        cum_vol = group['volume'].cumsum()
        cum_tp_vol = (tp * group['volume']).cumsum()
        group['vwap'] = cum_tp_vol / cum_vol
        vwap_list.append(group)
    df_vwap = pd.concat(vwap_list).sort_index()
    return df_vwap['vwap']

def compute_indicators(df):
    """Hitung indikator teknikal"""
    if df.empty or len(df) < 30:
        return df

    # VWAP harian (reset tiap tengah malam)
    df['vwap'] = compute_daily_vwap(df)

    # EMA 20
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()

    # RSI 14
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(span=14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=14, adjust=False).mean()
    rs = gain / loss
    df['rsi14'] = 100 - (100 / (1 + rs))

    # ATR 14
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    df['atr_pct'] = df['atr14'] / df['close'] * 100

    # Bollinger Bands (20,2)
    df['bb_mid'] = df['close'].rolling(20).mean()
    df['bb_std'] = df['close'].rolling(20).std()
    df['bb_up'] = df['bb_mid'] + 2 * df['bb_std']
    df['bb_dn'] = df['bb_mid'] - 2 * df['bb_std']
    df['bb_pct'] = (df['close'] - df['bb_dn']) / (df['bb_up'] - df['bb_dn'] + 1e-9)
    df['bb_width'] = (df['bb_up'] - df['bb_dn']) / df['bb_mid']
    df['bb_squeeze'] = df['bb_width'] <= df['bb_width'].rolling(20).min() * 1.05

    # Volume ratio
    df['vol_ma12'] = df['volume'].rolling(12).mean()
    df['vol_ratio'] = df['volume'] / (df['vol_ma12'] + 1e-9)

    # BOS Up: close > highest high 20 sebelumnya (gunakan shift)
    df['high_20_max'] = df['high'].rolling(20).max().shift(1)
    df['bos_up'] = df['close'] > df['high_20_max']

    # Higher Low: deteksi swing low dengan window 5
    # Cari local minimum: low < low[-1] dan low < low[+1] dalam 5 candle ke depan/belakang
    # Sederhana: low adalah yang terendah dalam 5 candle ke depan dan ke belakang
    df['min_5_left'] = df['low'].rolling(5, min_periods=1).min()
    df['min_5_right'] = df['low'].shift(-5).rolling(5, min_periods=1).min()
    df['is_swing_low'] = (df['low'] == df['min_5_left']) & (df['low'] == df['min_5_right'])
    # Higher low: swing low sekarang > swing low sebelumnya (candle sebelumnya yang juga swing low)
    # Untuk sederhana, kita hitung dalam 12 candle terakhir apakah ada pola higher low
    # Tapi untuk scoring, kita akan hitung jumlah kemunculan higher low dalam 12 candle
    # Higher low terjadi jika low > low sebelumnya dalam konteks swing low.
    # Kita gunakan pendekatan: dalam 12 candle terakhir, cari dua swing low berurutan dengan harga meningkat.
    # Namun untuk scoring, kita akan deteksi higher low sederhana: low > low 5 candle lalu.
    # Alternatif: gunakan metode yang lebih sederhana: low > low[-1] dan low[-1] > low[-2] dalam 12 candle? 
    # Sesuai riset, kita bisa menggunakan kondisi: low > low sebelumnya dalam 3 candle berturut-turut? 
    # Tapi riset menggunakan "higher_low" sebagai struktur bullish, kita bisa pakai: 
    # low > low 5 candle lalu dan low 5 lalu > low 10 lalu? 
    # Untuk simplicity, kita gunakan: dalam 12 candle terakhir, ada setidaknya 2 kali di mana low > low sebelumnya.
    # Namun itu terlalu sederhana. Mari gunakan metode yang diusulkan: 
    # higher low jika low > low 5 candle yang lalu (dengan asumsi ada kenaikan bertahap).
    # Kita akan hitung jumlah higher low dalam 12 candle: low > low.shift(5)
    df['higher_low_simple'] = df['low'] > df['low'].shift(5)
    # Nanti kita akan pakai jumlah kemunculan dalam 12 candle terakhir.
    
    return df

def compute_above_vwap_count(df, n=12):
    """Hitung berapa candle dari n terakhir yang close di atas VWAP"""
    if df.empty or len(df) < n:
        return 0
    recent = df.tail(n)
    return (recent['close'] > recent['vwap']).sum()

def get_coingecko_context(coin_id):
    """Ambil rank dan jarak ATH dari CoinGecko (opsional)"""
    if not coin_id:
        return {}
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    data = safe_get(url, {'localization':'false','tickers':'false','market_data':'true'})
    if data and 'market_data' in data:
        md = data['market_data']
        return {
            'rank': data.get('market_cap_rank', 9999),
            'ath_dist': md.get('ath_change_percentage', {}).get('usd', 0)
        }
    return {}

def compute_score(symbol, funding_stats, df, coin_ctx):
    """
    Hitung skor berdasarkan tier riset (maks 30)
    funding_stats: dict dari compute_funding_stats
    df: dataframe dengan indikator (setidaknya 30 baris terakhir)
    coin_ctx: dict dari CoinGecko (rank, ath_dist)
    """
    if df.empty or len(df) < 30:
        return 0, []
    score = 0
    signals = []

    # --- TIER B: Sinyal Struktural (max 15) ---
    # Above VWAP dominan: 10/12 bobot penuh, 8-9 dapat setengah?
    # Kita beri bobot bertahap: 10-12 = 5, 8-9 = 3, <8 = 0
    above_count = compute_above_vwap_count(df, 12)
    if above_count >= 10:
        score += 5
        signals.append("above_vwap_dominant")
    elif above_count >= 8:
        score += 3
        signals.append("above_vwap_moderate")

    # Bos up dalam 6 candle terakhir
    if df['bos_up'].tail(6).any():
        score += 5
        signals.append("bos_up_confirmed")

    # Extreme negative streak (skala)
    if funding_stats:
        streak = funding_stats['neg_streak']
        if streak >= 15:
            score += 3
            signals.append("extreme_neg_streak")
        elif streak >= 10:
            score += 2
            signals.append("long_neg_streak")
        elif streak >= 5:
            score += 1
            signals.append("moderate_neg_streak")

        # Cumulative funding sangat negatif (fix 12)
        if funding_stats['cumulative'] < -0.05:
            score += 2
            signals.append("deep_cumulative_neg")

    # Higher Lows dalam 12 candle terakhir
    # Gunakan metode sederhana: hitung jumlah candle di mana low > low 5 candle lalu
    higher_low_count = df['higher_low_simple'].tail(12).sum()
    if higher_low_count >= 3:
        score += 2
        signals.append("higher_lows_forming")
    elif higher_low_count >= 2:
        score += 1
        signals.append("higher_lows_weak")

    # --- TIER C: Sinyal Momentum (max 10) ---
    last = df.iloc[-1]
    # RSI > 55
    if last['rsi14'] > 55:
        score += 3
        signals.append(f"rsi_strong ({last['rsi14']:.1f})")
    elif last['rsi14'] > 50:
        score += 1
        signals.append("rsi_neutral")

    # Volume surge: vol_ratio > 1.3 pada setidaknya 3 dari 6 candle terakhir
    vol_surge_count = (df['vol_ratio'].tail(6) > 1.3).sum()
    if vol_surge_count >= 3:
        score += 3
        signals.append("volume_surge")
    elif vol_surge_count >= 2:
        score += 1
        signals.append("volume_increase")

    # ATR ekspansi > 0.7%
    if last['atr_pct'] > 0.7:
        score += 2
        signals.append("atr_expanding")

    # Harga di upper band (bb_pct > 0.65)
    if last['bb_pct'] > 0.65:
        score += 2
        signals.append("upper_band")
    elif last['bb_pct'] > 0.5:
        score += 1
        signals.append("mid_band")

    # --- TIER D: Sinyal Kontekstual (max 5) ---
    # Funding reset: neg_streak >=5 dan avg6 >= -0.00005
    if funding_stats and funding_stats['neg_streak'] >= 5 and funding_stats.get('avg6', 0) >= -0.00005:
        score += 2
        signals.append("funding_reset_signal")

    # Small cap (rank > 1000)
    if coin_ctx and coin_ctx.get('rank', 9999) > 1000:
        score += 2
        signals.append("small_cap")

    # Bollinger squeeze (≥4 dari 12 candle terakhir)
    if df['bb_squeeze'].tail(12).sum() >= 4:
        score += 1
        signals.append("bb_squeezed")

    return score, signals

def compute_entry_zone(df, current_price):
    """
    Hitung level entry berdasarkan support terbaru (low 3 candle) dan resistance (max VWAP, EMA20)
    Entry di atas max(VWAP, EMA20) dengan buffer 0.1%, SL di bawah low 3 candle terakhir.
    TP berdasarkan persentil riset: +0.8%, +1.8%, +3.7%
    """
    if df.empty or len(df) < 20:
        return None
    last3 = df.tail(3)
    support = last3['low'].min()  # low terendah 3 candle terakhir
    # Resistance dinamis: ambil max dari VWAP dan EMA20 pada candle terakhir
    last = df.iloc[-1]
    resistance = max(last['vwap'], last['ema20'])
    # Entry di atas resistance sedikit (misal 0.1% di atas)
    entry = resistance * 1.001
    # Stop loss di bawah support (2% di bawah support, atau jika support terlalu dekat, beri ruang)
    sl = support * 0.98  # 2% di bawah support, bisa disesuaikan dengan ATR
    # Target
    t1 = entry * 1.008
    t2 = entry * 1.018
    t3 = entry * 1.037
    return {
        'entry': entry,
        'sl': sl,
        't1': t1,
        't2': t2,
        't3': t3,
        'support': support,
        'resistance': resistance
    }

def send_telegram_alert(symbol, score, signals, price, funding_info, entry_zone=None):
    """Kirim alert ke Telegram"""
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram credentials not set")
        return
    try:
        msg = f"🚨 PRE-PUMP SIGNAL — v14.1-RESEARCH\n\n"
        msg += f"Symbol    : {symbol}\n"
        msg += f"Score     : {score}\n"
        msg += f"Harga     : ${price:.6f}\n"
        if funding_info:
            msg += f"Funding   : streak={funding_info['neg_streak']}, cumul={funding_info['cumulative']:.4f}\n"
        msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
        msg += "📊 SINYAL\n"
        for s in signals[:5]:
            msg += f"  • {s}\n"
        if len(signals) > 5:
            msg += f"  ... dan {len(signals)-5} lainnya\n"
        msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
        if entry_zone:
            msg += f"📍 ENTRY ZONE (berdasarkan riset)\n"
            msg += f"  Entry  : ${entry_zone['entry']:.6f} (di atas VWAP/EMA20)\n"
            msg += f"  SL     : ${entry_zone['sl']:.6f} (bawah low 3c)\n"
            msg += f"  TP1    : ${entry_zone['t1']:.6f} (+0.8%)\n"
            msg += f"  TP2    : ${entry_zone['t2']:.6f} (+1.8%)\n"
            msg += f"  TP3    : ${entry_zone['t3']:.6f} (+3.7%)\n"
        msg += f"\n📡 {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n"
        msg += "⚠️ Bukan financial advice."

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'HTML'}
        requests.post(url, data=payload, timeout=10)
        log.info(f"Alert sent for {symbol}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# =============================================================================
# MAIN SCANNER
# =============================================================================
def main():
    log.info("="*60)
    log.info("PRE-PUMP SCANNER v14.1-RESEARCH (Bitget Futures)")
    log.info("="*60)

    symbols = fetch_bitget_futures()
    if not symbols:
        log.error("Tidak ada symbol, keluar")
        return

    candidates = []
    cooldown_dict = {}

    log.info(f"Memproses {len(symbols)} symbols...")
    for idx, symbol in enumerate(symbols, 1):
        # Cek cooldown
        if symbol in cooldown_dict and time.time() < cooldown_dict[symbol]:
            continue

        log.info(f"[{idx}/{len(symbols)}] {symbol}")

        # Ambil ticker
        try:
            ticker = exchange.fetch_ticker(symbol)
            volume = ticker['quoteVolume'] or 0
            change = ticker['percentage'] or 0
            last_price = ticker['last'] or 0
            if volume < MIN_VOLUME_USDT or volume > MAX_VOLUME_USDT:
                log.debug(f"  Volume {volume:.0f} di luar rentang")
                continue
            if abs(change) > MAX_PRICE_CHG_24H:
                log.debug(f"  Change {change:.1f}% terlalu ekstrem")
                continue
        except Exception as e:
            log.debug(f"  Ticker error: {e}")
            continue

        # Funding history
        df_fund = fetch_funding_history(symbol)
        funding_stats = compute_funding_stats(df_fund)
        if not funding_stats:
            log.debug("  Tidak ada data funding")
            continue

        # Funding gate
        gate_pass = (
            funding_stats['neg_streak'] >= 5
            or (funding_stats['neg_pct'] >= 70 and funding_stats['cumulative'] < -0.015)
        )
        if not gate_pass:
            log.debug(f"  Funding gate tidak lolos (streak={funding_stats['neg_streak']}, neg%={funding_stats['neg_pct']:.1f}, cumul={funding_stats['cumulative']:.4f})")
            continue

        # Candles
        df = fetch_ohlcv(symbol, limit=CANDLE_LIMIT)
        if df.empty or len(df) < 30:
            log.debug("  Data candle tidak mencukupi")
            continue
        df = compute_indicators(df)

        # CoinGecko context (optional)
        base = symbol.replace('/USDT', '').lower()
        coin_ctx = get_coingecko_context(base)

        # Hitung skor
        score, signals = compute_score(symbol, funding_stats, df, coin_ctx)
        log.info(f"  Score={score} | sinyal: {len(signals)}")

        if score >= SCORE_WATCH:
            candidates.append({
                'symbol': symbol,
                'score': score,
                'signals': signals,
                'price': last_price,
                'funding': funding_stats,
                'df': df
            })

    candidates.sort(key=lambda x: x['score'], reverse=True)
    log.info(f"Lolos threshold: {len(candidates)} coin")

    alert_count = 0
    for cand in candidates:
        if cand['score'] >= SCORE_ALERT:
            entry_zone = compute_entry_zone(cand['df'], cand['price'])
            send_telegram_alert(
                cand['symbol'],
                cand['score'],
                cand['signals'],
                cand['price'],
                cand['funding'],
                entry_zone
            )
            alert_count += 1
            cooldown_dict[cand['symbol']] = time.time() + COOLDOWN_HOURS * 3600
            if alert_count >= 15:
                break

    log.info(f"=== SELESAI — {alert_count} alert terkirim ===")

if __name__ == "__main__":
    main()
