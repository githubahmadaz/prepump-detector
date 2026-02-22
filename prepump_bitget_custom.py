import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os
import json

# ================== KONFIGURASI ==================
TARGET_SYMBOLS = [
    "0GUSDT", "1000BONKUSDT", "1000RATSUSDT", "1000SATSUSDT", "1MBABYDOGEUSDT",
    "AAVEUSDT", "ACHUSDT", "ADAUSDT", "AEROUSDT", "AKTUSDT", "ALCHUSDT",
    "ALGOUSDT", "ANKRUSDT", "APEUSDT", "APTUSDT", "ARBUSDT", "ARCUSDT",
    "ASTERUSDT", "ASTRUSDT", "ATUSDT", "ATHUSDT", "ATOMUSDT", "AVAXUSDT",
    "AVNTUSDT", "AWEUSDT", "AXLUSDT", "AXSUSDT", "AZTECUSDT", "BUSDT",
    "B2USDT", "BANUSDT", "BANANAS31USDT", "BARDUSDT", "BATUSDT", "BEATUSDT",
    "BERAUSDT", "BGBUSDT", "BIOUSDT", "BIRBUSDT", "BLURUSDT", "BRETTUSDT",
    "BSVUSDT", "CAKEUSDT", "CELOUSDT", "CFXUSDT", "CHZUSDT", "COAIUSDT",
    "COINUSDT", "COMPUSDT", "COWUSDT", "CROUSDT", "CRVUSDT", "CVXUSDT",
    "CYSUSDT", "DASHUSDT", "DEEPUSDT", "DEXEUSDT", "DOTUSDT", "DRIFTUSDT",
    "DYDXUSDT", "EGLDUSDT", "EIGENUSDT", "ENAUSDT", "ENSUSDT", "ENSOUSDT",
    "ETCUSDT", "ETHFIUSDT", "FARTCOINUSDT", "FETUSDT", "FFUSDT", "FILUSDT",
    "FLOKIUSDT", "FLUIDUSDT", "FOGOUSDT", "FORMUSDT", "GALAUSDT", "GASUSDT",
    "GLMUSDT", "GPSUSDT", "GRASSUSDT", "GRTUSDT", "GUNUSDT", "GWEIUSDT",
    "HUSDT", "HBARUSDT", "HNTUSDT", "HOMEUSDT", "HYPEUSDT", "ICNTUSDT",
    "ICPUSDT", "IDUSDT", "IMXUSDT", "INJUSDT", "IOTAUSDT", "IPUSDT",
    "IRYSUSDT", "JASMYUSDT", "JSTUSDT", "JTOUSDT", "JUPUSDT", "KAIAUSDT",
    "KAITOUSDT", "KASUSDT", "KITEUSDT", "KMNOUSDT", "KSMUSDT", "LDOUSDT",
    "LINEAUSDT", "LINKUSDT", "LITUSDT", "LPTUSDT", "LRCUSDT", "LTCUSDT",
    "LUNAUSDT", "LUNCUSDT", "LYNUSDT", "MUSDT", "MANAUSDT", "MASKUSDT",
    "MEUSDT", "MEMEUSDT", "MERLUSDT", "MINAUSDT", "MOCAUSDT", "MONUSDT",
    "MOODENGUSDT", "MORPHOUSDT", "MOVEUSDT", "MYXUSDT", "NEARUSDT", "NEOUSDT",
    "NIGHTUSDT", "NMRUSDT", "NXPCUSDT", "ONDOUSDT", "OPUSDT", "ORCAUSDT",
    "ORDIUSDT", "PARTIUSDT", "PAXGUSDT", "PENDLEUSDT", "PENGUUSDT", "PEPEUSDT",
    "PIEVERSEUSDT", "PIPPINUSDT", "PLUMEUSDT", "PNUTUSDT", "POLUSDT", "POLYXUSDT",
    "POPCATUSDT", "POWERUSDT", "PUMPUSDT", "PYTHUSDT", "QUSDT", "QNTUSDT",
    "RAVEUSDT", "RAYUSDT", "RENDERUSDT", "RIVERUSDT", "ROSEUSDT", "RPLUSDT",
    "RSRUSDT", "RUNEUSDT", "SUSDT", "SAHARAUSDT", "SANDUSDT", "SEIUSDT",
    "SENTUSDT", "SHIBUSDT", "SIGNUSDT", "SIRENUSDT", "SKRUSDT", "SKYUSDT",
    "SNXUSDT", "SOMIUSDT", "SOONUSDT", "SPKUSDT", "SPXUSDT", "SSVUSDT",
    "STABLEUSDT", "STGUSDT", "STRKUSDT", "STXUSDT", "SUIUSDT", "SUPERUSDT",
    "TUSDT", "TAGUSDT", "TAOUSDT", "THETAUSDT", "TIAUSDT", "TONUSDT",
    "TOSHIUSDT", "TRBUSDT", "TRUMPUSDT", "TURBOUSDT", "UAIUSDT", "UBUSDT",
    "UMAUSDT", "UNIUSDT", "VANAUSDT", "VETUSDT", "VIRTUALUSDT", "VTHOUSDT",
    "VVVUSDT", "WUSDT", "WALUSDT", "WIFUSDT", "WLDUSDT", "WLFIUSDT",
    "XAUTUSDT", "XDCUSDT", "XLMUSDT", "XMRUSDT", "XPLUSDT", "XTZUSDT",
    "XVGUSDT", "ZAMAUSDT", "ZECUSDT", "ZENUSDT", "ZETAUSDT", "ZILUSDT",
    "ZORAUSDT", "ZROUSDT", "ZRXUSDT"
]

BASE_URL = "https://api.bitget.com"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}

# ==================================================

class PrePumpDetectorBitgetCustom:
    def __init__(self, target_symbols):
        self.target_symbols = target_symbols
        self.symbols = []
        self.history = {}
        self.btc_history = {'timestamp': [], 'close': [], 'high': [], 'low': [], 'volume': []}
        self.first_detected = {}
        self.alerted = set()
        
        self.telegram_token = os.environ.get("TELEGRAM_TOKEN")
        self.telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if not self.telegram_token or not self.telegram_chat_id:
            raise ValueError("TELEGRAM_TOKEN dan TELEGRAM_CHAT_ID harus diisi di environment variable")

    def _request(self, endpoint, params=None):
        url = BASE_URL + endpoint
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('code') == '00000':
                    return data['data']
                else:
                    print(f"API error {endpoint}: {data}")
                    return None
            else:
                print(f"HTTP {resp.status_code} untuk {endpoint}: {resp.text}")
                return None
        except Exception as e:
            print(f"Request error {endpoint}: {e}")
            return None

    def init(self):
        print("Memuat daftar kontrak futures Bitget (v2)...")
        contracts = self._request("/api/v2/mix/market/contracts", params={"productType": "USDT-FUTURES"})
        if contracts is None:
            print("Gagal memuat kontrak. Periksa koneksi.")
            return
        
        available = {c['symbol'] for c in contracts}
        print(f"Total kontrak USDT-M tersedia: {len(available)}")
        
        for symbol in self.target_symbols:
            if symbol in available:
                self.symbols.append(symbol)
                print(f"✓ {symbol} tersedia")
            else:
                print(f"✗ {symbol} tidak tersedia")
        
        print(f"\nTotal {len(self.symbols)} pair yang akan dipantau: {self.symbols}")
        
        for symbol in self.symbols:
            self.history[symbol] = {
                'timestamp': [], 'close': [], 'high': [], 'low': [], 'volume': [],
                'oi': [], 'funding': []
            }

    def fetch_ohlcv(self, symbol, limit=100):
        params = {
            "symbol": symbol,
            "granularity": "5m",
            "limit": limit,
            "productType": "USDT-FUTURES"
        }
        data = self._request("/api/v2/mix/market/candles", params)
        if not data or not isinstance(data, list):
            return None
        
        ohlcv = []
        for candle in data:
            try:
                ts = int(candle[0])
                o = float(candle[1])
                h = float(candle[2])
                l = float(candle[3])
                c = float(candle[4])
                v = float(candle[5])
                ohlcv.append([ts, o, h, l, c, v])
            except:
                continue
        return ohlcv

    def fetch_oi(self, symbol):
        params = {"symbol": symbol, "productType": "USDT-FUTURES"}
        data = self._request("/api/v2/mix/market/open-interest", params)
        if data and isinstance(data, list) and len(data) > 0:
            try:
                return float(data[0].get('openInterest', 0))
            except:
                return None
        return None

    def fetch_funding(self, symbol):
        """Ambil funding rate terkini via endpoint yang benar"""
        params = {"symbol": symbol, "productType": "USDT-FUTURES"}
        data = self._request("/api/v2/mix/market/current-fund-rate", params)  # ← endpoint yang benar
        if data and isinstance(data, list) and len(data) > 0:
            try:
                return float(data[0].get('fundingRate', 0))
            except:
                return None
        return None

    def fetch_orderbook(self, symbol, limit=100):
        params = {"symbol": symbol, "limit": limit, "productType": "USDT-FUTURES"}
        data = self._request("/api/v2/mix/market/depth", params)
        if data and isinstance(data, dict):
            return data
        return None

    def update_all_data(self):
        btc_data = self.fetch_ohlcv('BTCUSDT', limit=100)
        if btc_data:
            for candle in btc_data:
                ts, o, h, l, c, v = candle
                self.btc_history['timestamp'].append(ts)
                self.btc_history['close'].append(c)
                self.btc_history['high'].append(h)
                self.btc_history['low'].append(l)
                self.btc_history['volume'].append(v)
            for key in self.btc_history:
                self.btc_history[key] = self.btc_history[key][-100:]

        for symbol in self.symbols:
            self.update_symbol_data(symbol)

    def update_symbol_data(self, symbol):
        ohlcv = self.fetch_ohlcv(symbol, limit=100)
        if ohlcv:
            ts, o, h, l, c, v = ohlcv[-1]
            self.history[symbol]['timestamp'].append(ts)
            self.history[symbol]['close'].append(c)
            self.history[symbol]['high'].append(h)
            self.history[symbol]['low'].append(l)
            self.history[symbol]['volume'].append(v)
            for key in ['timestamp', 'close', 'high', 'low', 'volume']:
                self.history[symbol][key] = self.history[symbol][key][-100:]

        oi = self.fetch_oi(symbol)
        if oi is not None:
            self.history[symbol]['oi'].append(oi)
            self.history[symbol]['oi'] = self.history[symbol]['oi'][-100:]

        funding = self.fetch_funding(symbol)
        if funding is not None:
            self.history[symbol]['funding'].append(funding)
            self.history[symbol]['funding'] = self.history[symbol]['funding'][-100:]

    # ========== Kriteria deteksi ==========
    def calculate_atr(self, highs, lows, closes, period=10):
        if len(highs) < period+1:
            return None
        tr = []
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr.append(max(hl, hc, lc))
        if len(tr) < period:
            return None
        atr = sum(tr[-period:]) / period
        return atr

    def check_range_squeeze(self, symbol):
        if len(self.history[symbol]['close']) < 2:
            return False
        high = self.history[symbol]['high'][-1]
        low = self.history[symbol]['low'][-1]
        close = self.history[symbol]['close'][-1]
        range_pct = (high - low) / close
        return range_pct < 0.015

    def check_atr_decreasing(self, symbol):
        if len(self.history[symbol]['high']) < 20:
            return False
        highs = self.history[symbol]['high'][-20:]
        lows = self.history[symbol]['low'][-20:]
        closes = self.history[symbol]['close'][-20:]
        atr_now = self.calculate_atr(highs[-11:], lows[-11:], closes[-11:], 10)
        atr_prev = self.calculate_atr(highs[:-10], lows[:-10], closes[:-10], 10)
        if atr_now is None or atr_prev is None:
            return False
        return atr_now < atr_prev

    def check_volume_stable(self, symbol):
        if len(self.history[symbol]['volume']) < 20:
            return False
        vol_now = self.history[symbol]['volume'][-1]
        avg_vol = np.mean(self.history[symbol]['volume'][-20:-1])
        return vol_now < 2 * avg_vol

    def check_oi_no_spike(self, symbol):
        if len(self.history[symbol]['oi']) < 12:
            return False
        oi_now = self.history[symbol]['oi'][-1]
        oi_prev = self.history[symbol]['oi'][-12]
        if oi_prev == 0:
            return False
        change = abs(oi_now - oi_prev) / oi_prev
        return change < 0.05

    def check_funding_neutral(self, symbol):
        if len(self.history[symbol]['funding']) == 0:
            return False
        funding = self.history[symbol]['funding'][-1]
        return -0.0001 <= funding <= 0.0001

    def check_bid_ask_depth(self, symbol):
        ob = self.fetch_orderbook(symbol)
        if not ob:
            return False
        current_price = self.history[symbol]['close'][-1]
        bid_threshold = current_price * 0.99
        ask_threshold = current_price * 1.01
        bid_vol = 0
        ask_vol = 0
        for bid in ob.get('bids', []):
            if float(bid[0]) >= bid_threshold:
                bid_vol += float(bid[1])
        for ask in ob.get('asks', []):
            if float(ask[0]) <= ask_threshold:
                ask_vol += float(ask[1])
        return bid_vol > ask_vol

    def check_btc_resilience(self, symbol):
        if len(self.btc_history['close']) < 12 or len(self.history[symbol]['close']) < 12:
            return False
        btc_change = (self.btc_history['close'][-1] - self.btc_history['close'][-12]) / self.btc_history['close'][-12]
        coin_change = (self.history[symbol]['close'][-1] - self.history[symbol]['close'][-12]) / self.history[symbol]['close'][-12]
        if btc_change < -0.005:
            return coin_change > btc_change
        return True

    def evaluate_symbol(self, symbol):
        conditions = {}
        conditions['range'] = self.check_range_squeeze(symbol)
        conditions['atr'] = self.check_atr_decreasing(symbol)
        conditions['volume'] = self.check_volume_stable(symbol)
        conditions['oi'] = self.check_oi_no_spike(symbol)
        conditions['funding'] = self.check_funding_neutral(symbol)
        conditions['depth'] = self.check_bid_ask_depth(symbol)
        conditions['btc'] = self.check_btc_resilience(symbol)

        total_true = sum(1 for v in conditions.values() if v)
        return conditions, total_true

    def estimate_readiness(self, total_conditions):
        if total_conditions >= 6:
            return "High"
        elif total_conditions >= 4:
            return "Medium"
        else:
            return "Low"

    def send_telegram(self, message):
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            'chat_id': self.telegram_chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code != 200:
                print(f"Gagal kirim Telegram: {r.text}")
        except Exception as e:
            print(f"Error kirim Telegram: {e}")

    def run_once(self):
        try:
            print(f"\n{datetime.now().isoformat()} - Memperbarui data...")
            self.update_all_data()

            if len(self.btc_history['close']) >= 20:
                btc_ema = pd.Series(self.btc_history['close']).ewm(span=20).mean().values
                ema_slope = (btc_ema[-1] - btc_ema[-20]) / btc_ema[-20]
                if abs(ema_slope) > 0.02:
                    print("BTC sedang trending kuat, lewati deteksi.")
                    return

            for symbol in self.symbols:
                self.process_symbol(symbol)

        except Exception as e:
            print(f"Error: {e}")

    def process_symbol(self, symbol):
        conditions, total_true = self.evaluate_symbol(symbol)
        now = time.time()

        if total_true >= 4:
            first_time = symbol not in self.first_detected
            if first_time:
                self.first_detected[symbol] = now
                print(f"Kondisi pertama terdeteksi untuk {symbol}")
            else:
                duration = now - self.first_detected[symbol]
                if duration > 6 * 3600:
                    if symbol in self.first_detected:
                        del self.first_detected[symbol]
                    return

            if first_time and symbol not in self.alerted:
                price = self.history[symbol]['close'][-1]
                durasi_jam = (now - self.first_detected[symbol]) / 3600
                duration_str = f"{durasi_jam:.1f} jam"

                atr_status = "menurun" if conditions['atr'] else "tidak menurun"

                if len(self.history[symbol]['oi']) >= 12:
                    oi_now = self.history[symbol]['oi'][-1]
                    oi_prev = self.history[symbol]['oi'][-12]
                    change_pct = abs(oi_now - oi_prev) / oi_prev * 100
                    oi_change = f"{change_pct:.1f}%"
                else:
                    oi_change = "N/A"

                ob = self.fetch_orderbook(symbol)
                bid_ask_imbalance = "N/A"
                if ob:
                    price_now = price
                    bid_thresh = price_now * 0.99
                    ask_thresh = price_now * 1.01
                    bid_vol = sum(float(b[1]) for b in ob.get('bids', []) if float(b[0]) >= bid_thresh)
                    ask_vol = sum(float(a[1]) for a in ob.get('asks', []) if float(a[0]) <= ask_thresh)
                    if bid_vol + ask_vol > 0:
                        imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol) * 100
                        bid_ask_imbalance = f"{imbalance:.1f}%"

                readiness = self.estimate_readiness(total_true)

                message = (
                    f"🚀 <b>Pre-Pump Terdeteksi (Bitget V2)</b>\n"
                    f"Coin: {symbol}\n"
                    f"Harga: {price:.8f}\n"
                    f"Durasi kompresi: {duration_str}\n"
                    f"ATR: {atr_status}\n"
                    f"OI change: {oi_change}\n"
                    f"Bid/Ask imbalance: {bid_ask_imbalance}\n"
                    f"Estimasi kesiapan: {readiness}\n"
                    f"Kriteria terpenuhi: {total_true}/7"
                )
                self.send_telegram(message)
                self.alerted.add(symbol)
        else:
            if symbol in self.first_detected:
                del self.first_detected[symbol]
            if symbol in self.alerted:
                self.alerted.remove(symbol)


if __name__ == "__main__":
    detector = PrePumpDetectorBitgetCustom(TARGET_SYMBOLS)
    detector.init()
    detector.run_once()
