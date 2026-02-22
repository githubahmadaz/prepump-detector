import requests
import time
import statistics

TELEGRAM_TOKEN = "8313562725:AAHBczdbML0-htf6fYb0upk9ZMIHEz8vw4I"
TELEGRAM_CHAT_ID = "5351495323"

SYMBOLS = [
    "0GUSDT",
    "1000BONKUSDT",
    "1000RATSUSDT",
    "1000SATSUSDT",
    "1MBABYDOGEUSDT",
    "AAVEUSDT",
    "ACHUSDT",
    "ADAUSDT",
    "AEROUSDT",
    "AKTUSDT",
    "ALCHUSDT",
    "ALGOUSDT",
    "ANKRUSDT",
    "APEUSDT",
    "APTUSDT",
    "ARBUSDT",
    "ARCUSDT",
    "ASTERUSDT",
    "ASTRUSDT",
    "ATUSDT",
    "ATHUSDT",
    "ATOMUSDT",
    "AVAXUSDT",
    "AVNTUSDT",
    "AWEUSDT",
    "AXLUSDT",
    "AXSUSDT",
    "AZTECUSDT",
    "BUSDT",
    "B2USDT",
    "BANUSDT",
    "BANANAS31USDT",
    "BARDUSDT",
    "BATUSDT",
    "BEATUSDT",
    "BERAUSDT",
    "BGBUSDT",
    "BIOUSDT",
    "BIRBUSDT",
    "BLURUSDT",
    "BRETTUSDT",
    "BSVUSDT",
    "CAKEUSDT",
    "CELOUSDT",
    "CFXUSDT",
    "CHZUSDT",
    "COAIUSDT",
    "COINUSDT",
    "COMPUSDT",
    "COWUSDT",
    "CROUSDT",
    "CRVUSDT",
    "CVXUSDT",
    "CYSUSDT",
    "DASHUSDT",
    "DEEPUSDT",
    "DEXEUSDT",
    "DOTUSDT",
    "DRIFTUSDT",
    "DYDXUSDT",
    "EGLDUSDT",
    "EIGENUSDT",
    "ENAUSDT",
    "ENSUSDT",
    "ENSOUSDT",
    "ETCUSDT",
    "ETHFIUSDT",
    "FARTCOINUSDT",
    "FETUSDT",
    "FFUSDT",
    "FILUSDT",
    "FLOKIUSDT",
    "FLUIDUSDT",
    "FOGOUSDT",
    "FORMUSDT",
    "GALAUSDT",
    "GASUSDT",
    "GLMUSDT",
    "GPSUSDT",
    "GRASSUSDT",
    "GRTUSDT",
    "GUNUSDT",
    "GWEIUSDT",
    "HUSDT",
    "HBARUSDT",
    "HNTUSDT",
    "HOMEUSDT",
    "HYPEUSDT",
    "ICNTUSDT",
    "ICPUSDT",
    "IDUSDT",
    "IMXUSDT",
    "INJUSDT",
    "IOTAUSDT",
    "IPUSDT",
    "IRYSUSDT",
    "JASMYUSDT",
    "JSTUSDT",
    "JTOUSDT",
    "JUPUSDT",
    "KAIAUSDT",
    "KAITOUSDT",
    "KASUSDT",
    "KITEUSDT",
    "KMNOUSDT",
    "KSMUSDT",
    "LDOUSDT",
    "LINEAUSDT",
    "LINKUSDT",
    "LITUSDT",
    "LPTUSDT",
    "LRCUSDT",
    "LTCUSDT",
    "LUNAUSDT",
    "LUNCUSDT",
    "LYNUSDT",
    "MUSDT",
    "MANAUSDT",
    "MASKUSDT",
    "MEUSDT",
    "MEMEUSDT",
    "MERLUSDT",
    "MINAUSDT",
    "MOCAUSDT",
    "MONUSDT",
    "MOODENGUSDT",
    "MORPHOUSDT",
    "MOVEUSDT",
    "MYXUSDT",
    "NEARUSDT",
    "NEOUSDT",
    "NIGHTUSDT",
    "NMRUSDT",
    "NXPCUSDT",
    "ONDOUSDT",
    "OPUSDT",
    "ORCAUSDT",
    "ORDIUSDT",
    "PARTIUSDT",
    "PAXGUSDT",
    "PENDLEUSDT",
    "PENGUUSDT",
    "PEPEUSDT",
    "PIEVERSEUSDT",
    "PIPPINUSDT",
    "PLUMEUSDT",
    "PNUTUSDT",
    "POLUSDT",
    "POLYXUSDT",
    "POPCATUSDT",
    "POWERUSDT",
    "PUMPUSDT",
    "PYTHUSDT",
    "QUSDT",
    "QNTUSDT",
    "RAVEUSDT",
    "RAYUSDT",
    "RENDERUSDT",
    "RIVERUSDT",
    "ROSEUSDT",
    "RPLUSDT",
    "RSRUSDT",
    "RUNEUSDT",
    "SUSDT",
    "SAHARAUSDT",
    "SANDUSDT",
    "SEIUSDT",
    "SENTUSDT",
    "SHIBUSDT",
    "SIGNUSDT",
    "SIRENUSDT",
    "SKRUSDT",
    "SKYUSDT",
    "SNXUSDT",
    "SOMIUSDT",
    "SOONUSDT",
    "SPKUSDT",
    "SPXUSDT",
    "SSVUSDT",
    "STABLEUSDT",
    "STGUSDT",
    "STRKUSDT",
    "STXUSDT",
    "SUIUSDT",
    "SUPERUSDT",
    "TUSDT",
    "TAGUSDT",
    "TAOUSDT",
    "THETAUSDT",
    "TIAUSDT",
    "TONUSDT",
    "TOSHIUSDT",
    "TRBUSDT",
    "TRUMPUSDT",
    "TURBOUSDT",
    "UAIUSDT",
    "UBUSDT",
    "UMAUSDT",
    "UNIUSDT",
    "VANAUSDT",
    "VETUSDT",
    "VIRTUALUSDT",
    "VTHOUSDT",
    "VVVUSDT",
    "WUSDT",
    "WALUSDT",
    "WIFUSDT",
    "WLDUSDT",
    "WLFIUSDT",
    "XAUTUSDT",
    "XDCUSDT",
    "XLMUSDT",
    "XMRUSDT",
    "XPLUSDT",
    "XTZUSDT",
    "XVGUSDT",
    "ZAMAUSDT",
    "ZECUSDT",
    "ZENUSDT",
    "ZETAUSDT",
    "ZILUSDT",
    "ZORAUSDT",
    "ZROUSDT",
    "ZRXUSDT"

]

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

def get_price(symbol):
    try:
        url = f"https://api.bitget.com/api/mix/v1/market/ticker?symbol={symbol}&productType=umcbl"
        data = requests.get(url).json()
        return float(data['data']['last'])
    except:
        return None

def get_volume(symbol):
    try:
        url = f"https://api.bitget.com/api/mix/v1/market/ticker?symbol={symbol}&productType=umcbl"
        data = requests.get(url).json()
        return float(data['data']['quoteVolume'])
    except:
        return None

def get_oi(symbol):
    try:
        url = f"https://api.bitget.com/api/mix/v1/market/open-interest?symbol={symbol}&productType=umcbl"
        data = requests.get(url).json()
        return float(data['data']['openInterest'])
    except:
        return None

def get_candles(symbol):
    try:
        url = f"https://api.bitget.com/api/mix/v1/market/candles?symbol={symbol}&granularity=300&limit=20"
        data = requests.get(url).json()
        highs = [float(c[2]) for c in data]
        lows = [float(c[3]) for c in data]
        closes = [float(c[4]) for c in data]
        return highs, lows, closes
    except:
        return None, None, None

def get_btc_trend():
    try:
        url = "https://api.bitget.com/api/mix/v1/market/ticker?symbol=BTCUSDT&productType=umcbl"
        data = requests.get(url).json()
        change = float(data['data']['chg'])
        return change
    except:
        return 0

def calculate_score(symbol):

    price = get_price(symbol)
    volume = get_volume(symbol)
    oi = get_oi(symbol)
    highs, lows, closes = get_candles(symbol)

    if None in [price, volume, oi] or highs is None:
        return None

    score = 0

    # SMART MONEY ENTRY
    if oi > 0 and abs(highs[-1] - lows[-1]) / price < 0.01:
        score += 2

    # ABSORPTION
    avg_range = statistics.mean([(h-l) for h,l in zip(highs,lows)])
    if avg_range < price * 0.015:
        score += 2

    # VOLATILITY COMPRESSION
    if statistics.stdev(closes) < price * 0.01:
        score += 2

    # LIQUIDATION TRAP
    if oi > 1000000 and abs(closes[-1] - closes[-5]) < price * 0.01:
        score += 2

    # MARKET ENVIRONMENT
    btc_trend = get_btc_trend()
    if btc_trend > -0.5:
        score += 2

    return score

while True:
    for symbol in SYMBOLS:
        score = calculate_score(symbol)

        if score is not None and score >= 6:
            msg = f"PRE-PUMP STRUCTURE DETECTED\n{symbol}\nScore: {score}/10"
            send_telegram(msg)
            print(msg)

    time.sleep(900)
