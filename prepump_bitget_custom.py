import requests
import time

# ==============================
# ⚙️ TELEGRAM CONFIG
# ==============================
BOT_TOKEN = "ISI_BOT_TOKEN"
CHAT_ID = "ISI_CHAT_ID"

# ==============================
# ⚙️ TELEGRAM SENDER
# ==============================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg}
    try:
        requests.post(url, data=data)
    except:
        pass

# ==============================
# ⚙️ GET FUTURES DATA
# ==============================
def get_futures_tickers():
    url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=UMCBL"
    return requests.get(url).json()['data']

# ==============================
# 🧠 SCORING ENGINE
# ==============================
def calculate_score(price, volume, oi, funding):

    score = 0
    reasons = []

    # --- Pre Pump Base ---
    if abs(price) < 3:
        score += 1
        reasons.append("Stable Price")

    if volume > 1.3:
        score += 1
        reasons.append("Volume Rising")

    if oi > 1:
        score += 1
        reasons.append("OI Rising")

    if funding < 0:
        score += 1
        reasons.append("Bearish Crowd")

    # --- Whale Entry Mode ---
    if oi > 1.5 and volume > 1.5 and abs(price) < 2:
        score += 2
        reasons.append("Whale Entry")

    # --- Fake Accumulation Filter ---
    if volume > 1.5 and oi < 1:
        score -= 1
        reasons.append("Fake Accumulation")

    # --- Liquidation Trap ---
    if funding < 0 and oi > 1.2:
        score += 1
        reasons.append("Short Trap")

    # --- Parabolic Top Killer ---
    if price > 6:
        score -= 2
        reasons.append("Already Pumped")

    return score, reasons

# ==============================
# 🎯 TIMING ENGINE
# ==============================
def detect_entry_timing(price, oi, volume, funding):
    score = 0
    reasons = []

    if oi > 1:
        score += 1
        reasons.append("OI Rising")

    if volume > 1.2:
        score += 1
        reasons.append("Volume Expansion")

    if funding < 0:
        score += 1
        reasons.append("Bearish Crowd Trap")

    if abs(price) < 1:
        score += 1
        reasons.append("Price Compression")

    if oi > 1 and volume > 1.5 and abs(price) < 1:
        score += 2
        reasons.append("Whale Silent Entry")

    return score, reasons

# ==============================
# 🎯 ENTRY CLASSIFIER
# ==============================
def classify_entry(score):
    if score >= 4:
        return "SNIPER ENTRY"
    elif score >= 2:
        return "EARLY BUILDUP"
    else:
        return "WAIT"

# ==============================
# 🚀 MAIN SCANNER
# ==============================
def scan_market():

    tickers = get_futures_tickers()

    for coin in tickers:

        symbol = coin['symbol']
        price_change = float(coin['change24h'])
        volume_ratio = float(coin['baseVolume'])
        oi_change = float(coin['openInterest'])
        funding_rate = float(coin.get('fundingRate', 0))

        # Normalize (simple)
        volume_ratio = volume_ratio / 1000000
        oi_change = oi_change / 100000

        score, reasons = calculate_score(price_change, volume_ratio, oi_change, funding_rate)
        timing_score, timing_reasons = detect_entry_timing(price_change, oi_change, volume_ratio, funding_rate)
        entry_type = classify_entry(timing_score)

        if score >= 5:

            msg = f"""
PRE-PUMP SIGNAL

Symbol: {symbol}
Score: {score}
Timing Score: {timing_score}
Entry Type: {entry_type}

Signals:
{', '.join(reasons)}

Timing:
{', '.join(timing_reasons)}
"""
            send_telegram(msg)

# ==============================
# 🔁 LOOP
# ==============================
while True:
    try:
        scan_market()
        time.sleep(300)
    except:
        time.sleep(60)
