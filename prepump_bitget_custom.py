import requests
import pandas as pd
import numpy as np
import os

BASE_URL="https://api.bitget.com"

TARGET_SYMBOLS=[
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

TELEGRAM_TOKEN=os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID")

def req(endpoint,params=None):
    try:
        r=requests.get(BASE_URL+endpoint,params=params,timeout=10)
        d=r.json()
        if d.get("code")=="00000":
            return d.get("data")
    except:
        return None
    return None

def candles(symbol):
    d=req("/api/v2/mix/market/candles",
    {"symbol":symbol,"granularity":"5m","limit":40,"productType":"USDT-FUTURES"})
    if not d:return None
    df=pd.DataFrame(d,columns=["ts","o","h","l","c","v"]).astype(float)
    return df

def oi(symbol):
    d=req("/api/v2/mix/market/open-interest",
    {"symbol":symbol,"productType":"USDT-FUTURES"})
    if isinstance(d,dict):
        return float(d.get("openInterest",0))
    return None

def funding(symbol):
    d=req("/api/v2/mix/market/funding-rate",
    {"symbol":symbol,"productType":"USDT-FUTURES"})
    if isinstance(d,dict):
        return float(d.get("fundingRate",0))
    return None

def ob(symbol):
    d=req("/api/v2/mix/market/orderbook",
    {"symbol":symbol,"limit":50,"productType":"USDT-FUTURES"})
    if not d:return None
    bids=sum(float(x[1]) for x in d.get("bids",[]))
    asks=sum(float(x[1]) for x in d.get("asks",[]))
    return bids,asks

def btc_safe():
    df=candles("BTCUSDT")
    if df is None:return False
    ema=df.c.ewm(span=20).mean()
    slope=(ema.iloc[-1]-ema.iloc[-10])/ema.iloc[-10]
    return abs(slope)<0.02

def range_pct(df):
    last=df.iloc[-1]
    return (last.h-last.l)/last.c

def atr(df):
    h,l,c=df.h,df.l,df.c
    tr=np.maximum(h-l,abs(h-c.shift()),abs(l-c.shift()))
    return tr.rolling(10).mean().iloc[-1]

def oi_build(prev,now):
    if prev==0:return False
    change=(now-prev)/prev
    return 0<change<0.1

def analyze(symbol):

    df=candles(symbol)
    if df is None or len(df)<30:return None

    score=0

    if range_pct(df)<0.015:
        score+=1

    if atr(df.iloc[-11:])<atr(df.iloc[:-10]):
        score+=1

    fund=funding(symbol)
    if fund and -0.0001<fund<0.0001:
        score+=1

    depth=ob(symbol)
    if depth:
        bids,asks=depth
        if bids>asks:
            score+=1

    if df.v.iloc[-1]<df.v.iloc[-20:-1].mean()*1.8:
        score+=1

    oi_now=oi(symbol)
    oi_prev=oi(symbol)

    if oi_now and oi_prev and oi_build(oi_prev,oi_now):
        score+=1

    return score if score>=5 else None

def send(msg):
    if not TELEGRAM_TOKEN:return
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
    json={"chat_id":TELEGRAM_CHAT_ID,"text":msg})

def run():

    if not btc_safe():
        print("BTC trending, skip.")
        return

    alerts=[]

    for s in TARGET_SYMBOLS:
        sc=analyze(s)
        if sc:
            alerts.append(f"{s} Score:{sc}")

    if alerts:
        send("🚀 Early PrePump\n"+"\n".join(alerts))
        print(alerts)
    else:
        print("No signal")

if __name__=="__main__":
    run()
