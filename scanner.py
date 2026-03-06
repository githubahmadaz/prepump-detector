import requests
import pandas as pd
import numpy as np
import time
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

BITGET_URL = "https://api.bitget.com/api/mix/v1/market"


def send_telegram(msg):

    url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    data={
        "chat_id":CHAT_ID,
        "text":msg
    }

    requests.post(url,data=data)



def get_symbols():

    url=f"{BITGET_URL}/contracts?productType=umcbl"

    r=requests.get(url).json()

    symbols=[]

    for s in r["data"]:
        if "USDT" in s["symbol"]:
            symbols.append(s["symbol"])

    return symbols



def get_candles(symbol):

    url=f"{BITGET_URL}/candles"

    params={
        "symbol":symbol,
        "granularity":"15m",
        "limit":150
    }

    r=requests.get(url,params=params).json()

    df=pd.DataFrame(r["data"])

    df.columns=["time","open","high","low","close","volume"]

    df=df.astype(float)

    df=df[::-1]

    return df



def get_open_interest(symbol):

    url=f"{BITGET_URL}/open-interest"

    params={"symbol":symbol}

    r=requests.get(url,params=params).json()

    return float(r["data"]["openInterest"])



def calculate_indicators(df):

    df["vol_ma"]=df["volume"].rolling(20).mean()

    df["atr"]=(df["high"]-df["low"]).rolling(14).mean()

    df["range_high"]=df["high"].rolling(40).max()

    df["range_low"]=df["low"].rolling(40).min()

    return df



def detect_accumulation(df):

    volume_ratio=df["volume"].iloc[-1]/df["vol_ma"].iloc[-1]

    price_range=df["range_high"].iloc[-1]-df["range_low"].iloc[-1]

    cond1=volume_ratio>1.3
    cond2=price_range/df["close"].iloc[-1]<0.12

    return cond1 and cond2



def detect_liquidity_sweep(df):

    recent_low=df["low"].iloc[-2]

    prev_low=df["low"].rolling(20).min().iloc[-3]

    bullish_close=df["close"].iloc[-2]>df["open"].iloc[-2]

    return recent_low<prev_low and bullish_close



def detect_oi_growth(symbol):

    try:

        oi1=get_open_interest(symbol)

        time.sleep(1)

        oi2=get_open_interest(symbol)

        change=(oi2-oi1)/oi1*100

        return change>3

    except:
        return False



def build_deep_entry(df):

    support=df["range_low"].iloc[-1]

    resistance=df["range_high"].iloc[-1]

    atr=df["atr"].iloc[-1]

    range_size=resistance-support

    entry=support + range_size*0.25

    sl=support - atr*0.5

    tp1=resistance

    tp2=resistance + range_size

    return entry,sl,tp1,tp2,support,resistance



def scan():

    symbols=get_symbols()

    signals=[]

    for sym in symbols:

        try:

            df=get_candles(sym)

            df=calculate_indicators(df)

            acc=detect_accumulation(df)

            sweep=detect_liquidity_sweep(df)

            oi=detect_oi_growth(sym)

            score=sum([acc,sweep,oi])

            if score>=2:

                entry,sl,tp1,tp2,support,resistance=build_deep_entry(df)

                price=df["close"].iloc[-1]

                signals.append(
                    f"{sym}\n"
                    f"Price: {price:.4f}\n"
                    f"Deep Entry: {entry:.4f}\n"
                    f"SL: {sl:.4f}\n"
                    f"TP1: {tp1:.4f}\n"
                    f"TP2: {tp2:.4f}\n"
                    f"Support: {support:.4f}\n"
                    f"Resistance: {resistance:.4f}\n"
                )

        except:
            pass


    if signals:

        msg="🔥 DEEP ENTRY ACCUMULATION SIGNAL 🔥\n\n"

        for s in signals:
            msg+=s+"\n"

        send_telegram(msg)



if __name__=="__main__":

    while True:

        scan()

        time.sleep(3600)
