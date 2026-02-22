//@version=5
indicator("🚀 GOD MODE v2 – EARLY PUMP DETECTOR", overlay=false)

// ==========================
// ⚙️ INPUT
// ==========================
oiChange = input.float(2.0, "OI Change")
volumeChange = input.float(2.0, "Volume Change")
fundingRate = input.float(0.01, "Funding Rate")

scoreTrigger = input.float(8.5, "Signal Score")

// ==========================
// 📊 CORE MARKET STRUCTURE
// ==========================

// Momentum Compression (before pump)
compression = ta.stdev(close, 20) < ta.sma(ta.stdev(close,20),50) ? 1 : 0

// Volatility Expansion (start of move)
volExpansion = ta.stdev(close,10) > ta.sma(ta.stdev(close,10),50) ? 1 : 0

// Smart Money Absorption
absorption = volumeChange > 1.5 and math.abs(close - open) < ta.atr(14) ? 1 : 0

// ==========================
// 🐋 WHALE MODE
// ==========================
whaleLong = oiChange > 3 and volumeChange > 2 ? 1 : 0
whaleShort = oiChange < -3 and volumeChange > 2 ? 1 : 0

// ==========================
// 💀 LIQUIDATION TRAP FILTER
// ==========================
liqTrapLong = oiChange > 2 and close < open ? 1 : 0
liqTrapShort = oiChange < -2 and close > open ? 1 : 0

// ==========================
// 🚫 FAKE BREAKOUT FILTER
// ==========================
fakeBreakLong = ta.highest(close,10) == close and volumeChange < 1.2 ? 1 : 0
fakeBreakShort = ta.lowest(close,10) == close and volumeChange < 1.2 ? 1 : 0

// ==========================
// 🔺 PARABOLIC FILTER
// ==========================
parabolicTop = ta.rsi(close,14) > 78 ? 1 : 0
parabolicBottom = ta.rsi(close,14) < 22 ? 1 : 0

// ==========================
// 📈 TREND BIAS
// ==========================
trendBull = close > ta.ema(close,50) ? 1 : 0
trendBear = close < ta.ema(close,50) ? 1 : 0

// ==========================
// 🧠 SCORING ENGINE
// ==========================

longScore =
    (compression * 2) +
    (volExpansion * 2) +
    (absorption * 2) +
    (whaleLong * 3) +
    (trendBull * 1.5) +
    (fundingRate < 0 ? 1.5 : 0) -
    (liqTrapLong * 2) -
    (fakeBreakLong * 2) -
    (parabolicTop * 3)

shortScore =
    (compression * 2) +
    (volExpansion * 2) +
    (absorption * 2) +
    (whaleShort * 3) +
    (trendBear * 1.5) +
    (fundingRate > 0 ? 1.5 : 0) -
    (liqTrapShort * 2) -
    (fakeBreakShort * 2) -
    (parabolicBottom * 3)

// ==========================
// 🎯 SIGNAL
// ==========================
longSignal = longScore >= scoreTrigger
shortSignal = shortScore >= scoreTrigger

// ==========================
// 📊 PROBABILITY SCORE
// ==========================
probLong = math.min(longScore * 10, 100)
probShort = math.min(shortScore * 10, 100)

// ==========================
// 🎯 DYNAMIC TP SL
// ==========================
atr = ta.atr(14)

tpLong = close + atr * 2
slLong = close - atr * 1.5

tpShort = close - atr * 2
slShort = close + atr * 1.5

// ==========================
// 📢 TELEGRAM MESSAGE
// ==========================
longJSON = '{"signal":"LONG","prob":' + str.tostring(probLong) +
',"entry":' + str.tostring(close) +
',"tp":' + str.tostring(tpLong) +
',"sl":' + str.tostring(slLong) + '}'

shortJSON = '{"signal":"SHORT","prob":' + str.tostring(probShort) +
',"entry":' + str.tostring(close) +
',"tp":' + str.tostring(tpShort) +
',"sl":' + str.tostring(slShort) + '}'

alertcondition(longSignal, title="LONG SIGNAL", message=longJSON)
alertcondition(shortSignal, title="SHORT SIGNAL", message=shortJSON)

// ==========================
// 📉 VISUAL
// ==========================
plot(longScore, color=color.green, title="Long Score")
plot(shortScore, color=color.red, title="Short Score")

plotshape(longSignal, location=location.bottom, color=color.green, style=shape.labelup, text="LONG")
plotshape(shortSignal, location=location.top, color=color.red, style=shape.labeldown, text="SHORT")
