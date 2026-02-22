import ccxt

print("Mencoba koneksi ke Bitget...")
exchange = ccxt.bitget({
    'enableRateLimit': True,
})

try:
    # Coba load markets
    exchange.load_markets()
    print(f"✅ Berhasil! Total markets: {len(exchange.markets)}")
    
    # Tampilkan 10 sample
    print("\n10 sample markets:")
    count = 0
    for symbol in exchange.markets:
        if count < 10:
            print(f"  - {symbol}")
            count += 1
    
    # Cek apakah BTCUSDT ada
    if 'BTCUSDT' in exchange.markets:
        print("\n✅ BTCUSDT ditemukan!")
        print(f"  Detail: {exchange.markets['BTCUSDT']}")
    else:
        print("\n❌ BTCUSDT tidak ditemukan dalam markets")
        
except Exception as e:
    print(f"❌ Error: {e}")
