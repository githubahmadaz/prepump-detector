import ccxt
import requests

print("="*60)
print("METODE 1: Menggunakan CCXT dengan berbagai parameter")
print("="*60)

# Pendekatan 1: CCXT tanpa opsi khusus
print("\n1. Mencoba ccxt.bitget() tanpa opsi...")
exchange1 = ccxt.bitget({'enableRateLimit': True})
try:
    exchange1.load_markets()
    print(f"   ✅ Berhasil memuat {len(exchange1.markets)} markets (umum)")
    # Cek 5 sample
    sample = list(exchange1.markets.keys())[:5]
    print(f"   Sample: {sample}")
    if 'BTCUSDT' in exchange1.markets:
        print("   ✅ BTCUSDT DITEMUKAN!")
        print(f"      Detail: {exchange1.markets['BTCUSDT']}")
    else:
        print("   ❌ BTCUSDT tidak ditemukan")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Pendekatan 2: CCXT dengan opsi swap
print("\n2. Mencoba ccxt.bitget dengan options defaultType='swap'...")
exchange2 = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})
try:
    exchange2.load_markets()
    print(f"   ✅ Berhasil memuat {len(exchange2.markets)} markets (swap)")
    if 'BTCUSDT' in exchange2.markets:
        print("   ✅ BTCUSDT DITEMUKAN!")
    else:
        print("   ❌ BTCUSDT tidak ditemukan")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Pendekatan 3: CCXT dengan opsi future
print("\n3. Mencoba ccxt.bitget dengan options defaultType='future'...")
exchange3 = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})
try:
    exchange3.load_markets()
    print(f"   ✅ Berhasil memuat {len(exchange3.markets)} markets (future)")
    if 'BTCUSDT' in exchange3.markets:
        print("   ✅ BTCUSDT DITEMUKAN!")
    else:
        print("   ❌ BTCUSDT tidak ditemukan")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "="*60)
print("METODE 2: Panggil API Bitget Langsung (via requests)")
print("="*60)

# Pendekatan 4: Panggil API publik Bitget untuk futures
print("\n4. Memanggil API futures Bitget (v2/mix/market/contracts)...")
url = "https://api.bitget.com/api/v2/mix/market/contracts"
params = {
    'productType': 'USDT-FUTURES'  # Coba dengan tanda hubung
}
try:
    response = requests.get(url, params=params, timeout=10)
    if response.status_code == 200:
        data = response.json()
        if data['code'] == '00000':
            contracts = data['data']
            print(f"   ✅ API berhasil! Mendapatkan {len(contracts)} kontrak USDT-M futures")
            if len(contracts) > 0:
                print(f"   Contoh kontrak pertama: {contracts[0]['symbol']}")
                # Cek apakah BTCUSDT ada
                btc_found = any(c['symbol'] == 'BTCUSDT' for c in contracts)
                if btc_found:
                    print("   ✅ BTCUSDT DITEMUKAN di daftar kontrak!")
                else:
                    print("   ❌ BTCUSDT TIDAK DITEMUKAN di daftar kontrak")
            else:
                print("   ⚠️ Daftar kontrak kosong. Mungkin parameter salah.")
        else:
            print(f"   ❌ API error: {data}")
    else:
        print(f"   ❌ HTTP error {response.status_code}")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n5. Mencoba API dengan productType lain: 'umcbl' (format lama)...")
params2 = {'productType': 'umcbl'}
try:
    response = requests.get("https://api.bitget.com/api/mix/v1/market/contracts", params=params2, timeout=10)
    if response.status_code == 200:
        data = response.json()
        if data['code'] == '00000':
            contracts = data['data']
            print(f"   ✅ API v1 berhasil! Mendapatkan {len(contracts)} kontrak")
            if len(contracts) > 0:
                print(f"   Contoh kontrak pertama: {contracts[0]['symbol']}")
                btc_found = any(c['symbol'] == 'BTCUSDT' for c in contracts)
                if btc_found:
                    print("   ✅ BTCUSDT DITEMUKAN di API v1!")
                else:
                    print("   ❌ BTCUSDT TIDAK DITEMUKAN")
    else:
        print(f"   ❌ HTTP error {response.status_code}")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "="*60)
print("METODE 3: Lihat semua pasar yang mengandung USDT (via CCXT)")
print("="*60)

# Coba lihat semua pasar yang mengandung USDT
print("\n6. Mencari pasar yang mengandung 'USDT' di exchange1 (tanpa opsi)...")
usdt_markets = [s for s in exchange1.markets.keys() if 'USDT' in s]
print(f"   Total pasar mengandung USDT: {len(usdt_markets)}")
print(f"   10 sample pertama: {usdt_markets[:10]}")
