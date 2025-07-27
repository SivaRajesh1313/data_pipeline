import MetaTrader5 as mt5
import pandas as pd
import argparse
import os
from datetime import datetime, timedelta

# === Output Directory ===
DATA_DIR = "candles"
os.makedirs(DATA_DIR, exist_ok=True)

# === Timeframe Map ===
TIMEFRAME_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1
}

def connect_mt5():
    if not mt5.initialize():
        raise RuntimeError(f"Failed to connect to MetaTrader 5: {mt5.last_error()}")
    print("✅ Connected to MT5")

def fetch_ohlcv(symbol, tf_str, start, end):
    tf = TIMEFRAME_MAP.get(tf_str.upper())
    if tf is None:
        raise ValueError(f"Invalid timeframe: {tf_str}. Allowed: {list(TIMEFRAME_MAP.keys())}")
    
    utc_from = datetime.strptime(start, "%Y-%m-%d")
    utc_to = datetime.strptime(end, "%Y-%m-%d")

    print(f"📊 Fetching {symbol} [{tf_str}] from {start} to {end}...")
    rates = mt5.copy_rates_range(symbol, tf, utc_from, utc_to)

    if rates is None or len(rates) == 0:
        print(f"⚠️ No data for {symbol} on timeframe {tf_str}")
        return None

    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    return df

def save_csv(df, symbol, tf):
    path = os.path.join(DATA_DIR, f"{symbol}_{tf}.csv")
    df.to_csv(path, index=False)
    print(f"✅ Saved: {path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", required=True, help="Symbols to fetch (e.g., EURUSDm USDJPYm)")
    parser.add_argument("--timeframe", type=str, required=True, help="Timeframe (e.g., M15, H1)")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    connect_mt5()

    for symbol in args.symbols:
        df = fetch_ohlcv(symbol, args.timeframe, args.start, args.end)
        if df is not None:
            save_csv(df, symbol, args.timeframe)

    mt5.shutdown()
    print("🚪 Disconnected from MT5")

if __name__ == "__main__":
    main()
