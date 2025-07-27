import pandas as pd
from datetime import timedelta
import argparse
import os

def tag_news_to_candles(ohlcv_df, news_df, window_minutes=60):
    ohlcv_df = ohlcv_df.copy()
    news_df = news_df.copy()

    ohlcv_df['time'] = pd.to_datetime(ohlcv_df['time'])
    news_df['timestamp'] = pd.to_datetime(news_df['timestamp'])

    ohlcv_df.sort_values('time', inplace=True)
    news_df.sort_values('timestamp', inplace=True)

    ohlcv_df['news_impact'] = None
    ohlcv_df['news_event'] = None
    ohlcv_df['news_currency'] = None
    ohlcv_df['minutes_from_news'] = None

    for i, candle_time in enumerate(ohlcv_df['time']):
        window_start = candle_time - timedelta(minutes=window_minutes)
        window_end = candle_time + timedelta(minutes=window_minutes)

        nearby_news = news_df[
            (news_df['timestamp'] >= window_start) & (news_df['timestamp'] <= window_end)
        ]

        if not nearby_news.empty:
            nearby_news = nearby_news.copy()
            nearby_news['time_diff'] = (nearby_news['timestamp'] - candle_time).abs()
            closest = nearby_news.sort_values('time_diff').iloc[0]

            ohlcv_df.at[i, 'news_impact'] = closest['impact'].split()[0]
            ohlcv_df.at[i, 'news_event'] = closest['event']
            ohlcv_df.at[i, 'news_currency'] = closest['currency']
            ohlcv_df.at[i, 'minutes_from_news'] = int((candle_time - closest['timestamp']).total_seconds() / 60)

    return ohlcv_df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="Symbol (e.g., EURUSDm)")
    parser.add_argument("--timeframe", required=True, help="Timeframe (e.g., M15)")
    parser.add_argument("--news_file", required=True, help="Path to news CSV file")
    parser.add_argument("--window", type=int, default=60, help="±Window in minutes (default: 60)")
    args = parser.parse_args()

    candles_path = os.path.join("candles", f"{args.symbol}_{args.timeframe}.csv")
    if not os.path.exists(candles_path):
        raise FileNotFoundError(f"❌ OHLCV file not found: {candles_path}")
    if not os.path.exists(args.news_file):
        raise FileNotFoundError(f"❌ News file not found: {args.news_file}")

    print(f"📥 Loading candles: {candles_path}")
    ohlcv_df = pd.read_csv(candles_path)

    print(f"📥 Loading news: {args.news_file}")
    news_df = pd.read_csv(args.news_file)

    print(f"🔗 Tagging news events to candles within ±{args.window} min window...")
    tagged_df = tag_news_to_candles(ohlcv_df, news_df, window_minutes=args.window)

    output_path = os.path.join("cc", f"{args.symbol}_{args.timeframe}_tagged.csv")
    tagged_df.to_csv(output_path, index=False)
    print(f"✅ Saved tagged data: {output_path}")

if __name__ == "__main__":
    main()
