import argparse
import pandas as pd
import os
from datetime import timedelta

# === Filtering Settings ===
DEFAULT_FILTERS = {
    "allowed_impacts": ["Medium", "High"],
    "max_minutes_from_news": 60,
    "min_candle_range": 0.0003,  # e.g., ~3 pips for EURUSD
}

# === Labeling Settings ===
DEFAULT_LABEL_SETTINGS = {
    "horizon": 3,  # how many candles ahead to look
    "threshold": 0.0005  # price delta to consider a move significant (5 pips)
}

def is_news_relevant(row, symbol, filters):
    if pd.isna(row['news_currency']):
        return False

    symbol_base = symbol[:3]
    symbol_quote = symbol[3:6]
    relevant_currencies = [symbol_base, symbol_quote]

    return (
        row['news_impact'] in filters['allowed_impacts']
        and abs(row['minutes_from_news']) <= filters['max_minutes_from_news']
        and row['news_currency'] in relevant_currencies
    )

def is_candle_volatile(row, filters):
    return abs(row['high'] - row['low']) >= filters['min_candle_range']

def apply_filters(df, symbol, filters):
    mask_news = df.apply(lambda row: is_news_relevant(row, symbol, filters), axis=1)
    mask_volatility = df.apply(lambda row: is_candle_volatile(row, filters), axis=1)
    return df[mask_news & mask_volatility].copy()

def generate_labels(df, label_settings):
    df = df.copy()
    labels = []
    for i in range(len(df) - label_settings['horizon']):
        curr_close = df.iloc[i]['close']
        future_close = df.iloc[i + label_settings['horizon']]['close']
        delta = future_close - curr_close

        if delta > label_settings['threshold']:
            labels.append(1)  # up
        elif delta < -label_settings['threshold']:
            labels.append(-1)  # down
        else:
            labels.append(0)  # flat

    labels += [None] * label_settings['horizon']  # pad the end with None
    df['label'] = labels
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="Symbol name like EURUSDm")
    parser.add_argument("--timeframe", required=True, help="Timeframe like M15")
    parser.add_argument("--input_path", required=True, help="Path to tagged OHLCV CSV")
    parser.add_argument("--output_path", default=None, help="Path to save filtered + labeled output")
    parser.add_argument("--min_range", type=float, default=DEFAULT_FILTERS['min_candle_range'])
    parser.add_argument("--max_minutes", type=int, default=DEFAULT_FILTERS['max_minutes_from_news'])
    parser.add_argument("--impact", nargs="*", default=DEFAULT_FILTERS['allowed_impacts'])
    parser.add_argument("--label_horizon", type=int, default=DEFAULT_LABEL_SETTINGS['horizon'])
    parser.add_argument("--label_threshold", type=float, default=DEFAULT_LABEL_SETTINGS['threshold'])
    args = parser.parse_args()

    print(f"📥 Loading tagged data: {args.input_path}")
    df = pd.read_csv(args.input_path)

    # Update filter settings
    filters = {
        "allowed_impacts": args.impact,
        "max_minutes_from_news": args.max_minutes,
        "min_candle_range": args.min_range
    }
    label_settings = {
        "horizon": args.label_horizon,
        "threshold": args.label_threshold
    }

    print("🔎 Applying filters: impact, minutes_from_news, volatility...")
    filtered_df = apply_filters(df, args.symbol, filters)
    print(f"✅ Filtered rows: {len(filtered_df)} / {len(df)}")

    print("🏷️ Generating labels...")
    labeled_df = generate_labels(filtered_df, label_settings)

    if args.output_path is None:
        base = os.path.basename(args.input_path).replace(".csv", "")
        args.output_path = f"labeled/{base}_labeled.csv"

    os.makedirs(os.path.dirname(args.output_path), exist_ok=True)
    labeled_df.to_csv(args.output_path, index=False)
    print(f"💾 Saved labeled data: {args.output_path}")

if __name__ == "__main__":
    main()