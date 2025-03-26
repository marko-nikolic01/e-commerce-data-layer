import pandas as pd
import numpy as np
from datetime import datetime
import random
import schedule
import time

def weighted_percentage_change():
    weights = [1 / (i + 1) for i in range(11)]
    weights = [w / sum(weights) for w in weights]
    return np.random.choice(range(11), p=weights)

def flip_trend(current_trend):
    return (
        "Upward" if current_trend == "Downward" else "Downward"
    ) if random.random() < 0.2 else current_trend

def follow_trend(trend, price_change):
    if random.random() < 0.66:
        return trend, price_change if trend == "Upward" else -price_change
    else:
        return trend, random.choice([-price_change, price_change])

def main():
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"Generating products data : {timestamp}")

    # Read products data
    products_df = pd.read_csv("data/products.csv", dtype=str)
    used_df = pd.read_csv("data/used_products.csv", dtype=str)

    # Convert price columns to float
    products_df["Price"] = products_df["Price"].astype(float)
    used_df["Price"] = used_df["Price"].astype(float)

    # Update prices of previously used products
    used_updated = []
    for _, row in used_df.iterrows():
        stock_code = row["StockCode"]
        last_price = float(row["Price"])
        trend = row["Trend"]

        # Determine new price change based on weighted randomness
        percentage_change = weighted_percentage_change() / 100.0

        # Apply trend-following logic
        trend, effective_change = follow_trend(trend, percentage_change)

        # Calculate new price
        new_price = round(last_price * (1 + effective_change), 5)

        # Potentially flip the trend
        trend = flip_trend(trend)

        used_updated.append({
            "StockCode": stock_code,
            "Price": new_price,
            "Trend": trend
        })

    used_updated_df = pd.DataFrame(used_updated)

    # Filter out products that were already used
    used_ids = set(used_df["StockCode"])
    unused_df = products_df[~products_df["StockCode"].isin(used_ids)]

    # Select 100 random unused products
    new_selection = unused_df.sample(n=min(100, len(unused_df)), random_state=None).copy()

    # Assign random trends
    new_selection["Trend"] = np.random.choice(["Upward", "Downward"], size=len(new_selection))

    # Prepare new products data
    new_used_df = new_selection[["StockCode", "Price", "Trend"]]
    new_used_df.loc[:, "Price"] = new_used_df["Price"].astype(float)

    # Merge updated and new products, overwrite used_products.csv
    final_used_df = pd.concat([used_updated_df, new_used_df], ignore_index=True)
    final_used_df.to_csv("data/used_products.csv", index=False)

    # Add dates
    date_now = datetime.now().strftime("%Y-%m-%d")

    # Join with original product info to get names/descriptions
    enriched_df = pd.merge(
        final_used_df, products_df[["StockCode", "Name", "Description"]], 
        on="StockCode", how="left"
    )

    # Rename and format output
    enriched_df.rename(columns={
        "Name": "ProductName",
        "Description": "ProductDescription",
        "Price": "UnitPrice"
    }, inplace=True)
    enriched_df["Date"] = date_now

    output_df = enriched_df[["StockCode", "ProductName", "ProductDescription", "Date", "UnitPrice"]]

    # Create output file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"generated_data/products_{timestamp}.csv"

    # Create a CSV file from selected products
    output_df.to_csv(output_file, index=False)
    print(f"{len(output_df)} products written to {output_file}")

if __name__ == "__main__":
    schedule.every().day.at("00:00").do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
