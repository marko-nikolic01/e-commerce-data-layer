import pandas as pd
from datetime import datetime
import schedule
import time

def main():
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"Generating products data : {timestamp}")

    # Read country data
    countries_df = pd.read_csv("data/countries.csv", dtype=str)
    used_countries_df = pd.read_csv("data/used_countries.csv", dtype=str)

    # Filter out countries that were already used
    used_ids = set(used_countries_df["CountryID"])
    unused_df = countries_df[~countries_df["CountryID"].isin(used_ids)]

    # Don't do anything if all countries were used
    if unused_df.empty:
        return

    # Select 30 random unused countries
    selected_df = unused_df.sample(n=min(30, len(unused_df)), random_state=None)

    # Create output file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"generated_data/countries_{timestamp}.csv"

    # Create a CSV file from selected countries
    selected_df.to_csv(output_file, index=False)
    print(f"{len(selected_df)} countries written to {output_file}")

    # Mark selected countries as used
    selected_ids_df = selected_df[["CountryID"]]
    updated_used_df = pd.concat([used_countries_df, selected_ids_df], ignore_index=True).drop_duplicates()

    updated_used_df.to_csv("data/used_countries.csv", index=False)

if __name__ == "__main__":
    schedule.every().day.at("00:00").do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
