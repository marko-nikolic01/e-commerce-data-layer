import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import schedule
import time

# Probabilities
def weighted_choice(max_value, decay=1.0):
    weights = [1 / ((i + 1) ** decay) for i in range(max_value)]
    weights = [w / sum(weights) for w in weights]
    return np.random.choice(range(1, max_value + 1), p=weights)

def mutate_country_id(row, countries_df):
    base_country = row['CountryID'].split('-')[0]

    if np.random.rand() < 0.1:
        new_num = weighted_choice(6)
        return f"{base_country}-{new_num}"
    elif np.random.rand() < 0.03:
        new_base = random.choice(countries_df['CountryID'].values)
        new_num = weighted_choice(6)
        return f"{new_base}-{new_num}"
    else:
        return row['CountryID']

def main():
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"Generating products data : {timestamp}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"Generating logs data : {timestamp}")

    # Read data
    customers = pd.read_csv("data/customers.csv")
    countries = pd.read_csv("data/countries.csv")
    products = pd.read_csv("data/products.csv")
    used_products = pd.read_csv("data/used_products.csv")

    # Filter out unused products
    valid_stockcodes = set(used_products['StockCode'])
    products = products[products['StockCode'].isin(valid_stockcodes)]

    if  products.empty:
        return

    # Sample 250 random customers
    customers = customers.sample(n=250, replace=False).copy()

    # Mutate customer country IDs
    customers['CountryID'] = customers.apply(mutate_country_id, axis=1, args=(countries,))

    # Load last InvoiceNo
    invoice_path = "data/invoice.txt"
    if os.path.exists(invoice_path):
        with open(invoice_path, "r") as f:
            last_invoice = int(f.read().strip())
    else:
        last_invoice = 100000

    # Create invoices
    logs = []
    now = datetime.now()
    four_hours_ago = now - timedelta(hours=4)
    invoice_no = last_invoice

    for _, cust in customers.iterrows():
        num_invoices = weighted_choice(5, decay=1.5)

        for _ in range(num_invoices):
            invoice_no += 1
            invoice_date = four_hours_ago + timedelta(minutes=random.randint(0, 240))

            num_products = weighted_choice(10, decay=2)
            selected_products = products.sample(n=num_products, replace=False)

            for _, prod in selected_products.iterrows():
                quantity = weighted_choice(50, decay=1.75)
                logs.append({
                    "InvoiceNo": invoice_no,
                    "StockCode": prod["StockCode"],
                    "Quantity": quantity,
                    "InvoiceDate": invoice_date.strftime("%d/%m/%Y %H:%M"),
                    "CustomerID": cust["CustomerID"],
                    "Country": cust["CountryID"]
                })

    # Create output file name
    output_filename = f"generated_data/logs_{timestamp}.csv"

    # Create a CSV file from created logs
    pd.DataFrame(logs).to_csv(output_filename, index=False)

    # Update invoice.txt
    with open(invoice_path, "w") as f:
        f.write(str(invoice_no))

    print(f"Generated {len(logs)} log entries to {output_filename}")

if __name__ == "__main__":
    schedule.every().hour.at(":00").do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
