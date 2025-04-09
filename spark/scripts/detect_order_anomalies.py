import os
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from generate_sale_anomalies_email import generate_email_template
from send_email import send_email

def detect_order_anomalies(spark, sale_items, logs):

    # PostgreSQL configuration
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_URI = os.environ["POSTGRES_URI"]
    POSTGRES_DB = f"{POSTGRES_URI}/ecommerce"

    # PostgreSQL tables
    SALE_ITEMS_TABLE = "sale_items"
    SALE_ANOMALIES_TABLE = "sale_anomalies"

    # Configure PostgresSQL connection
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD, 
        "driver": "org.postgresql.Driver"
    }

    # Add customer ID and invoice date to sale items
    logs = logs.withColumnRenamed("StockCode", "LogStockCode")
    sale_items = sale_items.join(
        logs.select("InvoiceNo", "LogStockCode", "CustomerID", "InvoiceDateTS"),
        (sale_items["SaleInvoiceNo"] == logs["InvoiceNo"]) & (sale_items["StockCode"] == logs["LogStockCode"]),
        "left"
    )
    sale_items = sale_items.drop("InvoiceNo").drop("LogStockCode")

    # Get historical sale items
    historical_sale_items = spark.read.jdbc(url=POSTGRES_DB, table=SALE_ITEMS_TABLE, properties=properties)
    historical_sale_items = historical_sale_items \
        .withColumn("InvoiceDateTS", F.lit("1900-01-01").cast(TimestampType())) \
        .withColumn("CustomerID", F.lit("-1"))

    # Filter out historical sale items with stock codes not present in new sale items
    distinct_stockcodes = sale_items.select("StockCode").distinct()
    historical_sale_items = historical_sale_items.join(distinct_stockcodes, "StockCode", "inner")

    # Combine historical and new sale items
    combined_sale_items = historical_sale_items.unionByName(sale_items)

    # Specify a window to calculate standard deviation and median value for product quantities
    # Include all historical sale items for this calculation and partition by product
    window_spec = Window.partitionBy("StockCode").orderBy("InvoiceDateTS").rowsBetween(Window.unboundedPreceding, -1)

    # Calculate the median quantity, the standard deviation and the deviation for for each sale item
    sale_item_anomalies = combined_sale_items.withColumn("MedianQuantity", F.expr('percentile_approx(Quantity, 0.5)').over(window_spec))
    sale_item_anomalies = sale_item_anomalies.withColumn("StandardQuantityDeviation", F.stddev("Quantity").over(window_spec))
    sale_item_anomalies = sale_item_anomalies.withColumn(
        "QuantityDeviation", (F.col("Quantity") - F.col("MedianQuantity")) / F.col("StandardQuantityDeviation")
    )

    # Filter out old sale items and items that have deviation less than 2
    timespan_start = (datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)).strftime("%Y-%m-%d-%H-%M")
    sale_item_anomalies = sale_item_anomalies \
        .filter((F.abs(F.col("QuantityDeviation")) > 2) & (F.col("InvoiceDateTS") > F.to_timestamp(F.lit(timespan_start), "yyyy-MM-dd-HH-mm")))

    sale_item_anomalies = sale_item_anomalies.drop("Price") \
        .withColumnRenamed("InvoiceDateTS", "InvoiceDate")

    # Write sale item anomalies to PostgreSQL
    sale_item_anomalies.write.jdbc(url=POSTGRES_DB, table=SALE_ANOMALIES_TABLE, mode="append", properties=properties)

    # Send notification email
    if sale_item_anomalies.count() > 0:
        subject = "Sale quantity anomalies detected"
        body = generate_email_template(sale_item_anomalies)
        send_email(subject, body)