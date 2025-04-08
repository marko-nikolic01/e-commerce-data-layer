import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, to_date, to_timestamp, date_format, sum
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Replace region code with region name
def get_region(id):
    if id == "1":
        return "North"
    elif id == "2":
        return "South"
    elif id == "3":
        return "East"
    elif id == "4":
        return "West"
    elif id == "5":
        return "Center"
    elif id == "6":
        return "Islands"
    else:
        return "Unknown"

# Initialize Spark session with Hive and PostgreSQL support
spark = SparkSession.builder \
    .appName("Analyze e-commerce data") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.23.jar") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS path
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
LOGS_PARQUET_PATH = HDFS_NAMENODE + "/data/temp/logs"

# PostgreSQL configuration
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_URI = os.environ["POSTGRES_URI"]
POSTGRES_DB = f"{POSTGRES_URI}/ecommerce"

# Hive tables
COUNTRIES_TABLE="countries"
PRODUCTS_TABLE= "products"
LOGS_TABLE="logs"
UNPROCESSED_LOGS_TABLE="unprocessedlogs"
SALES_TABLE= "sales"
SALE_ITEMS_TABLE= "sale_items"

# Register region conversion function
region_udf = udf(get_region, StringType())

# Define analysis time-span (last 4 hours)
now = datetime.now()
timespan_end = (now.replace(minute=0, second=0, microsecond=0)).strftime("%Y-%m-%d-%H-%M")
timespan_start = (now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)).strftime("%Y-%m-%d-%H-%M")

# Load Hive tables for countries, products, logs and unprocessed logs
countries = spark.table(COUNTRIES_TABLE)

products = spark.table(PRODUCTS_TABLE) \
    .withColumn("Date", F.to_date("Date", "yyyy-MM-dd"))

logs = spark.table(LOGS_TABLE) \
    .withColumn("InvoiceDateTS", F.to_timestamp("InvoiceDate", "yyyy-MM-dd-HH-mm")) \
    .filter(F.col("InvoiceDateTS").between(
        F.to_timestamp(F.lit(timespan_start), "yyyy-MM-dd-HH-mm"),
        F.to_timestamp(F.lit(timespan_end), "yyyy-MM-dd-HH-mm")
    ))

unprocessed_logs = spark.table(UNPROCESSED_LOGS_TABLE) \
    .withColumn("InvoiceDateTS", F.to_timestamp("InvoiceDate", "yyyy-MM-dd-HH-mm"))

# Merge logs and unprocessed logs
logs = logs.union(unprocessed_logs)

# Separate CountryId and RegionId
split_cols = split(logs["Country"], "-")
logs = logs \
    .withColumn("CountryId", split_cols.getItem(0)) \
    .withColumn("RegionId", split_cols.getItem(1))

# Join logs with countries and filter out logs that don't have country information
unprocessed_logs = logs.join(countries, logs["CountryId"] == countries["CountryId"], "left_anti")
logs = logs.join(countries, logs["CountryId"] == countries["CountryId"], "left_semi")

# Enrich logs with country name and region name
logs = logs \
    .join(countries, "CountryId") \
    .withColumn("Region", region_udf("RegionId"))

# Create sale items by joining logs and products and calculating the sale item price by the invoice date and the product price on that date
logs = logs.withColumn("InvoiceDateShort", date_format("InvoiceDateTS", "yyyy-MM-dd"))

product_prices = products.select("StockCode", "UnitPrice", "Date", "ProductName").withColumnRenamed("Date", "ProductDate")

sale_items = logs \
    .join(product_prices, (logs["StockCode"] == product_prices["StockCode"]) &
          (logs["InvoiceDateShort"] == date_format(product_prices["ProductDate"], "yyyy-MM-dd"))) \
    .select(
        "InvoiceNo",
        logs["StockCode"],
        "ProductName",
        "Quantity",
        (logs["Quantity"] * product_prices["UnitPrice"]).alias("Price")
    )

# Create sales by joining logs and sale_items and calculating the sale total price with invoice item sum
sale_items = sale_items.withColumnRenamed("InvoiceNo", "SaleInvoiceNo")
sales = sale_items \
    .join(logs, sale_items["SaleInvoiceNo"] == logs["InvoiceNo"]) \
    .select(
        "InvoiceNo",
        "CustomerID",
        "CountryName",
        "Region",
        "InvoiceDateTS",
        "Price"
    )

sales = sales \
    .groupBy("InvoiceNo", "CustomerID", "CountryName", "Region", "InvoiceDateTS") \
    .agg(sum("Price").alias("TotalPrice")) \
    .withColumnRenamed("CountryName", "Country") \
    .withColumnRenamed("InvoiceDateTS", "InvoiceDate")

# Create products
products = products.withColumn("Date", to_date("Date", "yyyy-MM-dd"))

# Configure PostgresSQL connection
properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD, 
    "driver": "org.postgresql.Driver"
}

# Write sales, sale items and products to PostgreSQL
sales.write.jdbc(url=POSTGRES_DB, table=SALES_TABLE, mode="append", properties=properties)
sale_items.write.jdbc(url=POSTGRES_DB, table=SALE_ITEMS_TABLE, mode="append", properties=properties)
products.write.jdbc(url=POSTGRES_DB, table=PRODUCTS_TABLE, mode="append", properties=properties)

# Save unprocessed logs to a temporary file
unprocessed_logs = unprocessed_logs.withColumn(
        "InvoiceDate",
        date_format("InvoiceDateTS", "yyyy-MM-dd-HH-mm")
    ).drop("InvoiceDateTS", "CountryId", "RegionId") \
    .dropDuplicates(["InvoiceNo", "StockCode"])

unprocessed_logs.coalesce(1).write.mode("overwrite").parquet(LOGS_PARQUET_PATH)

# Stop Spark Session
spark.stop()
